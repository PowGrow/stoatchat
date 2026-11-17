use std::time::Duration;

use livekit_api::services::room::RoomClient;
use livekit_protocol::{
    ListRoomsRequest, ListRoomsResponse, ParticipantInfo, Room, RoomParticipantIdentity,
    WebhookEvent,
};
use prost::Message;
use redis_kiss::redis::{
    aio::{ConnectionLike, MultiplexedConnection},
    Arg, Cmd, Pipeline, RedisFuture, Value,
};
use rocket::{
    data::{Data, ToByteUnit},
    fairing::AdHoc,
    http::Status,
    post,
    response::status::Custom,
    routes,
    tokio::{self, sync::mpsc, sync::oneshot},
    State,
};

pub(super) struct PausedCas {
    pub(super) conn: MultiplexedConnection,
    pub(super) gate: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
}

impl ConnectionLike for PausedCas {
    fn req_packed_command<'a>(&'a mut self, command: &'a Cmd) -> RedisFuture<'a, Value> {
        let gate = if matches!(command.args_iter().next(), Some(Arg::Simple(b"EVAL"))) {
            self.gate.take()
        } else {
            None
        };
        Box::pin(async move {
            if let Some((arrived, resume)) = gate {
                arrived.send(()).unwrap();
                resume.await.unwrap();
            }
            self.conn.req_packed_command(command).await
        })
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        command: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        self.conn.req_packed_commands(command, offset, count)
    }

    fn get_db(&self) -> i64 {
        self.conn.get_db()
    }
}

pub(super) enum Lookup {
    Room(ListRoomsRequest, oneshot::Sender<Vec<Room>>),
    Participant(
        RoomParticipantIdentity,
        oneshot::Sender<Option<ParticipantInfo>>,
    ),
}

#[post("/twirp/livekit.RoomService/ListRooms", data = "<body>")]
async fn list_rooms(body: Data<'_>, requests: &State<mpsc::Sender<Lookup>>) -> Vec<u8> {
    let body = body.open(4096.bytes()).into_bytes().await.unwrap();
    let request = ListRoomsRequest::decode(body.value.as_slice()).unwrap();
    let (reply, response) = oneshot::channel();
    requests.send(Lookup::Room(request, reply)).await.unwrap();
    ListRoomsResponse {
        rooms: response.await.unwrap(),
    }
    .encode_to_vec()
}

#[post("/twirp/livekit.RoomService/GetParticipant", data = "<body>")]
async fn get_participant(
    body: Data<'_>,
    requests: &State<mpsc::Sender<Lookup>>,
) -> Custom<Vec<u8>> {
    let body = body.open(4096.bytes()).into_bytes().await.unwrap();
    let request = RoomParticipantIdentity::decode(body.value.as_slice()).unwrap();
    let (reply, response) = oneshot::channel();
    requests
        .send(Lookup::Participant(request, reply))
        .await
        .unwrap();
    match response.await.unwrap() {
        Some(info) => Custom(Status::Ok, info.encode_to_vec()),
        None => Custom(
            Status::ServiceUnavailable,
            br#"{"code":"unavailable","msg":"controlled outage"}"#.to_vec(),
        ),
    }
}

async fn next_lookup(requests: &mut mpsc::Receiver<Lookup>) -> Lookup {
    tokio::time::timeout(Duration::from_secs(4), requests.recv())
        .await
        .expect("expected a LiveKit lookup")
        .unwrap()
}

pub(super) fn webhook(
    kind: &str,
    room_sid: &str,
    sid: &str,
    created_at: i64,
    user: &str,
) -> WebhookEvent {
    WebhookEvent {
        event: kind.into(),
        room: Some(Room {
            name: "channel".into(),
            sid: room_sid.into(),
            ..Default::default()
        }),
        participant: Some(participant(user, sid)),
        created_at,
        ..Default::default()
    }
}

pub(super) fn participant(user: &str, sid: &str) -> ParticipantInfo {
    ParticipantInfo {
        identity: user.into(),
        sid: sid.into(),
        ..Default::default()
    }
}

pub(super) async fn room_lookup(requests: &mut mpsc::Receiver<Lookup>, sid: &str) {
    let Lookup::Room(request, reply) = next_lookup(requests).await else {
        panic!("expected ListRooms");
    };
    assert_eq!(request.names, ["channel"]);
    reply
        .send(vec![Room {
            name: "channel".into(),
            sid: sid.into(),
            ..Default::default()
        }])
        .unwrap();
}

pub(super) async fn participant_lookup(
    requests: &mut mpsc::Receiver<Lookup>,
    user: &str,
) -> oneshot::Sender<Option<ParticipantInfo>> {
    let Lookup::Participant(request, reply) = next_lookup(requests).await else {
        panic!("expected GetParticipant");
    };
    assert_eq!(request.room, "channel");
    assert_eq!(request.identity, user);
    reply
}

pub(super) async fn livekit() -> (
    RoomClient,
    mpsc::Receiver<Lookup>,
    rocket::Shutdown,
    tokio::task::JoinHandle<Result<rocket::Rocket<rocket::Ignite>, rocket::Error>>,
) {
    let (requests_tx, requests) = mpsc::channel(8);
    let (address_tx, address_rx) = oneshot::channel();
    let rocket = rocket::custom(rocket::Config {
        port: 0,
        log_level: rocket::config::LogLevel::Off,
        ..rocket::Config::debug_default()
    })
    .manage(requests_tx)
    .mount("/", routes![list_rooms, get_participant])
    .attach(AdHoc::on_liftoff("Mock LiveKit endpoint", |rocket| {
        Box::pin(async move {
            address_tx
                .send(format!(
                    "http://{}:{}",
                    rocket.config().address,
                    rocket.config().port
                ))
                .unwrap();
        })
    }))
    .ignite()
    .await
    .unwrap();
    let shutdown = rocket.shutdown();
    let server = tokio::spawn(rocket.launch());
    let endpoint = address_rx.await.unwrap();
    let livekit = RoomClient::with_api_key(&endpoint, "test-key", "test-secret");
    (livekit, requests, shutdown, server)
}
