use std::{collections::BTreeMap, time::Duration};

use livekit_api::services::room::RoomClient;
use livekit_protocol::{
    ListRoomsRequest, ListRoomsResponse, ParticipantInfo, Room, RoomParticipantIdentity,
    WebhookEvent,
};
use prost::Message;
use redis_kiss::redis::{
    aio::{ConnectionLike, MultiplexedConnection},
    cmd,
    streams::StreamRangeReply,
    Arg, Client, Cmd, Pipeline, RedisFuture, Value,
};
use revolt_database::voice::presence::{Presence, Scope};
use revolt_voice_ingress::participation::apply_participant_webhook;
use rocket::{
    data::{Data, ToByteUnit},
    fairing::AdHoc,
    http::Status,
    post,
    response::status::Custom,
    routes,
    tokio::{self, sync::mpsc, sync::oneshot, time::Instant},
    State,
};

const EVENTS: &str = "vp2:{voice}:events";
const SLOT: &str = "vp2:{voice}:slot:dXNlcg:s:c2VydmVy";
const ENDED_RACE: &str = "vp2:{voice}:ended:participant:bm9kZQ:UkFDRQ:UEEtcmFjZQ";

struct PausedCas {
    conn: MultiplexedConnection,
    gate: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
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

enum Lookup {
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

fn webhook(kind: &str, room_sid: &str, sid: &str, created_at: i64, user: &str) -> WebhookEvent {
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

fn participant(user: &str, sid: &str) -> ParticipantInfo {
    ParticipantInfo {
        identity: user.into(),
        sid: sid.into(),
        ..Default::default()
    }
}

async fn room_lookup(requests: &mut mpsc::Receiver<Lookup>, sid: &str) {
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

async fn participant_lookup(
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

async fn receive(
    mut conn: MultiplexedConnection,
    livekit: &RoomClient,
    scope: &Scope,
    event: &WebhookEvent,
) {
    apply_participant_webhook(
        &mut Presence::new(&mut conn),
        livekit,
        "node",
        scope,
        event,
        Instant::now(),
    )
    .await
    .unwrap();
}

async fn hashes(conn: &mut MultiplexedConnection) -> BTreeMap<String, Vec<u8>> {
    let keys: Vec<String> = cmd("KEYS")
        .arg("vp2:{voice}:*")
        .query_async(conn)
        .await
        .unwrap();
    let mut result = BTreeMap::new();
    for key in keys {
        let dump = cmd("DUMP").arg(&key).query_async(conn).await.unwrap();
        result.insert(key, dump);
    }
    result
}

#[rocket::async_test]
async fn participant_webhooks_fence_stale_generations() {
    let Ok(url) = std::env::var("NOOK_PRESENCE_TEST_REDIS_URL") else {
        eprintln!("skipped Redis integration: set NOOK_PRESENCE_TEST_REDIS_URL to an empty standalone instance");
        return;
    };
    let mut conn = Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    assert!(hashes(&mut conn).await.is_empty());

    let (requests_tx, mut requests) = mpsc::channel(8);
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
    let scope = Scope::Server("server".into());
    let now = chrono::Utc::now().timestamp();
    let join_old = webhook("participant_joined", "OLD", "PA-old", now, "user");
    let join_new = webhook("participant_joined", "NEW", "PA-new", now, "user");
    let leave_old = webhook("participant_left", "OLD", "PA-old", now, "user");

    for (event, room_sid, sid) in [(&join_old, "OLD", "PA-old"), (&join_new, "NEW", "PA-new")] {
        tokio::join!(receive(conn.clone(), &livekit, &scope, event), async {
            room_lookup(&mut requests, room_sid).await;
            participant_lookup(&mut requests, "user")
                .await
                .send(Some(participant("user", sid)))
                .unwrap();
        });
    }

    // Initialize mute/deafen as if a later voice-state update had already committed.
    let active: String = cmd("HGET")
        .arg(SLOT)
        .arg("active")
        .query_async(&mut conn)
        .await
        .unwrap();
    let mut active: serde_json::Value = serde_json::from_str(&active).unwrap();
    active["state"]["is_receiving"] = false.into();
    active["state"]["is_publishing"] = true.into();
    let _: () = cmd("HSET")
        .arg(SLOT)
        .arg("active")
        .arg(active.to_string())
        .query_async(&mut conn)
        .await
        .unwrap();

    receive(conn.clone(), &livekit, &scope, &leave_old).await;
    let after_late_leave = hashes(&mut conn).await;
    receive(conn.clone(), &livekit, &scope, &leave_old).await;
    tokio::join!(receive(conn.clone(), &livekit, &scope, &join_old), async {
        room_lookup(&mut requests, "NEW").await;
    });
    tokio::join!(receive(conn.clone(), &livekit, &scope, &join_new), async {
        room_lookup(&mut requests, "NEW").await;
        participant_lookup(&mut requests, "user")
            .await
            .send(Some(participant("user", "PA-new")))
            .unwrap();
    });
    assert_eq!(after_late_leave, hashes(&mut conn).await);
    let snapshot = Presence::new(&mut conn).read("user", &scope).await.unwrap();
    assert_eq!(snapshot.revision(), 3);
    assert_eq!(snapshot.generation().unwrap().participant_sid, "PA-new");
    assert!(!snapshot.state().unwrap().is_receiving);
    assert!(snapshot.state().unwrap().is_publishing);
    println!("same-second old/new; old/repeated Leave and old/repeated Join: rev 3, new SID and flags preserved");
    for key in after_late_leave.keys() {
        println!("  {key}");
    }

    // Room matches, but GetParticipant returns the replacement participant.
    let wrong_participant = webhook("participant_joined", "NEW", "PA-stale", now, "user");
    tokio::join!(
        receive(conn.clone(), &livekit, &scope, &wrong_participant),
        async {
            room_lookup(&mut requests, "NEW").await;
            participant_lookup(&mut requests, "user")
                .await
                .send(Some(participant("user", "PA-new")))
                .unwrap();
        }
    );
    assert_eq!(hashes(&mut conn).await, after_late_leave);

    // A failed lookup defers all writes. The retry gets fresh LiveKit evidence.
    let delayed = webhook("participant_joined", "NEW", "PA-stale", now - 61, "user");
    let mut observer = conn.clone();
    tokio::join!(receive(conn.clone(), &livekit, &scope, &delayed), async {
        room_lookup(&mut requests, "NEW").await;
        participant_lookup(&mut requests, "user")
            .await
            .send(None)
            .unwrap();
        room_lookup(&mut requests, "NEW").await;
        let reply = participant_lookup(&mut requests, "user").await;
        assert_eq!(hashes(&mut observer).await, after_late_leave);
        reply.send(Some(participant("user", "PA-new"))).unwrap();
    });
    assert_eq!(hashes(&mut conn).await, after_late_leave);
    println!("unavailable LiveKit and aged Join: fresh lookup on retry, Redis unchanged");

    let race_join = webhook("participant_joined", "RACE", "PA-race", now, "race");
    let race_leave = webhook("participant_left", "RACE", "PA-race", now, "race");
    let (arrived, at_cas) = oneshot::channel();
    let (resume, gate) = oneshot::channel();
    let mut paused = PausedCas {
        conn: conn.clone(),
        gate: Some((arrived, gate)),
    };
    let mut racing_presence = Presence::new(&mut paused);
    let (outcome, ()) = tokio::join!(
        apply_participant_webhook(
            &mut racing_presence,
            &livekit,
            "node",
            &scope,
            &race_join,
            Instant::now(),
        ),
        async {
            room_lookup(&mut requests, "RACE").await;
            participant_lookup(&mut requests, "race")
                .await
                .send(Some(participant("race", "PA-race")))
                .unwrap();
            tokio::time::timeout(Duration::from_secs(4), at_cas)
                .await
                .unwrap()
                .unwrap();
            receive(conn.clone(), &livekit, &scope, &race_leave).await;
            resume.send(()).unwrap();

            // A matching lookup after CAS conflict cannot undo the terminal marker.
            room_lookup(&mut requests, "RACE").await;
            participant_lookup(&mut requests, "race")
                .await
                .send(Some(participant("race", "PA-race")))
                .unwrap();
        }
    );
    outcome.unwrap();
    let snapshot = Presence::new(&mut conn).read("race", &scope).await.unwrap();
    assert_eq!(snapshot.revision(), 1);
    assert!(snapshot.generation().is_none());
    let ttl: i64 = cmd("TTL")
        .arg(ENDED_RACE)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(ttl, -1);
    println!("Leave with empty slot before Join CAS: rev 1, no active participation, terminal SID without TTL");

    // A future-dated Leave goes through reconciliation, even with a matching active SID.
    let future_leave = webhook("participant_left", "NEW", "PA-new", now + 120, "user");
    let before_future = hashes(&mut conn).await;
    tokio::join!(
        receive(conn.clone(), &livekit, &scope, &future_leave),
        async {
            room_lookup(&mut requests, "NEW").await;
            participant_lookup(&mut requests, "user")
                .await
                .send(Some(participant("user", "PA-new")))
                .unwrap();
        }
    );
    assert_eq!(before_future, hashes(&mut conn).await);

    let mut expiring = conn.clone();
    let mut expiring_presence = Presence::new(&mut expiring);
    let (outcome, pending_reply) = tokio::join!(
        apply_participant_webhook(
            &mut expiring_presence,
            &livekit,
            "node",
            &scope,
            &delayed,
            Instant::now() - Duration::from_millis(19_500),
        ),
        async {
            room_lookup(&mut requests, "NEW").await;
            participant_lookup(&mut requests, "user").await
        }
    );
    let _ = pending_reply.send(None);
    assert!(outcome.is_err());
    assert_eq!(before_future, hashes(&mut conn).await);
    println!("outage through retry deadline: error for redelivery, Redis unchanged");

    let leave_new = webhook("participant_left", "NEW", "PA-new", now, "user");
    receive(conn.clone(), &livekit, &scope, &leave_new).await;
    let after_leave = hashes(&mut conn).await;
    receive(conn.clone(), &livekit, &scope, &leave_new).await;
    assert_eq!(after_leave, hashes(&mut conn).await);
    let snapshot = Presence::new(&mut conn).read("user", &scope).await.unwrap();
    assert_eq!(snapshot.revision(), 4);
    assert!(snapshot.generation().is_none());
    let stream: StreamRangeReply = cmd("XRANGE")
        .arg(EVENTS)
        .arg("-")
        .arg("+")
        .query_async(&mut conn)
        .await
        .unwrap();
    let events: Vec<_> = stream
        .ids
        .iter()
        .map(|entry| {
            let payload = entry.get::<String>("payload").unwrap();
            let event: serde_json::Value = serde_json::from_str(&payload).unwrap();
            println!("stream {} {payload}", entry.id);
            event["type"].as_str().unwrap().to_owned()
        })
        .collect();
    assert_eq!(
        events,
        ["VoiceChannelJoin", "VoiceChannelJoin", "VoiceChannelLeave"]
    );
    assert!(requests.try_recv().is_err());
    shutdown.notify();
    server.await.unwrap().unwrap();
}
