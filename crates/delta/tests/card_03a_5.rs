#[macro_use]
extern crate rocket;
#[macro_use]
extern crate revolt_rocket_okapi;

#[path = "../src/routes/channels/voice_state_set.rs"]
mod voice_state_set;

use redis_kiss::redis::{cmd, streams::StreamRangeReply, Client};
use revolt_database::util::reference::Reference;
use revolt_database::voice::{
    get_channel_voice_state,
    presence::{ApplyOutcome, Generation, Presence, Scope, Transition},
    UserVoiceChannel,
};
use revolt_database::{Channel, Database, User};
use revolt_models::v0::{DataSetVoiceState, UserVoiceState};
use revolt_result::ErrorType;
use rocket::{serde::json::Json, State};
use serde_json::json;

#[rocket::async_test]
async fn delayed_put_cannot_change_a_replacement_in_the_same_channel() {
    let Ok(url) = std::env::var("NOOK_PUT_TEST_REDIS_URL") else {
        eprintln!("skipped Redis integration: set NOOK_PUT_TEST_REDIS_URL to an empty standalone database");
        return;
    };
    assert_eq!(std::env::var("REDIS_URI").as_deref(), Ok(url.as_str()));
    assert_eq!(std::env::var("STOAT_VOICE_PRESENCE_V2").as_deref(), Ok("1"));
    let client = Client::open(url).unwrap();
    let mut writer = client.get_multiplexed_async_connection().await.unwrap();
    let mut request = client.get_multiplexed_async_connection().await.unwrap();
    const STREAM: &str = "vp2:{voice}:events";
    let keys: u64 = cmd("DBSIZE").query_async(&mut writer).await.unwrap();
    assert_eq!(keys, 0);
    let channel = UserVoiceChannel {
        id: "channel".to_owned(),
        server_id: Some("server".to_owned()),
    };
    let scope = Scope::Server("server".to_owned());
    let user = "user";
    let db = Database::Reference(Default::default());
    let model: Channel = serde_json::from_value(json!({
        "channel_type": "TextChannel", "_id": channel.id, "server": "server",
        "name": "Voice", "voice": {}
    }))
    .unwrap();
    db.insert_channel(&model).await.unwrap();
    let put = |receiving| {
        voice_state_set::set_voice_state(
            State::from(&db),
            User {
                id: user.to_owned(),
                ..Default::default()
            },
            Reference::from_unchecked(&channel.id),
            Json(DataSetVoiceState {
                is_receiving: receiving,
            }),
        )
    };
    let old = Generation {
        node: "node".to_owned(),
        room_sid: "room".to_owned(),
        participant_sid: "old".to_owned(),
        channel_id: channel.id.clone(),
    };
    let new = Generation {
        participant_sid: "new".to_owned(),
        ..old.clone()
    };
    let state: UserVoiceState = serde_json::from_value(json!({
        "id": user,
        "joined_at": "2026-11-18T00:00:00.000Z",
        "is_receiving": true,
        "is_publishing": true,
        "screensharing": false,
        "camera": true
    }))
    .unwrap();

    let mut writer_presence = Presence::new(&mut writer);
    let initial = writer_presence.read(user, &scope).await.unwrap();
    assert_eq!(
        writer_presence
            .apply(&initial, Transition::Join(&old, &state))
            .await
            .unwrap(),
        ApplyOutcome::Applied(1)
    );

    // Suspend the request after the same read the production route performs.
    let mut request_presence = Presence::new(&mut request);
    let pinned = request_presence.read(user, &scope).await.unwrap();
    assert_eq!(pinned.generation(), Some(&old));
    assert_eq!(
        writer_presence
            .apply(&pinned, Transition::Join(&new, &state))
            .await
            .unwrap(),
        ApplyOutcome::Applied(2)
    );
    let error = request_presence
        .set_receiving(&pinned, &channel.id, Some(false))
        .await
        .unwrap_err();
    assert!(matches!(error.error_type, ErrorType::NotConnected));
    let latest = request_presence.read(user, &scope).await.unwrap();
    assert_eq!(latest.revision(), 2);
    assert_eq!(latest.generation(), Some(&new));
    assert_eq!(latest.state(), Some(&state));

    assert_eq!(put(Some(false)).await.unwrap().is_receiving, Some(false));
    let updated = request_presence.read(user, &scope).await.unwrap();
    let mut expected = state;
    expected.is_receiving = false;
    assert_eq!(updated.state(), Some(&expected));
    assert_eq!(updated.revision(), 3);

    let wrong_channel = request_presence
        .set_receiving(&updated, "other", Some(true))
        .await
        .unwrap_err();
    assert!(matches!(wrong_channel.error_type, ErrorType::NotConnected));
    assert_eq!(put(None).await.unwrap().is_receiving, None);
    let events: StreamRangeReply = cmd("XRANGE")
        .arg((STREAM, "-", "+"))
        .query_async(&mut writer)
        .await
        .unwrap();
    assert_eq!(events.ids.len(), 3);
    let payload: String = events.ids[2].get("payload").unwrap();
    let event: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(event["type"], "UserVoiceStateUpdate");
    assert_eq!(event["data"]["is_receiving"], false);
    assert_eq!(event["channel_id"], channel.id);
    assert_eq!(
        get_channel_voice_state(&channel)
            .await
            .unwrap()
            .unwrap()
            .participants,
        vec![expected]
    );

    let current = request_presence.read(user, &scope).await.unwrap();
    writer_presence = Presence::new(&mut writer);
    writer_presence
        .finish_room(&new.node, &new.room_sid)
        .await
        .unwrap();
    let error = request_presence
        .set_receiving(&current, &channel.id, Some(true))
        .await
        .unwrap_err();
    assert!(matches!(error.error_type, ErrorType::NotConnected));
    let error = put(Some(true)).await.unwrap_err();
    assert!(matches!(error.error_type, ErrorType::NotConnected));
    eprintln!(
        "PUT old SID => NotConnected, new SID => is_receiving=false, closed room => NotConnected"
    );
    verify_admin_writers(&client).await;
}

#[rocket::async_test]
async fn snapshot_ignores_stale_index_without_touching_the_new_participation() {
    let Ok(url) = std::env::var("NOOK_PUT_TEST_REDIS_URL") else {
        eprintln!("skipped Redis integration: set NOOK_PUT_TEST_REDIS_URL");
        return;
    };
    let mut info = Client::open(url).unwrap().get_connection_info().clone();
    info.redis.db += 1;
    let mut conn = Client::open(info)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let keys: u64 = cmd("DBSIZE").query_async(&mut conn).await.unwrap();
    assert_eq!(keys, 0);
    let mut presence = Presence::new(&mut conn);
    let scope = Scope::Server("server".into());
    let new = Generation {
        node: "node".into(),
        room_sid: "room-new".into(),
        participant_sid: "participant-new".into(),
        channel_id: "new".into(),
    };
    let state = UserVoiceState {
        id: "user".into(),
        joined_at: revolt_database::iso8601_timestamp::Timestamp::UNIX_EPOCH,
        is_receiving: true,
        is_publishing: false,
        screensharing: false,
        camera: false,
    };
    let initial = presence.read(&state.id, &scope).await.unwrap();
    presence
        .apply(&initial, Transition::Join(&new, &state))
        .await
        .unwrap();
    const OLD_INDEX: &str = "vp2:{voice}:channel:b2xk";
    let _: u64 = cmd("HSET")
        .arg((OLD_INDEX, &state.id, "s:c2VydmVy"))
        .query_async(&mut conn)
        .await
        .unwrap();
    let mut presence = Presence::new(&mut conn);
    assert!(presence.channel_states("old").await.unwrap().is_empty());
    assert_eq!(presence.channel_states("new").await.unwrap(), vec![state]);
    let current = presence.read("user", &scope).await.unwrap();
    assert_eq!(current.generation(), Some(&new));
    assert_eq!(current.revision(), 1);
    let members: u64 = cmd("HLEN")
        .arg(OLD_INDEX)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(members, 1);
    eprintln!("Snapshot skipped a stale index and preserved the replacement slot and indexes");
}

type RpcRequest = (
    String,
    Vec<u8>,
    rocket::tokio::sync::oneshot::Sender<Vec<u8>>,
);

#[post("/twirp/livekit.RoomService/<method>", data = "<body>")]
async fn room_service(
    method: String,
    body: rocket::data::Data<'_>,
    requests: &State<rocket::tokio::sync::mpsc::Sender<RpcRequest>>,
) -> Vec<u8> {
    use rocket::data::ToByteUnit;
    let bytes = body.open(4096.bytes()).into_bytes().await.unwrap().value;
    let (reply, response) = rocket::tokio::sync::oneshot::channel();
    requests.send((method, bytes, reply)).await.unwrap();
    response.await.unwrap()
}

async fn next_rpc(requests: &mut rocket::tokio::sync::mpsc::Receiver<RpcRequest>) -> RpcRequest {
    rocket::tokio::time::timeout(std::time::Duration::from_secs(4), requests.recv())
        .await
        .expect("expected a RoomService RPC")
        .unwrap()
}

async fn verify_admin_writers(client: &Client) {
    use livekit_protocol::{
        DeleteRoomRequest, ListRoomsRequest, ListRoomsResponse, Room, RoomParticipantIdentity,
    };
    use prost::Message;
    use revolt_config::LiveKitNode;
    use revolt_database::voice::{
        delete_voice_channel, is_in_voice_channel, remove_user_from_voice_channels,
        set_channel_node, VoiceClient,
    };
    use rocket::{
        fairing::AdHoc,
        tokio::{
            self,
            sync::{mpsc, oneshot},
        },
    };

    let (requests_tx, mut requests) = mpsc::channel(4);
    let (endpoint, ready) = oneshot::channel();
    let rocket = rocket::custom(rocket::Config {
        port: 0,
        log_level: rocket::config::LogLevel::Off,
        ..rocket::Config::debug_default()
    })
    .manage(requests_tx)
    .mount("/", routes![room_service])
    .attach(AdHoc::on_liftoff("RoomService fixture", |rocket| {
        Box::pin(async move {
            endpoint
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
    let voice = VoiceClient::new(std::collections::HashMap::from([(
        "node".into(),
        LiveKitNode {
            url: ready.await.unwrap(),
            key: "fixture-key".into(),
            secret: "fixture-secret".into(),
            lat: 0.0,
            lon: 0.0,
            private: false,
        },
    )]));

    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let channel = UserVoiceChannel {
        id: "admin-channel".into(),
        server_id: Some("admin-server".into()),
    };
    let scope = Scope::Server("admin-server".into());
    let user = "admin-target";
    let initiator = "admin-initiator";
    let state = UserVoiceState {
        id: user.into(),
        joined_at: revolt_database::iso8601_timestamp::Timestamp::UNIX_EPOCH,
        is_receiving: true,
        is_publishing: true,
        screensharing: true,
        camera: true,
    };
    let old = Generation {
        node: "node".into(),
        room_sid: "room-old".into(),
        participant_sid: "target-old".into(),
        channel_id: channel.id.clone(),
    };
    let new = Generation {
        room_sid: "room-new".into(),
        participant_sid: "target-new".into(),
        ..old.clone()
    };
    let replacement = Generation {
        room_sid: "room-replacement".into(),
        participant_sid: "target-replacement".into(),
        ..new.clone()
    };
    let mut presence = Presence::new(&mut conn);
    let initial = presence.read(user, &scope).await.unwrap();
    presence
        .apply(&initial, Transition::Join(&old, &state))
        .await
        .unwrap();
    let initial = presence.read(initiator, &scope).await.unwrap();
    let mut initiator_state = state.clone();
    initiator_state.id = initiator.into();
    let initiator_generation = Generation {
        participant_sid: "admin".into(),
        ..old.clone()
    };
    presence
        .apply(
            &initial,
            Transition::Join(&initiator_generation, &initiator_state),
        )
        .await
        .unwrap();

    let (result, ()) = tokio::join!(remove_user_from_voice_channels(&voice, user), async {
        let (method, body, reply) = next_rpc(&mut requests).await;
        assert_eq!(method, "RemoveParticipant");
        let request = RoomParticipantIdentity::decode(body.as_slice()).unwrap();
        assert_eq!(request.room, channel.id);
        assert_eq!(request.identity, user);
        let pinned = presence.read(user, &scope).await.unwrap();
        presence
            .apply(&pinned, Transition::Join(&new, &state))
            .await
            .unwrap();
        reply.send(Vec::new()).unwrap();
    });
    result.unwrap();
    let current = presence.read(user, &scope).await.unwrap();
    assert_eq!(current.generation(), Some(&new));
    assert_eq!(current.state(), Some(&state));
    assert!(is_in_voice_channel(initiator, &channel).await.unwrap());
    eprintln!("Force disconnect started for OLD, RPC returned after NEW joined, Redis preserved NEW and the initiator");

    let pinned = presence.read(user, &scope).await.unwrap();
    presence
        .set_receiving(&pinned, &channel.id, Some(false))
        .await
        .unwrap();
    presence
        .restrict_tracks(pinned, false, false)
        .await
        .unwrap();
    let revoked = presence.read(user, &scope).await.unwrap();
    let restricted = revoked.state().unwrap();
    assert!(
        !restricted.is_receiving
            && !restricted.is_publishing
            && !restricted.camera
            && !restricted.screensharing
    );
    eprintln!("ACL retry used the same SID and preserved a concurrent deafen change");

    set_channel_node(&channel.id, "node").await.unwrap();
    let (result, ()) = tokio::join!(delete_voice_channel(&voice, &channel), async {
        let (method, body, reply) = next_rpc(&mut requests).await;
        assert_eq!(method, "ListRooms");
        assert_eq!(
            ListRoomsRequest::decode(body.as_slice()).unwrap().names,
            [channel.id.as_str()]
        );
        reply
            .send(
                ListRoomsResponse {
                    rooms: vec![Room {
                        name: channel.id.clone(),
                        sid: new.room_sid.clone(),
                        ..Default::default()
                    }],
                }
                .encode_to_vec(),
            )
            .unwrap();
        let (method, body, reply) = next_rpc(&mut requests).await;
        assert_eq!(method, "DeleteRoom");
        assert_eq!(
            DeleteRoomRequest::decode(body.as_slice()).unwrap().room,
            channel.id
        );
        presence
            .apply(&revoked, Transition::Join(&replacement, &state))
            .await
            .unwrap();
        reply.send(Vec::new()).unwrap();
    });
    result.unwrap();
    let current = presence.read(user, &scope).await.unwrap();
    assert_eq!(current.generation(), Some(&replacement));
    assert_eq!(current.state(), Some(&state));
    assert!(is_in_voice_channel(initiator, &channel).await.unwrap());
    eprintln!(
        "Room cleanup used the captured SID, excluding a replacement room and other room SID"
    );

    let (result, ()) = tokio::join!(remove_user_from_voice_channels(&voice, user), async {
        let (method, body, reply) = next_rpc(&mut requests).await;
        assert_eq!(method, "RemoveParticipant");
        assert_eq!(
            RoomParticipantIdentity::decode(body.as_slice())
                .unwrap()
                .identity,
            user
        );
        reply.send(Vec::new()).unwrap();
    });
    result.unwrap();
    assert!(!is_in_voice_channel(user, &channel).await.unwrap());
    assert!(is_in_voice_channel(initiator, &channel).await.unwrap());
    assert!(requests.try_recv().is_err());
    shutdown.notify();
    server.await.unwrap().unwrap();
}
