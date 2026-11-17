#![cfg(feature = "voice")]

use std::collections::BTreeMap;

use redis_kiss::redis::{aio::MultiplexedConnection, cmd, streams::StreamRangeReply, Client};
use revolt_database::voice::presence::{
    ApplyOutcome::{Applied, Conflict, Unchanged},
    Generation, Presence, Scope, Transition,
};
use revolt_models::v0::UserVoiceState;
use serde_json::{json, Value};

const SLOT: &str = "vp2:{voice}:slot:dXNlcg:s:c2VydmVy";
const USER: &str = "vp2:{voice}:user:dXNlcg";
const OLD_CHANNEL: &str = "vp2:{voice}:channel:b2xk";
const NEW_CHANNEL: &str = "vp2:{voice}:channel:bmV3";
const OLD_ROOM: &str = "vp2:{voice}:room:bm9kZQ:b2xk";
const NEW_ROOM: &str = "vp2:{voice}:room:bm9kZQ:bmV3";
const EVENTS: &str = "vp2:{voice}:events";

async fn contents(conn: &mut MultiplexedConnection) -> BTreeMap<String, Vec<u8>> {
    let keys: Vec<String> = cmd("KEYS")
        .arg("vp2:{voice}:*")
        .query_async(conn)
        .await
        .unwrap();
    let mut contents = BTreeMap::new();
    for key in keys {
        let dump = cmd("DUMP").arg(&key).query_async(conn).await.unwrap();
        contents.insert(key, dump);
    }
    contents
}

async fn hash(conn: &mut MultiplexedConnection, key: &str) -> BTreeMap<String, String> {
    cmd("HGETALL").arg(key).query_async(conn).await.unwrap()
}

fn generation(channel: &str, participant: &str) -> Generation {
    Generation {
        channel_id: channel.into(),
        node: "node".into(),
        room_sid: channel.into(),
        participant_sid: participant.into(),
    }
}

fn voice_state() -> UserVoiceState {
    serde_json::from_value(json!({
        "id": "user",
        "joined_at": "2026-11-18T00:00:00.000Z",
        "is_receiving": false,
        "is_publishing": true,
        "screensharing": false,
        "camera": false
    }))
    .unwrap()
}

#[tokio::test]
async fn join_leave_cas_updates_slot_indexes_and_stream_together() {
    let Some(url) = std::env::var_os("NOOK_PRESENCE_TEST_REDIS_URL") else {
        eprintln!("skipped Redis integration: set NOOK_PRESENCE_TEST_REDIS_URL to an empty standalone instance");
        return;
    };
    let url = url.into_string().expect("Redis URL must be UTF-8");
    let mut conn = Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    assert!(contents(&mut conn).await.is_empty());
    let scope = Scope::Server("server".into());
    let old = generation("old", "participant-1");
    let new = generation("new", "participant-2");
    let join_old = Transition::Join(&old, &voice_state());
    let join_new = Transition::Join(&new, &voice_state());
    let leave_old = Transition::Leave(&old);
    let leave_new = Transition::Leave(&new);

    let mut presence = Presence::new(&mut conn);
    let initial = presence.read("user", &scope).await.unwrap();
    assert_eq!(initial.revision(), 0);
    assert_eq!(
        presence.apply(&initial, join_old).await.unwrap(),
        Applied(1)
    );
    let first_join = presence.read("user", &scope).await.unwrap();
    assert_eq!(first_join.generation(), Some(&old));

    let before = contents(&mut conn).await;
    assert_eq!(before.len(), 5);
    assert_eq!(hash(&mut conn, USER).await["s:c2VydmVy"], "old");
    assert_eq!(hash(&mut conn, OLD_CHANNEL).await["user"], "s:c2VydmVy");
    assert_eq!(
        hash(&mut conn, OLD_ROOM).await["user"],
        r#"["s:c2VydmVy","participant-1"]"#
    );
    let mut presence = Presence::new(&mut conn);
    assert_eq!(presence.apply(&initial, join_new).await.unwrap(), Conflict);
    assert_eq!(
        presence.apply(&first_join, join_old).await.unwrap(),
        Unchanged
    );
    assert_eq!(contents(&mut conn).await, before);

    let mut presence = Presence::new(&mut conn);
    assert_eq!(
        presence.apply(&first_join, leave_old).await.unwrap(),
        Applied(2)
    );
    let deleted = presence.read("user", &scope).await.unwrap();
    assert_eq!(deleted.revision(), 2);
    assert_eq!(deleted.generation(), None);
    let empty = contents(&mut conn).await;
    assert!([USER, OLD_CHANNEL, OLD_ROOM]
        .iter()
        .all(|key| !empty.contains_key(*key)));
    let mut presence = Presence::new(&mut conn);
    assert_eq!(presence.apply(&deleted, join_old).await.unwrap(), Unchanged);
    assert_eq!(
        presence.apply(&deleted, leave_old).await.unwrap(),
        Unchanged
    );
    assert_eq!(contents(&mut conn).await, empty);

    let mut presence = Presence::new(&mut conn);
    assert_eq!(
        presence.apply(&deleted, join_new).await.unwrap(),
        Applied(3)
    );
    let new_join = presence.read("user", &scope).await.unwrap();
    assert_eq!(new_join.generation(), Some(&new));
    let before = contents(&mut conn).await;
    let mut presence = Presence::new(&mut conn);
    assert_eq!(presence.apply(&initial, join_old).await.unwrap(), Conflict);
    assert_eq!(
        presence.apply(&first_join, leave_old).await.unwrap(),
        Conflict
    );
    assert_eq!(contents(&mut conn).await, before);

    let late = generation("old", "late-participant");
    let mut presence = Presence::new(&mut conn);
    assert_eq!(
        presence
            .apply(&new_join, Transition::Leave(&late))
            .await
            .unwrap(),
        Applied(4)
    );
    let after_late_leave = presence.read("user", &scope).await.unwrap();
    assert_eq!(after_late_leave.generation(), Some(&new));
    let after = contents(&mut conn).await;
    assert_eq!(after.len(), before.len() + 1);
    for key in [USER, NEW_CHANNEL, NEW_ROOM, EVENTS] {
        assert_eq!(after[key], before[key]);
    }
    assert_eq!(hash(&mut conn, USER).await["s:c2VydmVy"], "new");
    assert_eq!(hash(&mut conn, NEW_CHANNEL).await["user"], "s:c2VydmVy");
    assert_eq!(
        hash(&mut conn, NEW_ROOM).await["user"],
        r#"["s:c2VydmVy","participant-2"]"#
    );
    assert!(hash(&mut conn, OLD_CHANNEL).await.is_empty());

    let mut presence = Presence::new(&mut conn);
    assert_eq!(
        presence.apply(&after_late_leave, leave_new).await.unwrap(),
        Applied(5)
    );
    assert_eq!(hash(&mut conn, SLOT).await["rev"], "5");
    for key in [USER, NEW_CHANNEL, NEW_ROOM] {
        assert!(hash(&mut conn, key).await.is_empty());
    }

    let entries: StreamRangeReply = cmd("XRANGE")
        .arg(EVENTS)
        .arg("-")
        .arg("+")
        .query_async(&mut conn)
        .await
        .unwrap();
    let mut previous = (0, 0);
    let mut kinds = Vec::new();
    for entry in entries.ids {
        let (ms, sequence) = entry.id.split_once('-').unwrap();
        let current: (u64, u64) = (ms.parse().unwrap(), sequence.parse().unwrap());
        assert!(current > previous);
        previous = current;
        let channel: String = entry.get("channel_id").unwrap();
        let payload_json: String = entry.get("payload").unwrap();
        let payload: Value = serde_json::from_str(&payload_json).unwrap();
        assert_eq!(payload["id"], channel);
        assert!(payload.get("room_sid").is_none());
        kinds.push((channel, payload["type"].as_str().unwrap().to_owned()));
    }

    assert_eq!(
        kinds,
        [
            ("old".into(), "VoiceChannelJoin".into()),
            ("old".into(), "VoiceChannelLeave".into()),
            ("new".into(), "VoiceChannelJoin".into()),
            ("new".into(), "VoiceChannelLeave".into()),
        ]
    );
    println!("slot revisions 0 → 1 → 2 → 3 → 4 → 5; ordered stream: {kinds:?}");

    verify_fences_and_preflight(&mut conn, &scope).await;
}

async fn verify_fences_and_preflight(conn: &mut MultiplexedConnection, scope: &Scope) {
    let old = generation("old", "participant-3");
    let new = generation("new", "participant-4");
    let state = voice_state();
    let mut presence = Presence::new(conn);
    let empty = presence.read("user", scope).await.unwrap();
    assert_eq!(
        presence
            .apply(&empty, Transition::Leave(&old))
            .await
            .unwrap(),
        Applied(6)
    );
    assert_eq!(
        presence
            .apply(&empty, Transition::Join(&old, &state))
            .await
            .unwrap(),
        Conflict
    );
    let fenced = presence.read("user", scope).await.unwrap();
    assert_eq!(
        presence
            .apply(&fenced, Transition::Join(&old, &state))
            .await
            .unwrap(),
        Unchanged
    );
    assert!(fenced.state().is_none());

    let reopened = generation("old", "participant-5");
    assert_eq!(
        presence
            .apply(&fenced, Transition::Join(&reopened, &state))
            .await
            .unwrap(),
        Applied(7)
    );
    let joined = presence.read("user", scope).await.unwrap();

    let _: () = cmd("SET")
        .arg(NEW_ROOM)
        .arg("wrong type")
        .query_async(conn)
        .await
        .unwrap();
    let before = contents(conn).await;
    let mut presence = Presence::new(conn);
    assert!(presence
        .apply(&joined, Transition::Join(&new, &state))
        .await
        .is_err());
    assert_eq!(contents(conn).await, before);
    let _: () = cmd("DEL").arg(NEW_ROOM).query_async(conn).await.unwrap();

    let mut other_conn = conn.clone();
    let mut presence = Presence::new(conn);
    let mut other_presence = Presence::new(&mut other_conn);
    let move_to_new = Transition::Join(&new, &state);
    let (one, two) = futures::join!(
        presence.apply(&joined, move_to_new),
        other_presence.apply(&joined, move_to_new)
    );
    assert!(matches!(
        (one.unwrap(), two.unwrap()),
        (Applied(8), Conflict) | (Conflict, Applied(8))
    ));
    assert!(hash(conn, OLD_CHANNEL).await.is_empty());
    assert!(hash(conn, OLD_ROOM).await.is_empty());
    assert_eq!(hash(conn, USER).await["s:c2VydmVy"], "new");
    let latest: StreamRangeReply = cmd("XREVRANGE")
        .arg(EVENTS)
        .arg("+")
        .arg("-")
        .arg("COUNT")
        .arg(1)
        .query_async(conn)
        .await
        .unwrap();
    let moved = &latest.ids[0];
    assert_eq!(
        moved.get::<String>("from_channel_id").as_deref(),
        Some("old")
    );
    let payload: String = moved.get("payload").unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&payload).unwrap()["type"],
        "VoiceChannelMove"
    );

    let _: () = cmd("HSET")
        .arg(SLOT)
        .arg("rev")
        .arg("9007199254740992")
        .query_async(conn)
        .await
        .unwrap();
    let mut presence = Presence::new(conn);
    let wide_revision = presence.read("user", scope).await.unwrap();
    let leave_old = Transition::Leave(&reopened);
    assert_eq!(
        presence.apply(&wide_revision, leave_old).await.unwrap(),
        Applied(9007199254740993)
    );
    let before = contents(conn).await;
    let mut presence = Presence::new(conn);
    assert_eq!(
        presence.apply(&wide_revision, leave_old).await.unwrap(),
        Conflict
    );
    assert_eq!(contents(conn).await, before);

    let _: () = cmd("XSETID")
        .arg(EVENTS)
        .arg("18446744073709551615-18446744073709551615")
        .query_async(conn)
        .await
        .unwrap();
    let before = contents(conn).await;
    let mut presence = Presence::new(conn);
    let active = presence.read("user", scope).await.unwrap();
    assert!(presence
        .apply(&active, Transition::Leave(&new))
        .await
        .is_err());
    assert_eq!(contents(conn).await, before);
    println!("terminal-before-Join, competing CAS, wide revision and preflight failures verified");
}
