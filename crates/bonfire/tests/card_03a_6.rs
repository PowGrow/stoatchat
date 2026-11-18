use redis_kiss::redis::{cmd, Client};
use revolt_database::{
    events::client::EventV1,
    voice::presence::{ApplyOutcome, Generation, Presence, Scope, Transition, JOURNAL_BATCH_SIZE},
};
use revolt_models::v0::UserVoiceState;

const STREAM: &str = "vp2:{voice}:events";
const CHANNEL: &str = "voice";
const USER: &str = "participant";

#[async_std::test]
async fn reader_replays_after_ready_and_detects_trimmed_history() {
    let Ok(url) = std::env::var("NOOK_BONFIRE_TEST_REDIS_URL") else {
        eprintln!("skipped Redis integration: set NOOK_BONFIRE_TEST_REDIS_URL to an empty standalone database");
        return;
    };
    let client = Client::open(url).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    assert_eq!(
        cmd("DBSIZE")
            .query_async::<_, u64>(&mut conn)
            .await
            .unwrap(),
        0
    );
    let mut writer = conn.clone();
    let mut presence = Presence::new(&mut writer);
    let scope = Scope::Channel(CHANNEL.into());
    let old = Generation {
        node: "node".into(),
        room_sid: "room".into(),
        participant_sid: "old".into(),
        channel_id: CHANNEL.into(),
    };
    let new = Generation {
        participant_sid: "new".into(),
        ..old.clone()
    };
    let state: UserVoiceState = serde_json::from_value(serde_json::json!({
        "id": USER, "joined_at": "2026-11-18T00:00:00.000Z",
        "is_receiving": true, "is_publishing": false, "screensharing": false, "camera": false
    }))
    .unwrap();

    let initial = presence.read(USER, &scope).await.unwrap();
    presence
        .apply(&initial, Transition::Join(&old, &state))
        .await
        .unwrap();
    let mut reader = Presence::new(&mut conn);
    let (snapshot, before_join) = reader.ready_snapshot([CHANNEL, "empty"]).await.unwrap();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].id, CHANNEL);
    assert_eq!(snapshot[0].participants[0].id, USER);
    assert!(snapshot[0].participants[0].is_receiving);
    let new_state = UserVoiceState {
        is_receiving: false,
        ..state.clone()
    };
    let pinned = presence.read(USER, &scope).await.unwrap();
    presence
        .apply(&pinned, Transition::Join(&new, &new_state))
        .await
        .unwrap();
    let pinned = presence.read(USER, &scope).await.unwrap();
    presence
        .apply(&pinned, Transition::Leave(&old))
        .await
        .unwrap();

    let pinned = presence.read(USER, &scope).await.unwrap();
    assert_eq!(
        presence
            .apply(&pinned, Transition::Leave(&old))
            .await
            .unwrap(),
        ApplyOutcome::Unchanged
    );

    let mut entries = reader.events_after(&before_join).await.unwrap().unwrap();
    assert_eq!(entries.len(), 1);
    let join = entries.pop().unwrap();
    assert!(matches!(join.event, EventV1::VoiceChannelJoin { id, state }
        if id == CHANNEL && !state.is_receiving));
    assert!(reader
        .events_after(&join.id)
        .await
        .unwrap()
        .unwrap()
        .is_empty());

    let (restarted, cursor) = reader.ready_snapshot([CHANNEL]).await.unwrap();
    assert_eq!(cursor, join.id);
    assert!(!restarted[0].participants[0].is_receiving);

    // A burst spans two pages. An evicted cursor can still continue when the
    // first retained entry points back to it.
    for index in 0..=JOURNAL_BATCH_SIZE {
        let pinned = presence.read(USER, &scope).await.unwrap();
        presence
            .apply(&pinned, Transition::Receiving(&new, Some(index % 2 == 0)))
            .await
            .unwrap();
    }
    let _: usize = cmd("XTRIM")
        .arg(STREAM)
        .arg("MAXLEN")
        .arg(JOURNAL_BATCH_SIZE + 1)
        .query_async(&mut conn)
        .await
        .unwrap();
    let mut reader = Presence::new(&mut conn);
    assert!(reader.events_after(&before_join).await.unwrap().is_none());
    let first = reader.events_after(&cursor).await.unwrap().unwrap();
    assert_eq!(first.len(), JOURNAL_BATCH_SIZE);
    for (index, entry) in first.iter().enumerate() {
        assert!(
            matches!(&entry.event, EventV1::UserVoiceStateUpdate { data, .. }
            if data.is_receiving == Some(index % 2 == 0))
        );
    }
    let second = reader
        .events_after(&first.last().unwrap().id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.len(), 1);
    let cursor = &second[0].id;
    assert!(reader
        .events_after(cursor)
        .await
        .unwrap()
        .unwrap()
        .is_empty());

    let pinned = presence.read(USER, &scope).await.unwrap();
    presence
        .apply(&pinned, Transition::Leave(&new))
        .await
        .unwrap();
    let _: usize = cmd("XTRIM")
        .arg(STREAM)
        .arg("MAXLEN")
        .arg(0)
        .query_async(&mut conn)
        .await
        .unwrap();
    let mut reader = Presence::new(&mut conn);
    assert!(reader.events_after(cursor).await.unwrap().is_none());
    let (snapshot, cursor) = reader.ready_snapshot([CHANNEL]).await.unwrap();
    assert!(snapshot.is_empty());
    assert!(reader
        .events_after(&cursor)
        .await
        .unwrap()
        .unwrap()
        .is_empty());
}
