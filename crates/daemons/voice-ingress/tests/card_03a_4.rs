mod support;

use std::time::Duration;

use livekit_api::services::room::RoomClient;
use livekit_protocol::{ParticipantInfo, TrackInfo, TrackSource, TrackType, WebhookEvent};
use redis_kiss::redis::{aio::MultiplexedConnection, cmd, streams::StreamRangeReply, Client};
use revolt_database::voice::presence::{Generation, Presence, Scope, Transition};
use revolt_models::v0::UserVoiceState;
use revolt_voice_ingress::{
    participation::apply_participant_webhook,
    room_state::{apply_room_finished, apply_track_webhook},
};
use rocket::tokio::{self, sync::oneshot, time::Instant};
use support::{
    participant, participant_lookup, participant_removal, room_lookup, webhook, PausedCas,
};

const EVENTS: &str = "vp2:{voice}:events";

async fn join(
    conn: &mut MultiplexedConnection,
    scope: &Scope,
    room_sid: &str,
    sid: &str,
    user: &str,
) {
    let mut presence = Presence::new(conn);
    let snapshot = presence.read(user, scope).await.unwrap();
    let generation = Generation {
        room_sid: room_sid.into(),
        participant_sid: sid.into(),
        node: "node".into(),
        channel_id: "channel".into(),
    };
    let state = UserVoiceState {
        id: user.into(),
        joined_at: revolt_database::iso8601_timestamp::Timestamp::UNIX_EPOCH,
        is_receiving: false,
        is_publishing: false,
        camera: false,
        screensharing: false,
    };
    presence
        .apply(&snapshot, Transition::Join(&generation, &state))
        .await
        .unwrap();
}

fn track(sid: &str, muted: bool) -> TrackInfo {
    TrackInfo {
        sid: sid.into(),
        source: TrackSource::Microphone as i32,
        muted,
        ..Default::default()
    }
}

fn current_track(muted: bool) -> ParticipantInfo {
    let mut current = participant("user", "PA-new");
    current.tracks = vec![track("TR-B", muted)];
    current
}

fn track_event(kind: &str, room_sid: &str, participant_sid: &str, track_sid: &str) -> WebhookEvent {
    let mut event = webhook(kind, room_sid, participant_sid, 0, "user");
    event.track = Some(track(track_sid, kind == "track_muted"));
    event
}

async fn receive_track(
    mut conn: MultiplexedConnection,
    livekit: &RoomClient,
    scope: &Scope,
    event: &WebhookEvent,
) {
    apply_track_webhook(
        &mut Presence::new(&mut conn),
        livekit,
        "node",
        scope,
        event,
        Instant::now(),
        None,
    )
    .await
    .unwrap();
}

#[rocket::async_test]
async fn room_and_track_webhooks_reconcile_sid_and_publication() {
    let Ok(url) = std::env::var("NOOK_ROOM_TRACK_TEST_REDIS_URL") else {
        eprintln!("skipped Redis integration: set NOOK_ROOM_TRACK_TEST_REDIS_URL to an empty standalone instance");
        return;
    };
    let mut conn = Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let keys: Vec<String> = cmd("KEYS")
        .arg("vp2:{voice}:*")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(keys.is_empty());
    let (livekit, mut requests, shutdown, server) = support::livekit().await;
    let scope = Scope::Server("server".into());
    join(&mut conn, &scope, "OLD", "PA-old", "user").await;
    join(&mut conn, &scope, "OLD", "PA-other", "other").await;
    join(&mut conn, &scope, "NEW", "PA-new", "user").await;

    let published = track_event("track_published", "NEW", "PA-new", "TR-B");
    tokio::join!(
        receive_track(conn.clone(), &livekit, &scope, &published),
        async {
            room_lookup(&mut requests, "NEW").await;
            participant_lookup(&mut requests, "user")
                .await
                .send(Some(current_track(false)))
                .unwrap();
        }
    );
    let active = Presence::new(&mut conn).read("user", &scope).await.unwrap();
    assert!(active.state().unwrap().is_publishing);

    let old_room = webhook("room_finished", "OLD", "", 0, "");
    let (arrived, at_cas) = oneshot::channel();
    let (_resume, gate) = oneshot::channel();
    let mut paused = PausedCas {
        conn: conn.clone(),
        gate: Some((arrived, gate)),
    };
    let mut presence = Presence::new(&mut paused);
    let (interrupted, ()) = tokio::join!(
        apply_room_finished(
            &mut presence,
            "node",
            &old_room,
            Instant::now() - Duration::from_secs(19),
        ),
        async {
            tokio::time::timeout(Duration::from_secs(4), at_cas)
                .await
                .unwrap()
                .unwrap();
            let ttl: i64 = cmd("TTL")
                .arg("vp2:{voice}:ended:room:bm9kZQ:T0xE")
                .query_async(&mut conn)
                .await
                .unwrap();
            assert_eq!(ttl, -1);
            assert!(Presence::new(&mut conn)
                .read("other", &scope)
                .await
                .unwrap()
                .state()
                .is_some());
        },
    );
    assert!(interrupted.is_err());

    for _ in 0..2 {
        apply_room_finished(
            &mut Presence::new(&mut conn),
            "node",
            &old_room,
            Instant::now(),
        )
        .await
        .unwrap();
    }
    let old_track = track_event("track_unpublished", "OLD", "PA-old", "TR-A");
    receive_track(conn.clone(), &livekit, &scope, &old_track).await;
    let mut presence = Presence::new(&mut conn);
    let still_active = presence.read("user", &scope).await.unwrap();
    assert_eq!(still_active.revision(), active.revision());
    assert_eq!(still_active.generation().unwrap().room_sid, "NEW");
    assert!(still_active.state().unwrap().is_publishing);
    assert!(!still_active.state().unwrap().is_receiving);
    assert!(presence
        .read("other", &scope)
        .await
        .unwrap()
        .state()
        .is_none());
    println!(
        "room_finished OLD fences before cleanup; retry removes only OLD members; old-room track leaves NEW intact"
    );

    let old_unpublish = track_event("track_unpublished", "NEW", "PA-new", "TR-A");
    let late_mute = track_event("track_muted", "NEW", "PA-new", "TR-B");
    for event in [&old_unpublish, &late_mute] {
        tokio::join!(
            receive_track(conn.clone(), &livekit, &scope, event),
            async {
                room_lookup(&mut requests, "NEW").await;
                participant_lookup(&mut requests, "user")
                    .await
                    .send(Some(current_track(false)))
                    .unwrap();
            }
        );
        assert!(
            Presence::new(&mut conn)
                .read("user", &scope)
                .await
                .unwrap()
                .state()
                .unwrap()
                .is_publishing
        );
    }
    println!("old unpublish A and late mute B leave unmuted B publishing");

    let (arrived, at_cas) = oneshot::channel();
    let (resume, gate) = oneshot::channel();
    let mut paused = PausedCas {
        conn: conn.clone(),
        gate: Some((arrived, gate)),
    };
    let mut presence = Presence::new(&mut paused);
    let (outcome, ()) = tokio::join!(
        apply_track_webhook(
            &mut presence,
            &livekit,
            "node",
            &scope,
            &late_mute,
            Instant::now(),
            None,
        ),
        async {
            room_lookup(&mut requests, "NEW").await;
            participant_lookup(&mut requests, "user")
                .await
                .send(Some(current_track(true)))
                .unwrap();
            tokio::time::timeout(Duration::from_secs(4), at_cas)
                .await
                .unwrap()
                .unwrap();
            let unmute = track_event("track_unmuted", "NEW", "PA-new", "TR-B");
            tokio::join!(
                receive_track(conn.clone(), &livekit, &scope, &unmute),
                async {
                    room_lookup(&mut requests, "NEW").await;
                    participant_lookup(&mut requests, "user")
                        .await
                        .send(Some(current_track(false)))
                        .unwrap();
                }
            );
            resume.send(()).unwrap();
            room_lookup(&mut requests, "NEW").await;
            participant_lookup(&mut requests, "user")
                .await
                .send(Some(current_track(false)))
                .unwrap();
        }
    );
    outcome.unwrap();
    assert!(
        Presence::new(&mut conn)
            .read("user", &scope)
            .await
            .unwrap()
            .state()
            .unwrap()
            .is_publishing
    );
    println!("unmute with unchanged flags fences a stale mute already waiting at CAS");

    let join_pending = webhook(
        "participant_joined",
        "RACE",
        "PA-pending",
        chrono::Utc::now().timestamp(),
        "pending",
    );
    let (arrived, at_cas) = oneshot::channel();
    let (resume, gate) = oneshot::channel();
    let mut paused = PausedCas {
        conn: conn.clone(),
        gate: Some((arrived, gate)),
    };
    let mut presence = Presence::new(&mut paused);
    let (outcome, ()) = tokio::join!(
        apply_participant_webhook(
            &mut presence,
            &livekit,
            "node",
            &scope,
            &join_pending,
            Instant::now()
        ),
        async {
            room_lookup(&mut requests, "RACE").await;
            participant_lookup(&mut requests, "pending")
                .await
                .send(Some(participant("pending", "PA-pending")))
                .unwrap();
            tokio::time::timeout(Duration::from_secs(4), at_cas)
                .await
                .unwrap()
                .unwrap();
            let finish = webhook("room_finished", "RACE", "", 0, "");
            apply_room_finished(
                &mut Presence::new(&mut conn),
                "node",
                &finish,
                Instant::now(),
            )
            .await
            .unwrap();
            resume.send(()).unwrap();
        }
    );
    outcome.unwrap();
    assert!(Presence::new(&mut conn)
        .read("pending", &scope)
        .await
        .unwrap()
        .state()
        .is_none());
    println!("room_finished fences a pending Join even when the room index is empty");

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
            println!("stream {} {payload}", entry.id);
            serde_json::from_str::<serde_json::Value>(&payload).unwrap()
        })
        .collect();
    assert_eq!(events.len(), 5);
    assert_eq!(events[3]["type"], "UserVoiceStateUpdate");
    assert_eq!(events[3]["data"]["is_publishing"], true);
    assert_eq!(events[4]["type"], "VoiceChannelLeave");
    assert_eq!(events[4]["user"], "other");

    let mut limits = revolt_config::config()
        .await
        .features
        .limits
        .default
        .clone();
    limits.video_resolution = [1280, 720];
    let excessive_video = TrackInfo {
        sid: "TR-video".into(),
        r#type: TrackType::Video as i32,
        source: TrackSource::Camera as i32,
        width: 1920,
        height: 1080,
        ..Default::default()
    };
    let mut published = track_event("track_published", "NEW", "PA-new", "TR-video");
    published.track = Some(excessive_video.clone());
    let mut request_conn = conn.clone();
    let mut presence = Presence::new(&mut request_conn);
    let (outcome, ()) = tokio::join!(
        apply_track_webhook(
            &mut presence,
            &livekit,
            "node",
            &scope,
            &published,
            Instant::now(),
            Some(&limits)
        ),
        async {
            room_lookup(&mut requests, "NEW").await;
            let mut current = current_track(false);
            current.tracks.push(excessive_video);
            participant_lookup(&mut requests, "user")
                .await
                .send(Some(current))
                .unwrap();
            let reply = participant_removal(&mut requests, "user").await;
            join(&mut conn, &scope, "NEW", "PA-next", "user").await;
            reply.send(()).unwrap();
        },
    );
    outcome.unwrap();
    let replacement = presence.read("user", &scope).await.unwrap();
    assert_eq!(replacement.generation().unwrap().participant_sid, "PA-next");
    println!("verified oversized track triggered removal, CAS preserved replacement participation");
    assert!(requests.try_recv().is_err());
    shutdown.notify();
    server.await.unwrap().unwrap();
}
