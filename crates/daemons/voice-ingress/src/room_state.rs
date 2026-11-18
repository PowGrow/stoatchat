use livekit_api::services::room::RoomClient;
use livekit_protocol::{TrackInfo, TrackSource, TrackType, WebhookEvent};
use redis_kiss::redis::aio::ConnectionLike;
use revolt_config::FeaturesLimits;
use revolt_database::voice::presence::{ApplyOutcome, Presence, Scope, TrackState, Transition};
use revolt_result::{create_error, Result, ToRevoltError};
use rocket::tokio::time::{sleep, timeout, timeout_at, Instant};

use crate::participation::{
    current_participant, webhook_participation, LOOKUP_TIMEOUT, RETRY_BUDGET, RETRY_DELAY,
};

/// Marks an authenticated room_finished SID terminal and drains its indexed presence.
pub async fn apply_room_finished<C: ConnectionLike + Send>(
    presence: &mut Presence<'_, C>,
    node: &str,
    event: &WebhookEvent,
    received_at: Instant,
) -> Result<()> {
    let room = event.room.as_ref().to_internal_error()?;
    timeout_at(
        received_at + RETRY_BUDGET,
        presence.finish_room(node, &room.sid),
    )
    .await
    .map_err(|_| retry_budget_error(node, event))?
    .to_internal_error()
}

/// Uses an authenticated track event to refresh the current participant's publication state.
/// Lookup failures defer writes. A revision conflict forces a new LiveKit observation.
pub async fn apply_track_webhook<C: ConnectionLike + Send>(
    presence: &mut Presence<'_, C>,
    livekit: &RoomClient,
    node: &str,
    scope: &Scope,
    event: &WebhookEvent,
    received_at: Instant,
    limits: Option<&FeaturesLimits>,
) -> Result<()> {
    let (user_id, generation) = webhook_participation(node, event)?;
    let track = event.track.as_ref().to_internal_error()?;
    if track.sid.is_empty() {
        return Err(create_error!(InternalError));
    }

    let deadline = received_at + RETRY_BUDGET;
    timeout_at(deadline, async {
        while Instant::now() < deadline {
            let snapshot = presence.read(user_id, scope).await.to_internal_error()?;
            if snapshot.generation() != Some(&generation) {
                return Ok(());
            }

            let lookup = timeout(
                LOOKUP_TIMEOUT,
                current_participant(livekit, &generation, user_id),
            );
            let participant = match lookup.await {
                Ok(Ok(Some(participant))) => participant,
                Ok(Ok(None)) => return Ok(()),
                failure => {
                    log::debug!("LiveKit track lookup deferred: {failure:?}");
                    sleep(RETRY_DELAY).await;
                    continue;
                }
            };

            if Instant::now() >= deadline {
                break;
            }
            if let Some(limits) = limits {
                if participant.tracks.iter().any(|current| {
                    current.sid == track.sid && track_exceeds_limits(current, limits)
                }) {
                    let _ = livekit
                        .remove_participant(&generation.channel_id, user_id)
                        .await;
                    return presence.retire(snapshot).await.to_internal_error();
                }
            }
            // An unpublished SID can be absent while a replacement of the same
            // source is active. Use the complete set of current tracks.
            let tracks = publication_state(&participant.tracks);
            match presence
                .apply(&snapshot, Transition::Tracks(&generation, tracks))
                .await
                .to_internal_error()?
            {
                ApplyOutcome::Applied(_) | ApplyOutcome::Unchanged => return Ok(()),
                ApplyOutcome::Conflict => sleep(RETRY_DELAY).await,
            }
        }
        Err(retry_budget_error(node, event))
    })
    .await
    .unwrap_or_else(|_| Err(retry_budget_error(node, event)))
}

/// Returns whether a published track violates the configured data or video limits.
pub fn track_exceeds_limits(track: &TrackInfo, limits: &FeaturesLimits) -> bool {
    if track.r#type == TrackType::Data as i32 {
        return true;
    }
    if track.r#type != TrackType::Video as i32 {
        return false;
    }
    let [width, height] = limits.video_resolution;
    let pixels = u64::from(track.width) * u64::from(track.height);
    let [min_aspect, max_aspect] = limits.video_aspect_ratio;
    (width != 0 && height != 0 && pixels > u64::from(width) * u64::from(height))
        || (min_aspect != max_aspect
            && !(min_aspect..=max_aspect).contains(&(track.width as f32 / track.height as f32)))
}

fn publication_state(tracks: &[TrackInfo]) -> TrackState {
    let publishing = |source| {
        tracks
            .iter()
            .any(|track| !track.sid.is_empty() && !track.muted && track.source == source as i32)
    };
    TrackState {
        microphone: publishing(TrackSource::Microphone),
        camera: publishing(TrackSource::Camera),
        screensharing: publishing(TrackSource::ScreenShare)
            || publishing(TrackSource::ScreenShareAudio),
    }
}

fn retry_budget_error(node: &str, event: &WebhookEvent) -> revolt_result::Error {
    log::warn!(
        "LiveKit {} webhook {} on node {node} exhausted the retry budget; reconciliation required",
        event.event,
        event.id
    );
    create_error!(InternalError)
}
