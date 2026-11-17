use std::time::Duration;

use livekit_api::services::{room::RoomClient, ServiceError, TwirpError, TwirpErrorCode};
use livekit_protocol::{ParticipantInfo, WebhookEvent};
use redis_kiss::redis::aio::ConnectionLike;
use revolt_database::{
    iso8601_timestamp::{Duration as TimestampDuration, Timestamp},
    voice::presence::{ApplyOutcome, Generation, Presence, Scope, Transition},
};
use revolt_models::v0::UserVoiceState;
use revolt_result::{create_error, Result, ToRevoltError};
use rocket::tokio::time::{sleep, timeout, timeout_at, Instant};

const MAX_DIRECT_AGE_SECONDS: i64 = 60;
pub(crate) const LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);
pub(crate) const RETRY_BUDGET: Duration = Duration::from_secs(20);
pub(crate) const RETRY_DELAY: Duration = Duration::from_millis(250);

/// Reconciles an authenticated participant webhook against the named LiveKit node.
/// A retry-budget error leaves delivery unacknowledged so any redelivery can reconcile afresh.
pub async fn apply_participant_webhook<C: ConnectionLike + Send>(
    presence: &mut Presence<'_, C>,
    livekit: &RoomClient,
    node: &str,
    scope: &Scope,
    event: &WebhookEvent,
    received_at: Instant,
) -> Result<()> {
    let (user_id, generation) = webhook_participation(node, event)?;
    let state;
    let transition = match event.event.as_str() {
        "participant_joined" => {
            let joined_at = Timestamp::UNIX_EPOCH
                .checked_add(TimestampDuration::seconds(event.created_at))
                .to_internal_error()?;
            state = UserVoiceState {
                joined_at,
                id: user_id.to_owned(),
                is_receiving: true,
                is_publishing: false,
                screensharing: false,
                camera: false,
            };
            Transition::Join(&generation, &state)
        }
        "participant_left" => Transition::Leave(&generation),
        _ => return Err(create_error!(InternalError)),
    };

    let deadline = received_at + RETRY_BUDGET;
    let budget_exhausted = || {
        log::warn!(
            "LiveKit participant webhook {} on node {node} exhausted the retry budget; reconciliation required",
            event.id
        );
        create_error!(InternalError)
    };
    timeout_at(deadline, async {
        while Instant::now() < deadline {
            let snapshot = presence.read(user_id, scope).await.to_internal_error()?;
            let joining = matches!(transition, Transition::Join(..));
            // A fresh Leave fences its SID even if LiveKit still has an in-flight lookup.
            let age = chrono::Utc::now()
                .timestamp()
                .saturating_sub(event.created_at);
            if joining || !(0..=MAX_DIRECT_AGE_SECONDS).contains(&age) {
                let lookup = timeout(
                    LOOKUP_TIMEOUT,
                    current_participant(livekit, &generation, user_id),
                );
                match lookup.await {
                    Ok(Ok(current)) if current.is_some() != joining => return Ok(()),
                    Ok(Ok(_)) => {}
                    failure => {
                        log::debug!("LiveKit participation lookup deferred: {failure:?}");
                        sleep(RETRY_DELAY).await;
                        continue;
                    }
                }
            }
            if Instant::now() >= deadline {
                break;
            }
            match presence
                .apply(&snapshot, transition)
                .await
                .to_internal_error()?
            {
                ApplyOutcome::Applied(_) | ApplyOutcome::Unchanged => return Ok(()),
                ApplyOutcome::Conflict => sleep(RETRY_DELAY).await,
            }
        }
        Err(budget_exhausted())
    })
    .await
    .unwrap_or_else(|_| Err(budget_exhausted()))
}

pub(crate) fn webhook_participation<'a>(
    node: &str,
    event: &'a WebhookEvent,
) -> Result<(&'a str, Generation)> {
    let room = event.room.as_ref().to_internal_error()?;
    let participant = event.participant.as_ref().to_internal_error()?;
    if [
        &room.name,
        &room.sid,
        &participant.identity,
        &participant.sid,
    ]
    .iter()
    .any(|field| field.is_empty())
        || node.is_empty()
    {
        return Err(create_error!(InternalError));
    }
    Ok((
        &participant.identity,
        Generation {
            node: node.to_owned(),
            room_sid: room.sid.clone(),
            participant_sid: participant.sid.clone(),
            channel_id: room.name.clone(),
        },
    ))
}

pub(crate) async fn current_participant(
    livekit: &RoomClient,
    generation: &Generation,
    user_id: &str,
) -> std::result::Result<Option<ParticipantInfo>, ServiceError> {
    let rooms = livekit
        .list_rooms(vec![generation.channel_id.clone()])
        .await?;
    if !rooms
        .iter()
        .any(|room| room.name == generation.channel_id && room.sid == generation.room_sid)
    {
        return Ok(None);
    }
    match livekit
        .get_participant(&generation.channel_id, user_id)
        .await
    {
        Ok(participant) => Ok(Some(participant).filter(|participant| {
            participant.identity == user_id && participant.sid == generation.participant_sid
        })),
        Err(ServiceError::Twirp(TwirpError::Twirp(error)))
            if error.code == TwirpErrorCode::NOT_FOUND =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}
