//! Versioned voice presence stored in a standalone Redis-compatible instance.

use std::collections::HashMap;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use redis_kiss::redis::{aio::ConnectionLike, cmd, ErrorKind, RedisError, RedisResult};
use revolt_models::v0::{PartialUserVoiceState, UserVoiceState};
use revolt_result::{create_error, Result, ToRevoltError};
use serde::{Deserialize, Serialize};

use crate::events::client::EventV1;

const PREFIX: &str = "vp2:{voice}";
const APPLY: &str = include_str!("presence.lua");

/// A user shares one slot per server and has independent slots for private channels.
#[derive(Clone, Debug)]
pub enum Scope {
    Server(String),
    Channel(String),
}

impl Scope {
    fn from_key(key: &str) -> RedisResult<Self> {
        let (kind, encoded_id) = key
            .split_once(':')
            .ok_or_else(|| invalid("invalid voice scope"))?;
        let id = URL_SAFE_NO_PAD
            .decode(encoded_id)
            .ok()
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .filter(|id| !id.is_empty())
            .ok_or_else(|| invalid("invalid voice scope ID"))?;
        match kind {
            "s" => Ok(Self::Server(id)),
            "c" => Ok(Self::Channel(id)),
            _ => Err(invalid("invalid voice scope kind")),
        }
    }

    fn key(&self) -> String {
        match self {
            Self::Server(id) => format!("s:{}", URL_SAFE_NO_PAD.encode(id)),
            Self::Channel(id) => format!("c:{}", URL_SAFE_NO_PAD.encode(id)),
        }
    }

    fn id(&self) -> &str {
        match self {
            Self::Server(id) | Self::Channel(id) => id,
        }
    }
}

/// LiveKit coordinates of a single confirmed participation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Generation {
    pub node: String,
    pub room_sid: String,
    pub participant_sid: String,
    pub channel_id: String,
}

fn room_key_suffix(node: &str, room_sid: &str) -> String {
    format!(
        "{}:{}",
        URL_SAFE_NO_PAD.encode(node),
        URL_SAFE_NO_PAD.encode(room_sid)
    )
}

/// Publication state derived from one authoritative LiveKit participant lookup.
#[derive(Clone, Copy)]
pub struct TrackState {
    pub microphone: bool,
    pub camera: bool,
    pub screensharing: bool,
}

#[derive(Deserialize, Serialize)]
struct Active {
    generation: Generation,
    state: UserVoiceState,
}

/// The revision and participation read together from one slot.
pub struct Snapshot {
    user_id: String,
    scope: Scope,
    revision: u64,
    active_json: Option<String>,
    active: Option<Active>,
}

impl Snapshot {
    /// Zero denotes a slot that has never been written.
    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn generation(&self) -> Option<&Generation> {
        self.active.as_ref().map(|active| &active.generation)
    }

    pub fn state(&self) -> Option<&UserVoiceState> {
        self.active.as_ref().map(|active| &active.state)
    }
}

/// A Join must be confirmed against LiveKit after reading the snapshot.
/// A Leave identifies the exact participation that ended.
#[derive(Clone, Copy)]
pub enum Transition<'a> {
    Join(&'a Generation, &'a UserVoiceState),
    Leave(&'a Generation),
    /// Fences earlier track lookups even when the resulting flags are unchanged.
    Tracks(&'a Generation, TrackState),
    Receiving(&'a Generation, Option<bool>),
}

#[derive(Debug, Eq, PartialEq)]
pub enum ApplyOutcome {
    Applied(u64),
    /// The snapshot has changed. Read the slot and revalidate external evidence.
    Conflict,
    /// Duplicate transition or an observation for inactive or terminal coordinates.
    Unchanged,
}

pub struct Presence<'a, C> {
    conn: &'a mut C,
}

impl<'a, C: ConnectionLike + Send> Presence<'a, C> {
    pub fn new(conn: &'a mut C) -> Self {
        Self { conn }
    }

    /// Reads a slot without modifying Redis. An empty slot retains its last revision.
    pub async fn read(&mut self, user_id: &str, scope: &Scope) -> RedisResult<Snapshot> {
        if user_id.is_empty() || scope.id().is_empty() {
            return Err(invalid("voice presence requires a user and a scope"));
        }
        let key = slot_key(&URL_SAFE_NO_PAD.encode(user_id), &scope.key());
        let (revision, active_json): (Option<String>, Option<String>) = cmd("HMGET")
            .arg(key)
            .arg(&["rev", "active"])
            .query_async(self.conn)
            .await?;
        let revision = match (&revision, &active_json) {
            (None, None) => 0,
            (Some(revision), Some(_)) => revision
                .parse::<u64>()
                .ok()
                .filter(|value| *value > 0 && value.to_string() == *revision)
                .ok_or_else(|| invalid("invalid voice presence revision"))?,
            _ => return Err(invalid("incomplete voice presence slot")),
        };
        let active = active_json
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(serde_json::from_str)
            .transpose()
            .map_err(json_error)?;
        Ok(Snapshot {
            user_id: user_id.to_owned(),
            scope: scope.clone(),
            revision,
            active_json,
            active,
        })
    }

    /// Updates deafen for exactly the participation captured at the start of PUT.
    /// A stale revision or terminal participation returns NotConnected without a retry.
    pub async fn set_receiving(
        &mut self,
        snapshot: &Snapshot,
        channel_id: &str,
        receiving: Option<bool>,
    ) -> Result<()> {
        let generation = snapshot
            .generation()
            .filter(|generation| generation.channel_id == channel_id)
            .ok_or_else(|| create_error!(NotConnected))?;
        match self
            .apply(snapshot, Transition::Receiving(generation, receiving))
            .await
            .to_internal_error()?
        {
            ApplyOutcome::Applied(_) => Ok(()),
            ApplyOutcome::Conflict | ApplyOutcome::Unchanged => Err(create_error!(NotConnected)),
        }
    }

    /// Fences new writes for this room SID, then removes its indexed participations.
    /// Retrying after interruption drains the remaining members without duplicate events.
    pub async fn finish_room(&mut self, node: &str, room_sid: &str) -> RedisResult<()> {
        if node.is_empty() || room_sid.is_empty() {
            return Err(invalid("voice room requires a node and SID"));
        }
        let room = room_key_suffix(node, room_sid);
        // A Join either commits before this marker and is indexed, or sees it and
        // declines. Put the marker first so a retry can resume a partial drain.
        cmd("SET")
            .arg(format!("{PREFIX}:ended:room:{room}"))
            .arg("1")
            .arg("NX")
            .query_async::<_, Option<String>>(self.conn)
            .await?;
        let members: HashMap<String, String> = cmd("HGETALL")
            .arg(format!("{PREFIX}:room:{room}"))
            .query_async(self.conn)
            .await?;
        for (user_id, member) in members {
            let (scope, participant_sid): (String, String) =
                serde_json::from_str(&member).map_err(json_error)?;
            let scope = Scope::from_key(&scope)?;
            loop {
                let snapshot = self.read(&user_id, &scope).await?;
                let Some(generation) = snapshot.generation().filter(|generation| {
                    generation.node == node
                        && generation.room_sid == room_sid
                        && generation.participant_sid == participant_sid
                }) else {
                    break;
                };
                match self.apply(&snapshot, Transition::Leave(generation)).await? {
                    ApplyOutcome::Conflict => continue,
                    ApplyOutcome::Applied(_) | ApplyOutcome::Unchanged => break,
                }
            }
        }
        Ok(())
    }

    /// Atomically applies a transition to a snapshot read from this Redis instance.
    /// After a transport error, reread the slot before using external evidence again.
    pub async fn apply(
        &mut self,
        snapshot: &Snapshot,
        transition: Transition<'_>,
    ) -> RedisResult<ApplyOutcome> {
        let current = snapshot.generation();
        let (operation, generation) = match transition {
            Transition::Join(generation, _) => ("join", generation),
            Transition::Leave(generation) => ("leave", generation),
            Transition::Tracks(generation, _) => ("tracks", generation),
            Transition::Receiving(generation, _) => ("receiving", generation),
        };
        if [
            &generation.channel_id,
            &generation.node,
            &generation.room_sid,
            &generation.participant_sid,
        ]
        .iter()
        .any(|part| part.is_empty())
            || matches!(&snapshot.scope, Scope::Channel(id) if *id != generation.channel_id)
        {
            return Err(invalid("invalid voice participation coordinates for slot"));
        }

        let scope = snapshot.scope.key();
        let user_id = &snapshot.user_id;
        let encoded_user = URL_SAFE_NO_PAD.encode(user_id);
        let (next_active, event, from_channel) = match transition {
            Transition::Join(_, state) if state.id != *user_id => {
                return Err(invalid("voice state user does not match slot"));
            }
            Transition::Join(_, state) if current != Some(generation) => {
                let active = serde_json::to_string(&Active {
                    generation: generation.clone(),
                    state: state.clone(),
                })
                .map_err(json_error)?;
                if let Some(old) = current.filter(|old| old.channel_id != generation.channel_id) {
                    let event = EventV1::VoiceChannelMove {
                        user: user_id.clone(),
                        from: old.channel_id.clone(),
                        to: generation.channel_id.clone(),
                        state: state.clone(),
                    };
                    (active, Some(event), old.channel_id.as_str())
                } else {
                    let event = EventV1::VoiceChannelJoin {
                        id: generation.channel_id.clone(),
                        state: state.clone(),
                    };
                    (active, Some(event), "")
                }
            }
            Transition::Leave(_) if current == Some(generation) => {
                let event = EventV1::VoiceChannelLeave {
                    id: generation.channel_id.clone(),
                    user: user_id.clone(),
                };
                (String::new(), Some(event), "")
            }
            Transition::Tracks(_, tracks) => {
                let Some(mut state) = snapshot
                    .state()
                    .filter(|_| current == Some(generation))
                    .cloned()
                else {
                    return Ok(ApplyOutcome::Unchanged);
                };
                let partial = PartialUserVoiceState {
                    is_publishing: (state.is_publishing != tracks.microphone)
                        .then_some(tracks.microphone),
                    camera: (state.camera != tracks.camera).then_some(tracks.camera),
                    screensharing: (state.screensharing != tracks.screensharing)
                        .then_some(tracks.screensharing),
                    ..Default::default()
                };
                if partial.is_publishing.is_none()
                    && partial.camera.is_none()
                    && partial.screensharing.is_none()
                {
                    (snapshot.active_json.clone().unwrap_or_default(), None, "")
                } else {
                    state.is_publishing = tracks.microphone;
                    state.camera = tracks.camera;
                    state.screensharing = tracks.screensharing;
                    let active = serde_json::to_string(&Active {
                        generation: generation.clone(),
                        state,
                    })
                    .map_err(json_error)?;
                    let event = EventV1::UserVoiceStateUpdate {
                        id: user_id.clone(),
                        channel_id: generation.channel_id.clone(),
                        data: partial,
                    };
                    (active, Some(event), "")
                }
            }
            Transition::Receiving(_, receiving) => {
                let Some(mut state) = snapshot
                    .state()
                    .filter(|_| current == Some(generation))
                    .cloned()
                else {
                    return Ok(ApplyOutcome::Unchanged);
                };
                if let Some(value) = receiving {
                    state.is_receiving = value;
                    let active = serde_json::to_string(&Active {
                        generation: generation.clone(),
                        state,
                    })
                    .map_err(json_error)?;
                    let event = EventV1::UserVoiceStateUpdate {
                        id: user_id.clone(),
                        channel_id: generation.channel_id.clone(),
                        data: PartialUserVoiceState {
                            is_receiving: receiving,
                            ..Default::default()
                        },
                    };
                    (active, Some(event), "")
                } else {
                    (String::new(), None, "")
                }
            }
            Transition::Join(..) | Transition::Leave(_) => (String::new(), None, ""),
        };
        let payload = event
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(json_error)?
            .unwrap_or_default();
        let (old_channel, old_room) = current.map(index_keys).unwrap_or_default();
        let (new_channel, new_room) = index_keys(generation);
        let room = room_key_suffix(&generation.node, &generation.room_sid);
        let participant = URL_SAFE_NO_PAD.encode(&generation.participant_sid);
        let room_member =
            serde_json::to_string(&(&scope, &generation.participant_sid)).map_err(json_error)?;
        let next_revision = snapshot.revision.checked_add(1);

        let outcome: String = cmd("EVAL")
            .arg(APPLY)
            .arg(9)
            .arg(slot_key(&encoded_user, &scope))
            .arg(format!("{PREFIX}:user:{encoded_user}"))
            .arg(old_channel)
            .arg(old_room)
            .arg(new_channel)
            .arg(new_room)
            .arg(format!("{PREFIX}:ended:participant:{room}:{participant}"))
            .arg(format!("{PREFIX}:ended:room:{room}"))
            .arg(format!("{PREFIX}:events"))
            .arg(snapshot.revision.to_string())
            .arg(snapshot.active_json.as_deref().unwrap_or_default())
            .arg(
                next_revision
                    .map(|revision| revision.to_string())
                    .unwrap_or_default(),
            )
            .arg(operation)
            .arg(user_id)
            .arg(scope)
            .arg(&generation.channel_id)
            .arg(room_member)
            .arg(next_active)
            .arg(payload)
            .arg(from_channel)
            .query_async(self.conn)
            .await?;

        match outcome.as_str() {
            "applied" => next_revision
                .map(ApplyOutcome::Applied)
                .ok_or_else(|| invalid("voice presence revision exhausted")),
            "conflict" => Ok(ApplyOutcome::Conflict),
            "unchanged" => Ok(ApplyOutcome::Unchanged),
            _ => Err(invalid("unexpected voice presence CAS reply")),
        }
    }
}

fn slot_key(encoded_user: &str, scope: &str) -> String {
    format!("{PREFIX}:slot:{encoded_user}:{scope}")
}

fn index_keys(generation: &Generation) -> (String, String) {
    (
        format!(
            "{PREFIX}:channel:{}",
            URL_SAFE_NO_PAD.encode(&generation.channel_id)
        ),
        format!(
            "{PREFIX}:room:{}",
            room_key_suffix(&generation.node, &generation.room_sid)
        ),
    )
}

fn invalid(message: &'static str) -> RedisError {
    (ErrorKind::TypeError, message).into()
}

fn json_error(error: serde_json::Error) -> RedisError {
    (
        ErrorKind::TypeError,
        "invalid voice presence payload",
        error.to_string(),
    )
        .into()
}
