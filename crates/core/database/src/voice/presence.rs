//! Versioned voice presence stored in a standalone Redis-compatible instance.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use redis_kiss::redis::{aio::ConnectionLike, cmd, ErrorKind, RedisError, RedisResult};
use revolt_models::v0::UserVoiceState;
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

impl Generation {
    fn room_key_suffix(&self) -> String {
        format!(
            "{}:{}",
            URL_SAFE_NO_PAD.encode(&self.node),
            URL_SAFE_NO_PAD.encode(&self.room_sid)
        )
    }
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
}

#[derive(Debug, Eq, PartialEq)]
pub enum ApplyOutcome {
    Applied(u64),
    /// The snapshot has changed. Read the slot and revalidate external evidence.
    Conflict,
    /// Duplicate transition or a Join for terminal coordinates.
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
            _ => (String::new(), None, ""),
        };
        let payload = event
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(json_error)?
            .unwrap_or_default();
        let (old_channel, old_room) = current.map(index_keys).unwrap_or_default();
        let (new_channel, new_room) = index_keys(generation);
        let room = generation.room_key_suffix();
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
        format!("{PREFIX}:room:{}", generation.room_key_suffix()),
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
