use revolt_database::{
    util::{permissions::perms, reference::Reference},
    voice::{get_channel_node, is_in_voice_channel, UserVoiceChannel, VoiceClient},
    Database, SoundParent, User,
};
use revolt_permissions::{calculate_channel_permissions, ChannelPermission};
use revolt_result::{create_error, Result, ToRevoltError};

use rocket::State;
use rocket_empty::EmptyResponse;

/// LiveKit data message topic for soundboard play notifications
static SOUNDBOARD_TOPIC: &str = "soundboard";

/// # Play Sound
///
/// Play a soundboard sound in a voice channel you are connected to.
/// All participants of the call receive a data message and play the sound locally.
#[openapi(tag = "Sounds")]
#[post("/<target>/sounds/<sound_id>/play")]
pub async fn play_sound(
    db: &State<Database>,
    voice_client: &State<VoiceClient>,
    user: User,
    target: Reference<'_>,
    sound_id: Reference<'_>,
) -> Result<EmptyResponse> {
    if !voice_client.is_enabled() {
        return Err(create_error!(LiveKitUnavailable));
    }

    let channel = target.as_channel(db).await?;

    if channel.voice().is_none() {
        return Err(create_error!(NotAVoiceChannel));
    }

    // Only participants of the call may trigger sounds
    let user_voice_channel = UserVoiceChannel::from_channel(&channel);
    if !is_in_voice_channel(&user.id, &user_voice_channel).await? {
        return Err(create_error!(NotConnected));
    }

    // Require permission to speak in the channel
    let mut permissions = perms(db, &user).channel(&channel);
    calculate_channel_permissions(&mut permissions)
        .await
        .throw_if_lacking_channel_permission(ChannelPermission::Speak)?;

    // The sound must belong to the server this channel is in
    let sound = sound_id.as_sound(db).await?;
    match &sound.parent {
        SoundParent::Server { id } => {
            if channel.server() != Some(id.as_str()) {
                return Err(create_error!(NotFound));
            }
        }
        SoundParent::Detached => return Err(create_error!(NotFound)),
    }

    let node = get_channel_node(channel.id())
        .await?
        .ok_or_else(|| create_error!(NotConnected))?;

    let payload = serde_json::to_vec(&serde_json::json!({
        "sound_id": sound.id,
        "channel_id": channel.id(),
        "user_id": user.id,
    }))
    .to_internal_error()?;

    voice_client
        .send_data(&node, channel.id(), SOUNDBOARD_TOPIC, payload)
        .await
        .map(|_| EmptyResponse)
}
