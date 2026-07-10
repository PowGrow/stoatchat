use revolt_database::{
    events::client::EventV1,
    util::reference::Reference,
    voice::{is_in_voice_channel, update_voice_state, UserVoiceChannel},
    Database, User,
};
use revolt_models::v0;
use revolt_result::{create_error, Result};

use rocket::{serde::json::Json, State};

/// # Set Voice State
///
/// Update your own voice state in a voice channel you are connected to
/// and broadcast it to the channel.
#[openapi(tag = "Voice")]
#[put("/<target>/voice_state", data = "<data>")]
pub async fn set_voice_state(
    db: &State<Database>,
    user: User,
    target: Reference<'_>,
    data: Json<v0::DataSetVoiceState>,
) -> Result<Json<v0::PartialUserVoiceState>> {
    let data = data.into_inner();

    let channel = target.as_channel(db).await?;

    if channel.voice().is_none() {
        return Err(create_error!(NotAVoiceChannel));
    }

    let user_voice_channel = UserVoiceChannel::from_channel(&channel);

    if !is_in_voice_channel(&user.id, &user_voice_channel).await? {
        return Err(create_error!(NotConnected));
    }

    let partial = v0::PartialUserVoiceState {
        is_receiving: data.is_receiving,
        ..Default::default()
    };

    update_voice_state(&user_voice_channel, &user.id, &partial).await?;

    EventV1::UserVoiceStateUpdate {
        id: user.id.clone(),
        channel_id: channel.id().to_string(),
        data: partial.clone(),
    }
    .p(channel.id().to_string())
    .await;

    Ok(Json(partial))
}
