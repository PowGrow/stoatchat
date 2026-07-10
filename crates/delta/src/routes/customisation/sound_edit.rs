use revolt_database::{
    util::{permissions::DatabasePermissionQuery, reference::Reference},
    Database, PartialSound, SoundParent, User,
};
use revolt_models::v0;
use revolt_permissions::{calculate_server_permissions, ChannelPermission};
use revolt_result::{create_error, Result};
use rocket::{serde::json::Json, State};
use validator::Validate;

/// # Edit Sound
///
/// Edit a soundboard sound by its id.
#[openapi(tag = "Sounds")]
#[patch("/sound/<sound_id>", data = "<data>")]
pub async fn edit_sound(
    db: &State<Database>,
    user: User,
    sound_id: Reference<'_>,
    data: Json<v0::DataEditSound>,
) -> Result<Json<v0::Sound>> {
    let data = data.into_inner();
    data.validate().map_err(|error| {
        create_error!(FailedValidation {
            error: error.to_string()
        })
    })?;

    let mut sound = sound_id.as_sound(db).await?;

    match &sound.parent {
        SoundParent::Server { id } => {
            let server = db.fetch_server(id.as_str()).await?;

            let mut query = DatabasePermissionQuery::new(db, &user).server(&server);
            calculate_server_permissions(&mut query)
                .await
                .throw_if_lacking_channel_permission(ChannelPermission::ManageCustomisation)?;
        }
        SoundParent::Detached => return Err(create_error!(NotAuthenticated)),
    }

    if data.name.is_none() && data.emoji.is_none() && data.volume.is_none() {
        return Ok(Json(sound.into()));
    }

    let partial = PartialSound {
        name: data.name,
        emoji: data.emoji,
        volume: data.volume.map(|volume| volume.min(100)),
    };
    sound.update(db, partial).await?;

    Ok(Json(sound.into()))
}
