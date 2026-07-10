use revolt_config::config;
use revolt_database::{util::permissions::DatabasePermissionQuery, Database, File, Sound, User};
use revolt_models::v0;
use revolt_permissions::{calculate_server_permissions, ChannelPermission};
use revolt_result::{create_error, Result};
use validator::Validate;

use rocket::{serde::json::Json, State};

/// # Create New Sound
///
/// Create a soundboard sound by its Autumn upload id.
#[openapi(tag = "Sounds")]
#[put("/sound/<sound_id>", data = "<data>")]
pub async fn create_sound(
    db: &State<Database>,
    user: User,
    sound_id: String,
    data: Json<v0::DataCreateSound>,
) -> Result<Json<v0::Sound>> {
    let config = config().await;

    let data = data.into_inner();
    data.validate().map_err(|error| {
        create_error!(FailedValidation {
            error: error.to_string()
        })
    })?;

    // Validate we have permission to write into parent
    match &data.parent {
        v0::SoundParent::Server { id } => {
            let server = db.fetch_server(id).await?;

            // Check for permission
            let mut query = DatabasePermissionQuery::new(db, &user).server(&server);
            calculate_server_permissions(&mut query)
                .await
                .throw_if_lacking_channel_permission(ChannelPermission::ManageCustomisation)?;

            // Check that we haven't hit the sound limit
            let sounds = db.fetch_sounds_by_parent_id(&server.id).await?;
            if sounds.len() >= config.features.limits.global.server_sounds {
                return Err(create_error!(TooManySounds {
                    max: config.features.limits.global.server_sounds,
                }));
            }
        }
        v0::SoundParent::Detached => return Err(create_error!(InvalidOperation)),
    };

    // Find the relevant attachment
    File::use_sound(db, &sound_id, &sound_id, &user.id).await?;

    // Create the sound object
    let sound = Sound {
        id: sound_id,
        parent: data.parent.into(),
        creator_id: user.id,
        name: data.name,
        emoji: data.emoji,
        volume: data.volume.unwrap_or(100).min(100),
    };

    // Save sound
    sound.create(db).await?;
    Ok(Json(sound.into()))
}
