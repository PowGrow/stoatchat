use revolt_database::{
    util::{permissions::DatabasePermissionQuery, reference::Reference},
    Database, SoundParent, User,
};
use revolt_permissions::{calculate_server_permissions, ChannelPermission};
use revolt_result::Result;

use rocket::State;
use rocket_empty::EmptyResponse;

/// # Delete Sound
///
/// Delete a soundboard sound by its id.
#[openapi(tag = "Sounds")]
#[delete("/sound/<sound_id>")]
pub async fn delete_sound(
    db: &State<Database>,
    user: User,
    sound_id: Reference<'_>,
) -> Result<EmptyResponse> {
    // Fetch the sound
    let sound = sound_id.as_sound(db).await?;

    // If we uploaded the sound, then we have permission to delete it
    if sound.creator_id != user.id {
        // Otherwise, validate we have permission to delete from parent
        match &sound.parent {
            SoundParent::Server { id } => {
                let server = db.fetch_server(id.as_str()).await?;

                // Check for permission
                let mut query = DatabasePermissionQuery::new(db, &user).server(&server);
                calculate_server_permissions(&mut query)
                    .await
                    .throw_if_lacking_channel_permission(ChannelPermission::ManageCustomisation)?;
            }
            SoundParent::Detached => return Ok(EmptyResponse),
        };
    }

    // Delete the sound
    sound.delete(db).await.map(|_| EmptyResponse)
}
