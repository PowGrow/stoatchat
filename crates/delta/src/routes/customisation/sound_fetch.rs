use revolt_database::{util::reference::Reference, Database, User};
use revolt_models::v0;
use revolt_result::Result;
use rocket::{serde::json::Json, State};

/// # Fetch Sound
///
/// Fetch a soundboard sound by its id.
#[openapi(tag = "Sounds")]
#[get("/sound/<sound_id>")]
pub async fn fetch_sound(
    db: &State<Database>,
    _user: User,
    sound_id: Reference<'_>,
) -> Result<Json<v0::Sound>> {
    sound_id.as_sound(db).await.map(|sound| Json(sound.into()))
}
