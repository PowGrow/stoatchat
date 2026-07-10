use revolt_models::v0;
use revolt_result::Result;

use crate::events::client::EventV1;
use crate::Database;

auto_derived!(
    /// Soundboard sound
    pub struct Sound {
        /// Unique Id
        #[serde(rename = "_id")]
        pub id: String,
        /// What owns this sound
        pub parent: SoundParent,
        /// Uploader user id
        pub creator_id: String,
        /// Sound name
        pub name: String,
        /// Related emoji (unicode emoji or custom emoji id)
        #[serde(skip_serializing_if = "Option::is_none")]
        pub emoji: Option<String>,
        /// Default playback volume in percent (0 to 100)
        pub volume: u32,
    }

    /// Parent Id of the sound
    #[serde(tag = "type")]
    pub enum SoundParent {
        Server { id: String },
        Detached,
    }

    /// Partial representation of a sound
    pub struct PartialSound {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub emoji: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub volume: Option<u32>,
    }
);

#[allow(clippy::disallowed_methods)]
impl Sound {
    /// Get parent id
    fn parent(&self) -> &str {
        match &self.parent {
            SoundParent::Server { id } => id,
            SoundParent::Detached => "",
        }
    }

    /// Create a sound
    pub async fn create(&self, db: &Database) -> Result<()> {
        db.insert_sound(self).await?;

        EventV1::SoundCreate(self.clone().into())
            .p(self.parent().to_string())
            .await;

        Ok(())
    }

    /// Delete a sound
    pub async fn delete(self, db: &Database) -> Result<()> {
        EventV1::SoundDelete {
            id: self.id.to_string(),
        }
        .p(self.parent().to_string())
        .await;

        db.detach_sound(&self).await
    }

    /// Update a sound
    pub async fn update(&mut self, db: &Database, partial: PartialSound) -> Result<()> {
        if let Some(name) = partial.name.clone() {
            self.name = name;
        }

        if let Some(emoji) = partial.emoji.clone() {
            self.emoji = if emoji.is_empty() { None } else { Some(emoji) };
        }

        if let Some(volume) = partial.volume {
            self.volume = volume;
        }

        db.update_sound(&self.id, &partial).await?;

        EventV1::SoundUpdate {
            id: self.id.clone(),
            data: v0::PartialSound {
                name: partial.name.clone(),
                emoji: partial.emoji.clone(),
                volume: partial.volume,
            },
        }
        .p(self.parent().to_string())
        .await;

        Ok(())
    }
}
