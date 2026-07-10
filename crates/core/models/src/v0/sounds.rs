#[cfg(feature = "validator")]
use validator::Validate;

auto_derived!(
    /// Soundboard sound
    pub struct Sound {
        /// Unique Id
        #[cfg_attr(feature = "serde", serde(rename = "_id"))]
        pub id: String,
        /// What owns this sound
        pub parent: SoundParent,
        /// Uploader user id
        pub creator_id: String,
        /// Sound name
        pub name: String,
        /// Related emoji (unicode emoji or custom emoji id)
        #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
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

    /// Create a new sound
    #[cfg_attr(feature = "validator", derive(Validate))]
    pub struct DataCreateSound {
        /// Sound name
        #[cfg_attr(feature = "validator", validate(length(min = 1, max = 32)))]
        pub name: String,
        /// Parent information
        pub parent: SoundParent,
        /// Related emoji
        #[cfg_attr(feature = "validator", validate(length(min = 1, max = 32)))]
        pub emoji: Option<String>,
        /// Default playback volume in percent (0 to 100)
        #[cfg_attr(feature = "validator", validate(range(min = 0, max = 100)))]
        pub volume: Option<u32>,
    }

    /// Partial sound representation
    #[derive(Default)]
    pub struct PartialSound {
        #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
        pub name: Option<String>,
        #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
        pub emoji: Option<String>,
        #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
        pub volume: Option<u32>,
    }

    /// Edit sound information
    #[cfg_attr(feature = "validator", derive(Validate))]
    pub struct DataEditSound {
        /// Sound name
        #[cfg_attr(feature = "validator", validate(length(min = 1, max = 32)))]
        pub name: Option<String>,
        /// Related emoji (empty string to remove)
        #[cfg_attr(feature = "validator", validate(length(max = 32)))]
        pub emoji: Option<String>,
        /// Default playback volume in percent (0 to 100)
        #[cfg_attr(feature = "validator", validate(range(min = 0, max = 100)))]
        pub volume: Option<u32>,
    }
);
