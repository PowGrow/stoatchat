use std::collections::HashSet;

use revolt_database::events::client::EventV1;

/// Journal-backed events in V2. Private call handoffs still use pubsub.
pub fn is_journal_event(event: &EventV1) -> bool {
    matches!(
        event,
        EventV1::VoiceChannelJoin { .. }
            | EventV1::VoiceChannelLeave { .. }
            | EventV1::VoiceChannelMove { .. }
            | EventV1::UserVoiceStateUpdate { .. }
    )
}

/// A move reaches both sides, but only reveals endpoints visible to the socket.
pub fn for_subscriptions(event: EventV1, subscribed: &HashSet<String>) -> Option<EventV1> {
    match event {
        EventV1::VoiceChannelJoin { ref id, .. } | EventV1::VoiceChannelLeave { ref id, .. } => {
            subscribed.contains(id).then_some(event)
        }
        EventV1::UserVoiceStateUpdate { ref channel_id, .. } => {
            subscribed.contains(channel_id).then_some(event)
        }
        EventV1::VoiceChannelMove {
            user,
            from,
            to,
            state,
        } => match (subscribed.contains(&from), subscribed.contains(&to)) {
            (true, true) => Some(EventV1::VoiceChannelMove {
                user,
                from,
                to,
                state,
            }),
            (true, false) => Some(EventV1::VoiceChannelLeave { id: from, user }),
            (false, true) => Some(EventV1::VoiceChannelJoin { id: to, state }),
            (false, false) => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use revolt_models::v0::UserVoiceState;

    #[test]
    fn moves_only_expose_subscribed_channels() {
        let state: UserVoiceState = serde_json::from_value(serde_json::json!({
            "id": "user", "joined_at": "2026-11-18T00:00:00.000Z",
            "is_receiving": true, "is_publishing": false, "screensharing": false, "camera": false
        }))
        .unwrap();
        let movement = || EventV1::VoiceChannelMove {
            user: "user".into(),
            from: "old".into(),
            to: "new".into(),
            state: state.clone(),
        };

        assert!(matches!(
            for_subscriptions(movement(), &HashSet::from(["old".into()])),
            Some(EventV1::VoiceChannelLeave { id, user }) if id == "old" && user == "user"
        ));
        assert!(matches!(
            for_subscriptions(movement(), &HashSet::from(["new".into()])),
            Some(EventV1::VoiceChannelJoin { id, state }) if id == "new" && state.id == "user"
        ));
        assert!(matches!(
            for_subscriptions(movement(), &HashSet::from(["old".into(), "new".into()])),
            Some(EventV1::VoiceChannelMove { .. })
        ));
        assert!(for_subscriptions(movement(), &HashSet::new()).is_none());
    }
}
