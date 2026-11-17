use std::env;

use revolt_database::DatabaseInfo;
use revolt_database::{voice::VoiceClient, AMQP};
use revolt_result::Result;
use rocket::{build, routes, Config};
use std::net::Ipv4Addr;

mod api;
mod guard;

struct VersionedPresence(bool);

#[rocket::main]
async fn main() -> Result<(), rocket::Error> {
    revolt_config::configure!(voice_ingress);

    let amqp = AMQP::new_auto().await;

    let database = DatabaseInfo::Auto.connect().await.unwrap();
    let voice_client = VoiceClient::from_revolt_config().await;
    // The published image keeps the legacy writer until the coordinated Redis cutover.
    let versioned_presence =
        VersionedPresence(env::var("STOAT_VOICE_PRESENCE_V2").as_deref() == Ok("1"));

    let _rocket = build()
        .manage(database)
        .manage(voice_client)
        .manage(versioned_presence)
        .manage(amqp)
        .mount("/", routes![api::ingress])
        .configure(Config {
            port: 8500,
            address: Ipv4Addr::new(0, 0, 0, 0).into(),
            ..Default::default()
        })
        .ignite()
        .await?
        .launch()
        .await?;

    Ok(())
}
