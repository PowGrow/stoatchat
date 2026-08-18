use revolt_rocket_okapi::revolt_okapi::openapi3::OpenApi;
use rocket::Route;

// Routes are taken from rocket_authifier as-is, except for create_account:
// upstream never checks whether an invite has already been claimed, so we
// ship our own copy of that one route.
use rocket_authifier::routes::account::{
    change_email, change_password, confirm_deletion, delete_account, disable_account,
    fetch_account, password_reset, resend_verification, send_password_reset, verify_email,
};

pub mod create_account;

pub fn routes() -> (Vec<Route>, OpenApi) {
    openapi_get_routes_spec![
        create_account::create_account,
        resend_verification::resend_verification,
        confirm_deletion::confirm_deletion,
        fetch_account::fetch_account,
        delete_account::delete_account,
        disable_account::disable_account,
        change_password::change_password,
        change_email::change_email,
        verify_email::verify_email,
        password_reset::password_reset,
        send_password_reset::send_password_reset
    ]
}
