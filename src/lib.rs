//! kastrup as a library: the kastrup program is `app::run`, and gluon
//! uses the same code.

pub mod app;
pub mod chat_send;
pub mod completion_ipc;
pub mod config;
pub mod database;
pub mod email_send;
pub mod feeder;
pub mod log;
pub mod mailfile;
pub mod message;
pub mod organizer;
pub mod poller;
pub mod read_sync;
pub mod source;
pub mod sources;
pub mod triage;

// Helpers the other modules call at the crate root.
pub(crate) use app::{days_to_ymd, home_dir};
