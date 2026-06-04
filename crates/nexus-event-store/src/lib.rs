#![deny(clippy::disallowed_types)]

pub mod store;
pub mod sqlite;
pub mod postgres;
pub mod rows;

pub use store::{EventStore, StoreError};
pub use sqlite::SqliteEventStore;
pub use postgres::PostgresEventStore;
