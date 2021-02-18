//! client api.
//!
//! Clients can
//!   - start and stop instance
//!   - open and close database
//!   - start, commit or rollback transaction
//!   - create, read, and write single object, or scan through a set of objects
//!   - set configuration options
//!

mod buf_mgr;
mod common;
mod log_mgr;
mod storage;
mod system;
mod tran_mgr;
mod block_mgr;

pub use common::errors::Error;
pub use common::defs::ObjectId;
pub use system::instance::Instance;
pub use system::instance::Object;
pub use system::instance::ObjectWrite;
pub use system::instance::Read;
pub use system::instance::Write;
pub use system::instance::Transaction;

