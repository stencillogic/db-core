//! Minimal schema-less database management system with ACID guaranties.
//!
//! # Examples
//!
//! ```
//! use db_core::instance::Instance;
//! use db_core::FileState;
//! use db_core::FileType;
//! use db_core::FileDesc;
//! use db_core::instance::Read;
//! use db_core::instance::Write;
//! use db_core::config::ConfigMt;
//! use std::path::Path;
//!
//!
//! //
//! // Database initialization (this is one-time action).
//! // 
//!
//!
//! // Block size for the database.
//! let block_size = 8192;
//!
//!
//! // Create an empty directory for the database files.
//! let dspath = "/tmp/db-core-test-db";
//! if Path::new(dspath).exists() {
//!     std::fs::remove_dir_all(dspath).expect("Failed to delete test dir on cleanup");
//! }
//! std::fs::create_dir(dspath).expect("Failed to create test dir");
//!
//! // Transaction log directory.
//! let log_dir = "/tmp/db-core-test-tranlog";
//! if Path::new(log_dir).exists() {
//!     std::fs::remove_dir_all(log_dir).expect("Failed to delete test dir on cleanup");
//! }
//! std::fs::create_dir(log_dir).expect("Failed to create test dir");
//!
//!
//! // Define initial database files. There should be at least one file of each type (data store,
//! // versioning store, checkpont store). 
//! let mut fdset = vec![];
//! let desc1 = FileDesc {
//!     state:          FileState::InUse,
//!     file_id:        3,
//!     extent_size:    16,
//!     extent_num:     3,
//!     max_extent_num: 65500,
//!     file_type:      FileType::DataStoreFile,
//! };
//! let desc2 = FileDesc {
//!     state:          FileState::InUse,
//!     file_id:        4,
//!     extent_size:    10,
//!     extent_num:     3,
//!     max_extent_num: 65500,
//!     file_type:      FileType::VersioningStoreFile,
//! };
//! let desc3 = FileDesc {
//!     state:          FileState::InUse,
//!     file_id:        5,
//!     extent_size:    10,
//!     extent_num:     3,
//!     max_extent_num: 65500,
//!     file_type:      FileType::CheckpointStoreFile,
//! };
//!
//! fdset.push(desc1);
//! fdset.push(desc2);
//! fdset.push(desc3);
//!
//! // Create a database.
//! Instance::initialize_datastore(dspath, block_size, &fdset).expect("Failed to init datastore");
//!
//!
//! //
//! // Startup and termination, transaction management, read and write data.
//! //
//!
//!
//! // Some random data.
//! let data = b"Hello, world!";
//!
//!
//! // Prepare configuration.
//! let conf = ConfigMt::new();
//! let mut c = conf.get_conf();
//! c.set_log_dir(log_dir.to_owned());
//! c.set_datastore_path(dspath.to_owned());
//! drop(c);
//!
//! // Start instance and open existing database.
//! let instance = Instance::new(conf.clone()).expect("Failed to create instance");
//!
//! // Begin transaction.
//! let mut trn = instance.begin_transaction().expect("Failed to begin transaction");
//!
//! // Create a new object.
//! let file_id = 3;
//! let mut obj = instance.open_create(file_id, &mut trn, data.len()).expect("Failed to create object");
//! let obj_id = obj.get_id();
//!
//! // Write some data.
//! obj.write_next(data).expect("Failed to write");
//! drop(obj);
//!
//! // Commit transaction.
//! instance.commit(trn).expect("Failed to commit");
//!
//! // Begin transaction.
//! let mut trn = instance.begin_transaction().expect("Failed to begin transaction");
//!
//! // Open object for reading and read some data.
//! let mut obj = instance.open_read(&obj_id, &trn).expect("Failed to open for reading");
//! let mut read_buf = vec![0u8;data.len()];
//! let mut read = 0;
//! let len = read_buf.len();
//! while read < len {
//!     let r = obj.read_next(&mut read_buf[read..len]).expect("Failed to read");
//!     if r == 0 {break;}
//!     read += r;
//! }
//! assert_eq!(read_buf, data);
//! drop(obj);
//!
//! // Delete object (if object is in use wait for other transaction to finish for 1 second).
//! let wait_lock_ms = 1000;
//! instance.delete(&obj_id, &mut trn, wait_lock_ms).expect("Failed to delete object");
//!
//! // Rollback transaction.
//! instance.rollback(trn).expect("Failed to rollback");
//!
//! // Spawn another instance in a different thread.
//! let ss = instance.get_shared_state().expect("Failed to get shared state");
//!
//! let th = std::thread::spawn(move || {
//!     let instance2 = Instance::from_shared_state(ss).expect("Failed to create instance");
//!     // ...
//!     instance2.terminate();
//! });
//!
//! // Add a database file.
//! let new_file_id = instance.add_datafile(FileType::DataStoreFile, 1000, 10, 1000).expect("Failed to add data file");
//!
//! // Terminate instance.
//! instance.terminate();
//!
//! ```
//!
//! ### Notes.
//!
//! 1. Block size is defined at the moment of database creation and can't be changed later.
//! 2. Each `file_id` must be unique. `file_id` 0, 1, and 2 are reserved and can't be used.
//!

mod buf_mgr;
mod common;
mod log_mgr;
mod storage;
mod system;
mod tran_mgr;
mod block_mgr;

pub use system::config;
pub use system::instance;
pub use common::errors;

pub use common::defs::ObjectId;
pub use common::defs::SeekFrom;
pub use storage::datastore::FileState;
pub use storage::datastore::FileType;
pub use storage::datastore::FileDesc;

