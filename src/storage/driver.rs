/// Interface for storage

use crate::common::errors::Error;
use crate::common::defs::ObjectId;
use crate::common::defs::Vector;
use crate::common::defs::SeekFrom;
use crate::common::defs::SharedSequences;
use crate::storage::block_driver::BlockStorageDriver;
use crate::storage::block_driver::BlockStorageSharedState;
use crate::storage::block_driver::Cursor;
use crate::storage::block_driver::ReplayState;
use crate::system::config::ConfigMt;
use crate::storage::datastore::FileType;


/// Shared state represents parts that can be sent between threads safely, because StorageDriver
/// may not let send itself directly.
pub struct StorageDriverSharedState {
    ss: BlockStorageSharedState,
}


/// Handle for accessing objects being written or read.
pub struct Handle {
    cursor: Cursor,
}

/// Handle for replaying object changes.
pub struct ReplayHandle {
    rs: ReplayState,
}

impl ReplayHandle {
    pub fn update(&mut self, v: &Vector, tsn: u64, csn: u64) {
        self.rs.update(v, tsn, csn);
    }
}


/// Storage driver is abstration for providing access to data storage.
/// It serves as wrapper for actual implementation, e.g. block store.
pub struct StorageDriver {
    driver: BlockStorageDriver,
}


impl<'b> StorageDriver {

    pub fn new(conf: ConfigMt, csns: SharedSequences) -> Result<Self, Error> {

        let driver = BlockStorageDriver::new(conf.clone(), csns.clone())?;
        Ok(StorageDriver{
            driver,
        })
    }

    /// Build instance of storage driver using shared state.
    pub fn from_shared_state(ss: StorageDriverSharedState) -> Result<Self, Error> {
        Ok(StorageDriver { 
            driver: BlockStorageDriver::from_shared_state(ss.ss)?,
        })
    }

    pub fn get_shared_state(&self) -> Result<StorageDriverSharedState, Error> {
        Ok(StorageDriverSharedState {
            ss: self.driver.get_shared_state()?,
        })
    }

    /// Return tsn if object is currently being locked / written by other transaction.
    pub fn is_locked(&self, obj_id: &ObjectId) -> Result<Option<u64>, Error> {
        self.driver.is_locked(obj_id)
    }

    /// Create a new object.
    pub fn create(&self, file_id: u16, tsn: u64, csn: u64, initial_size: usize) -> Result<(ObjectId, Handle), Error> {
        let (o, c) = self.driver.create(file_id, tsn, csn, initial_size)?;
        Ok((o, Handle {cursor: c}))
    }

    /// Delete existing object.
    pub fn delete(&self, obj_id: &ObjectId, tsn: u64, csn: u64) -> Result<u64, Error>  {
        self.driver.delete(obj_id, tsn, csn)
    }

    /// Begin writing to an existing object.
    pub fn begin_write(&self, obj_id: &ObjectId, tsn: u64, csn: u64) -> Result<Handle, Error> {
        let cursor = self.driver.begin_write(obj_id, tsn, csn)?;
        Ok(Handle {
            cursor,
        })
    }

    /// Begin reading an existing object.
    pub fn begin_read(&self, obj_id: &ObjectId, tsn: u64, csn: u64) -> Result<Handle, Error> {
        let cursor = self.driver.begin_read(obj_id, tsn, csn)?;
        Ok(Handle {
            cursor,
        })
    }

    /// Read data from an object opened for read.
    pub fn read(&self, h: &mut Handle, buf: &mut [u8]) -> Result<usize, Error> {
        self.driver.read(&mut h.cursor, buf)
    }

    /// Write data to object opened for write.
    pub fn write(&'b self, h: &'b mut Handle, data: &[u8]) -> Result<(Vector, usize, u64), Error> {
        self.driver.write(&mut h.cursor, data)
    }

    /// seek to a certain position in an opened object.
    pub fn seek(&self, h: &mut Handle, from: SeekFrom, pos: u64, obj_id: &ObjectId) -> Result<u64, Error> {
        self.driver.seek(&mut h.cursor, from, pos, obj_id)
    }

    /// Rollback changes made by a transaction.
    pub fn rollback_transaction(&self, tsn: u64) -> Result<(), Error> {
        self.driver.rollback_transaction(tsn)
    }

    /// Perform checkpoint. See concrete implementation for details.
    pub fn checkpoint(&self, checkpoint_csn: u64) -> Result<(), Error> {
        self.driver.checkpoint(checkpoint_csn)
    }

    /// Restore state of storage to as of last checkpoint.
    pub fn restore_checkpoint(&self, checkpoint_csn: u64) -> Result<(), Error> {
        self.driver.restore_checkpoint(checkpoint_csn)
    }

    /// Perform transaction finalization in the storage regardless of commit or rollback.
    pub fn finish_tran(&self, tsn: u64) {
        self.driver.finish_tran(tsn)
    }

    pub fn terminate(self) {
        self.driver.terminate();
    }

    /// Add a new file to datastore.
    pub fn add_datafile(&self, file_type: FileType, extent_size: u16, extent_num: u16, max_extent_num: u16) -> Result<u16, Error> {
        self.driver.add_datafile(file_type, extent_size, extent_num, max_extent_num)
    }

    /// Begin replay of an object.
    pub fn begin_replay(&self, obj: &ObjectId, entry_pos: u16, tsn: u64, csn: u64) -> ReplayHandle {
        ReplayHandle {
            rs: ReplayState::new(obj, entry_pos, tsn, csn)
        }
    }

    /// Replay changes from the log.
    pub fn replay(&self, rh: &mut ReplayHandle, data: &[u8]) -> Result<(), Error> {
        self.driver.replay(&mut rh.rs, data)
    } 
}

