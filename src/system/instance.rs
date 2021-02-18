/// Interface to the instance.


use crate::common::errors::Error;
use crate::common::errors::ErrorKind;
use crate::common::defs::Sequence;
use crate::common::defs::ObjectId;
use crate::common::defs::SharedSequences;
use crate::tran_mgr::tran_mgr::TranMgr;
use crate::log_mgr::log_mgr::RecType;
use crate::log_mgr::log_mgr::LogMgr;
use crate::log_mgr::log_mgr::LogReader;
use crate::system::config::ConfigMt;
use crate::system::checkpointer::Checkpointer;
use crate::storage::driver::StorageDriver;
use crate::storage::driver::Handle;
use crate::storage::driver::StorageDriverSharedState;
use crate::storage::datastore::FileType;
use crate::storage::datastore::FileDesc;
use crate::storage::datastore::DataStore;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;


/// Instance provides interface for the client to interact with the system: initiate and complete
/// transaction, write and read data, etc.
pub struct Instance {
    conf:               ConfigMt,
    csns:               SharedSequences,
    tran_mgr:           TranMgr,
    log_mgr:            LogMgr,
    storage_driver:     StorageDriver,
    checkpointer:       Arc<Checkpointer>,
}

/// System instance
impl Instance {

    pub fn new(conf: ConfigMt) -> Result<Instance, Error> {

        let tran_mgr =          TranMgr::new(conf.clone())?;
        let log_mgr =           LogMgr::new(conf.clone())?;

        let csn =               Sequence::new(log_mgr.starting_csn());
        let latest_commit_csn = Arc::new(AtomicU64::new(log_mgr.latest_commit_csn()));
        let checkpoint_csn =    Sequence::new(0);
        let csns = SharedSequences {
                csn,
                latest_commit_csn,
                checkpoint_csn,
            };

        let storage_driver =    StorageDriver::new(conf.clone(), csns.clone())?;

        let checkpointer =      Arc::new(Checkpointer::new(log_mgr.clone(), 
                                                           csns.clone(),
                                                           conf.clone())?);

        let ret = Instance {
            conf,
            csns,
            tran_mgr,
            log_mgr,
            storage_driver,
            checkpointer,
        };

        ret.restore_state()?;

        Ok(ret)
    }

    /// Initialize datastore: create data, checkpoint, versioning store files, and lock file.
    pub fn initialize_datastore(path: &str, block_size: usize, desc_set: &[FileDesc]) -> Result<(), Error> {
        DataStore::initialize_datastore(path, block_size, desc_set)
    }

    /// Add a new file to datastore.
    pub fn add_datafile(&self, file_type: FileType, extent_size: u16, extent_num: u16, max_extent_num: u16) -> Result<(), Error> {
        self.storage_driver.add_datafile(file_type, extent_size, extent_num, max_extent_num)
    }

    /// begin a new transaction
    /// return tns of newely started transaction on success
    pub fn begin_transaction(&self) -> Result<Transaction, Error> {
        let csn = self.csns.csn.get_cur();
        let tsn = self.tran_mgr.start_tran();

        Ok(Transaction {
            instance:   &self,
            tsn,
            start_csn:  csn,
            last_write_csn:  csn,
            read_committed_csn: self.csns.latest_commit_csn.load(Ordering::Relaxed),
        })
    }

    /// commit transaction
    pub fn commit(self, t: Transaction) -> Result<(), Error> {
        if t.last_write_csn > t.start_csn {
            let commit_csn = self.csns.csn.get_next();
            self.log_mgr.write_commit(commit_csn, t.tsn)?;
            self.update_latest_commit_csn(commit_csn);
        }
        self.storage_driver.finish_tran(t.tsn);
        self.tran_mgr.delete_tran(t.tsn);
        Ok(())
    }

    /// rollback transaction
    pub fn rollback(self, t: Transaction) -> Result<(), Error> {
        if t.last_write_csn > t.start_csn {
            self.log_mgr.write_rollback(t.last_write_csn, t.tsn)?;
            self.storage_driver.rollback_transaction(t.tsn)?;
        }
        self.storage_driver.finish_tran(t.tsn);
        self.tran_mgr.delete_tran(t.tsn);
        Ok(())
    }

    /// open an existing object for read
    /// after object is opened it is possible to read and seek through object data
    pub fn open_read(&self, obj_id: &ObjectId, t: &Transaction) -> Result<Object, Error> {
        let handle = self.storage_driver.begin_read(obj_id, t.tsn, t.read_committed_csn)?;
        Ok(Object {
            id: *obj_id,
            instance: &self,
            handle,
            cur_pos: 0,
        })
    }

    /// open an existing object for modification by its id
    /// after object is opened it is possible to read, write and seek through object data
    /// operation puts lock on the object which is released after commit or rollback.
    /// if timeout is -1 then wait indefinitely, otherwise wait for requested time in ms before
    /// returning error.
    pub fn open_write<'a>(&'a self, obj_id: &ObjectId, t: &'a mut Transaction, timeout: i64) -> Result<ObjectWrite, Error> {

        let _guard = self.tran_mgr.lock_object(t.tsn, obj_id);

        if let Some(txn) = self.storage_driver.is_locked(obj_id)? {
            if !self.tran_mgr.wait_for(txn, timeout) {
                return Err(Error::timeout());
            }
        }

        t.last_write_csn = self.csns.csn.get_next();

        let handle = self.storage_driver.begin_write(obj_id, t.tsn, t.last_write_csn)?;

        Ok(ObjectWrite {
            obj: Object {
                id: *obj_id,
                instance: &self,
                handle,
                cur_pos: 0,
            },
            txn: t,
        })
    }

    /// create a new object and open it for write
    /// after object is opened it is possible to read, write and seek object data
    /// operation puts lock on the object which is released after commit or rollback
    pub fn open_create<'a>(&'a self, t: &'a mut Transaction, initial_size: usize) -> Result<ObjectWrite, Error> {

        t.last_write_csn = self.csns.csn.get_next();
        let (id, handle) = self.storage_driver.create(t.tsn, t.last_write_csn, initial_size)?;

        Ok(ObjectWrite {
            obj: Object {
                id,
                instance: &self,
                handle,
                cur_pos: 0,
            },
            txn: t,
        })
    }

    /// Delete object
    pub fn delete(&self, obj_id: &ObjectId, t: &mut Transaction, timeout: i64) -> Result<(), Error> {

        let _guard = self.tran_mgr.lock_object(t.tsn, obj_id);

        if let Some(txn) = self.storage_driver.is_locked(obj_id)? {
            self.tran_mgr.wait_for(txn, timeout);
        }

        t.last_write_csn = self.csns.csn.get_next();
        let checkpoint_csn = self.storage_driver.delete(obj_id, t.tsn, t.last_write_csn)?;
        self.log_mgr.write_delete(t.last_write_csn, checkpoint_csn, t.tsn, &obj_id)
    }

    fn update_latest_commit_csn(&self, csn: u64) {
        let mut cur_csn = self.csns.latest_commit_csn.load(Ordering::Relaxed);
        while cur_csn < csn {
            cur_csn = self.csns.latest_commit_csn.compare_and_swap(cur_csn, csn, Ordering::Relaxed);
        }
    }

    fn restore_state(&self) -> Result<(), Error> {

        let mut log_reader = self.log_mgr.get_reader()?;
        let checkpoint_csn = log_reader.seek_to_latest_checkpoint()?;

        self.csns.checkpoint_csn.set(checkpoint_csn);

        self.storage_driver.restore_checkpoint(checkpoint_csn)?;

        let tsn = self.replay_changes(log_reader)?;

        self.tran_mgr.set_tsn(tsn);

        Ok(())
    }

    fn replay_changes(&self, mut log_reader: LogReader) -> Result<u64, Error> {
        let mut trn_set = HashMap::new();
        let mut max_tsn = 0;

        while let Some(lrh) = log_reader.read_next()? {

            let tsn = lrh.tsn;
            let csn = lrh.csn;

            if tsn > max_tsn {
                max_tsn = tsn;
            }

            match lrh.rec_type {
                RecType::Commit => {
                    trn_set.remove(&tsn);
                    self.update_latest_commit_csn(csn);
                },
                RecType::Rollback => {
                    self.storage_driver.rollback_transaction(lrh.tsn)?;
                    trn_set.remove(&tsn);
                },
                RecType::Data => {
                    if !trn_set.contains_key(&tsn) {
                        trn_set.insert(tsn, HashMap::new());
                    }

                    let obj_set = trn_set.get_mut(&tsn).unwrap();

                    let obj = log_reader.get_object_id();
                    if !obj_set.contains_key(&obj) {
                        let (_, handle) = match self.storage_driver.begin_write(&obj, tsn, csn) {
                            Ok(handle) => Ok((obj, handle)),
                            Err(e) => {
                                if e.kind() == ErrorKind::ObjectDoesNotExist {
                                    self.storage_driver.create(tsn, csn, log_reader.get_data().len())
                                } else {
                                    Err(e)
                                }
                            }
                        }?;

                        obj_set.insert(obj, (handle, 0));
                    }

                    let mut obj_state = obj_set.get_mut(&obj).unwrap();

                    let write_pos = log_reader.get_data_pos();
                    if write_pos > obj_state.1 {
                        self.storage_driver.seek(&mut obj_state.0, write_pos - obj_state.1)?;
                        obj_state.1 = write_pos;
                    }

                    self.write_data_fully(&mut obj_state.0, log_reader.get_data())?;
                    obj_state.1 += log_reader.get_data().len() as u64;
                },
                RecType::Delete => {
                    if !trn_set.contains_key(&tsn) {
                        trn_set.insert(tsn, HashMap::new());
                    }

                    self.storage_driver.delete(&log_reader.get_object_id(), tsn, csn)?;
                },
                RecType::CheckpointBegin => {
                    return Err(Error::unexpected_checkpoint());
                },
                RecType::CheckpointCompleted => {
                    return Err(Error::unexpected_checkpoint());
                },
                RecType::Unspecified => {
                },
            }
        }

        for tsn in trn_set.keys() {
            self.storage_driver.rollback_transaction(*tsn)?;
        }

        Ok(max_tsn)
    }

    fn write_data_fully(&self, mut handle: &mut Handle, data: &[u8]) -> Result<(), Error> {

        let mut written = 0;

        while written < data.len() {
            let (w, _) = self.storage_driver.write(&mut handle, &data[written..])?;
            written += w;
        }

        Ok(())
    }

    /// Build instance using shared state.
    pub fn from_shared_state(ss: InstanceSharedState) -> Result<Self, Error> {

        let InstanceSharedState {
            conf,
            csns,
            tran_mgr,
            log_mgr,
            checkpointer,
            storage_ss,
        } = ss;

        let storage_driver =    StorageDriver::from_shared_state(storage_ss)?;

        Ok(Instance {
            conf,
            csns,
            tran_mgr,
            log_mgr,
            storage_driver,
            checkpointer,
        })
    }

    /// Return shared state that can be shared between threads.
    pub fn get_shared_state(&self) -> Result<InstanceSharedState, Error> {
        Ok(InstanceSharedState {
            conf:           self.conf.clone(),
            csns:           self.csns.clone(),
            tran_mgr:       self.tran_mgr.clone(),
            log_mgr:        self.log_mgr.clone(),
            checkpointer:   self.checkpointer.clone(),
            storage_ss:     self.storage_driver.get_shared_state()?,
        })
    }

    /// Terminate the instance. If several instances are running and sharing state then function
    /// has no effect for any of them but the last one.
    /// It is up to client to make sure all transactions are finished, otherwise unfinished
    /// transactions will be rolled back on the next start.
    pub fn terminate(self) {
        let Instance {
            conf: _,
            csns: _,
            tran_mgr: _,
            log_mgr,
            storage_driver,
            checkpointer
        } = self;

        storage_driver.terminate();

        log_mgr.terminate();

        if let Ok(checkpointer) = Arc::try_unwrap(checkpointer) {
            checkpointer.terminate();
        }
    }

}

/// Transaction
pub struct Transaction<'a> {
    instance: &'a Instance,
    tsn: u64,
    start_csn: u64,
    last_write_csn: u64,
    read_committed_csn: u64,
}

impl<'a> Transaction<'a> {

    pub fn update_read_csn(&mut self) {
        self.read_committed_csn = self.instance.csns.latest_commit_csn.load(Ordering::Relaxed);
    }
}

pub struct Object<'a> {
    id:         ObjectId,
    instance:   &'a Instance,
    handle:     Handle,
    cur_pos:    u64,
}


impl<'a> Object<'a> {

    /// seek to certain position in opened object
    fn seek(&mut self, pos: u64) -> Result<u64, Error> {
        let res = self.instance.storage_driver.seek(&mut self.handle, pos)?;
        self.cur_pos += res;
        Ok(res)
    }

    /// get chunk of data for reading
    /// after sucessfull call slice of the data bytes is returned
    /// if no data available, slice is empty
    fn read_next(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        self.instance.storage_driver.read(&mut self.handle, buf)
    }

    fn get_id(&self) -> ObjectId {
        self.id
    }

}


pub struct ObjectWrite<'a> {
    obj: Object<'a>,
    txn: &'a Transaction<'a>,
}

impl<'a> ObjectWrite<'a> {

    fn write_next(&mut self, data: &[u8]) -> Result<(), Error> {

        let mut written = 0;

        while written < data.len() {
            let (w, checkpoint_csn) = self.obj.instance.storage_driver.write(&mut self.obj.handle, &data[written..])?;
            written += w;
            self.obj.instance.log_mgr.write_data(self.txn.last_write_csn, checkpoint_csn, self.txn.tsn, &self.obj.id, self.obj.cur_pos, data)?;
            self.obj.instance.checkpointer.register_processed_data_size(data.len() as u64);
        }

        self.obj.cur_pos += data.len() as u64;

        Ok(())
    }
}


pub trait Read {

    fn get_id(&self) -> ObjectId;

    fn seek(&mut self, pos: u64) -> Result<u64, Error>;

    fn read_next(&mut self, buf: &mut [u8]) -> Result<usize, Error>;
}

pub trait Write {

    fn write_next(&mut self, data: &[u8]) -> Result<(), Error>;
}


impl<'a> Write for ObjectWrite<'a> {

    fn write_next(&mut self, data: &[u8]) -> Result<(), Error> {
        self.write_next(data)
    }
}

impl<'a> Read for ObjectWrite<'a> {

    fn seek(&mut self, pos: u64) -> Result<u64, Error> {
        self.obj.seek(pos)
    }

    fn read_next(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        self.obj.read_next(buf)
    }

    fn get_id(&self) -> ObjectId {
        self.obj.get_id()
    }
}

impl<'a> Read for Object<'a> {

    fn seek(&mut self, pos: u64) -> Result<u64, Error> {
        self.seek(pos)
    }

    fn read_next(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        self.read_next(buf)
    }

    fn get_id(&self) -> ObjectId {
        self.get_id()
    }
}


/// Parts of instance that are shared between threads.
pub struct InstanceSharedState {
    conf:           ConfigMt,
    csns:           SharedSequences,
    tran_mgr:       TranMgr,
    log_mgr:        LogMgr,
    checkpointer:   Arc<Checkpointer>,
    storage_ss:     StorageDriverSharedState, 
}


#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
/*
    #[test]
    fn test_create_and_destroy() {
        let conf = ConfigMt::new();
        let instance = Instance::new(conf).expect("Failed to create instance");
    }


    #[test]
    fn test_send_to_thrread() {
        let conf = ConfigMt::new();
        let instance = Instance::new(conf).expect("Failed to create instance");

        let ss = instance.get_shared_state().expect("Failed to get shared state");

        let th = std::thread::spawn(move || {
            let instance2 = Instance::from_shared_state(ss).expect("Failed to create instance");
            let trn = instance2.begin_transaction().expect("Failed to begin transaction 1");
            instance2.rollback(trn);
        });

        let trn = instance.begin_transaction().expect("Failed to begin transaction 2");
        instance.rollback(trn);

        th.join();
    }
*/
}
