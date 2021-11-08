/// Interface to the instance.


use crate::common::errors::Error;
use crate::common::defs::Sequence;
use crate::common::defs::ObjectId;
use crate::common::defs::SeekFrom;
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
        let checkpoint_csn =    Sequence::new(1);
        let csns = SharedSequences {
                csn,
                latest_commit_csn,
                checkpoint_csn,
            };

        let storage_driver =    StorageDriver::new(conf.clone(), csns.clone())?;

        let checkpointer =      Arc::new(Checkpointer::new(log_mgr.clone(), 
                                                           csns.clone(),
                                                           conf.clone(),
                                                           tran_mgr.clone())?);

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

    /// Add a new file to datastore and return it's file_id.
    pub fn add_datafile(&self, file_type: FileType, extent_size: u16, extent_num: u16, max_extent_num: u16) -> Result<u16, Error> {
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
    pub fn commit(&self, t: Transaction) -> Result<(), Error> {
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
    pub fn rollback(&self, t: Transaction) -> Result<(), Error> {
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

        let guard = self.tran_mgr.lock_object(t.tsn, obj_id);

        if let Some(txn) = self.storage_driver.is_locked(obj_id)? {
            if txn != t.tsn {
                if !self.tran_mgr.wait_for(txn, timeout) {
                    return Err(Error::timeout());
                }
            }
        }

        t.last_write_csn = self.csns.csn.get_next();

        let handle = self.storage_driver.begin_write(obj_id, t.tsn, t.last_write_csn)?;

        drop(guard);

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
    pub fn open_create<'a>(&'a self, file_id: u16, t: &'a mut Transaction, initial_size: usize) -> Result<ObjectWrite, Error> {

        t.last_write_csn = self.csns.csn.get_next();
        let (id, handle) = self.storage_driver.create(file_id, t.tsn, t.last_write_csn, initial_size)?;

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

        let guard = self.tran_mgr.lock_object(t.tsn, obj_id);

        if let Some(txn) = self.storage_driver.is_locked(obj_id)? {
            if txn != t.tsn {
                if !self.tran_mgr.wait_for(txn, timeout) {
                    return Err(Error::timeout());
                }
            }
        }

        t.last_write_csn = self.csns.csn.get_next();

        let checkpoint_csn = self.storage_driver.delete(obj_id, t.tsn, t.last_write_csn)?;

        drop(guard);

        self.log_mgr.write_delete(t.last_write_csn, checkpoint_csn, t.tsn, &obj_id)
    }

    fn update_latest_commit_csn(&self, csn: u64) {
        let cur_csn = self.csns.latest_commit_csn.load(Ordering::Relaxed);
        while let Err(cur_csn)  = self.csns.latest_commit_csn.compare_exchange(cur_csn, csn, Ordering::Relaxed, Ordering::Relaxed) {
            if cur_csn >= csn {
                return
            }
        }
    }

    fn restore_state(&self) -> Result<(), Error> {

        let mut log_reader = self.log_mgr.get_reader()?;
        if let Some((checkpoint_csn, tsn)) = log_reader.seek_to_latest_checkpoint()? {

            self.tran_mgr.set_tsn(tsn);
            self.csns.checkpoint_csn.set(checkpoint_csn);
            self.storage_driver.restore_checkpoint(checkpoint_csn)?;
        } else {
            log_reader = self.log_mgr.get_reader()?;
        }

        let tsn = self.replay_changes(log_reader)?;
        self.tran_mgr.set_tsn(tsn);

        Ok(())
    }

    fn replay_changes(&self, mut log_reader: LogReader) -> Result<u64, Error> {
        let mut trn_set = HashMap::new();
        let mut max_tsn = 1;

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
                    self.storage_driver.finish_tran(tsn);
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
                        let vec = log_reader.get_vector();
                        let rh = self.storage_driver.begin_replay(&vec.obj_id(), vec.entry_pos(), tsn, csn);
                        obj_set.insert(obj, rh);
                    }

                    let rh = obj_set.get_mut(&obj).unwrap();
                    let vec = log_reader.get_vector();
                    rh.update(&vec, tsn, csn);
                    self.storage_driver.replay(rh, log_reader.get_data())?;
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

    /// seek to certain position in an opened object.
    fn seek(&mut self, from: SeekFrom, pos: u64) -> Result<u64, Error> {
        let res = self.instance.storage_driver.seek(&mut self.handle, from, pos, &self.id)?;
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
            let (mut vector, w, checkpoint_csn) = self.obj.instance.storage_driver.write(&mut self.obj.handle, &data[written..])?;
            let written_data = &data[written..written+w];
            self.obj.instance.log_mgr.write_data(self.txn.last_write_csn, checkpoint_csn, self.txn.tsn, &self.obj.id, &mut vector, written_data)?;
            self.obj.instance.checkpointer.register_processed_data_size(written_data.len() as u64);
            written += w;
        }

        self.obj.cur_pos += data.len() as u64;

        Ok(())
    }
}


pub trait Read {

    fn get_id(&self) -> ObjectId;

    fn seek(&mut self, from: SeekFrom, pos: u64) -> Result<u64, Error>;

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

    fn seek(&mut self, from: SeekFrom, pos: u64) -> Result<u64, Error> {
        self.obj.seek(from, pos)
    }

    fn read_next(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        self.obj.read_next(buf)
    }

    fn get_id(&self) -> ObjectId {
        self.obj.get_id()
    }
}

impl<'a> Read for Object<'a> {

    fn seek(&mut self, from: SeekFrom, pos: u64) -> Result<u64, Error> {
        self.seek(from, pos)
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
    use crate::storage::datastore::FileState;
    use crate::storage::datastore::FileType;
    use crate::storage::datastore::FileDesc;
    use std::path::Path;


    fn init_datastore(dspath: &str, block_size: usize) -> Vec<FileDesc> {

        if Path::new(dspath).exists() {
            std::fs::remove_dir_all(dspath).expect("Failed to delete test dir on cleanup");
        }
        std::fs::create_dir(dspath).expect("Failed to create test dir");

        let mut fdset = vec![];
        let desc1 = FileDesc {
            state:          FileState::InUse,
            file_id:        3,
            extent_size:    16,
            extent_num:     3,
            max_extent_num: 65500,
            file_type:      FileType::DataStoreFile,
        };
        let desc2 = FileDesc {
            state:          FileState::InUse,
            file_id:        4,
            extent_size:    10,
            extent_num:     3,
            max_extent_num: 65500,
            file_type:      FileType::VersioningStoreFile,
        };
        let desc3 = FileDesc {
            state:          FileState::InUse,
            file_id:        5,
            extent_size:    10,
            extent_num:     3,
            max_extent_num: 65500,
            file_type:      FileType::CheckpointStoreFile,
        };

        fdset.push(desc1);
        fdset.push(desc2);
        fdset.push(desc3);

        Instance::initialize_datastore(dspath, block_size, &fdset).expect("Failed to init datastore");
        fdset
    }


    fn create_data(len: usize) -> Vec<u8> {
        let mut ret = vec![];
        for _i in 0..len {
            ret.push(rand::random::<u8>());
        }
        ret
    }

    fn write_full(obj: &mut ObjectWrite, data: &[u8]) {
        obj.write_next(data).expect("Failed to write data");
    }

    fn read_full<T: Read>(obj: &mut T, read_buf: &mut [u8]) {
        let mut read = 0;
        let len = read_buf.len();
        while read < len {
            let r = obj.read_next(&mut read_buf[read..len]).expect("Failed to read");
            if r == 0 {break;}
            read += r;
        }
        assert_eq!(read, len);
    }

    fn read_and_check<T: Read>(obj: &mut T, data: &[u8]) {
        let mut read_buf = vec![0u8;data.len()];
        read_full(obj, &mut read_buf);
        assert_eq!(read_buf, data);
    }

    #[test]
    fn test_instance() {
        let dspath = "/tmp/test_instance_098123";
        let log_dir = "/tmp/test_instance_56445";
        let block_size = 8192;
        let file_id1 = 3;

        let _init_fdesc = init_datastore(dspath, block_size);

        if Path::new(log_dir).exists() {
            std::fs::remove_dir_all(log_dir).expect("Failed to delete test dir on cleanup");
        }
        std::fs::create_dir(log_dir).expect("Failed to create test dir");

        let conf = ConfigMt::new();
        let mut c = conf.get_conf();
        c.set_log_dir(log_dir.to_owned());
        c.set_datastore_path(dspath.to_owned());
        drop(c);

        let instance = Instance::new(conf.clone()).expect("Failed to create instance");


        // create second instance from shared state and move to other thread 


        let ss = instance.get_shared_state().expect("Failed to get shared state");

        let th = std::thread::spawn(move || {
            let instance2 = Instance::from_shared_state(ss).expect("Failed to create instance");
            let trn = instance2.begin_transaction().expect("Failed to begin transaction 1");
            instance2.rollback(trn).expect("Failed to rollback_transaction");
            instance2.terminate();
        });

        let trn = instance.begin_transaction().expect("Failed to begin transaction 2");
        instance.rollback(trn).expect("Failed to rollback_transaction");

        th.join().unwrap();


        // add data file 


        let file_id2 = instance.add_datafile(FileType::DataStoreFile, 1000, 10, 1000).expect("Failed to add data file");



        // read, write & delete


        let data = create_data(block_size * 3);

        let mut trn = instance.begin_transaction().expect("Failed to begin transaction");

        let mut ocr = instance.open_create(file_id1, &mut trn, 200).expect("Failed to create object");
        let obj_id = ocr.get_id();
        write_full(&mut ocr, &data);
        drop(ocr);

        let mut ord = instance.open_read(&obj_id, &trn).expect("Failed to open for reading");
        assert_eq!(ord.get_id(), obj_id);
        read_and_check(&mut ord, &data);
        drop(ord);

        let data2 = create_data(block_size);
        let mut owr = instance.open_write(&obj_id, &mut trn, 1000).expect("Failed to open for writing");
        assert_eq!(owr.get_id(), obj_id);
        write_full(&mut owr, &data2);
        owr.seek(SeekFrom::Current, (block_size + block_size/2) as u64).expect("Failed to seek");
        write_full(&mut owr, &data2);
        drop(owr);

        let mut new_data = vec![0; block_size * 3 + block_size / 2];
        new_data[0..data.len()].copy_from_slice(&data);
        new_data[0..data2.len()].copy_from_slice(&data2);
        let offset = block_size * 2 + block_size / 2;
        new_data[offset..offset + data2.len()].copy_from_slice(&data2);

        let mut ord = instance.open_read(&obj_id, &trn).expect("Failed to open for reading");
        assert_eq!(ord.get_id(), obj_id);
        read_and_check(&mut ord, &new_data);
        drop(ord);

        instance.delete(&obj_id, &mut trn, 1000).expect("Failed to delete object");
        instance.commit(trn).expect("Failed to commit transaction");

        let mut trn = instance.begin_transaction().expect("Failed to begin transaction");
        let res = instance.open_read(&obj_id, &trn);
        assert!(res.is_err());

        let mut ocr = instance.open_create(file_id2, &mut trn, 300).expect("Failed to create object");
        let obj_id = ocr.get_id();
        write_full(&mut ocr, &data);
        drop(ocr);
        instance.commit(trn).expect("Failed to commit transaction");

        instance.terminate();


        // open existing database
        

        let instance = Instance::new(conf.clone()).expect("Failed to create instance");

        let trn = instance.begin_transaction().expect("Failed to begin transaction");

        let mut ord = instance.open_read(&obj_id, &trn).expect("Failed to open for reading");
        read_and_check(&mut ord, &data);
        drop(ord);


        instance.rollback(trn).expect("Failed to rollback transaction");


        // concurrent writes & reads


        // create an object and update it concurrently
        let mut trn = instance.begin_transaction().expect("Failed to begin transaction");
        let mut ocr = instance.open_create(file_id1, &mut trn, 4).expect("Failed to create object");
        let obj_id = ocr.get_id();
        write_full(&mut ocr, &[0u8,0u8,0u8,0u8]);
        drop(ocr);
        instance.commit(trn).expect("Failed to commit transaction");

        let instance_num = 4;
        let iterations = 100;
        let mut threads = vec![];
        for instn in 0..instance_num {

            let ss = instance.get_shared_state().expect("Failed to get shared state");

            let th = std::thread::spawn(move || {
                let mut data = [0u8;4];
                let instance2 = Instance::from_shared_state(ss).expect("Failed to create instance");
                for itrn in 0..iterations {
                    let mut trn = instance2.begin_transaction().expect("Failed to begin transaction 1");
                    let mut owr = instance2.open_write(&obj_id, &mut trn, -1).expect("Failed to create object");
                    read_full(&mut owr, &mut data);
                    let val = u32::from_ne_bytes(data) + 1;
                    owr.seek(SeekFrom::Start, 0).expect("Failed to seek");
                    write_full(&mut owr, &val.to_ne_bytes());
                    drop(owr);
                    instance2.commit(trn).expect("Failed to commit transaction");
                }
                instance2.terminate();
            });

            threads.push(th);
        }

        for th in threads.drain(..) {
            th.join().unwrap();
        }

        let mut data = [0u8;4];
        let mut trn = instance.begin_transaction().expect("Failed to begin transaction");
        let mut ord = instance.open_read(&obj_id, &mut trn).expect("Failed to open object");
        read_full(&mut ord, &mut data);
        assert_eq!(iterations * instance_num, u32::from_ne_bytes(data));
        instance.commit(trn).expect("Failed to commit transaction");

        instance.terminate();
    }

}
