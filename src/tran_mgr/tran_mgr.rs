/// Transaction state management


use crate::common::errors::Error;
use crate::system::config::ConfigMt;
use crate::common::defs::ObjectId;
use crate::common::defs::Sequence;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Condvar;
use std::collections::HashSet;
use std::collections::HashMap;
use std::time::Duration;


/// TranMgr keeps track of transactions.
#[derive(Clone)]
pub struct TranMgr {
    tsn:        Sequence,
    nbkt:       usize,
    nobj_bkt:   usize,
    trn_set:    Arc<Vec<(Mutex<HashSet<u64>>, Condvar)>>,
    obj_locks:  Arc<Vec<(Mutex<HashMap<ObjectId, u64>>, Condvar)>>,
}

impl TranMgr {

    pub fn new(conf: ConfigMt) -> Result<Self, Error> {

        let conf = conf.get_conf();
        let nbkt = *conf.get_tran_mgr_n_buckets() as usize;
        let ntran = *conf.get_tran_mgr_n_tran() as usize;
        let nobj_bkt = *conf.get_tran_mgr_n_obj_buckets() as usize;
        let nobj_lock = *conf.get_tran_mgr_n_obj_lock() as usize;

        let mut trn_set = Vec::with_capacity(nbkt);
        for _ in 0..nbkt {
            trn_set.push((Mutex::new(HashSet::with_capacity(ntran)), Condvar::new()));
        }

        let mut obj_locks = Vec::with_capacity(nobj_bkt);
        for _ in 0..nobj_bkt {
            obj_locks.push((Mutex::new(HashMap::with_capacity(nobj_lock)), Condvar::new()));
        }

        Ok(TranMgr {
            tsn: Sequence::new(1),
            nbkt,
            nobj_bkt,
            trn_set: Arc::new(trn_set),
            obj_locks: Arc::new(obj_locks),
        })
    }

    /// Set initial tsn.
    pub fn set_tsn(&self, tsn: u64) {
        self.tsn.set(tsn);
    }

    /// Get current tsn.
    pub fn get_tsn(&self) -> u64 {
        self.tsn.get_cur()
    }

    /// Register a new transaction and return its tsn.
    pub fn start_tran(&self) -> u64 {
        let tsn = self.tsn.get_next();
        let b = (tsn % self.nbkt as u64) as usize;
        let (lock, _) = &self.trn_set[b];
        let mut hm = lock.lock().unwrap();
        hm.insert(tsn);
        tsn
    }

    /// Unregister transaction with specified tsn.
    pub fn delete_tran(&self, tsn: u64) {
        let b = (tsn % self.nbkt as u64) as usize;
        let (lock, cvar) = &self.trn_set[b];
        let mut hm = lock.lock().unwrap();
        hm.remove(&tsn);
        cvar.notify_all();
    }

    /// Lock an object with certain object id in transaction with specified tsn.
    pub fn lock_object<'a>(&'a self, tsn: u64, obj_id: &'a ObjectId) -> ObjectLockGuard<'a> {
        let b = obj_id.obj_bkt(self.nobj_bkt);
        let (lock, cvar) = &self.obj_locks[b];
        let mut hm = lock.lock().unwrap();

        if let Some(t) = hm.get(obj_id) {
            if *t != tsn {
                while hm.contains_key(obj_id) {
                    hm = cvar.wait(hm).unwrap();
                }
            }
        }

        hm.insert(*obj_id, tsn);

        ObjectLockGuard {
            obj_id,
            trman: &self,
        }
    }

    /// Wait for transaction with specified tsn and timeout in milliseconds to finish.
    /// In case of timeout returns false, otherwise true.
    pub fn wait_for(&self, tsn: u64, timeout: i64) -> bool {
        let b = (tsn % self.nbkt as u64) as usize;
        let (lock, cvar) = &self.trn_set[b];
        let mut hm = lock.lock().unwrap();
        if timeout >= 0 {
            while hm.contains(&tsn) {
                let (h, w) = cvar.wait_timeout(hm, Duration::from_millis(timeout as u64)).unwrap();
                if w.timed_out() {
                    return false;
                }
                hm = h;
            }
        } else {
            while hm.contains(&tsn) {
                let (h, _w) = cvar.wait_timeout(hm, Duration::from_millis(1000u64)).unwrap();
                hm = h;
            }
        }
        return true;
    }

    fn unlock_object(&self, obj_id: &ObjectId) {
        let b = obj_id.obj_bkt(self.nobj_bkt);
        let (lock, cvar) = &self.obj_locks[b];
        let mut hm = lock.lock().unwrap();

        hm.remove(obj_id);

        cvar.notify_one();
    }
}


pub struct ObjectLockGuard<'a> {
    obj_id: &'a ObjectId,
    trman:     &'a TranMgr,
}

impl<'a> Drop for ObjectLockGuard<'a> {

    fn drop(&mut self) {
        self.trman.unlock_object(self.obj_id);
    }
}


#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_tran_mgr() {

        let conf = ConfigMt::new();
        let c = conf.get_conf();
        drop(c);

        let tm = TranMgr::new(conf).expect("Failed to create transaction manager");

        let tsn = 1;
        let obj = ObjectId::init(1,1,1,1);

        tm.set_tsn(tsn);

        let tsn = tm.start_tran();
        let lock = tm.lock_object(tsn, &obj);
        assert!(!tm.wait_for(tsn, 100));
        drop(lock);
        tm.delete_tran(tsn);

        let tsn = tm.start_tran();
        let _lock = tm.lock_object(tsn, &obj);
        tm.delete_tran(tsn);

        assert!(tm.wait_for(tsn, 100));
    }
}
