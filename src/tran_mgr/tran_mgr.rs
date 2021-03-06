/// Transaction state management


use crate::common::errors::Error;
use crate::system::config::ConfigMt;
use crate::common::defs::ObjectId;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Condvar;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::collections::HashSet;
use std::collections::HashMap;
use std::time::Duration;


/// TranMgr keeps track of transactions.
#[derive(Clone)]
pub struct TranMgr {
    tsn:        Arc<AtomicU64>,
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
            tsn: Arc::new(AtomicU64::new(0)),
            nbkt,
            nobj_bkt,
            trn_set: Arc::new(trn_set),
            obj_locks: Arc::new(obj_locks),
        })
    }

    pub fn set_tsn(&self, tsn: u64) {
        self.tsn.store(tsn, Ordering::Relaxed);
    }

    pub fn start_tran(&self) -> u64 {
        let tsn = self.get_next_tsn();

        let b = (tsn % self.nbkt as u64) as usize;
        let (lock, _) = &self.trn_set[b];
        let mut hm = lock.lock().unwrap();
        hm.insert(tsn);
        tsn
    }

    pub fn delete_tran(&self, tsn: u64) {
        let b = (tsn % self.nbkt as u64) as usize;
        let (lock, cvar) = &self.trn_set[b];
        let mut hm = lock.lock().unwrap();
        hm.remove(&tsn);
        cvar.notify_all();
    }

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
                hm = cvar.wait(hm).unwrap();
            }
        }
        return true;
    }

    fn get_next_tsn(&self) -> u64 {
        self.tsn.load(Ordering::Relaxed)
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
