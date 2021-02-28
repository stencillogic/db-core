/// Checkpoint manager initiates and controls checkpoints.
/// Checkpoint procedure is the following:
///
/// 1. Checkpoint sequence number is incremented.
/// 2. Checkpointer thread writes record in the log about start of new checkpoint.
/// 3. Checkpointer thread calls storage driver to process checkpoint with the new sequence number.
/// 4. After storage driver completes checkpoint procedure checkpointer thread writes record in the
///    log about completion. 
///
/// System will do changes replay on its start only starting form the last completed checkpoint.


use crate::common::errors::Error;
use crate::common::intercom::SyncNotification;
use crate::common::defs::SharedSequences;
use crate::system::config::ConfigMt;
use crate::log_mgr::log_mgr::LogMgr;
use crate::storage::driver::StorageDriver;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::Duration;
use log::error;
use log::warn;
use log::info;


const CONDVAR_WAIT_INTERVAL_MS: u64 = 1000;


pub struct Checkpointer {
    checkpointer_thread:            JoinHandle<()>,
    terminate:                      Arc<AtomicBool>,
    checkpoint_ready:               SyncNotification<bool>,
    processed_data_threashold:      u64,
    processed_data_size:            AtomicU64,
    checkpoint_req_count:           Arc<AtomicU32>,
}

impl Checkpointer {

    pub fn new(log_mgr:             LogMgr, 
               csns:                SharedSequences, 
               conf:                ConfigMt
               ) -> Result<Self, Error> 
    {
        let processed_data_threashold = *conf.get_conf().get_checkpoint_data_threshold();
        let terminate = Arc::new(AtomicBool::new(false));

        let checkpoint_ready = SyncNotification::new(false);
        let checkpoint_req_count = Arc::new(AtomicU32::new(0));

        let terminate2 = terminate.clone();
        let checkpoint_ready2 = checkpoint_ready.clone();
        let checkpoint_req_count2 = checkpoint_req_count.clone();

        let checkpointer_thread = std::thread::spawn(move || {
            Self::checkpointer_thread(conf,
                                      terminate2, 
                                      checkpoint_ready2, 
                                      log_mgr.clone(),
                                      csns.clone(), 
                                      checkpoint_req_count2);
        });

        assert!(processed_data_threashold > 0);

        let processed_data_size = AtomicU64::new(0);

        Ok(Checkpointer {
            checkpointer_thread,
            terminate,
            checkpoint_ready,
            processed_data_threashold,
            processed_data_size,
            checkpoint_req_count,
        })
    }

    pub fn register_processed_data_size(&self, size: u64) {
        let prev_size = self.processed_data_size.fetch_add(size, Ordering::Relaxed);
        if prev_size < self.processed_data_threashold && prev_size + size >= self.processed_data_threashold {
            if let Err(e) = self.initiate_checkpoint() {
                warn!("Failed to initiate checkpoint, error: {}", e);
            }
            self.processed_data_size.store(0, Ordering::Relaxed);
        }
    }

    pub fn terminate(self) {
        self.terminate.store(true, Ordering::Relaxed);
        self.checkpointer_thread.join().unwrap();
    }


    fn initiate_checkpoint(&self) -> Result<(), Error> {

        let mut req_count = self.checkpoint_req_count.load(Ordering::Relaxed);

        while req_count < 2 {
            let new_req_count = self.checkpoint_req_count.compare_and_swap(req_count, req_count + 1, Ordering::Relaxed);
            if new_req_count == req_count {
                if req_count == 0 {
                    self.checkpoint_ready.send(true, false);
                }
                break;
            } else {
                req_count = new_req_count;
            }
        }

        Ok(())
    }


    fn checkpointer_thread(conf:                ConfigMt,
                           terminate:           Arc<AtomicBool>, 
                           checkpoint_ready:    SyncNotification<bool>, 
                           log_mgr:             LogMgr,
                           csns:                SharedSequences,
                           checkpoint_req_count: Arc<AtomicU32>)
    {
        match StorageDriver::new(conf, csns.clone()) {
            Ok(sd) => {
                let mut checkpoint_csn = csns.checkpoint_csn.get_cur();
                loop {
                    if let Some(mut lock) = checkpoint_ready.wait_for_interruptable(
                        &mut (|state| -> bool { *state }),
                        &mut (|| -> bool { terminate.load(Ordering::Relaxed) }),
                        Duration::from_millis(CONDVAR_WAIT_INTERVAL_MS)
                    ) {
                        *lock = false;
                        drop(lock);

                        let mut req_count = checkpoint_req_count.load(Ordering::Relaxed);
                        while req_count > 0 {
                            req_count = checkpoint_req_count.fetch_sub(1, Ordering::Relaxed);

                            checkpoint_csn += 1;

                            // Write to log to be sure all subsequent writes related to this checkpoint go after this record.
                            if let Err(e) = log_mgr.write_checkpoint_begin(checkpoint_csn, csns.latest_commit_csn.load(Ordering::Relaxed)) {
                                error!("Failed to write to log about checkpoint initiation, error: {}", e);
                            } else {
                                info!("Checkpoint initiated");
                                csns.checkpoint_csn.set(checkpoint_csn);

                                if let Err(e) = sd.checkpoint(checkpoint_csn) {
                                    error!("Failed to perform checkpoint: {}", e);
                                } else {
                                    if let Err(e) = log_mgr.write_checkpoint_completed(checkpoint_csn-1, csns.latest_commit_csn.load(Ordering::Relaxed)) {
                                        error!("Failed to write to log about checkpoint completion, error: {}", e);
                                    } else {
                                        info!("Checkpoint completed successfully");
                                    }
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }
            },
            Err(e) => {
                error!("Failed to initialize checkpointer thread, storage driver failure, error: {}", e);
            },
        }
    }
}

