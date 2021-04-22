/// Transaction log management

use crate::common::errors::Error;
use crate::common::defs::ObjectId;
use crate::common::defs::Sequence;
use crate::system::config::ConfigMt;
use crate::log_mgr::fs::BufferedFileStream;
use crate::log_mgr::fs::FileStream;
use crate::log_mgr::fs::FileOps;
use crate::log_mgr::io::LogWriter;

pub use crate::log_mgr::io::LogReader;
pub use crate::log_mgr::io::LogRecordHeader;
pub use crate::log_mgr::io::RecType;


#[derive(Clone)]
pub struct LogMgr {
    writer: LogWriter,
    log_dir: String,
    starting_csn: u64,
    latest_commit_csn: u64,
}

impl LogMgr {

    pub fn new(conf: ConfigMt) -> Result<Self, Error> {
        let conf = conf.get_conf();
        let log_dir = conf.get_log_dir().to_owned();
        let max_log_file_size = *conf.get_max_log_file_size();
        let buf_sz = *conf.get_log_writer_buf_size();

        let file_id = FileOps::init_file_logging(&log_dir, max_log_file_size)?;

        let fs = FileStream::new(log_dir.clone(), max_log_file_size, file_id, 4, false, true)?;
        let mut lr = LogReader::new(fs)?;
        let (start_pos, lsn, starting_csn, latest_commit_csn) = lr.find_write_position()?;
        drop(lr);

        let bfs = BufferedFileStream::new(log_dir.clone(), max_log_file_size, buf_sz as usize, file_id, start_pos)?;

        let lsn = Sequence::new(lsn);

        let writer = LogWriter::new(bfs, lsn)?;

        Ok(LogMgr {
            writer,
            log_dir,
            starting_csn,
            latest_commit_csn,
        })
    }

    pub fn write_data(&self, csn: u64, checkpoint_csn: u64, tsn: u64, obj_id: &ObjectId, pos: u64, data: &[u8]) -> Result<(), Error> {
        self.writer.write_data(csn, checkpoint_csn, tsn, obj_id, pos, data)
    }

    pub fn write_commit(&self, csn: u64, tsn: u64) -> Result<(), Error> {
        self.writer.write_commit(csn, tsn)
    }

    pub fn write_rollback(&self, csn: u64, tsn: u64) -> Result<(), Error> {
        self.writer.write_rollback(csn, tsn)
    }

    pub fn write_checkpoint_begin(&self, checkpoint_csn: u64, latest_commit_csn: u64) -> Result<(), Error> {
        self.writer.write_checkpoint_begin(checkpoint_csn, latest_commit_csn)
    }

    pub fn write_checkpoint_completed(&self, checkpoint_csn: u64, latest_commit_csn: u64) -> Result<(), Error> {
        self.writer.write_checkpoint_completed(checkpoint_csn, latest_commit_csn)
    }

    pub fn write_delete(&self, csn: u64, checkpoint_csn: u64, tsn: u64, obj_id: &ObjectId) -> Result<(), Error> {
        self.writer.write_delete(csn, checkpoint_csn, tsn, obj_id)
    }

    pub fn get_reader(&self) -> Result<LogReader, Error> {
        let file_id = FileOps::find_latest_log_file(&self.log_dir)?;
        let fs = FileStream::new(self.log_dir.clone(), 0, file_id, 4, false, true)?;
        Ok(LogReader::new(fs)?)
    }

    pub fn starting_csn(&self) -> u64 {
        self.starting_csn
    }

    pub fn latest_commit_csn(&self) -> u64 {
        self.latest_commit_csn
    }

    pub fn terminate(self) {
        self.writer.terminate();
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::path::Path;

    #[test]
    fn test_log_mgr() {
        let log_dir = "/tmp/test_log_mgr_34566576";

        if Path::new(log_dir).exists() {
            std::fs::remove_dir_all(log_dir).expect("Failed to delete test dir on cleanup");
        }
        std::fs::create_dir(log_dir).expect("Failed to create test dir");

        let mut conf = ConfigMt::new();
        let mut c = conf.get_conf();
        c.set_log_dir(log_dir.to_owned());
        drop(c);

        let lm = LogMgr::new(conf.clone()).expect("Failed to create log mgr");

        let csn = 123;
        let checkpoint_csn = 124;
        let tsn = 125;
        let obj_id = ObjectId::init(100,101,102,103); 
        let pos = 666; 
        let data = [0,1,2,3,4,5,6,7,8,9];
        let latest_commit_csn = 126;

        lm.write_data(csn, checkpoint_csn-1, tsn, &obj_id, pos, &data).expect("Failed to write data");
        lm.write_commit(csn+1, tsn+1).expect("Failed to write commit");
        lm.write_rollback(csn+2, tsn+2).expect("Failed to write rollback");
        lm.write_checkpoint_begin(checkpoint_csn, latest_commit_csn).expect("Failed to write checkpoint csn");
        lm.write_checkpoint_completed(checkpoint_csn, latest_commit_csn+1).expect("Failed to write checkpoint completed");
        lm.write_delete(csn+3, checkpoint_csn, tsn+3, &obj_id).expect("Failed to delete");
        lm.write_data(csn+4, checkpoint_csn, tsn+4, &obj_id, pos, &data).expect("Failed to write data");
        lm.write_commit(csn+5, tsn+5).expect("Failed to write commit");
        lm.write_data(csn+6, checkpoint_csn, tsn+6, &obj_id, pos, &data).expect("Failed to write data");

        lm.terminate();

        let lm = LogMgr::new(conf.clone()).expect("Failed to create log mgr");

        let starting_csn = lm.starting_csn();
        assert_eq!(starting_csn, csn+6);

        let latest_commit_csn = lm.latest_commit_csn();
        assert_eq!(latest_commit_csn, csn+5);


        let mut lr = lm.get_reader().expect("Failed to get log reader");
        let (start_pos, lsn, starting_csn, latest_commit_csn) = lr.find_write_position().expect("Failed to find write position");
        assert_eq!(start_pos, 451);
        assert_eq!(lsn, 9);
        assert_eq!(starting_csn, csn+6);
        assert_eq!(latest_commit_csn, csn+5);

        let mut lr = lm.get_reader().expect("Failed to get log reader");
        let ccsn = lr.seek_to_latest_checkpoint().expect("Failed to get latest checkpoint");
        assert_eq!(ccsn, checkpoint_csn);
        let lrh = lr.read_next().expect("Failed to get latest checkpoint");
        let lrh = lr.read_next().expect("Failed to get latest checkpoint");
        let obj = lr.get_object_id();
        assert_eq!(obj, obj_id);
        let data_pos = lr.get_data_pos();
        assert_eq!(data_pos, pos);
        let data1 = lr.get_data();
        assert_eq!(data1, data);

        lm.terminate();
    }
}
