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

        let fs = FileStream::new(log_dir.clone(), max_log_file_size, file_id, 4, false)?;
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
        let fs = FileStream::new(self.log_dir.clone(), 0, file_id, 4, false)?;
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

    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
