/// Transaction log writer & reader

use crate::common::errors::Error;
use crate::common::errors::ErrorKind;
use crate::common::misc::SliceToIntConverter;
use crate::common::crc32;
use crate::common::defs::Sequence;
use crate::log_mgr::fs::BufferedFileStream;
use crate::log_mgr::fs::FileStream;
use crate::common::defs::ObjectId;
use std::io::Read;
use std::io::Write;
use std::io::Seek;
use std::io::SeekFrom;


const OBJECT_ID_WRITE_SZ: usize = 2 * 4;
const LRH_WRITE_SZ: usize = 8 * 4 + 1 + 4;


#[derive(Debug)]
pub struct LogRecordHeader {
    pub lsn:            u64,
    pub csn:            u64,
    pub checkpoint_csn: u64,
    pub tsn:            u64,
    pub rec_type:       RecType,
    pub crc32:          u32,
}

impl LogRecordHeader {
    fn new() -> Self {
        LogRecordHeader {
            lsn:            0,
            csn:            0,
            checkpoint_csn: 0,
            tsn:            0,
            rec_type:       RecType::Unspecified,
            crc32:          0,
        }
    }
}

#[derive(Eq, Hash, PartialEq, Clone, Copy, Debug)]
pub enum RecType {
    Unspecified = 0,
    Commit = 1,
    Rollback = 2,
    Data = 3,
    Delete = 4,
    CheckpointBegin = 5,
    CheckpointCompleted = 6,
}



/// Log writer
#[derive(Clone)]
pub struct LogWriter {
    out_stream:         BufferedFileStream,
    lsn:                Sequence,
}

impl LogWriter {

    pub fn new(out_stream: BufferedFileStream, lsn: Sequence) -> Result<LogWriter, Error> {
        Ok(LogWriter {
            out_stream,
            lsn,
        })
    }

    pub fn write_data(&self, csn: u64, checkpoint_csn: u64, tsn: u64, obj: &ObjectId, pos: u64, data: &[u8]) -> Result<(), Error> {
        let mut lrh = self.prepare_lrh(csn, checkpoint_csn, tsn, RecType::Data);
        LogOps::calc_obj_id_crc(&mut lrh.crc32, obj);
        lrh.crc32 = crc32::crc32_num(lrh.crc32, pos);
        lrh.crc32 = crc32::crc32_num(lrh.crc32, data.len() as u32);
        lrh.crc32 = crc32::crc32_arr(lrh.crc32, data);
        lrh.crc32 = crc32::crc32_finalize(lrh.crc32);

        let mut dst_locked = self.out_stream.get_for_write(LRH_WRITE_SZ + OBJECT_ID_WRITE_SZ + 8 + 4 + data.len()).unwrap();
        let mut slice: &mut [u8] = &mut dst_locked;

        self.write_header(&lrh, &mut slice)?;
        self.write_obj_id(obj, &mut slice)?;
        slice.write_all(&pos.to_ne_bytes())?;
        slice.write_all(&(data.len() as u32).to_ne_bytes())?;
        slice.write_all(data)?;
        slice.flush()?;

        drop(dst_locked);

        Ok(())
    }

    pub fn write_commit(&self, csn: u64, tsn: u64) -> Result<(), Error>  {
        let _lsn = self.write_header_only_rec(csn, 0, tsn, RecType::Commit)?;
        self.out_stream.flush();
        Ok(())
    }

    pub fn write_rollback(&self, csn: u64, tsn: u64) -> Result<(), Error>  {
        self.write_header_only_rec(csn, 0, tsn, RecType::Rollback)?;
        Ok(())
    }

    pub fn write_checkpoint_begin(&self, checkpoint_csn: u64, latest_commit_csn: u64) -> Result<(), Error>  {
        let mut lrh = self.prepare_lrh(0, checkpoint_csn, 0, RecType::CheckpointBegin);
        lrh.crc32 = crc32::crc32_num(lrh.crc32, latest_commit_csn);
        lrh.crc32 = crc32::crc32_finalize(lrh.crc32);

        let mut dst_locked = self.out_stream.get_for_write(LRH_WRITE_SZ + 8).unwrap();
        let mut slice: &mut [u8] = &mut dst_locked;

        self.write_header(&lrh, &mut slice)?;
        slice.write_all(&latest_commit_csn.to_ne_bytes())?;
        slice.flush()?;
        drop(dst_locked);

        self.out_stream.flush();

        Ok(())
    }

    pub fn write_checkpoint_completed(&self, checkpoint_csn: u64, latest_commit_csn: u64) -> Result<(), Error>  {
        let mut lrh = self.prepare_lrh(0, checkpoint_csn, 0, RecType::CheckpointCompleted);
        lrh.crc32 = crc32::crc32_num(lrh.crc32, latest_commit_csn);
        lrh.crc32 = crc32::crc32_finalize(lrh.crc32);

        let mut dst_locked = self.out_stream.get_for_write(LRH_WRITE_SZ + 8).unwrap();
        let mut slice: &mut [u8] = &mut dst_locked;

        self.write_header(&lrh, &mut slice)?;
        slice.write_all(&latest_commit_csn.to_ne_bytes())?;
        slice.flush()?;
        drop(dst_locked);

        self.out_stream.flush();

        Ok(())
    }


    pub fn write_delete(&self, csn: u64, checkpoint_csn: u64, tsn: u64, obj: &ObjectId) -> Result<(), Error> {
        let mut lrh = self.prepare_lrh(csn, checkpoint_csn, tsn, RecType::Delete);
        LogOps::calc_obj_id_crc(&mut lrh.crc32, obj);
        lrh.crc32 = crc32::crc32_finalize(lrh.crc32);

        let mut dst_locked = self.out_stream.get_for_write(LRH_WRITE_SZ + 8).unwrap();
        let mut slice: &mut [u8] = &mut dst_locked;

        self.write_header(&lrh, &mut slice)?;
        self.write_obj_id(obj, &mut slice)?;

        drop(dst_locked);

        Ok(())
    }

    fn write_header_only_rec(&self, csn: u64, checkpoint_csn: u64, tsn: u64, rec_type: RecType) -> Result<u64, Error>  {
        let mut lrh = self.prepare_lrh(csn, checkpoint_csn, tsn, rec_type);
        lrh.crc32 = crc32::crc32_finalize(lrh.crc32);

        let mut dst_locked = self.out_stream.get_for_write(LRH_WRITE_SZ).unwrap();
        let mut slice: &mut [u8] = &mut dst_locked;
        self.write_header(&lrh, &mut slice)?;
        drop(dst_locked);

        Ok(lrh.lsn)
    }

    fn write_header(&self, lrh: &LogRecordHeader, slice: &mut &mut [u8]) -> std::io::Result<()> {
        (*slice).write_all(&lrh.lsn.to_ne_bytes())?;
        (*slice).write_all(&lrh.csn.to_ne_bytes())?;
        (*slice).write_all(&lrh.checkpoint_csn.to_ne_bytes())?;
        (*slice).write_all(&lrh.tsn.to_ne_bytes())?;
        (*slice).write_all(&[(lrh.rec_type as u8)])?;
        (*slice).write_all(&lrh.crc32.to_ne_bytes())?;
        (*slice).flush()?;

        Ok(())
    }

    fn write_obj_id(&self, obj: &ObjectId, slice: &mut &mut [u8]) -> std::io::Result<()> {
        (*slice).write_all(&obj.file_id.to_ne_bytes())?;
        (*slice).write_all(&obj.extent_id.to_ne_bytes())?;
        (*slice).write_all(&obj.block_id.to_ne_bytes())?;
        (*slice).write_all(&obj.entry_id.to_ne_bytes())?;
        (*slice).flush()?;

        Ok(())
    }

    fn prepare_lrh(&self, csn: u64, checkpoint_csn: u64, tsn: u64, rec_type: RecType) -> LogRecordHeader {
        let mut lrh = LogRecordHeader::new();
        let mut crc32;

        lrh.lsn = self.lsn.get_next();
        lrh.tsn = tsn;
        lrh.csn = csn;
        lrh.checkpoint_csn = checkpoint_csn;
        lrh.rec_type = rec_type;

        crc32 = crc32::crc32_begin();
        LogOps::calc_header_crc(&mut crc32, &lrh);
        lrh.crc32 = crc32;

        lrh
    }

    pub fn terminate(self) {
        self.out_stream.terminate();
    }
}


// Log reader
pub struct LogReader {
    fs:                 FileStream,
    data_buf:           Vec<u8>,
    obj:                ObjectId,
    data_pos:           u64,
    stop_pos:           u32,
    lsn:                u64,
    csn:                u64,
    latest_commit_csn:  u64,
    checkpoint_csn:     u64,
}

impl LogReader {

    pub fn new(fs: FileStream) -> Result<Self, Error> {

        Ok(LogReader {
            fs,
            data_buf:          Vec::new(),
            obj:               ObjectId::new(),
            data_pos:          0,
            stop_pos:          0,
            lsn:               0,
            csn:               0,
            latest_commit_csn: 0,
            checkpoint_csn:    0,
        })
    }

    pub fn find_write_position(&mut self) -> Result<(u32, u64, u64, u64), Error> {
        while let Some(_lrh) = self.read_next()? { }

        Ok((self.stop_pos, self.lsn, self.csn, self.latest_commit_csn))
    }

    pub fn seek_to_latest_checkpoint(&mut self) -> Result<u64, Error> {

        let mut seek_pos = 0;
        let mut start_seek_pos = 0;
        let mut csn = 0;

        // find latest completed checkpoint
        while let Some(lrh) = self.read_next()? {

            if lrh.rec_type == RecType::CheckpointBegin {
                start_seek_pos = self.fs.get_cur_pos()?;
                csn = lrh.checkpoint_csn;
            } else if lrh.rec_type == RecType::CheckpointCompleted {
                if csn == lrh.checkpoint_csn {
                    seek_pos = start_seek_pos;
                    self.checkpoint_csn = csn;
                }
            }
        }

        if seek_pos > 0 {
            self.fs.seek(SeekFrom::Start(seek_pos))?;

            Ok(self.checkpoint_csn)
        } else {
            Err(Error::checkpoint_not_found())
        }
    }

    pub fn read_next(&mut self) -> Result<Option<LogRecordHeader>, Error> {
        let data_len: u32;
        let mut crc32;
        let mut u32_buf = [0u8; 4];
        let mut u64_buf = [0u8; 8];
        let mut latest_commit_csn = 0;
        let mut checkpoint_csn = 0;

        match self.read_header() {
            Ok(lrh) => {
                crc32 = crc32::crc32_begin();
                LogOps::calc_header_crc(&mut crc32, &lrh);

                match lrh.rec_type {
                    RecType::Commit => {
                        latest_commit_csn = lrh.csn;
                    },
                    RecType::Rollback => {
                    },
                    RecType::CheckpointBegin => {
                        self.fs.read_exact(&mut u64_buf)?;
                        checkpoint_csn = u64::from_ne_bytes(u64_buf);
                        crc32 = crc32::crc32_num(crc32, checkpoint_csn);

                        if self.latest_commit_csn < checkpoint_csn {
                            latest_commit_csn = checkpoint_csn;
                        }
                    },
                    RecType::CheckpointCompleted => {
                        self.fs.read_exact(&mut u64_buf)?;
                        checkpoint_csn = u64::from_ne_bytes(u64_buf);
                        crc32 = crc32::crc32_num(crc32, checkpoint_csn);

                        if self.latest_commit_csn < checkpoint_csn {
                            latest_commit_csn = checkpoint_csn;
                        }
                    },
                    RecType::Data => {
                        self.obj = self.read_object_id()?;
                        LogOps::calc_obj_id_crc(&mut crc32, &self.obj);

                        self.fs.read_exact(&mut u64_buf)?;
                        self.data_pos = u64::from_ne_bytes(u64_buf);
                        crc32 = crc32::crc32_num(crc32, self.data_pos);

                        self.fs.read_exact(&mut u32_buf)?;
                        data_len = u32::from_ne_bytes(u32_buf);
                        crc32 = crc32::crc32_num(crc32, data_len);

                        if data_len > 0 {
                            self.data_buf.resize(data_len as usize, 0);

                            self.fs.read_exact(&mut self.data_buf)?;
                            crc32 = crc32::crc32_arr(crc32, &self.data_buf);
                        }
                    },
                    RecType::Delete => {
                        self.obj = self.read_object_id()?;
                        LogOps::calc_obj_id_crc(&mut crc32, &self.obj);
                    },
                    RecType::Unspecified => {},
                }

                crc32 = crc32::crc32_finalize(crc32);

                if crc32 == lrh.crc32 {
                    self.stop_pos = self.fs.get_cur_pos()? as u32;
                    self.lsn = lrh.lsn;
                    self.csn = lrh.csn;
                    if lrh.rec_type == RecType::Commit || 
                        lrh.rec_type == RecType::CheckpointBegin || 
                        lrh.rec_type == RecType::CheckpointCompleted 
                    {
                        self.latest_commit_csn = latest_commit_csn;
                        if lrh.rec_type == RecType::CheckpointBegin || lrh.rec_type == RecType::CheckpointCompleted {
                            self.checkpoint_csn = checkpoint_csn;
                        }
                    }

                    return Ok(Some(lrh));
                } else {
                    return Ok(None);
                }
            },
            Err(e) => {
                if ErrorKind::IoError == e.kind() {
                    let ioe = e.io_err().unwrap();
                    if ioe.kind() == std::io::ErrorKind::UnexpectedEof {
                        return Ok(None);
                    } else {
                        return Err(Error::io_error(ioe));
                    }
                }

                return Err(e);
            }
        }
    }

    pub fn get_object_id(&self) -> ObjectId {
        self.obj
    }

    pub fn get_data_pos(&self) -> u64 {
        self.data_pos
    } 

    pub fn get_data(&self) -> &[u8] {
        &self.data_buf
    } 

    fn read_header(&mut self) -> Result<LogRecordHeader, Error> {
        let mut lrh = LogRecordHeader::new();

        let mut u32_buf = [0u8; 4];
        let mut u64_buf = [0u8; 8];
        let mut byte = [0u8];
        self.fs.read_exact(&mut u64_buf)?;
        lrh.lsn = u64::from_ne_bytes(u64_buf);
        self.fs.read_exact(&mut u64_buf)?;
        lrh.csn = u64::from_ne_bytes(u64_buf);
        self.fs.read_exact(&mut u64_buf)?;
        lrh.checkpoint_csn = u64::from_ne_bytes(u64_buf);
        self.fs.read_exact(&mut u64_buf)?;
        lrh.tsn = u64::from_ne_bytes(u64_buf);
        self.fs.read(&mut byte)?;
        lrh.rec_type = match byte[0] {
            0 => RecType::Unspecified,
            1 => RecType::Commit,
            2 => RecType::Rollback,
            3 => RecType::Data,
            4 => RecType::Delete,
            5 => RecType::CheckpointBegin,
            6 => RecType::CheckpointCompleted,
            _ => panic!("Unexpected record type in the log"),
        };
        self.fs.read_exact(&mut u32_buf)?;
        lrh.crc32 = u32::from_ne_bytes(u32_buf);

        Ok(lrh)
    }

    fn read_object_id(&mut self) -> Result<ObjectId, Error> {
        let mut u64_buf = [0u8; 8];
        let mut ret = ObjectId::new();

        self.fs.read_exact(&mut u64_buf)?;

        ret.file_id = u16::slice_to_int(&u64_buf[0..2]).unwrap();
        ret.extent_id = u16::slice_to_int(&u64_buf[2..4]).unwrap();
        ret.block_id = u16::slice_to_int(&u64_buf[4..6]).unwrap();
        ret.entry_id = u16::slice_to_int(&u64_buf[6..8]).unwrap();

        Ok(ret)
    }
}


/// Utility functions for working with log
pub struct LogOps {}

impl LogOps {

    fn calc_header_crc(crc32: &mut u32, lrh: &LogRecordHeader) {
        *crc32 = crc32::crc32_num(*crc32, lrh.lsn);
        *crc32 = crc32::crc32_num(*crc32, lrh.csn);
        *crc32 = crc32::crc32_num(*crc32, lrh.checkpoint_csn);
        *crc32 = crc32::crc32_num(*crc32, lrh.tsn);
        *crc32 = crc32::crc32_num(*crc32, lrh.rec_type as u8);
    }

    fn calc_obj_id_crc(crc32: &mut u32, obj: &ObjectId) {
        *crc32 = crc32::crc32_num(*crc32, obj.file_id);
        *crc32 = crc32::crc32_num(*crc32, obj.extent_id);
        *crc32 = crc32::crc32_num(*crc32, obj.block_id);
        *crc32 = crc32::crc32_num(*crc32, obj.entry_id);
    }
}


