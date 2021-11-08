/// All about transaction log on a file system

use crate::common::errors::Error;
use crate::common::intercom::SyncNotification;
use crate::log_mgr::buf::DoubleBuf;
use crate::log_mgr::buf::Slice;
use std::fs::File;
use std::io::{Write, Read};
use std::io::{Seek, SeekFrom};
use std::fs::OpenOptions;
use log::info;
use std::path::{PathBuf, Path};
use std::sync::{Arc, atomic::AtomicBool, atomic::Ordering};
use std::thread::JoinHandle;
use log::error;
use std::time::Duration;



const LOG_FILE_PREFIX: &str = "log";
const LOG_FILE_MAGIC: [u8; 3] = [b'L', b'G', 0xAB];
const LOG_FILE_ONLINE_BIT: u8 = 0xCD; 
const RETRY_DURATION_SEC: u64 = 1;


/// FileStream is read & write interface for transaction log in file system.
pub struct FileStream {
    f: File,
    max_file_size: u32,
    log_dir: String,
    rotation: Option<FileRotation>,
    file_id: u32,
    offset: u32,
}


impl FileStream {

    pub fn new(log_dir: String, max_file_size: u32, file_id: u32, start_pos: u32, enable_rotation: bool, read: bool) -> Result<FileStream, Error> {

        let file = FileOps::build_file_name(&log_dir, file_id);

        let mut f = OpenOptions::new()
            .create(false)
            .read(read)
            .write(true)
            .truncate(false)
            .open(file)?;

        let min_offset = (LOG_FILE_MAGIC.len() + std::mem::size_of::<u8>()) as u32;
        let offset = if start_pos < min_offset { min_offset } else { start_pos };

        let rotation = if enable_rotation {
            if !FileOps::build_file_name(&log_dir, file_id + 1).exists() {
                let _next_file = FileOps::create_log_file(&log_dir, file_id+1, max_file_size, false);
            }
            Some(FileRotation::new(&log_dir, max_file_size))
        } else {
            None
        };

        f.seek(SeekFrom::Start(offset as u64))?;
        
        Ok(FileStream {
            f,
            max_file_size,
            log_dir,
            rotation,
            file_id,
            offset,
        })
    }

    pub fn get_cur_pos(&mut self) -> std::io::Result<u64> {
        Ok(self.f.seek(SeekFrom::Current(0))?)
    }

    fn reopen(&mut self) -> std::io::Result<()> {
        let file_id = self.file_id + 1;

        let mut file = PathBuf::from(&self.log_dir);
        file.push(LOG_FILE_PREFIX);
        file.set_extension(file_id.to_string());

        if let Some(rotation) = &self.rotation {
            if !file.exists() {
                rotation.wait_for_file(file_id);
            }

            rotation.request_new_file(file_id + 1);
        }

        let mut f = OpenOptions::new()
            .create(false)
            .write(true)
            .truncate(false)
            .open(file)?;
        
        // mark online
        f.seek(SeekFrom::Start(3))?;
        f.write_all(&[LOG_FILE_ONLINE_BIT])?;
        f.flush()?;

        self.f = f;
        self.offset = self.get_cur_pos()? as u32;
        self.file_id = file_id;

        Ok(())
    }

    pub fn terminate(self) {
        if let Some(rotation) = self.rotation {
            rotation.terminate();
        }
    }
}

impl Write for FileStream {

    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {

        if self.offset > self.max_file_size {
            self.reopen()?;
        }

        let ret = self.f.write(buf);
        if let Ok(written) = ret {
            self.offset += written as u32;
        }
        ret
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.f.flush()
    }
}

impl Read for FileStream {

    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.f.read(buf)
    }
}

impl Seek for FileStream {

    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.f.seek(pos)
    }
}


/// Wrapper around FileStream for direct read & buffered write to a transaction log
#[derive(Clone)]
pub struct BufferedFileStream {
    writer_thread:  Arc<JoinHandle<()>>,
    terminate:      Arc<AtomicBool>,
    buf:            DoubleBuf<u8>,
}

impl BufferedFileStream {

    pub fn new(log_dir: String, max_file_size: u32, buf_sz: usize, file_id: u32, start_pos: u32) -> Result<Self, Error> {

        let terminate = Arc::new(AtomicBool::new(false));

        let terminate2 = terminate.clone();

        let db = DoubleBuf::new(buf_sz)?;

        let db2 = db.clone();

        let fs = FileStream::new(log_dir, max_file_size, file_id, start_pos, true, false)?;

        let retry_duration = Duration::new(RETRY_DURATION_SEC, 0);

        let writer_thread = std::thread::spawn(move || {

            Self::write_log_loop(fs, db2, terminate2, retry_duration);
        });

        Ok(BufferedFileStream {
            writer_thread: Arc::new(writer_thread),
            terminate,
            buf: db,
        })
    }

    pub fn get_for_write(&self, reserve_size: usize) -> Result<Slice<u8>, ()> {
        self.buf.reserve_slice(reserve_size, false)
    }

    pub fn flush(&self) {
        self.buf.flush()
    }

    pub fn terminate(self) {
        if let Ok(jh) = Arc::try_unwrap(self.writer_thread) {
            self.terminate.store(true, Ordering::Relaxed);

            self.buf.seal_buffers();

            jh.join().unwrap();
        }
    }

    fn write_log_loop(mut fs: FileStream, buf: DoubleBuf<u8>, terminate: Arc<AtomicBool>, retry_duration: Duration) {

        let mut terminated_cnt = 0;

        loop {

            let (slice, buf_id) = buf.reserve_for_read();

            while let Err(e) = fs.write_all(slice) {
                error!("Failed to write to transaction log file: {}", e);
                std::thread::sleep(retry_duration);
            }

            while let Err(e) = fs.flush() {
                error!("Failed to flush transaction log file: {}", e);
                std::thread::sleep(retry_duration);
            }

            if terminate.load(Ordering::Relaxed) {
                buf.set_buf_terminated(buf_id);

                terminated_cnt += 1;

                if terminated_cnt == buf.get_buf_cnt() {
                    fs.terminate();
                    break;
                }
 
            } else {

                buf.set_buf_appendable(buf_id);
            }
        }
    }
}



/// Log file rotation
struct FileRotation {
    new_file_req:       SyncNotification<FileRotationReq>,
    file_created:       SyncNotification<u32>,
    rotation_thread:    JoinHandle<()>,
}

impl FileRotation {

    pub fn new(log_dir: &String, max_file_size: u32) -> FileRotation {
        let new_file_req = SyncNotification::new(FileRotationReq::Noop);
        let file_created = SyncNotification::new(0);

        let new_file_req2 = new_file_req.clone();
        let file_created2 = file_created.clone();

        let log_dir2 = log_dir.clone();

        let rotation_thread = std::thread::spawn(move || {

            let mut terminate = false; 

            loop {
                let mut file_id = 0;

                let mut check = |val: &FileRotationReq| -> bool {
                    match *val {
                        FileRotationReq::CreateFile(val) => {
                            file_id = val;
                            return false;
                        },
                        FileRotationReq::Terminate => {
                            terminate = true;
                            return false;
                        },
                        FileRotationReq::Noop => return true,
                    }
                };

                let mut locked_val = new_file_req2.wait_for(&mut check);
                *locked_val = FileRotationReq::Noop;
                if terminate {
                    return;
                } else {
                    let res = FileOps::create_log_file(&log_dir2, file_id, max_file_size, false);
                    if let Err(e) = res {
                        error!("Failed to create a new log file {}", e);
                    } else {
                        file_created2.send(file_id, true);
                    }
                }
            }
        });

        FileRotation {
            new_file_req,
            file_created,
            rotation_thread,
        }
    }

    pub fn wait_for_file(&self, file_id: u32) {
        let mut check = |val: &u32| -> bool {*val != file_id};
        let mut locked_val = self.file_created.wait_for(&mut check);
        *locked_val = 0;
    }

    pub fn request_new_file(&self, file_id: u32) {
        self.new_file_req.send(FileRotationReq::CreateFile(file_id), true);
    }

    pub fn terminate(self) {
        self.new_file_req.send(FileRotationReq::Terminate, true);
        self.rotation_thread.join().unwrap();
    }
}


#[derive(Clone, Copy, PartialEq, Debug)]
enum FileRotationReq {
    Noop,
    CreateFile(u32),
    Terminate,
}


/// Log file related utility operations
pub struct FileOps { }

impl FileOps {

    pub fn find_latest_log_file(log_dir: &str) -> Result<u32, Error> {

        let mut max_id = 0;

        for entry in std::fs::read_dir(log_dir)? {

            let entry = entry?;
            let path = entry.path();

            if let Ok(ftype) = entry.file_type() {
                if ftype.is_file() {
                    if let Some(stem) = path.file_stem() {
                        if stem == LOG_FILE_PREFIX {
                            if let Some(extension) = path.extension() {
                                if let Ok(num) = extension.to_string_lossy().parse::<u32>() {
                                    if num > max_id {
                                        if FileOps::check_if_online(&path)? {
                                            max_id = num;
                                        }
                                    }
                                } else {
                                    info!("Skipping entry in transaction log directory {:?}: extension is not i32 number", entry.path());
                                }
                            } else {
                                info!("Skipping entry in transaction log directory {:?}: no file extension", entry.path());
                            }
                        } else {
                            info!("Skipping entry in transaction log directory {:?}: file name doesn't match {}", entry.path(), LOG_FILE_PREFIX);
                        }
                    } else {
                        info!("Skipping entry in transaction log directory {:?}: no stem in file name", entry.path());
                    }
                } else {
                    info!("Skipping entry in transaction log directory {:?}: entry is not a file", entry.path());
                }
            } else {
                info!("Skipping entry in transaction log directory {:?}: unable to determine file type", entry.path());
            }
        }

        Ok(max_id)
    }

    pub fn build_file_name(log_dir: &str, file_id: u32) -> PathBuf {

        let mut file = PathBuf::from(log_dir);
        file.push(LOG_FILE_PREFIX);
        file.set_extension(file_id.to_string());
        file
    }

    pub fn create_log_file(log_dir: &str, file_id: u32, size: u32, mark_online: bool) -> Result<(), Error> {
        let file = FileOps::build_file_name(log_dir, file_id);

        let mut f = OpenOptions::new()
            .create_new(true)
            .write(true)
            .truncate(false)
            .open(file)?;

        f.set_len(size as u64)?;

        f.write_all(&LOG_FILE_MAGIC)?;
        if mark_online {
            f.write_all(&[LOG_FILE_ONLINE_BIT])?;
        }

        f.sync_all()?;

        Ok(())
    }

    pub fn check_if_online(path: &Path) -> std::io::Result<bool> {
        let mut f = OpenOptions::new()
            .create(false)
            .read(true)
            .write(false)
            .truncate(false)
            .open(path)?;

        let mut magic = [0,0,0];
        let mut online_bit = [0];
        f.read_exact(&mut magic)?;
        for i in 0..magic.len() {
            if magic[i] != LOG_FILE_MAGIC[i] {
                return Ok(false);
            }
        }

        f.read_exact(&mut online_bit)?;

        Ok(online_bit[0] == LOG_FILE_ONLINE_BIT)
    }

    pub fn init_file_logging(log_dir: &str, file_size: u32) -> Result<u32, Error> {

        let file_id = FileOps::find_latest_log_file(log_dir)?;
        if file_id == 0 {
            if !FileOps::build_file_name(&log_dir, 1).exists() {
                FileOps::create_log_file(log_dir, 1, file_size, true)?;
            }
            Ok(1)
        } else {
            Ok(file_id)
        }
    }
}


#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_file_stream() {
        let log_dir = "/tmp/test_fs_435354345";
        let max_file_size = 100;
        let file_id = 1;
        let start_pos = 0;
        let enable_rotation = true;
        let buf_sz = 100;

        if Path::new(log_dir).exists() {
            std::fs::remove_dir_all(log_dir).expect("Failed to delete test dir on cleanup");
        }
        std::fs::create_dir(log_dir).expect("Failed to create test dir");


        FileOps::init_file_logging(log_dir, max_file_size).expect("Failed to init logging");


        // FileStream


        let mut fs = FileStream::new(log_dir.to_owned(), max_file_size, file_id, start_pos, enable_rotation, false).expect("Failed to create file stream");
        assert_eq!(fs.get_cur_pos().unwrap(), 4);

        let buf = [1,2,3,4,5,6,7,8,0,1,2,3,4,5];
        fs.write_all(&buf).unwrap();
        fs.flush().unwrap();

        assert_eq!(fs.get_cur_pos().unwrap(), buf.len() as u64 + 4);

        for _ in 0..20 {
            fs.write_all(&buf).unwrap();
        }
        fs.flush().unwrap();

        fs.terminate();


        // BufferedFileStream


        let bfs = BufferedFileStream::new(log_dir.to_owned(), max_file_size, buf_sz, 2, (11 * buf.len() + 4) as u32).expect("Failed to create BufferedFileStream");

        for _ in 0..20 {
            let mut slice = bfs.get_for_write(buf.len()).unwrap();
            (&mut slice).copy_from_slice(&buf);
        }
        bfs.flush();

        bfs.terminate();
    }

}
