//! Datastore provides access to data blocks, versioning store, and checkpoint data on disk.
//! 
//! A set of files represent datastore on the disk. Each file consists of individual blocks.
//! There are several types of blocks:
//!  - file header block - the first block of the file, contains metadata about the file.
//!  - extent header block - the first block of each extent.
//!  - free info block - optional block containing bitmap of free blocks/extents in extent/file.
//!  - data block - a block containing data itself.
//!
//! Sample file structure:
//!
//! +---- file header ----+
//! |  file header block  |
//! | [ free info block ] |
//! | [ free info block ] |
//! | [      ...        ] |
//! +----- extent 1 ------+
//! | extent header block |
//! | [ free info block ] |
//! | [ free info block ] |
//! | [      ...        ] |
//! |     data block      |
//! |     data block      |
//! |     data block      |
//! |     data block      |
//! |        ...          |
//! +----- extent 2 ------+
//! | extent header block |
//! | [ free info block ] |
//! | [ free info block ] |
//! | [      ...        ] |
//! |     data block      |
//! |     data block      |
//! |     data block      |
//! |     data block      |
//! |        ...          |
//! +---------------------+
//!
//! The first 4 bytes of block is crc32 sum. 
//! After crc depending on block type follow several metadata fields.
//! After metadata follows free info bitmap for header blocks, or data entries list for data
//! blocks.
//!
//! Extent is an extention unit for a data file, a continuous sequence of blocks.
//!
//! Entries start from the bottom and are added from bottom to top. 
//!
//! Exact fields and structure is given in module crate::block_mgr::block.
//!

use crate::common::errors::Error;
use crate::common::defs::BlockId;
use crate::common::crc32;
use crate::common::misc::SliceToIntConverter;
use crate::common::misc::alloc_buf;
use crate::common::misc::dealloc_buf;
use crate::block_mgr::block::DataBlock;
use crate::block_mgr::block::FreeInfoSection;
use crate::block_mgr::block::FreeInfoHeaderSection;
use crate::block_mgr::block::ExtentHeaderBlock;
use crate::block_mgr::block::FileHeaderBlock;
use crate::block_mgr::block::FreeInfoBlock;
use crate::block_mgr::block::BasicBlock;
use crate::block_mgr::block::FIBLOCK_HEADER_LEN;
use crate::block_mgr::block::EHBLOCK_HEADER_LEN;
use crate::block_mgr::block::FHBLOCK_HEADER_LEN;
use crate::buf_mgr::buf_mgr::Pinned;
use crate::buf_mgr::buf_mgr::BlockArea;
use crate::storage::fs_ops;
use crate::system::config::ConfigMt;
use std::io::{Read, Write, Seek, SeekFrom};
use std::sync::RwLock;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLockReadGuard;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering; 
use std::collections::HashMap;
use std::cell::Ref;
use std::cell::RefCell;
use fs2::FileExt;
use log::warn;
use std::ops::DerefMut;


pub const DATA_FILE_MAGIC: u8 = 0x14;
pub const VERSIONING_FILE_MAGIC: u8 = 0x85;
pub const CHECKPOINT_FILE_MAGIC: u8 = 0x35;
pub const LOCK_FILE: &str = "db.lock";

pub const MAX_EXTENT_SIZE: usize = 65500;
pub const MIN_BLOCK_SIZE: usize = 1024;
pub const MAX_BLOCK_SIZE: usize = 65536;

pub const DEFAULT_BLOCK_SIZE: usize = 4096;

pub const MAX_FI_BLOCKS: usize = MAX_EXTENT_SIZE / 8 / MIN_BLOCK_SIZE + 1;  // max number of free info blocks (excluding header).

pub const MIN_EXTENT_SIZE: usize = MAX_FI_BLOCKS + 2;  // max fi blocks + header block + at least one data block.

const FILE_IN_USE_MARK: u8 = 1;


/// Datastore letes manipulate data files.
pub struct DataStore {
    desc_repo:          FileDescRepo,
    block_size:         usize,
    block_fill_size:    usize,
    files:              RefCell<HashMap<u16, std::fs::File>>,
    block_buf:          RefCell<BlockArea>,
    path:               String,
    lock_file:          Arc<std::fs::File>,
    seq_file_id:        Arc<AtomicU16>,
    path_buf:           RefCell<String>,
}


impl DataStore {

    /// Create instance of DataStore.
    pub fn new(conf: ConfigMt) -> Result<Self, Error> {
        let block_fill_ratio = *conf.get_conf().get_block_fill_ratio();
        let path = conf.get_conf().get_datastore_path().clone();

        let mut ret = Self::open_datastore(path)?;
        ret.block_fill_size = ret.block_size * block_fill_ratio as usize / 100;
        Ok(ret)
    }

    /// Initialize datastore: create data, checkpoint, versioning store files, and lock file.
    pub fn initialize_datastore(path: &str, block_size: usize, desc_set: &[FileDesc]) -> Result<(), Error> {
        if (block_size as usize) < MIN_BLOCK_SIZE || (block_size as usize) > MAX_BLOCK_SIZE 
            || !block_size.is_power_of_two() {
            return Err(Error::incorrect_block_size());
        }

        // create lock file, and lock datastore
        let mut lock_file = std::path::PathBuf::new();

        lock_file.push(path);
        lock_file.push(LOCK_FILE);

        let f = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(lock_file)?;

        f.try_lock_exclusive()?;

        // initialize empty datastore
        let desc_repo =          FileDescRepo::new(vec![], vec![]);
        let block_fill_size =    block_size * 80 / 100;
        let files =              HashMap::new();
        let block_buf =          BlockArea::new(alloc_buf(block_size)?, block_size);
        let path =               path.to_owned();
        let lock_file =          Arc::new(f);
        let seq_file_id =        Arc::new(AtomicU16::new(2));

        let ds = DataStore {
            desc_repo, 
            block_size,
            block_fill_size,
            files: RefCell::new(files),
            block_buf: RefCell::new(block_buf),
            path,
            lock_file,
            seq_file_id,
            path_buf: RefCell::new(String::new()),
        };

        // add data files
        for desc in desc_set {
            ds.add_datafile(desc.file_type, desc.extent_size, desc.extent_num, desc.max_extent_num)?;
        }

        Ok(())
    }

    /// Load block from disk.
    pub fn load_block(&self, block_id: &BlockId, file_state: FileState) -> Result<Ref<BlockArea>, Error> {

        self.seek_to_block(block_id, file_state)?;
        let files = self.files.borrow();
        let mut f = files.get(&block_id.file_id).unwrap();

        let mut block_buf = self.block_buf.borrow_mut();
        f.read_exact(&mut block_buf)?;
        drop(files);
        drop(block_buf);

        let block_buf = self.block_buf.borrow();

        // check crc
        let mut crc32 = crc32::crc32_begin();
        crc32 = crc32::crc32_arr(crc32, &block_buf[4..]);
        crc32 = crc32::crc32_finalize(crc32);

        if u32::slice_to_int(&block_buf[0..4]).unwrap() != crc32 {
            Err(Error::block_crc_mismatch())
        } else {
            Ok(block_buf)
        }
    }

    /// Return file descriptive information.
    pub fn get_file_desc(&self, file_id: u16) -> Option<FileDesc> {
        self.desc_repo.get_by_file_id(file_id, FileState::InUse)
    }

    /// Return block size in bytes.
    pub fn get_block_size(&self) -> usize {
        self.block_size
    }

    /// Return how much bytes are taken in a full block.
    pub fn block_fill_size(&self) -> usize {
        self.block_fill_size
    }

    /// Write a block to disk.
    pub fn write_block<T: BasicBlock>(&self, block: &mut T, file_state: FileState) -> Result<(), Error> {
        let block_id = block.get_id();

        self.seek_to_block(&block_id, file_state)?;

        block.prepare_for_write();

        let files = self.files.borrow();
        let mut f = files.get(&block_id.file_id).unwrap();
        f.write_all(block.slice())?;
        drop(files);

        Ok(())
    }

    /// Return iterator over checkpoint store files.
    pub fn get_checkpoint_files(&self, files: &mut Vec<FileDesc>) {
        self.desc_repo.fill_files_vec(files, FileType::CheckpointStoreFile, FileState::InUse);
    }

    /// Return iterator over versioning store files.
    pub fn get_versioning_files(&self, files: &mut Vec<FileDesc>) {
        self.desc_repo.fill_files_vec(files, FileType::VersioningStoreFile, FileState::InUse);
    }

    /// Return iterator over data files.
    pub fn get_data_files(&self, files: &mut Vec<FileDesc>) {
        self.desc_repo.fill_files_vec(files, FileType::DataStoreFile, FileState::InUse);
    }

    /// Clone this instance. Clone operations opens separate file descriptors for each of data
    /// files.
    pub fn clone(&self) -> Result<Self, Error> {
        let desc_repo          = self.desc_repo.clone();
        let block_size         = self.block_size;
        let block_fill_size    = self.block_fill_size;
        let block_buf          = BlockArea::new(alloc_buf(block_size)?, block_size);

        let mut files          = HashMap::new(); 
        let mut path_buf       = String::new();
        for (file_id, _) in self.files.borrow().iter() {
            let desc = self.desc_repo.get_by_file_id(*file_id, FileState::InUse).unwrap();

            self.desc_repo.get_path(desc.file_id, &mut path_buf);
            let f = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(false)
                    .open(&path_buf)?;

            files.insert(*file_id, f);
        }

        let path        = self.path.clone();
        let lock_file   = self.lock_file.clone();
        let seq_file_id = self.seq_file_id.clone();

        Ok(DataStore {
            desc_repo, 
            block_size,
            block_fill_size,
            files: RefCell::new(files),
            block_buf: RefCell::new(block_buf),
            path,
            lock_file,
            seq_file_id,
            path_buf: RefCell::new(String::new()),
        })
    }

    /// Add a new extent to a file.
    pub fn add_extent(&self, file_id: u16, file_state: FileState) -> Result<(), Error> {

        let desc = self.desc_repo.get_by_file_id(file_id, file_state).unwrap();

        if desc.state != file_state {
            return Err(Error::file_does_not_exist());
        }

        if desc.extent_num == desc.max_extent_num {
            return Err(Error::extent_limit_reached());
        }

        // get mutex for file, and in critical section increase file size and format extent.
        let slock = self.desc_repo.lock_file(file_id).ok_or(Error::file_does_not_exist())?;
        let mutex = slock.get(&file_id).unwrap();
        let file_lock = mutex.lock().unwrap();

        let extent_start_pos = ((desc.extent_num - 1) as u64 * desc.extent_size as u64 
            + (self.calc_file_fi_block_num(desc.max_extent_num as usize) as u64) + 1) * self.block_size as u64;

        let files = self.files.borrow();
        let mut f = files.get(&file_id).unwrap();
        f.set_len(extent_start_pos + (desc.extent_size as u64 * self.block_size as u64))?;
        f.seek(SeekFrom::Start(extent_start_pos))?;

        // write extent header block and free info block(s)
        let mut block_id = BlockId {
            file_id,
            extent_id: desc.extent_num,
            block_id: 0,
        };

        let stub_pin = AtomicU64::new(1000);

        self.zero_block_buf();

        let mut ehb = ExtentHeaderBlock::new(block_id, 0, Pinned::<BlockArea>::new(self.block_buf.borrow().clone(), &stub_pin));
        let fi_block_num = self.calc_extent_fi_block_num(desc.extent_size as usize);
        ehb.set_fi_size(desc.extent_size);
        ehb.set_full_cnt(fi_block_num as u16 + 1);
        for i in 0..fi_block_num + 1 {              // header and fi blocks are always full
            ehb.fi_slice_mut()[i/8] |= 1 << (i%8);
        }

        self.write_block_direct(&mut ehb, &f)?;

        if ehb.fi_slice().len() * 8 < desc.extent_size as usize {
            self.add_fi_blocks(&mut block_id, fi_block_num, &stub_pin, &f)?;
        }

        // format data blocks
        self.zero_block_buf();

        let mut db = DataBlock::new(block_id, 0, Pinned::<BlockArea>::new(self.block_buf.borrow().clone(), &stub_pin));
        for _ in 0..desc.extent_size {
            block_id.block_id += 1;
            db.set_id(block_id);
            self.write_block_direct(&mut db, &f)?;
        }

        f.sync_data()?;
        drop(files);

        self.desc_repo.add_extent(file_id);

        drop(file_lock);
        drop(slock);
        
        Ok(())
    }

    /// Add a new file to datastore. extent_num is the number of data extents, and excludes extent 0
    /// which is file header.
    pub fn add_datafile(&self, file_type: FileType, extent_size: u16, extent_num: u16, max_extent_num: u16) -> Result<u16, Error> {

        if (extent_size as usize) < MIN_EXTENT_SIZE || (extent_size as usize) > MAX_EXTENT_SIZE {
            return Err(Error::incorrect_extent_size());
        }

        if extent_num < 2 || max_extent_num < extent_num {
            return Err(Error::incorrect_extent_settings());
        }

        let file_id = self.next_file_id()?;

        let mut file_path = std::path::PathBuf::new();
        file_path.push(&self.path);
        file_path.push(format!("{}.dat", file_id));

        if let Some(file_path) = file_path.to_str() {
            let path = file_path.to_owned();

            let mut desc = FileDesc {
                state: FileState::Initializing,
                file_id,
                extent_size,
                extent_num: 0,  
                max_extent_num,
                file_type,
            };

            self.desc_repo.add_file(desc, path.clone());

            desc.extent_num = extent_num;
            self.create_file(&desc, &path)?;

            self.desc_repo.add_extent(desc.file_id);

            for _ in 0..extent_num - 1 {
                self.add_extent(desc.file_id, FileState::Initializing)?;
            }

            // set "in use"
            self.set_file_in_use(file_id)?;
        } else {
            return Err(Error::failed_to_build_path());
        }

        Ok(file_id)
    }

    /// Return number of free info blocks for an extent of a certain size.
    pub fn calc_extent_fi_block_num(&self, extent_size: usize) -> usize {
        // we don't subtract header and fi blocks from extent size that means unused fi block can
        // be created.
        let byte_sz = (extent_size + 1) / 8;
        let hdrfilen = self.block_size - EHBLOCK_HEADER_LEN;
        if byte_sz > hdrfilen {
            let divizor = self.block_size - FIBLOCK_HEADER_LEN;
            (byte_sz - hdrfilen + divizor - 1) / divizor
        } else {
            0
        }
    }


    // private


    // calc number of fi blocks for file.
    fn calc_file_fi_block_num(&self, max_extent_num: usize) -> usize {
        let byte_sz = (max_extent_num + 1) / 8;
        let hdrfilen = self.block_size - FHBLOCK_HEADER_LEN;
        if byte_sz > hdrfilen {
            let divizor = self.block_size - FIBLOCK_HEADER_LEN;
            (byte_sz - hdrfilen + divizor - 1) / divizor
        } else {
            0
        }
    }

    // add free info blocks to file/extent header
    fn add_fi_blocks(&self, mut block_id: &mut BlockId, fi_block_num: usize, stub_pin: &AtomicU64, f: &std::fs::File) -> Result<(), Error> {
        self.zero_block_buf();

        let mut fib = FreeInfoBlock::new(*block_id, 0, Pinned::<BlockArea>::new(self.block_buf.borrow().clone(), stub_pin));

        for _ in 0..fi_block_num {
            block_id.block_id += 1;
            fib.set_id(*block_id);
            self.write_block_direct(&mut fib, &f)?;
        }

        Ok(())
    }


    // open a datastore
    fn open_datastore(path: String) -> Result<DataStore, Error> {

        let mut lock_file = std::path::PathBuf::new();

        lock_file.push(&path);
        lock_file.push(LOCK_FILE);

        // try to acquire exlusive lock for datastore
        let f = std::fs::OpenOptions::new()
                .write(true)
                .create_new(false)
                .open(lock_file)?;

        f.try_lock_exclusive()?;

        let (files, desc_set, block_size, paths)   = Self::load_files(&path)?;
        let block_buf           = BlockArea::new(alloc_buf(block_size)?, block_size);

        let desc_repo           = FileDescRepo::new(desc_set, paths);
        let block_fill_size     = block_size * 80 / 100;

        let mut seq_file_id = 2;
        for (file_id, _) in files.iter() {
            if *file_id > seq_file_id {
                seq_file_id = *file_id;
            }
        }

        let path                = String::from(path);
        let lock_file           = Arc::new(f);
        let seq_file_id         = Arc::new(AtomicU16::new(seq_file_id));

        Ok(DataStore {
            desc_repo, 
            block_size,
            block_fill_size,
            files: RefCell::new(files),
            block_buf: RefCell::new(block_buf),
            path,
            lock_file,
            seq_file_id,
            path_buf: RefCell::new(String::new()),
        })
    }


    /// find and load datastore files
    fn load_files(datastore_path: &str) -> Result<(HashMap<u16, std::fs::File>, Vec<FileDesc>, usize, Vec<String>), Error> {
        let mut desc_set = Vec::new();
        let mut paths = Vec::new();
        let mut files = HashMap::new();
        let mut block_size = DEFAULT_BLOCK_SIZE;

        fs_ops::traverse_dir(std::path::Path::new(datastore_path), false, |entry| -> Result<(), Error> {
            if entry.path().is_file() && 
                entry.path().extension() == Some(std::ffi::OsStr::new("dat"))
            {
                if let Some(path) = entry.path().to_str() {
                    let (f, desc, block_sz) = Self::open_file(&path)?;
                    files.insert(desc.file_id, f);
                    paths.push(path.to_owned());
                    desc_set.push(desc);
                    block_size = block_sz;
                }
            }

            Ok(())
        })?;

        Ok((files, desc_set, block_size, paths))
    }


    // open a file and read header information.
    fn open_file(path: &str) -> Result<(std::fs::File, FileDesc, usize), Error> {

        let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(false)
                .open(path)?;

        // crc              - u32: header block crc
        // checkpoint_csn   - u64: checkpoint csn
        // original_id      - 3 x u16: in checkpoint blocks points to the original block id.
        // magic            - u8: either magic for regular data file, or for checkpoint, or versioning data.
        // use_mark         - u8: 1 - file is initialized and is in use, 0 - otherwise.
        // block_size       - u16: block size
        // extent_size      - u16: extent_size size
        // max_extent_num   - u16: maximum allowed number of extents in this file
        // file_id          - u16: file id
        // fi_full_cnt      - u16: number of full blocks in the extent (number of set bits in free_info)
        // fi_length        - u16: number of bits in free_blocks bitmask
        let mut header = [0u8; FHBLOCK_HEADER_LEN];

        f.read_exact(&mut header)?;
        let file_type = match header[18] {
            DATA_FILE_MAGIC => FileType::DataStoreFile,
            CHECKPOINT_FILE_MAGIC => FileType::CheckpointStoreFile,
            VERSIONING_FILE_MAGIC => FileType::VersioningStoreFile,
            _ => return Err(Error::magic_mismatch())
        };

        if header[19] != FILE_IN_USE_MARK {
            return Err(Error::data_file_not_initialized());
        }

        let block_size = u16::slice_to_int(&header[20..22])?;
        let extent_size = u16::slice_to_int(&header[22..24])?;
        let max_extent_num = u16::slice_to_int(&header[24..26])?;
        let file_id = u16::slice_to_int(&header[26..28])?;
        let extent_num = if file_type == FileType::CheckpointStoreFile {

            // checkpoint store files don't track extent_num in the header
            let byte_sz = (max_extent_num + 1) / 8;
            let hdrfilen = block_size - FHBLOCK_HEADER_LEN as u16;
            let fhe_size =  if byte_sz > hdrfilen {
                let divizor = block_size - FIBLOCK_HEADER_LEN as u16;
                (byte_sz - hdrfilen + divizor - 1) / divizor
            } else {
                0
            } + 1;
            let fhe_size = fhe_size * block_size;
            ((f.seek(SeekFrom::End(0))? - fhe_size as u64) / extent_size as u64 / block_size as u64) as u16 + 1
        } else if file_type == FileType::VersioningStoreFile {
            // for versioning files existing extents are discarded
            1
        } else {
            u16::slice_to_int(&header[30..32])?
        };

        Ok((
            f,
            FileDesc {
                state: FileState::InUse,
                file_id,
                extent_size,
                extent_num,
                max_extent_num,
                file_type,
            },
            block_size as usize
        ))
    }

    fn zero_block_buf(&self) {
        let ba: &mut BlockArea = &mut self.block_buf.borrow_mut();
        for b in ba.deref_mut() { *b = 0; }
    }

    fn create_file(&self, desc: &FileDesc, path: &str) -> Result<(), Error> {
        let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(path)?;

        let mut block_id = BlockId {
            file_id: desc.file_id,
            extent_id: 0,
            block_id: 0,
        };

        let stub_pin = AtomicU64::new(1000);

        self.zero_block_buf();

        self.load_fhb_from_desc(desc);
        let mut fhb = FileHeaderBlock::new(block_id, 0, Pinned::<BlockArea>::new(self.block_buf.borrow().clone(), &stub_pin));
        fhb.set_full_cnt(1);            // extent 0 is always full
        fhb.fi_slice_mut()[0] = 0x01;
        fhb.set_block_size(self.get_block_size() as u16);
        fhb.set_in_use(false);
        self.write_block_direct(&mut fhb, &f)?;

        if fhb.fi_slice().len() * 8 < desc.max_extent_num as usize {
            let fi_block_num = self.calc_file_fi_block_num(desc.max_extent_num as usize);
            self.add_fi_blocks(&mut block_id, fi_block_num, &stub_pin, &f)?;
        }

        let mut files = self.files.borrow_mut();
        files.insert(desc.file_id, f);
        drop(files);

        Ok(())
    }

    fn set_file_in_use(&self, file_id: u16) -> Result<(), Error> {
        let block_id = BlockId {
            file_id,
            extent_id: 0,
            block_id: 0,
        };
        let stub_pin = AtomicU64::new(1000);
        let ba = self.load_block(&block_id, FileState::Initializing)?;
        let mut fhb = FileHeaderBlock::new(block_id, 0, Pinned::<BlockArea>::new(ba.clone(), &stub_pin));
        drop(ba);

        fhb.set_in_use(true);
        self.write_block(&mut fhb, FileState::Initializing)?;

        self.desc_repo.set_state(file_id, FileState::InUse);

        Ok(())
    }

    // load file header block from file description.
    fn load_fhb_from_desc(&self, desc: &FileDesc) {
        let data = &mut self.block_buf.borrow_mut();
        data[18] = desc.file_type as u8;
        data[22..24].copy_from_slice(&desc.extent_size.to_ne_bytes());
        data[24..26].copy_from_slice(&desc.max_extent_num.to_ne_bytes());
        data[26..28].copy_from_slice(&desc.file_id.to_ne_bytes());
        data[30..32].copy_from_slice(&desc.extent_num.to_ne_bytes());
    }

    /// return next free file id for a new data file
    fn next_file_id(&self) -> Result<u16, Error> {
        let mut current = self.seq_file_id.load(Ordering::Relaxed);
        loop {
            if current == std::u16::MAX {
                return Err(Error::file_id_overflow());
            }

            if let Err(ret) = self.seq_file_id.compare_exchange(current, current+1, Ordering::Relaxed, Ordering::Relaxed) {
                current = ret;
            } else {
                return Ok(current + 1);
            }
        }
    }


    // check if block exists, and seek in file to block position.
    fn seek_to_block(&self, block_id: &BlockId, file_state: FileState) -> Result<(), Error> {
        if let Some(desc) = self.desc_repo.get_by_file_id(block_id.file_id, file_state) {
            let fhe_size = self.calc_file_fi_block_num(desc.max_extent_num as usize) + 1;   // size of extent 0
            if desc.extent_num > block_id.extent_id {
                if (desc.extent_size > block_id.block_id && block_id.extent_id > 0 ) || (block_id.extent_id == 0 && block_id.block_id < fhe_size as u16) {
                    let mut files = self.files.borrow_mut();
                    let mut f = if files.contains_key(&block_id.file_id) {
                        files.get(&block_id.file_id).unwrap()
                    } else {
                        let mut path_buf = self.path_buf.borrow_mut();
                        self.desc_repo.get_path(desc.file_id, &mut path_buf);
                        let file = std::fs::OpenOptions::new()
                                .read(true)
                                .write(true)
                                .create_new(false)
                                .open(&path_buf as &str)?;

                        files.insert(block_id.file_id, file);
                        files.get(&block_id.file_id).unwrap()
                    };

                    let eff_extent_pos = if block_id.extent_id == 0 {
                        0
                    } else {
                        (block_id.extent_id - 1) as u64 * desc.extent_size as u64 + fhe_size as u64
                    };

                    let file_pos = (eff_extent_pos + block_id.block_id as u64) * self.block_size as u64;
                    f.seek(SeekFrom::Start(file_pos))?;

                    Ok(())
                } else {
                    Err(Error::block_does_not_exist())
                }
            } else {
                Err(Error::extent_does_not_exist())
            }
        } else {
            Err(Error::file_does_not_exist())
        }
    }

    // Write a block to disk without seeking.
    pub fn write_block_direct<T: BasicBlock>(&self, block: &mut T, mut f: &std::fs::File) -> Result<(), Error> {
        block.prepare_for_write();
        f.write_all(block.slice())?;
        Ok(())
    }
}


impl Drop for DataStore {

    fn drop(&mut self) {

        for (_, f) in self.files.borrow_mut().drain() {
            if let Err(e) = f.sync_all() {
                warn!("Error while closing datastore file: {}", e);
            }
        }

        if Arc::strong_count(&self.lock_file) == 1 {
            if let Err(e) = self.lock_file.unlock() {
                warn!("Error while closing datastore lock file: {}", e);
            }
        }

        let block_buf = self.block_buf.borrow();
        let v = dealloc_buf(block_buf.data_ptr(), block_buf.size());
        drop(v);
    }
}


/// State of database file.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Hash)]
pub enum FileState {
    Initializing = 0,
    InUse = 1,
}

/// Type of a database file.
#[derive(Hash, Eq, PartialEq, Clone, Copy, Debug)]
pub enum FileType {
    DataStoreFile = DATA_FILE_MAGIC as isize,
    CheckpointStoreFile = CHECKPOINT_FILE_MAGIC as isize,
    VersioningStoreFile = VERSIONING_FILE_MAGIC as isize,
}



/// Descriptive information about a data store file.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct FileDesc {
    pub state:          FileState,
    pub file_id:        u16,
    pub extent_size:    u16,
    pub extent_num:     u16,
    pub max_extent_num: u16,
    pub file_type:      FileType,
}


/// Repository of data store files metadata shared by all threads.
#[derive(Clone)]
struct FileDescRepo {
    fd:     Arc<RwLock<HashMap<u16, (FileDesc, String)>>>,
    locks:  Arc<RwLock<HashMap<u16, Mutex<()>>>>,
}

impl FileDescRepo {

    fn new(mut desc_set: Vec<FileDesc>, mut paths: Vec<String>) -> Self {
        let mut hm = HashMap::new();
        let mut hm2 = HashMap::new();
        for desc in desc_set.drain(..) {
            hm.insert(desc.file_id, (desc, paths.remove(0)));
            hm2.insert(desc.file_id, Mutex::new(()));
        }
        let fd = Arc::new(RwLock::new(hm));
        let locks = Arc::new(RwLock::new(hm2));

        FileDescRepo {
            fd,
            locks,
        }
    }

    fn add_file(&self, desc: FileDesc, path: String) {
        let mut xlock = self.fd.write().unwrap();
        xlock.insert(desc.file_id, (desc, path));
        let mut xlock = self.locks.write().unwrap();
        xlock.insert(desc.file_id, Mutex::new(()));
    }

    fn fill_files_vec(&self, files: &mut Vec<FileDesc>, file_type: FileType, state: FileState) {
        files.truncate(0);
        let slock = self.fd.read().unwrap();
        for (_, desc) in slock.iter() {
            if desc.0.file_type == file_type && desc.0.state == state {
                files.push(desc.0);
            }
        }
    }

    fn get_by_file_id(&self, file_id: u16, state: FileState) -> Option<FileDesc> {
        let slock = self.fd.read().unwrap();
        if let Some(desc) = slock.get(&file_id) {
            if desc.0.state == state {
                Some(desc.0)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn get_path(&self, file_id: u16, dst: &mut String) {
        let slock = self.fd.read().unwrap();
        if let Some(desc) = slock.get(&file_id) {
            dst.truncate(0);
            dst.push_str(&desc.1);
        }
    }

    fn add_extent(&self, file_id: u16) {
        let mut xlock = self.fd.write().unwrap();
        let mut desc = xlock.get_mut(&file_id).unwrap();
        desc.0.extent_num += 1;
    }

    fn set_state(&self, file_id: u16, state: FileState) {
        let mut xlock = self.fd.write().unwrap();
        let mut desc = xlock.get_mut(&file_id).unwrap();
        desc.0.state = state;
    }


    /// Acquire lock for a certain file_id, and return desc along the way.
    fn lock_file(&self, file_id: u16) -> Option<RwLockReadGuard<HashMap<u16, Mutex<()>>>> {
        let slock = self.locks.read().unwrap();
        if slock.contains_key(&file_id) {
            Some(slock)
        } else {
            None
        }
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use std::path::Path;

    #[test]
    fn test_datastore() {

        let dspath = "/tmp/test456343567578".to_owned();
        let block_size = 8192;
        
        if Path::new(&dspath).exists() {
            std::fs::remove_dir_all(&dspath).expect("Failed to delete test dir on cleanup");
        }
        std::fs::create_dir(&dspath).expect("Failed to create test dir");


        let conf = ConfigMt::new();
        let mut c = conf.get_conf();
        c.set_datastore_path(dspath.clone());
        drop(c);

        let mut fdset = vec![];
        let desc1 = FileDesc {
            state:          FileState::InUse,
            file_id:        3,
            extent_size:    10,
            extent_num:     2,
            max_extent_num: 2,
            file_type:      FileType::DataStoreFile,
        };
        let mut desc2 = FileDesc {
            state:          FileState::InUse,
            file_id:        4,
            extent_size:    10,
            extent_num:     2,
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

        DataStore::initialize_datastore(&dspath, block_size, &fdset).expect("Failed to init datastore");
        desc2.extent_num = 1;
        fdset[1].extent_num = 1; // versioning store has no extents allocated

        let ds = DataStore::new(conf.clone()).expect("Failed to open datastore");

        for desc in fdset {
            for extent_id in 0..desc.extent_num {
                let mut block_id = BlockId {
                    file_id: desc.file_id,
                    extent_id,
                    block_id: 0,
                };

                let bn = if extent_id == 0 {
                    ds.calc_file_fi_block_num(desc.max_extent_num as usize)
                } else {
                    desc.extent_num as usize
                };

                for bid in 0..bn {
                    block_id.block_id = bid as u16;
                    let block: Ref<BlockArea> = ds.load_block(&block_id, FileState::InUse).expect("Failed to load block");
                    drop(block);
                }
            }
        }

        let desc = ds.get_file_desc(3).expect("No file description for file id found");
        assert_eq!(desc, desc1);
        let desc = ds.get_file_desc(4).expect("No file description for file id found");
        assert_eq!(desc, desc2);
        let desc = ds.get_file_desc(5).expect("No file description for file id found");
        assert_eq!(desc, desc3);

        let bsz = ds.get_block_size();
        assert_eq!(block_size, bsz);

        let bfsz = ds.block_fill_size();
        assert_eq!(*conf.get_conf().get_block_fill_ratio() as usize * block_size / 100, bfsz);

        let block_id = BlockId {
            file_id: 3,
            extent_id: 1,
            block_id: 5,
        };
        let stub_pin =  AtomicU64::new(1000);
        let block_buf = BlockArea::new(alloc_buf(block_size).expect("Allocation failure"), block_size);
        let mut db = DataBlock::new(block_id, 0, Pinned::<BlockArea>::new(block_buf.clone(), &stub_pin));
        ds.write_block(&mut db, FileState::InUse).expect("Failed to write block to disk");
        drop(db);
        dealloc_buf(block_buf.data_ptr(), block_size);

        let mut files = Vec::new();
        ds.get_checkpoint_files(&mut files);
        assert_eq!(1, files.len());
        assert_eq!(desc3, files[0]);

        ds.get_versioning_files(&mut files);
        assert_eq!(1, files.len());
        assert_eq!(desc2, files[0]);

        ds.get_data_files(&mut files);
        assert_eq!(1, files.len());
        assert_eq!(desc1, files[0]);

        let ds2 = ds.clone().expect("Failed to clone ds");

        ds.add_extent(5, FileState::InUse).expect("Failed to add extent");
        let desc = ds.get_file_desc(5).expect("No file description for file id found");
        assert_eq!(desc.extent_num, desc3.extent_num + 1);

        let mut block_id = BlockId {
            file_id: 5,
            extent_id: desc.extent_num-1,
            block_id: 0,
        };
        for i in 0..desc.extent_size {
            block_id.block_id = i;
            let block: Ref<BlockArea> = ds.load_block(&block_id, FileState::InUse).expect("Failed to load block");
            drop(block);
        }

        let file_id = ds2.add_datafile(FileType::DataStoreFile, 16, 3, 3).expect("Failed to add a data file");
        let desc = ds.get_file_desc(file_id).expect("No file description for file id found");
        assert_eq!(FileType::DataStoreFile, desc.file_type);
        assert_eq!(16, desc.extent_size);
        assert_eq!(3, desc.extent_num);
        assert_eq!(3, desc.max_extent_num);
    }
}

