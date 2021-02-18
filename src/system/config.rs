/// Configuration


use crate::common::errors::Error;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use log::warn;


#[derive(Clone)]
pub struct ConfigMt {
    conf: Arc<Mutex<Config>>,
}

impl ConfigMt {
    pub fn new() -> ConfigMt {
        ConfigMt {
            conf: Arc::new(Mutex::new(Config::new()))
        }
    }

    pub fn get_conf(&self) -> MutexGuard<Config> {
        self.conf.lock().unwrap()
    }
}

macro_rules! gen_config {
    ( $( $name:ident, $data_type:ty, $default_val:expr, $get_fn:ident, $set_fn:ident, $str_name:literal, $conv_fn:path ), *) => {
        pub struct Config {
            $(
                $name: $data_type,
            )*
        }

        impl Config {
        
            pub fn new() -> Config {
                Config {
                    $(
                        $name: $default_val, 
                    )*
                }
            }

            $(
                pub fn $get_fn(&self) -> &$data_type {
                    &self.$name
                }

                pub fn $set_fn(&mut self, $name: $data_type) {
                    self.$name = $name;
                }
            )*

            fn process_config_file_entry(&mut self, name: &str, val: &str) -> Result<(), Error> {

                match name {
                    $(
                        $str_name => { self.$name = $conv_fn(val)?; },
                    )*
                    _ => warn!("Skipping unexpected config entry: {}", name)
                };

                Ok(())
            }
        }
    }
}


impl Config {

    pub fn load(&mut self, file_path: &str) -> Result<(), Error> {
        let f = BufReader::new(OpenOptions::new()
            .create(false)
            .write(false)
            .read(true)
            .truncate(false)
            .open(file_path)?);

        for line in f.lines() {
            if let Ok(line) = line {
                if let Ok((name, val)) = Self::process_config_file_line(&line) {
                    self.process_config_file_entry(name, val)?;
                }
            }
        }

        Ok(())
    }

    fn process_config_file_line<'a>(s: &'a str) -> Result<(&'a str, &'a str), ()> {
        let line = s.as_bytes();
        let mut p = 0;

        // skip space
        while line[p] == b' ' || line[p] == b'\t' { p += 1; };

        // check for comment line
        if line[p] == b'#' { return Err(()) }

        // read 'name' part
        let p1 = p;
        while (line[p] >= b'a' && line[p] <= b'z') 
            || (line[p] >= b'A' && line[p] <= b'Z')
            || (line[p] >= b'0' && line[p] <= b'9')
            || line[p] == b'_' || line[p] == b'-' 
        {
                p+= 1;
        }
        if p == p1 { return Err(()) }
        let p2 = p;

        // read '='
        while line[p] == b' ' || line[p] == b'\t' { p += 1; };
        if line[p] != b'=' { return Err(()) }
        while line[p] == b' ' || line[p] == b'\t' { p += 1; };

        // return 'name' and 'val'
        Ok((&s[p1..p2], &s[p..]))
    }

    fn load_string(value: &str) -> Result<String, Error> {
        Ok(String::from(value))
    }

    fn load_u32_val(value: &str) -> Result<u32, Error> {
        let ret = str::parse::<u32>(value)?;
        Ok(ret)
    }

    fn load_u64_val(value: &str) -> Result<u64, Error> {
        let ret = str::parse::<u64>(value)?;
        Ok(ret)
    }
}

gen_config![log_dir,            String, "trnlog".to_owned(),    get_log_dir,                   set_log_dir,                   "log_dir", Config::load_string,
    datastore_path,             String, ".".to_owned(),         get_datastore_path,            set_datastore_path,            "datastore_path", Config::load_string,
    max_log_file_size,          u32,    10*1024*1024,           get_max_log_file_size,         set_max_log_file_size,         "max_log_file_size", Config::load_u32_val,
    log_writer_buf_size,        u32,    1048576,                get_log_writer_buf_size,       set_log_writer_buf_size,       "log_writer_buf_size", Config::load_u32_val,
    tran_mgr_n_buckets,         u32,    128,                    get_tran_mgr_n_buckets,        set_tran_mgr_n_buckets,        "tran_mgr_n_buckets", Config::load_u32_val,
    tran_mgr_n_tran,            u32,    1024,                   get_tran_mgr_n_tran,           set_tran_mgr_n_tran,           "tran_mgr_n_tran", Config::load_u32_val,
    tran_mgr_n_obj_buckets,     u32,    128,                    get_tran_mgr_n_obj_buckets,    set_tran_mgr_n_obj_buckets,    "tran_mgr_n_obj_buckets", Config::load_u32_val,
    tran_mgr_n_obj_lock,        u32,    1024,                   get_tran_mgr_n_obj_lock,       set_tran_mgr_n_obj_lock,       "tran_mgr_n_obj_lock", Config::load_u32_val,
    block_mgr_n_lock,           u32,    1024,                   get_block_mgr_n_lock,          set_block_mgr_n_lock,          "block_mgr_n_lock", Config::load_u32_val,
    free_info_n_file_lock,      u32,    16,                     get_free_info_n_file_lock,     set_free_info_n_file_lock,     "free_info_n_file_lock", Config::load_u32_val,
    free_info_n_extent_lock,    u32,    128,                    get_free_info_n_extent_lock,   set_free_info_n_extent_lock,   "free_info_n_extent_lock", Config::load_u32_val,
    block_buf_size,             u64,    32*1024*1024,           get_block_buf_size,            set_block_buf_size,            "block_buf_size", Config::load_u64_val,
    checkpoint_data_threshold,  u64,    10*1024*1024,           get_checkpoint_data_threshold, set_checkpoint_data_threshold, "checkpoint_data_threshold", Config::load_u64_val,
    version_retain_time,        u32,    3600,                   get_version_retain_time,       set_version_retain_time,       "version_retain_time", Config::load_u32_val,
    block_fill_ratio,           u32,    80,                     get_block_fill_ratio,          set_block_fill_ratio,          "block_fill_ratio", Config::load_u32_val,
    writer_num,                 u32,    2,                      get_writer_num,                set_writer_num,                "writer_num", Config::load_u32_val];

        

        
