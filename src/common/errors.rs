//!
//! Error codes and related functions
//!


use std;


#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    repr: ErrorRepr
}


impl Error {

    fn new(kind: ErrorKind, repr: ErrorRepr) -> Error {
        Error {
            kind,
            repr
        }
    }

    /// Returns kind of error.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}


impl std::error::Error for Error { }


macro_rules! gen_error_kinds {
    ( $($kind:ident, $msg:literal), *) => {
        #[derive(Debug, PartialEq, Copy, Clone)]
        pub enum ErrorKind {
            $(
                $kind,
            )*
        }

        impl Error {
            pub fn str_desc(&self) -> &str {
                match self.kind {
                    $(
                        ErrorKind::$kind => $msg,
                    )*
                }
            }
        }
    };
}


macro_rules! gen_create_fun {
    ( $($kind:ident, $create_fun:ident), *) => {
        impl Error {
            $(
                #[inline]
                pub fn $create_fun() -> Self {
                    Self::new(ErrorKind::$kind, ErrorRepr::Simple)
                }
            )*
        }
    };

    ( $($kind:ident, $create_fun:ident, $fun_arg:path), *) => {
        impl Error {
            $(
                #[inline]
                pub fn $create_fun(e: $fun_arg) -> Self {
                    Self::new(ErrorKind::$kind, ErrorRepr::$kind(e))
                }
            )*
        }
    }
}


macro_rules! gen_error_repr {
    ($( $from_type:ty, $error_kind:ident, $fun_name:ident ), *) => {

        #[derive(Debug)]
        enum ErrorRepr {
            Simple,
            $(
                $error_kind($from_type),
            )*
        }

        $(
            impl From<$from_type> for Error {
                fn from(error: $from_type) -> Self {
                    Error::new(ErrorKind::$error_kind, ErrorRepr::$error_kind(error))
                }
            }
        )*

        impl std::fmt::Display for Error {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
                write!(f, "{}", self.str_desc())?;
                match &self.repr {
                    $(
                        ErrorRepr::$error_kind(e) => write!(f, ": {}", e),
                    )*
                    _ => Ok(())
                }?;
        
                Ok(())
            }
        }

        impl Error {
        
            $(
                pub fn $fun_name(self) -> Option<$from_type> {
                    match self.repr {
                        ErrorRepr::$error_kind(e) => Some(e),
                        _ => None
                    }
                }
            )*
        }

    };
}


gen_error_kinds![
    NoError                         , "no error",
    NotImplemented                  , "not implemented",
    IoError                         , "io error",
    IncorrectAllocationSize         , "incorrect allocation size",
    PathIsTooLong                   , "path is too long",
    Utf8ValidationError             , "utf-8 validation error",
    AllocationFailure               , "allocation failure",
    IncorrectLayout                 , "incorrect layout",
    ArrayIsFull                     , "array is full",
    MagicMismatch                   , "magic mismatch", 
    DataFileNotInitialized          , "data file was not properly initialized",
    SliceConversionError            , "unexpected conversion failure",
    LockError                       , "lock failure",
    FailedToBuildPath               , "failed to build path",
    FileIdOverflow                  , "overflow of file_id for data files",
    ExtentLimitReached              , "data file extent limit reached",
    IncorrectExtentSize             , "incorrect extent size (less than minimum size, or greater than maximum size)",
    IncorrectBlockSize              , "incorrect block size (less than minimum size, or greater than maximum size, or not is power of two)",
    IncorrectExtentSettings         , "initial extent number is too small or greater than the maximum allowed number of extents",
    LoggerIsTerminated              , "logger is terminated",
    StringParseError                , "string parse error",
    FileNotOpened                   , "file is not opened",
    ExtentDoesNotExist              , "extent with specified id does not exist",
    FileDoesNotExist                , "file with specified id does not exist",
    BlockDoesNotExist               , "block with specified id does not exist",
    ObjectDoesNotExist              , "object with specified id does not exist",
    ObjectIsDeleted                 , "object with specified id was deleted",
    UnexpectedCheckpoint            , "unexpected checkpoint record appeared while reading transaction log",
    BlockChecksumMismatch           , "block checksum mismatch",
    TimeOperationError              , "internal error while performing operation on time value",
    DbSizeLimitReached              , "configured database size limits have been reached, can't add more data",
    TryLockError                    , "lock operation was not successful",
    BlockCrcMismatch                , "crc cehcksum failed for block",
    Timeout                         , "operations timed out",
    CheckpointStoreSizeLimitReached , "checkpoint store size limit reached"
];


gen_create_fun![
    NoError                         , no_error                      , 
    NotImplemented                  , not_implemented               , 
    IncorrectAllocationSize         , incorrect_allocation_size     , 
    PathIsTooLong                   , path_is_too_long              , 
    AllocationFailure               , allocation_failure            , 
    ArrayIsFull                     , array_is_full                 , 
    MagicMismatch                   , magic_mismatch                , 
    DataFileNotInitialized          , data_file_not_initialized     , 
    LockError                       , lock_error                    , 
    FailedToBuildPath               , failed_to_build_path          , 
    FileIdOverflow                  , file_id_overflow              , 
    ExtentLimitReached              , extent_limit_reached          , 
    IncorrectExtentSize             , incorrect_extent_size         , 
    IncorrectBlockSize              , incorrect_block_size          ,  
    IncorrectExtentSettings         , incorrect_extent_settings     ,
    LoggerIsTerminated              , logger_is_terminated          ,
    FileNotOpened                   , file_not_opened               , 
    ExtentDoesNotExist              , extent_does_not_exist         ,
    FileDoesNotExist                , file_does_not_exist           ,
    BlockDoesNotExist               , block_does_not_exist          ,
    ObjectDoesNotExist              , object_does_not_exist         ,
    ObjectIsDeleted                 , object_is_deleted             ,
    UnexpectedCheckpoint            , unexpected_checkpoint         ,
    BlockChecksumMismatch           , block_checksum_mismatch       ,
    DbSizeLimitReached              , db_size_limit_reached         ,
    TryLockError                    , try_lock_error                ,
    BlockCrcMismatch                , block_crc_mismatch            ,
    Timeout                         , timeout,
    CheckpointStoreSizeLimitReached , checkpoint_store_size_limit_reached
];


gen_create_fun![
    IoError                     , io_error                      , std::io::Error,
    Utf8ValidationError         , utf8_validation_error         , std::str::Utf8Error,
    IncorrectLayout             , incorrect_layout              , std::alloc::LayoutErr,
    SliceConversionError        , slice_conversion_error        , std::array::TryFromSliceError,
    StringParseError            , string_parse_error            , std::num::ParseIntError,
    TimeOperationError          , time_operation_error          , std::time::SystemTimeError
];


gen_error_repr![
    std::io::Error, IoError, io_err,
    std::str::Utf8Error, Utf8ValidationError, utf8_err,
    std::alloc::LayoutErr, IncorrectLayout, layout_err,
    std::array::TryFromSliceError, SliceConversionError, slice_err,
    std::num::ParseIntError, StringParseError, string_parse_err,
    std::time::SystemTimeError, TimeOperationError, time_operation_err
];

