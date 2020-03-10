#[cfg(test)]
#[macro_use]
extern crate quickcheck_macros;

mod arrayview;
pub mod btreeindex;
pub mod flatfile;
mod mem_page;
pub mod storage;
use flatfile::MmapedAppendOnlyFile;

const METADATA_FILE: &'static str = "metadata";
const TREE_FILE: &'static str = "pages";
const TREE_SETTINGS_FILE: &'static str = "settings";
// const BACKUP_FILE: &'static str = "commit_backup";
const APPENDER_FILE_PATH: &'static str = "flatfile";

use mem_page::MemPage;

use crate::btreeindex::BTree;
use std::borrow::Borrow;
use std::convert::TryInto;
use std::fmt::Debug;
use std::fs::OpenOptions;
use std::path::Path;

use thiserror::Error;

// TODO: rename to file offset or something?
type Value = u64;

#[derive(Error, Debug)]
pub enum BTreeStoreError {
    #[error("couldn't create file")]
    IOError(#[from] std::io::Error),
    #[error("invalid directory {0}")]
    InvalidDirectory(&'static str),
    #[error("unknown error")]
    Unknown,
    #[error("duplicated key")]
    DuplicatedKey,
    #[error("key not found")]
    KeyNotFound,
    #[error("wrong magic number")]
    WrongMagicNumber,
}

pub struct BTreeStore<K>
where
    K: Key,
{
    index: BTree<K>,
    flatfile: MmapedAppendOnlyFile,
}

impl<K> BTreeStore<K>
where
    K: Key,
{
    pub fn new(
        path: impl AsRef<Path>,
        key_buffer_size: u32,
        page_size: u16,
    ) -> Result<BTreeStore<K>, BTreeStoreError> {
        std::fs::create_dir_all(path.as_ref())?;

        let flatfile = MmapedAppendOnlyFile::new(path.as_ref().join(APPENDER_FILE_PATH))?;

        let tree_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path.as_ref().join(TREE_FILE))?;

        let static_settings_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path.as_ref().join(TREE_SETTINGS_FILE))?;

        let metadata_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(path.as_ref().join(METADATA_FILE))?;

        let index = BTree::<K>::new(
            metadata_file,
            tree_file,
            static_settings_file,
            page_size.try_into().unwrap(),
            key_buffer_size,
        )?;

        Ok(BTreeStore { index, flatfile })
    }

    pub fn open(directory: impl AsRef<Path>) -> Result<BTreeStore<K>, BTreeStoreError> {
        if !directory.as_ref().is_dir() {
            return Err(BTreeStoreError::InvalidDirectory("path is not a directory"));
        }

        let metadata = directory.as_ref().join(METADATA_FILE);

        let file = directory.as_ref().join(TREE_FILE);

        let static_file = directory.as_ref().join(TREE_SETTINGS_FILE);

        let index = BTree::open(metadata, file, static_file)?;

        let mut flatfile = directory.as_ref().to_path_buf();
        flatfile.push(APPENDER_FILE_PATH);

        let appender = MmapedAppendOnlyFile::new(flatfile)?;

        Ok(BTreeStore {
            index,
            flatfile: appender,
        })
    }

    pub fn insert(&self, key: K, blob: &[u8]) -> Result<(), BTreeStoreError> {
        let offset = self.flatfile.append(&blob)?;

        let result = self.index.insert_one(key, offset.into());

        self.flatfile.sync()?;
        self.index.checkpoint()?;

        result
    }

    /// insert many values in one transaction (with only one fsync)
    pub fn insert_many<B: AsRef<[u8]>>(
        &self,
        iter: impl IntoIterator<Item = (K, B)>,
    ) -> Result<(), BTreeStoreError> {
        let mut offsets: Vec<(K, u64)> = vec![];
        for (key, blob) in iter {
            let offset = self.flatfile.append(blob.as_ref())?;
            offsets.push((key, offset.into()));
        }

        self.index.insert_many(offsets.drain(..))?;

        self.flatfile.sync()?;
        self.index.checkpoint()?;
        Ok(())
    }

    pub fn get(&self, key: &K) -> Result<Option<Box<[u8]>>, BTreeStoreError> {
        self.index
            .lookup(&key)
            .map(|pos| self.flatfile.get_at((pos).into()))
            .transpose()
            .map_err(|e| e.into())
    }
}

// the reference in this trait is because at some point we could just serve bytes directly as
// references to an mmaped area, and so we could just read the values directly from there (without copies)
// this trait is only used for keys currently, but the idea is to use it both for keys and blobs
pub trait Storeable<'a>: Sized {
    type Error: std::error::Error + Send + Sync;
    type Output: Borrow<Self> + 'a;
    fn write(&self, buf: &mut [u8]) -> Result<(), Self::Error>;
    fn read(buf: &'a [u8]) -> Result<Self::Output, Self::Error>;
    fn as_output(self) -> Self::Output;
}

pub trait Key: for<'a> Storeable<'a> + Ord + Clone + Debug {}
impl<T> Key for T
where
    T: for<'a> Storeable<'a> + Ord + Clone + Debug,
    for<'a> <Self as Storeable<'a>>::Output: Borrow<T>,
{
}

#[cfg(test)]
mod tests {
    use super::Storeable;
    use crate::BTreeStore;
    use byteorder::{ByteOrder, LittleEndian};
    #[derive(Debug, Clone, Ord, Eq, PartialEq, PartialOrd)]
    pub struct U64Key(pub u64);

    impl<'a> Storeable<'a> for U64Key {
        type Error = std::io::Error;
        type Output = Self;

        fn write(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
            Ok(LittleEndian::write_u64(buf, self.0))
        }

        fn read(buf: &'a [u8]) -> Result<Self::Output, Self::Error> {
            Ok(U64Key(LittleEndian::read_u64(buf)))
        }

        fn as_output(self) -> Self::Output {
            self
        }
    }

    #[test]
    fn is_send() {
        // test (at compile time) that certain types implement the auto-trait Send, either directly for
        // pointer-wrapping types or transitively for types with all Send fields

        fn is_send<T: Send>() {
            // dummy function just used for its parameterized type bound
        }

        is_send::<U64Key>();
        is_send::<BTreeStore<U64Key>>();
    }

    #[test]
    fn is_sync() {
        // test (at compile time) that certain types implement the auto-trait Sync

        fn is_sync<T: Sync>() {
            // dummy function just used for its parameterized type bound
        }

        is_sync::<U64Key>();
        is_sync::<BTreeStore<U64Key>>();
    }
}
