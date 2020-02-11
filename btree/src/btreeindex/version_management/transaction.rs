use super::Version;
use crate::btreeindex::{
    borrow::{Immutable, Mutable},
    page_manager::PageManager,
    PageHandle, PageId, Pages,
};
use crate::mem_page::MemPage;
use crate::Key;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::sync::{Arc, MutexGuard, RwLock};
use traits::ReadTransaction as _;

pub enum MutPage<'a> {
    NeedsShadow {
        old_id: PageId,
        page: PageHandle<'a, Mutable>,
    },
    AlreadyInTransaction(PageHandle<'a, Mutable>),
}

pub mod traits {
    use super::*;
    pub trait ReadTransaction {
        fn root(&self) -> PageId;
        fn get_page(&self, id: PageId) -> Option<PageHandle<Immutable>>;
    }

    pub trait WriteTransaction: ReadTransaction {
        fn add_new_node(&mut self, mem_page: MemPage, key_buffer_size: u32) -> PageId;

        fn mut_page(&mut self, id: PageId) -> MutPage;

        fn delete_node(&mut self, id: PageId);

        /// commit creates a new version of the tree, it doesn't sync the file, but it makes the version
        /// available to new readers
        fn commit<K: Key>(self);
    }
}

pub struct ReadTransaction {
    version: Arc<Version>,
    pages: Arc<RwLock<Pages>>,
}

impl ReadTransaction {
    pub(super) fn new(version: Arc<Version>, pages: Arc<RwLock<Pages>>) -> Self {
        ReadTransaction { version, pages }
    }
}

impl traits::ReadTransaction for ReadTransaction {
    fn root(&self) -> PageId {
        self.version.root
    }

    fn get_page(&self, id: PageId) -> Option<PageHandle<Immutable>> {
        let pages = self.pages.read().unwrap();
        pages.get_page(id)
    }
}

/// staging area for batched insertions, it will keep track of pages already shadowed and reuse them,
/// it can be used to create a new `Version` at the end with all the insertions done atomically
pub(crate) struct InsertTransaction<'locks> {
    pub current_root: PageId,
    pub shadows: HashMap<PageId, PageId>,
    pub old_ids: Vec<PageId>,
    pub current: Option<usize>,
    pub page_manager: MutexGuard<'locks, PageManager>,
    pub pages: Arc<RwLock<Pages>>,
    pub versions: MutexGuard<'locks, VecDeque<Arc<Version>>>,
    pub current_version: Arc<RwLock<Arc<Version>>>,
    pub version: Arc<Version>,
}

impl<'locks> traits::ReadTransaction for InsertTransaction<'locks> {
    fn root(&self) -> PageId {
        self.current_root
    }

    fn get_page(&self, id: PageId) -> Option<PageHandle<Immutable>> {
        // TODO: this should return from extra
        self.pages.read().unwrap().get_page(id)
    }
}

impl<'locks> traits::WriteTransaction for InsertTransaction<'locks> {
    fn add_new_node(&mut self, mem_page: crate::mem_page::MemPage, key_buffer_size: u32) -> PageId {
        let id = self.page_manager.new_id();

        let pages = self.pages.read().unwrap();

        pages.mut_page(id).unwrap().as_slice(|page| {
            page.copy_from_slice(mem_page.as_ref());
        });

        id
    }

    fn mut_page(&mut self, id: PageId) -> MutPage {
        let new_id = self.shadows.get(&id);

        if let Some(shadow_id) = new_id {
            // we need a mapping from old_ids -> new_ids
            let pages = self.pages.read().unwrap();
            let handle = pages
                .mut_page(*shadow_id)
                .expect("already fetched transaction was not allocated");

            MutPage::AlreadyInTransaction(handle)
        } else {
            let pages = self.pages.read().unwrap();
            let old_id = id;
            self.old_ids.push(old_id);
            let new_id = self.page_manager.new_id();

            let result = pages.make_shadow(old_id, new_id);

            match result {
                Ok(()) => (),
                Err(()) => unimplemented!("resize storage"),
            };

            // infallible
            let handle = pages.mut_page(new_id).unwrap();

            MutPage::NeedsShadow {
                old_id,
                page: handle,
            }
        }
    }

    fn delete_node(&mut self, id: PageId) {
        self.old_ids.push(id);
    }

    /// commit creates a new version of the tree, it doesn't sync the file, but it makes the version
    /// available to new readers
    fn commit<K>(mut self)
    where
        K: Key,
    {
        let transaction = super::WriteTransaction {
            new_root: self.current_root,
            shadowed_pages: self.old_ids,
            // Pages allocated at the end, basically
            next_page_id: self.page_manager.next_page(),
        };

        let mut current_version = self.current_version.write().unwrap();

        self.versions.push_back(current_version.clone());

        *current_version = Arc::new(Version {
            root: self.current_root,
            transaction,
        });
    }
}

impl<'txmanager> InsertTransaction<'txmanager> {
    /// create a staging area for a single insert
    pub(crate) fn backtrack<'me, K>(&'me mut self) -> super::InsertBacktrack<'me, 'txmanager, K>
    where
        K: Key,
    {
        super::InsertBacktrack {
            builder: self,
            backtrack: vec![],
            new_root: None,
            phantom_key: PhantomData,
        }
    }

    pub(crate) fn current_root(&self) -> PageId {
        self.current_root
    }
}
