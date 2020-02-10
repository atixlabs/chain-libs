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
        fn get_page<'a>(&'a self, id: PageId) -> Option<PageHandle<'a, Immutable>>;
    }

    pub trait WriteTransaction: ReadTransaction {
        fn add_new_node(&mut self, mem_page: MemPage, key_buffer_size: u32) -> PageId;

        fn mut_page(&mut self, id: PageId) -> Option<MutPage>;

        fn delete_node(&mut self, id: PageId);

        /// commit creates a new version of the tree, it doesn't sync the file, but it makes the version
        /// available to new readers
        fn commit<K: Key>(self);
    }
}

pub struct ReadTransaction {
    version: Arc<Version>,
    pages: Pages,
}

impl ReadTransaction {
    pub(super) fn new(version: Arc<Version>, pages: Pages) -> Self {
        ReadTransaction { version, pages }
    }
}

impl traits::ReadTransaction for ReadTransaction {
    fn root(&self) -> PageId {
        self.version.root
    }

    fn get_page(&self, id: PageId) -> Option<PageHandle<Immutable>> {
        // if let Some(page) = self.ownership.borrow_mut().get_mut(&id) {
        //     let id = page.id();
        //     let raw_ptr = page.mem_page.as_mut().as_mut_ptr();
        //     return Some(PageHandle {
        //         id,
        //         raw_ptr,
        //         _lifetime_marker: PhantomData,
        //         borrow: Immutable {},
        //     });
        // }

        let page = self.pages.get_page(id);

        // if let Some(page) = page {
        //     {
        //         let page = page.get_mut();
        //         self.ownership.borrow_mut().insert(id, page);
        //     }
        //     self.get_page(id)
        // } else {
        //     None
        // }
        unimplemented!()
    }
}

/// staging area for batched insertions, it will keep track of pages already shadowed and reuse them,
/// it can be used to create a new `Version` at the end with all the insertions done atomically
pub(crate) struct InsertTransaction<'locks> {
    pub current_root: PageId,
    pub extra: HashMap<PageId, ()>,
    pub old_ids: Vec<PageId>,
    pub current: Option<usize>,
    pub page_manager: MutexGuard<'locks, PageManager>,
    pub versions: MutexGuard<'locks, VecDeque<Arc<Version>>>,
    pub current_version: Arc<RwLock<Arc<Version>>>,
    pub version: Arc<Version>,
    pub pages: Pages,
}

impl<'locks> traits::ReadTransaction for InsertTransaction<'locks> {
    fn root(&self) -> PageId {
        self.current_root
    }

    fn get_page(&self, id: PageId) -> Option<PageHandle<Immutable>> {
        unimplemented!()
        // if let Some(page) = self.extra.get_mut(&id) {
        //     let id = page.id();
        //     let raw_ptr = page.mem_page.as_mut().as_mut_ptr();
        //     return Some(PageHandle {
        //         id,
        //         raw_ptr,
        //         _lifetime_marker: PhantomData,
        //         borrow: Immutable {},
        //     });
        // }

        // let page = self.pages.get_page(id);

        // if let Some(page) = page {
        //     {
        //         let page = page.get_mut();
        //         self.extra.insert(id, page);
        //     }
        //     self.get_page(id)
        // } else {
        //     None
        // }
    }
}

impl<'locks> traits::WriteTransaction for InsertTransaction<'locks> {
    fn add_new_node(&mut self, mem_page: crate::mem_page::MemPage, key_buffer_size: u32) -> PageId {
        unimplemented!()
        // let id = self.page_manager.new_id();
        // let page = Page {
        //     page_id: id,
        //     mem_page,
        //     key_buffer_size,
        // };

        // // TODO: handle this error
        // self.extra.insert(page.page_id, page);
        // id
    }

    fn mut_page(&mut self, id: PageId) -> Option<MutPage> {
        let already_fetched = self.extra.contains_key(&id);

        // let handle = self
        //     .get_page(id)
        //     .map(|inmutable_handle| inmutable_handle.make_mut());

        // handle.map(|handle| {
        //     if already_fetched {
        //         MutPage::AlreadyInTransaction(handle)
        //     } else {
        //         let old_id = handle.id;
        //         self.old_ids.push(old_id);
        //         handle.id = self.page_manager.new_id();
        //         MutPage::NeedsShadow {
        //             old_id,
        //             page: handle,
        //         }
        //     }
        // })
        unimplemented!()
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
        unimplemented!()
        // let pages = self.pages;

        // for (_id, page) in self.extra.drain() {
        //     pages.write_page(page).unwrap();
        // }

        // let transaction = super::WriteTransaction {
        //     new_root: self.current_root,
        //     shadowed_pages: self.old_ids,
        //     // Pages allocated at the end, basically
        //     next_page_id: self.page_manager.next_page(),
        // };

        // let mut current_version = self.current_version.write().unwrap();

        // self.versions.push_back(current_version.clone());

        // *current_version = Arc::new(Version {
        //     root: self.current_root,
        //     transaction,
        // });
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
