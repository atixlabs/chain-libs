use super::Version;
use crate::btreeindex::{Node, Page, PageId, Pages};
use crate::mem_page::MemPage;
use crate::Key;
use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

pub mod markers {
    pub enum Immutable {}
    pub enum Mutable {}
}
pub struct PageHandle<'a, Borrow> {
    id: PageId,
    raw_ptr: *mut u8,
    _lifetime_marker: PhantomData<&'a Page>,
    _borrow_marker: PhantomData<Borrow>,
}

impl<'a> PageHandle<'a, markers::Immutable> {
    pub fn as_node<K, R>(
        &self,
        page_size: usize,
        key_buffer_size: usize,
        f: impl FnOnce(Node<K, &[u8]>) -> R,
    ) -> R
    where
        K: Key,
    {
        let page: &'a [u8] = unsafe { std::slice::from_raw_parts(self.raw_ptr, page_size) };
        let node = Node::<K, &[u8]>::from_raw(page.as_ref(), key_buffer_size);
        f(node)
    }
}

pub mod traits {
    use super::*;
    pub trait ReadTransaction {
        fn root(&self) -> PageId;
        fn get_page<'a>(&'a self, id: PageId) -> Option<PageHandle<'a, markers::Immutable>>;
    }

    pub enum MutPage {
        NeedsShadow { old_id: PageId, page: Page },
        AlreadyInTransaction(Page),
    }

    pub trait WriteTransaction: ReadTransaction {
        fn add_new_node(&mut self, mem_page: MemPage, key_buffer_size: u32) -> PageId;

        fn mut_page(&mut self, id: PageId) -> Option<MutPage>;

        fn delete_node(&mut self, id: PageId);

        fn add_shadow(&self, old_id: PageId, shadow: Page);

        fn has_next(&self) -> bool;

        /// commit creates a new version of the tree, it doesn't sync the file, but it makes the version
        /// available to new readers
        fn commit<K: Key>(self);
    }
}

pub struct ReadTransaction {
    version: Arc<Version>,
    pages: Pages,
    ownership: RefCell<HashMap<PageId, Page>>,
}

impl ReadTransaction {
    pub(super) fn new(version: Arc<Version>, pages: Pages) -> Self {
        ReadTransaction {
            version,
            pages,
            ownership: RefCell::new(HashMap::new()),
        }
    }
}

impl traits::ReadTransaction for ReadTransaction {
    fn root(&self) -> PageId {
        self.version.root
    }

    fn get_page(&self, id: PageId) -> Option<PageHandle<markers::Immutable>> {
        if let Some(page) = self.ownership.borrow_mut().get_mut(&id) {
            let id = page.id();
            let raw_ptr = page.mem_page.as_mut().as_mut_ptr();
            return Some(PageHandle {
                id,
                raw_ptr,
                _lifetime_marker: PhantomData,
                _borrow_marker: PhantomData,
            });
        }

        let page = self.pages.get_page(id);

        if let Some(page) = page {
            {
                let page = page.get_mut();
                self.ownership.borrow_mut().insert(id, page);
            }
            self.get_page(id)
        } else {
            None
        }
    }
}

/// staging area for batched insertions, it will keep track of pages already shadowed and reuse them,
/// it can be used to create a new `Version` at the end with all the insertions done atomically
pub(crate) struct InsertTransaction<'index, 'locks: 'index> {
    pages: &'index Pages,
    current_root: PageId,
    extra: HashMap<PageId, Page>,
    old_ids: Vec<PageId>,
    current: Option<usize>,
    page_manager: MutexGuard<'locks, PageManager>,
    versions: MutexGuard<'locks, VecDeque<Arc<Version>>>,
    current_version: Arc<RwLock<Arc<Version>>>,
}

impl<'txmanager, 'index: 'txmanager> InsertTransactionBuilder<'txmanager, 'index> {
    /// create a staging area for a single insert
    pub(crate) fn backtrack<'me, K>(&'me mut self) -> InsertBacktrack<'me, 'txmanager, 'index, K>
    where
        K: Key,
    {
        InsertBacktrack {
            builder: self,
            backtrack: vec![],
            new_root: None,
            phantom_key: PhantomData,
        }
    }

    pub(crate) fn delete_node(&mut self, id: PageId) {
        self.old_ids.push(id);
    }

    pub(crate) fn add_new_node(
        &mut self,
        mem_page: crate::mem_page::MemPage,
        key_buffer_size: u32,
    ) -> PageId {
        let id = self.page_manager.new_id();
        let page = Page {
            page_id: id,
            mem_page,
            key_buffer_size,
        };

        // TODO: handle this error
        self.extra.insert(page.page_id, page);
        id
    }

    pub(crate) fn current_root(&self) -> PageId {
        self.current_root
    }

    pub(crate) fn mut_page(&mut self, id: PageId) -> Option<(Option<PageId>, Page)> {
        match self.extra.remove(&id) {
            Some(page) => Some((None, page)),
            None => {
                let page = match self.pages.get_page(id).map(|page| page.get_mut()) {
                    Some(page) => page,
                    None => return None,
                };

                let mut shadow = page;
                let old_id = shadow.page_id;
                shadow.page_id = self.page_manager.new_id();

                Some((Some(old_id), shadow))
            }
        }
    }

    pub(crate) fn add_shadow(&mut self, old_id: PageId, shadow: Page) {
        self.extra.insert(shadow.page_id, shadow);
        self.old_ids.push(old_id);
    }

    pub(crate) fn has_next(&self) -> bool {
        self.current.is_some()
    }

    /// commit creates a new version of the tree, it doesn't sync the file, but it makes the version
    /// available to new readers
    pub(crate) fn commit<K>(mut self)
    where
        K: Key,
    {
        let pages = self.pages;

        for (_id, page) in self.extra.drain() {
            pages.write_page(page).unwrap();
        }

        let transaction = WriteTransaction {
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

    // not really needed because the destructor has basically the same effect right now
    pub(crate) fn abort(self) {
        unimplemented!()
    }
}

