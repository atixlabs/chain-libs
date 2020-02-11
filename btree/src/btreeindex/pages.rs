use crate::btreeindex::node::Node;
use crate::btreeindex::PageId;
use crate::storage::MmapStorage;
use crate::Key;
use crate::MemPage;
use byteorder::{ByteOrder, LittleEndian};
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};

/// An abstraction over a paged file, Pages is kind of an array but backed from disk. Page represents at the moment
/// a heap allocated read/write page, while PageRef is a wrapper to share a read only page in an Arc
/// when we move to mmap, this things may change to take advantage of zero copy.

#[derive(Clone)]
pub struct Pages {
    storage: Arc<RwLock<MmapStorage>>,
    page_size: u16,
    // TODO: we need to remove this from here
    key_buffer_size: u32,
}

// TODO: move this unsafe impls to MmapStorage? although what is most safe is saying that RwLock<MmapStorage> is Sync + Send
unsafe impl Send for Pages {}
unsafe impl Sync for Pages {}

pub struct PagesInitializationParams {
    pub storage: MmapStorage,
    pub page_size: u16,
    pub key_buffer_size: u32,
}

impl Pages {
    pub fn new(params: PagesInitializationParams) -> Self {
        let PagesInitializationParams {
            storage,
            page_size,
            key_buffer_size,
        } = params;

        let storage = Arc::new(RwLock::new(storage));

        Pages {
            storage,
            page_size,
            key_buffer_size,
        }
    }

    pub fn get_page<'a>(&'a self, id: PageId) -> Option<PageHandle<'a, borrow::Immutable>> {
        // TODO: Check the page is actually in range
        // TODO: check mutable aliasing
        let handle = PageHandle::new(id, &self.storage, u64::from(self.page_size));

        Some(handle)
    }

    pub fn mut_page<'a>(&'a self, id: PageId) -> Result<PageHandle<'a, borrow::Mutable>, ()> {
        // TODO: add checks so the same page is not mutated more than once
        let mut storage = self.storage.read().unwrap();
        let from = u64::from(id.checked_sub(1).expect("0 page is used as a null ptr"))
            * u64::from(self.page_size);

        // Make sure there is a mapped area for this page
        match unsafe { storage.get_mut(from, from + u64::from(self.page_size)) } {
            Ok(page) => Ok(PageHandle::new(
                id,
                &self.storage,
                u64::from(self.page_size),
            )),
            Err(_) => Err(()),
        }
    }

    pub fn make_shadow(&self, old_id: PageId, new_id: PageId) -> Result<(), ()> {
        unimplemented!()
    }

    pub fn extend(&mut self, to: PageId) -> Result<(), std::io::Error> {
        let mut storage = self.storage.write().unwrap();

        let from = u64::from(to.checked_sub(1).expect("0 page is used as a null ptr"))
            * u64::from(self.page_size);

        storage.resize(from + u64::from(self.page_size))
    }

    pub(crate) fn sync_file(&self) -> Result<(), std::io::Error> {
        self.storage
            .write()
            .expect("Coulnd't acquire tree index lock")
            .sync()
    }
}

pub mod borrow {
    use super::*;
    pub enum Immutable {}
    pub enum Mutable {}
}

pub struct PageHandle<'a, Borrow> {
    id: PageId,
    storage: &'a RwLock<MmapStorage>,
    borrow_marker: PhantomData<Borrow>,
    page_size: u64,
}

impl<'a, T> PageHandle<'a, T> {
    fn new(id: PageId, storage: &'a RwLock<MmapStorage>, page_size: u64) -> Self {
        PageHandle {
            id,
            storage,
            borrow_marker: PhantomData,
            page_size,
        }
    }

    fn fetch_from(&self) -> u64 {
        u64::from(
            self.id
                .checked_sub(1)
                .expect("0 page is used as a null ptr"),
        ) * u64::from(self.page_size)
    }

    pub fn id(&self) -> PageId {
        self.id
    }
}

impl<'a> PageHandle<'a, borrow::Immutable> {
    pub fn as_node<K, R>(
        &self,
        page_size: u64,
        key_buffer_size: usize,
        f: impl FnOnce(Node<K, &[u8]>) -> R,
    ) -> R
    where
        K: Key,
    {
        let storage = self.storage.read().unwrap();

        let page = unsafe { storage.get(self.fetch_from(), page_size) };

        let node = Node::<K, &[u8]>::from_raw(page.as_ref(), key_buffer_size);

        f(node)
    }
}

impl<'a> PageHandle<'a, borrow::Mutable> {
    pub fn as_node_mut<K, R>(
        &self,
        page_size: u64,
        key_buffer_size: usize,
        f: impl FnOnce(Node<K, &mut [u8]>) -> R,
    ) -> R
    where
        K: Key,
    {
        let storage = self.storage.read().unwrap();

        // resizing here would make it harder to avoid deadlocks, so we must ensure that the given page
        // is already in range
        let page = unsafe {
            storage
                .get_mut(self.fetch_from(), page_size)
                .expect("mutable page handle shouldn't need to resize")
        };

        let node = Node::<K, &mut [u8]>::from_raw(page.as_mut(), key_buffer_size);

        f(node)
    }

    pub fn as_slice(&self, f: impl FnOnce(&mut [u8])) {
        let storage = self.storage.read().unwrap();

        let page = unsafe { storage.get_mut(self.fetch_from(), self.page_size).unwrap() };

        f(page);
    }
}

#[cfg(test)]
mod test {}
