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

    pub fn mut_page(&self, id: PageId) -> PageHandle<borrow::Mutable> {
        let mut storage = self.storage.write().unwrap();
        let from = u64::from(id.checked_sub(1).expect("0 page is used as a null ptr"))
            * u64::from(self.page_size);

        let page = match storage.get_mut(from, self.page_size.into()) {
            Ok(page) => page,
            Err(_) => unimplemented!(),
        };

        let handle = PageHandle {
            id,
            _lifetime_marker: PhantomData,
            borrow: borrow::Mutable {
                raw_ptr: page.as_mut().as_mut_ptr(),
            },
        };

        handle
    }

    pub fn get_page<'a>(&'a self, id: PageId) -> Option<PageHandle<borrow::Immutable>> {
        // TODO: Check the id is in range?
        let storage = self.storage.read().unwrap();
        let from = u64::from(id.checked_sub(1).expect("0 page is used as a null ptr"))
            * u64::from(self.page_size);

        let page = storage
            .get(from, self.page_size.into())
            .expect("page not in range");

        let handle = PageHandle {
            id,
            _lifetime_marker: PhantomData,
            borrow: borrow::Immutable {
                raw_ptr: page.as_ref().as_ptr(),
            },
        };

        Some(handle)
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
    pub struct Immutable {
        pub raw_ptr: *const u8,
    }
    pub struct Mutable {
        pub raw_ptr: *mut u8,
    }
}

pub struct PageHandle<'a, Borrow> {
    id: PageId,
    _lifetime_marker: PhantomData<&'a [u8]>,
    borrow: Borrow,
}

impl<'a> PageHandle<'a, borrow::Immutable> {
    pub fn as_node<K, R>(
        &self,
        page_size: usize,
        key_buffer_size: usize,
        f: impl FnOnce(Node<K, &[u8]>) -> R,
    ) -> R
    where
        K: Key,
    {
        let page: &'a [u8] = unsafe { std::slice::from_raw_parts(self.borrow.raw_ptr, page_size) };
        let node = Node::<K, &[u8]>::from_raw(page.as_ref(), key_buffer_size);
        f(node)
    }

    unsafe fn make_mut(self) -> PageHandle<'a, borrow::Mutable> {
        let PageHandle { id, borrow, .. } = self;

        PageHandle {
            id,
            _lifetime_marker: PhantomData,
            borrow: borrow::Mutable {
                raw_ptr: borrow.raw_ptr as *mut u8,
            },
        }
    }
}

impl<'a> PageHandle<'a, borrow::Mutable> {
    pub fn as_node_mut<K, R>(
        &self,
        page_size: usize,
        key_buffer_size: usize,
        f: impl FnOnce(Node<K, &mut [u8]>) -> R,
    ) -> R
    where
        K: Key,
    {
        let page: &'a mut [u8] =
            unsafe { std::slice::from_raw_parts_mut(self.borrow.raw_ptr, page_size) };
        let node = Node::<K, &mut [u8]>::from_raw(page.as_mut(), key_buffer_size);
        f(node)
    }

    pub fn id(&self) -> PageId {
        self.id
    }
}

#[cfg(test)]
mod test {}
