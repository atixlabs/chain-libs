use crate::btreeindex::node::{marker, Node};
use crate::btreeindex::PageId;
use crate::storage::{MmapStorage, Storage};
use crate::Key;
use crate::MemPage;
use byteorder::{ByteOrder, LittleEndian};
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};

/// An abstraction over a paged file, Pages is kind of an array but backed from disk. Page represents at the moment
/// a heap allocated read/write page, while PageRef is a wrapper to share a read only page in an Arc
/// when we move to mmap, this things may change to take advantage of zero copy.

pub(crate) struct Pages {
    storage: RwLock<MmapStorage>,
    page_size: u16,
    // TODO: we need to remove this from here
    key_buffer_size: u32,
}

// TODO: move this unsafe impls to MmapStorage? although what is most safe is saying that RwLock<MmapStorage> is Sync + Send
unsafe impl Send for Pages {}
unsafe impl Sync for Pages {}

pub(crate) struct PagesInitializationParams {
    pub(crate) storage: MmapStorage,
    pub(crate) page_size: u16,
    pub(crate) key_buffer_size: u32,
}

impl Pages {
    pub(crate) fn new(params: PagesInitializationParams) -> Self {
        let PagesInitializationParams {
            storage,
            page_size,
            key_buffer_size,
        } = params;

        let storage = RwLock::new(storage);

        Pages {
            storage,
            page_size,
            key_buffer_size,
        }
    }

    fn read_page(&self, id: PageId) -> MemPage {
        let storage = self.storage.read().unwrap();
        let buf = storage
            .get(
                u64::from(id.checked_sub(1).expect("0 page is used as a null ptr"))
                    * u64::from(self.page_size),
                self.page_size.into(),
            )
            .unwrap();

        let page_size = self.page_size.try_into().unwrap();
        let mut page = MemPage::new(page_size);

        // Ideally, we don't want to make any copies here, but that would require making the mmaped
        // storage thread safe (specially if the mmap gets remapped)
        page.as_mut().copy_from_slice(&buf[..page_size]);

        page
    }

    pub(crate) fn write_page<T>(&self, page: Page<T>) -> Result<(), std::io::Error> {
        let mem_page = &page.mem_page;
        let page_id = page.page_id;

        let mut storage = self.storage.write().unwrap();

        storage
            .put(
                u64::from(page_id.checked_sub(1).unwrap()) * u64::try_from(mem_page.len()).unwrap(),
                &mem_page.as_ref(),
            )
            .unwrap();

        Ok(())
    }

    pub(crate) fn get_page<'a, Kind>(&'a self, id: PageId) -> Option<PageRef<Kind>> {
        // TODO: Check the id is in range?
        let page = self.read_page(id);

        let page_ref = PageRef::new(Page {
            page_id: id,
            key_buffer_size: self.key_buffer_size,
            mem_page: page,
            marker: PhantomData,
        });

        Some(PageRef(page_ref.0.clone()))
    }

    pub(crate) fn sync_file(&self) -> Result<(), std::io::Error> {
        self.storage
            .write()
            .expect("Coulnd't acquire tree index lock")
            .sync()
    }
}

pub(crate) struct Page<Kind> {
    pub page_id: PageId,
    pub key_buffer_size: u32,
    pub mem_page: MemPage,
    marker: PhantomData<Kind>,
}

#[derive(Clone)]
pub(crate) struct PageRef<Kind>(Arc<Page<Kind>>);

unsafe impl<Kind: Send> Send for PageRef<Kind> {}
unsafe impl<Kind: Sync> Sync for PageRef<Kind> {}

impl<Kind> std::ops::Deref for PageRef<Kind> {
    type Target = Arc<Page<Kind>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Kind> Page<Kind> {
    pub(crate) fn as_node<K, R>(&self, f: impl FnOnce(Node<K, &[u8], Kind>) -> R) -> R
    where
        K: Key,
    {
        let page: &[u8] = self.mem_page.as_ref();
        let node = Node::<K, &[u8], Kind>::from_raw(
            page.as_ref(),
            self.key_buffer_size.try_into().unwrap(),
        );
        f(node)
    }

    pub(crate) fn as_node_mut<K, R>(&mut self, f: impl FnOnce(Node<K, &mut [u8], Kind>) -> R) -> R
    where
        K: Key,
    {
        let page = self.mem_page.as_mut();
        let node = Node::<K, &mut [u8], Kind>::from_raw_mut(
            page.as_mut(),
            self.key_buffer_size.try_into().unwrap(),
        );
        f(node)
    }

    pub(crate) fn id(&self) -> PageId {
        self.page_id
    }
}

impl Page<marker::LeafOrInternal> {
    pub fn downcast<To>(self) -> Page<To> {
        let Page {
            page_id,
            key_buffer_size,
            mem_page,
            marker,
        } = self;

        Page {
            page_id,
            key_buffer_size,
            mem_page,
            marker: PhantomData,
        }
    }
}

impl<Kind> PageRef<Kind> {
    pub(crate) fn new(page: Page<Kind>) -> Self {
        PageRef(Arc::new(page))
    }

    pub(crate) fn as_node<K, R>(&self, f: impl FnOnce(Node<K, &[u8], Kind>) -> R) -> R
    where
        K: Key,
    {
        self.0.as_node(f)
    }

    /// Clone this given page, this is similar as Cow::get_mut()
    pub(crate) fn get_mut(&self) -> Page<Kind> {
        let page = &self.0;
        Page {
            page_id: page.page_id,
            key_buffer_size: page.key_buffer_size,
            mem_page: page.mem_page.clone(),
            marker: PhantomData,
        }
    }
}

#[cfg(test)]
mod test {}
