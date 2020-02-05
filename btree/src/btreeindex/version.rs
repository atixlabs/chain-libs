use super::metadata::Metadata;
use super::page_manager::PageManager;

use super::pages::*;
use super::PageId;
use super::{marker, Node};
use crate::mem_page::MemPage;
use crate::Key;
use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;

use std::sync::{Arc, Mutex, MutexGuard, RwLock};

pub struct TransactionManager {
    latest_version: Arc<RwLock<Arc<Version>>>,
    versions: Mutex<VecDeque<Arc<Version>>>,
    page_manager: Mutex<PageManager>,
}

pub struct Version {
    root: PageId,
    transaction: WriteTransaction,
}

/// delta-like structure, it has the list of pages that can be collected after no readers are using them
pub struct WriteTransaction {
    new_root: PageId,
    shadowed_pages: Vec<PageId>,
    next_page_id: PageId,
}

/// this has locks, so no new transaction can occur while this is synced to disk
pub struct Checkpoint<'a> {
    pub new_metadata: Metadata,
    page_manager: MutexGuard<'a, PageManager>,
    versions: MutexGuard<'a, VecDeque<Arc<Version>>>,
}

impl Version {
    pub fn root(&self) -> PageId {
        self.root
    }
}

pub enum WriteTransactionBuilder<'a, 'index> {
    Insert(InsertTransactionBuilder<'a, 'index>),
}

/// staging area for batched insertions, it will keep track of pages already shadowed and reuse them,
/// it can be used to create a new `Version` at the end with all the insertions done atomically
pub struct InsertTransactionBuilder<'index, 'locks: 'index> {
    pages: &'index Pages,
    pub current_root: PageId,
    pub shadowed_pages: HashMap<PageId, Page<marker::LeafOrInternal>>,
    old_ids: Vec<PageId>,
    current: Option<usize>,
    page_manager: MutexGuard<'locks, PageManager>,
    versions: MutexGuard<'locks, VecDeque<Arc<Version>>>,
    current_version: Arc<RwLock<Arc<Version>>>,
}

pub enum MutPage {
    AlreadyInTransaction(Page<marker::LeafOrInternal>),
    Shadows {
        old_id: PageId,
        page: Page<marker::LeafOrInternal>,
    },
}

impl TransactionManager {
    pub fn new(metadata: &Metadata) -> TransactionManager {
        let latest_version = Arc::new(RwLock::new(Arc::new(Version {
            root: metadata.root,
            transaction: WriteTransaction {
                new_root: metadata.root,
                shadowed_pages: vec![],
                next_page_id: metadata.page_manager.next_page(),
            },
        })));

        let versions = Mutex::new(VecDeque::new());
        let page_manager = Mutex::new(metadata.page_manager.clone());

        TransactionManager {
            latest_version,
            versions,
            page_manager,
        }
    }

    pub fn latest_version(&self) -> Arc<Version> {
        self.latest_version.read().unwrap().clone()
    }

    pub fn read_transaction(&self) -> Arc<Version> {
        self.latest_version()
    }

    pub fn insert_transaction<'me, 'index: 'me>(
        &'me self,
        pages: &'index Pages,
    ) -> InsertTransactionBuilder<'me, 'me> {
        let page_manager = self.page_manager.lock().unwrap();
        let versions = self.versions.lock().unwrap();

        InsertTransactionBuilder {
            current_root: self.latest_version().root(),
            shadowed_pages: HashMap::new(),
            old_ids: vec![],
            pages,
            current: None,
            page_manager,
            versions,
            current_version: self.latest_version.clone(),
        }
    }

    /// collect versions without readers, in order to reuse its pages (the ones that are shadow in transactions after that)
    pub fn collect_pending(&self) -> Option<Checkpoint> {
        let mut page_manager = self.page_manager.lock().unwrap();
        let mut versions = self.versions.lock().unwrap();

        let mut pages_to_release = vec![];
        let mut next_page_at_end = None;
        let mut new_root = None;

        while versions.len() > 0 && Arc::strong_count(versions.front().unwrap()) == 1 {
            // there is no race conditions between the check and this, because versions is locked and count == 1 means is the only reference
            let version = versions.pop_front().unwrap();
            // FIXME: remove this loop?
            for id in version.transaction.shadowed_pages.iter().cloned() {
                pages_to_release.push(id)
            }

            next_page_at_end = Some(version.transaction.next_page_id);
            new_root = Some(version.transaction.new_root);
        }

        let next_page: PageId = if let Some(next_page) = next_page_at_end {
            next_page
        } else {
            return None;
        };

        for page in pages_to_release {
            page_manager.remove_page(page);
        }

        let page_manager_to_commit = PageManager {
            next_page,
            ..page_manager.clone()
        };

        Some(Checkpoint {
            new_metadata: Metadata {
                root: new_root.unwrap(),
                page_manager: page_manager_to_commit,
            },
            page_manager,
            versions,
        })
    }
}

impl<'txmanager, 'index: 'txmanager> InsertTransactionBuilder<'txmanager, 'index> {
    pub fn delete_node(&mut self, id: PageId) {
        self.old_ids.push(id);
    }

    pub fn add_new_node(&mut self, mem_page: crate::mem_page::MemPage) -> PageId {
        let id = self.page_manager.new_id();
        let page = Page {
            page_id: id,
            mem_page,
            key_buffer_size,
            marker: PhantomData,
        };

        self.shadowed_pages.insert(page.page_id, page);
        id
    }

    pub fn current_root(&self) -> PageId {
        self.current_root
    }

    pub fn mut_page(&mut self, id: PageId) -> Option<MutPage> {
        match self.shadowed_pages.remove(&id) {
            Some(page) => Some(MutPage::AlreadyInTransaction(page)),
            None => {
                let page = match self.pages.get_page(id).map(|page| page.get_mut()) {
                    Some(page) => page,
                    None => return None,
                };

                let mut shadow = page;
                let old_id = shadow.page_id;
                shadow.page_id = self.page_manager.new_id();

                Some(MutPage::Shadows {
                    old_id,
                    page: shadow,
                })
            }
        }
    }

    pub fn add_shadow(&mut self, old_id: PageId, shadow: Page<marker::LeafOrInternal>) {
        self.shadowed_pages.insert(shadow.page_id, shadow);
        self.old_ids.push(old_id);
    }

    pub fn has_next(&self) -> bool {
        self.current.is_some()
    }

    /// commit creates a new version of the tree, it doesn't sync the file, but it makes the version
    /// available to new readers
    pub fn commit<K>(mut self)
    where
        K: Key,
    {
        let pages = self.pages;

        for (_id, page) in self.shadowed_pages.drain() {
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

#[cfg(test)]
mod tests {
    // #[test]
    // fn active_pages_do_not_get_collected() {
    //     unimplemented!()
    // }
}
