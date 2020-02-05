use super::node::{marker, InternalInsertStatus, LeafInsertStatus, Node};
use super::version::InsertTransactionBuilder;
use super::{Page, PageId};
use crate::mem_page::MemPage;
use crate::{BTreeStoreError, Key, Value};
use std::marker::PhantomData;

pub enum Unpopulated {}
pub enum InLeaf {}
pub enum InPath {}

/// this is basically a stack, but it will rename pointers and interact with the builder in order to reuse
/// already cloned pages
pub(crate) struct InsertBacktrack<'txbuilder, 'txmanager: 'txbuilder, 'index: 'txmanager, K, State>
where
    K: Key,
{
    builder: &'txbuilder mut InsertTransactionBuilder<'txmanager, 'index>,
    backtrack: Vec<(Option<PageId>, Page<marker::LeafOrInternal>)>,
    new_root: Option<PageId>,
    _key_marker: PhantomData<[K]>,
    _state_marker: PhantomData<State>,
}

impl<'txbuilder, 'txmanager: 'txbuilder, 'index: 'txmanager, K>
    InsertBacktrack<'txbuilder, 'txmanager, 'index, K, Unpopulated>
where
    K: Key,
{
    pub fn new<'builder>(
        builder: &'builder mut InsertTransactionBuilder<'txmanager, 'index>,
    ) -> InsertBacktrack<'builder, 'txmanager, 'index, K, Unpopulated>
    where
        K: Key,
    {
        InsertBacktrack {
            builder,
            backtrack: vec![],
            new_root: None,
            _key_marker: PhantomData,
            _state_marker: PhantomData,
        }
    }

    pub(crate) fn search_for(
        self,
        key: &K,
    ) -> InsertBacktrack<'txbuilder, 'txmanager, 'index, K, InLeaf> {
        let mut current = self.builder.current_root();

        loop {
            let (old_id, page) = self.builder.mut_page(current).unwrap();

            let found_leaf = page.as_node(|node: Node<K, &[u8], marker::LeafOrInternal>| {
                if let Some(inode) = node.try_as_internal() {
                    let upper_pivot = match inode.keys().binary_search(key) {
                        Ok(pos) => Some(pos + 1),
                        Err(pos) => Some(pos),
                    }
                    .filter(|pos| pos < &inode.children().len());

                    if let Some(upper_pivot) = upper_pivot {
                        current = inode.children().get(upper_pivot).unwrap().clone();
                    } else {
                        let last = inode.children().len().checked_sub(1).unwrap();
                        current = inode.children().get(last).unwrap().clone();
                    }
                    false
                } else {
                    true
                }
            });

            self.backtrack.push((old_id, page));

            if found_leaf {
                break;
            }
        }

        let InsertBacktrack {
            builder,
            backtrack,
            new_root,
            ..
        } = self;

        InsertBacktrack {
            builder,
            backtrack,
            new_root,
            _key_marker: PhantomData,
            _state_marker: PhantomData,
        }
    }
}

impl<'txbuilder, 'txmanager: 'txbuilder, 'index: 'txmanager, K>
    InsertBacktrack<'txbuilder, 'txmanager, 'index, K, InLeaf>
where
    K: Key,
{
    pub(crate) fn on_next<R>(
        self,
        f: impl FnOnce(&mut Page<marker::Leaf>) -> R,
    ) -> (
        InsertBacktrack<'txbuilder, 'txmanager, 'index, K, InPath>,
        R,
    ) {
        let (old_id, last) = self.backtrack.pop().expect("no leaf found in path");

        let id = last.page_id;

        if self.backtrack.is_empty() {
            assert!(self.new_root.is_none());
            self.new_root = Some(id);
        }

        let last = last.coerce::<marker::Leaf>();
        let result = f(&mut last);
        let last = last.coerce::<marker::LeafOrInternal>();

        if let Some(old_id) = old_id {
            self.rename_parent(old_id, id);
            self.builder.add_shadow(old_id, last);
        } else {
            self.builder.shadowed_pages.insert(id, last);
        }

        let InsertBacktrack {
            builder,
            backtrack,
            new_root,
        } = self;

        let rest = InsertBacktrack {
            builder,
            backtrack,
            new_root,
            _key_marker: PhantomData,
            _state_marker: PhantomData,
        };

        (rest, result)
    }
}

impl<'txbuilder, 'txmanager: 'txbuilder, 'index: 'txmanager, K>
    InsertBacktrack<'txbuilder, 'txmanager, 'index, K, InPath>
where
    K: Key,
{
    pub(crate) fn on_next<R>(&mut self, f: impl FnOnce(&mut Page<marker::Internal>) -> R) -> R {
        let (old_id, last) = match self.backtrack.pop() {
            Some(pair) => pair,
            None => return None,
        };

        let id = last.page_id;

        if self.backtrack.is_empty() {
            assert!(self.new_root.is_none());
            self.new_root = Some(id);
        }

        let last = last.coerce();
        let result = f(&mut last);
        let last = last.coerce();

        if let Some(old_id) = old_id {
            self.rename_parent(old_id, id);
            self.builder.add_shadow(old_id, last);
        } else {
            self.builder.shadowed_pages.insert(id, last);
        }

        self.builder.shadowed_pages.get_mut(&id);

        result
    }
}

impl<'txbuilder, 'txmanager: 'txbuilder, 'index: 'txmanager, K, State>
    InsertBacktrack<'txbuilder, 'txmanager, 'index, K, State>
where
    K: Key,
{
    pub(crate) fn rename_parent(&mut self, old_id: PageId, new_id: PageId) {
        let parent = match self.backtrack.last_mut() {
            Some((_, parent)) => parent,
            None => return,
        };

        parent.as_node_mut(|mut node: Node<K, &mut [u8], marker::LeafOrInternal>| {
            // this can't fail, the parent of a node is necessarily an internal node
            let mut node = node.try_as_internal().unwrap();
            let pos_to_update = match node.children().linear_search(&old_id) {
                Some(pos) => pos,
                None => unreachable!(),
            };

            node.children_mut().update(pos_to_update, &new_id).unwrap();
        });
    }

    pub(crate) fn has_next(&self) -> bool {
        self.backtrack.last().is_some()
    }

    pub(crate) fn current_root(&self) -> PageId {
        self.builder.current_root()
    }

    pub(crate) fn add_new_node(&mut self, mem_page: MemPage) -> PageId {
        self.builder.add_new_node(mem_page)
    }

    pub(crate) fn new_root(&mut self, mem_page: MemPage, key_buffer_size: u32) {
        let id = self.builder.add_new_node(mem_page, key_buffer_size);
        self.new_root = Some(id);
    }
}

// impl<'txbuilder, 'txmanager: 'txbuilder, 'index: 'txmanager, K> Drop
//     for InsertBacktrack<'txbuilder, 'txmanager, 'index, K, InPath>
// where
//     K: Key,
// {
//     fn drop(&mut self) {
//         while let Some(_) = InsertBacktrack::<'txbuilder, 'txmanager, 'index, K>::get_next(self) {
//             ()
//         }
//
//         self.builder.current_root = self.new_root.unwrap();
//     }
// }

fn allocate<Kind>(key_size: usize, page_size: usize) {
    let uninit = MemPage::new(page_size);
    Node::<_, _, Kind>::new(key_size, uninit)
}

fn insert<'a, K: Key>(
    tx: &mut InsertTransactionBuilder<'a, 'a>,
    key: K,
    value: Value,
) -> Result<(), BTreeStoreError> {
    let backtrack = InsertBacktrack::new(tx);
    let mut backtrack = backtrack.search_for(&key);

    let (rest, needs_recurse) = backtrack.on_next(|leaf| -> Result<_, BTreeStoreError> {
        insert_in_leaf(&mut leaf, key, value).and_then(|next_step| {
            next_step.map(|(split_key, new_node)| (leaf.id(), split_key, new_node))
        })
    })?;

    if let Some((leaf_id, split_key, new_node)) = needs_recurse {
        let id = backtrack.add_new_node(new_node.to_page());

        if backtrack.has_next() {
            insert_in_internals(split_key, id, &mut backtrack)?;
        } else {
            let new_root = create_internal_node(leaf_id, id, split_key);
            backtrack.new_root(new_root.to_page());
        }
    }

    Ok(())
}

fn insert_in_leaf<K: Key>(
    leaf: &mut Page<marker::Leaf>,
    key: K,
    value: Value,
) -> Result<Option<(K, Node<K, MemPage, marker::Leaf>)>, BTreeStoreError> {
    let update = {
        let insert_status = leaf.as_node_mut(move |mut node: Node<K, &mut [u8], _>| {
            node.as_leaf_mut().insert(key, value, &mut allocate)
        });

        match insert_status {
            LeafInsertStatus::Ok => None,
            LeafInsertStatus::DuplicatedKey(_k) => {
                return Err(crate::BTreeStoreError::DuplicatedKey)
            }
            LeafInsertStatus::Split(split_key, node) => Some((split_key, node)),
        }
    };

    Ok(update)
}

// this function recurses on the backtrack splitting internal nodes as needed
fn insert_in_internals<K: Key>(
    key: K,
    to_insert: PageId,
    backtrack: &mut InsertBacktrack<K, InPath>,
) -> Result<(), BTreeStoreError> {
    // let mut split_key = key;
    // let mut right_id = to_insert;
    // loop {
    //     let (current_id, new_split_key, new_node) = {
    //         let node = backtrack.get_next().unwrap();
    //         let node_id = node.id();

    //         match node.as_node_mut(|mut node| {
    //             node.as_internal_mut()
    //                 .insert(split_key, right_id, &mut allocate)
    //         }) {
    //             InternalInsertStatus::Ok => return Ok(()),
    //             InternalInsertStatus::Split(split_key, new_node) => (node_id, split_key, new_node),
    //             _ => unreachable!(),
    //         }
    //     };

    //     let new_id =
    //         backtrack.add_new_node(new_node.to_page(), self.static_settings.key_buffer_size);

    //     if backtrack.has_next() {
    //         // set values to insert in next iteration (recurse on parent)
    //         split_key = new_split_key;
    //         right_id = new_id;
    //     } else {
    //         let left_id = current_id;
    //         let right_id = new_id;
    //         let new_root = self.create_internal_node(left_id, right_id, new_split_key);

    //         backtrack.new_root(new_root.to_page(), self.static_settings.key_buffer_size);
    //         return Ok(());
    //     }
    // }
    unimplemented!()
}

// Used when the current root needs a split
fn create_internal_node<K: Key>(
    left_child: PageId,
    right_child: PageId,
    key: K,
) -> Node<K, MemPage, marker::Internal> {
    let page = MemPage::new(self.static_settings.page_size.try_into().unwrap());
    let mut node = Node::<_, _, marker::Internal>::new(
        self.static_settings.key_buffer_size.try_into().unwrap(),
        page,
    );

    node.as_internal_mut()
        .insert_first(key, left_child, right_child);

    node
}
