pub mod internal_node;
pub mod leaf_node;

use std::marker::PhantomData;

use crate::Key;
pub(crate) use internal_node::{InternalInsertStatus, InternalNode};
pub(crate) use leaf_node::{LeafInsertStatus, LeafNode};

const LEN_SIZE: usize = 8;
const TAG_SIZE: usize = 8;

pub struct Node<K, T> {
    data: T,
    key_buffer_size: usize,
    phantom: PhantomData<[K]>,
}

pub trait NodePageRef {
    fn as_node<K, R>(&self, key_buffer_size: usize, f: impl FnOnce(Node<K, &[u8]>) -> R) -> R
    where
        K: Key;
}

pub trait NodePageRefMut: NodePageRef {
    fn as_node_mut<K, R>(
        &mut self,
        key_buffer_size: usize,
        f: impl FnOnce(Node<K, &mut [u8]>) -> R,
    ) -> R
    where
        K: Key;
}

pub(crate) enum NodeTag {
    Internal = 0,
    Leaf = 1,
}

pub enum RebalanceResult {
    TakeFromLeft,
    TakeFromRight,
    MergeIntoLeft,
    MergeIntoSelf,
}

pub enum SiblingsArg<N: NodePageRef> {
    Left(N),
    Right(N),
    Both(N, N),
}

impl<N: NodePageRef> SiblingsArg<N> {
    pub fn new_from_options(left_sibling: Option<N>, right_sibling: Option<N>) -> Self {
        match (left_sibling, right_sibling) {
            (Some(left), Some(right)) => SiblingsArg::Both(left, right),
            (Some(left), None) => SiblingsArg::Left(left),
            (None, Some(right)) => SiblingsArg::Right(right),
            (None, None) => unreachable!(),
        }
    }
}

impl<'b, K, T> Node<K, T>
where
    K: Key,
    T: AsMut<[u8]> + AsRef<[u8]> + 'b,
{
    pub(crate) fn new_internal(key_buffer_size: usize, buffer: T) -> Node<K, T> {
        let mut buffer = buffer;
        buffer.as_mut()[0..TAG_SIZE].copy_from_slice(&0u64.to_le_bytes());
        InternalNode::<K, &mut [u8]>::init(key_buffer_size, &mut buffer.as_mut()[8..]);
        Node {
            data: buffer,
            key_buffer_size,
            phantom: PhantomData,
        }
    }

    pub(crate) fn new_leaf(key_buffer_size: usize, buffer: T) -> Node<K, T> {
        let mut buffer = buffer;
        buffer.as_mut()[0..TAG_SIZE].copy_from_slice(&1u64.to_le_bytes());
        LeafNode::<K, &mut [u8]>::init(key_buffer_size, &mut buffer.as_mut()[8..]);
        Node {
            data: buffer,
            key_buffer_size,
            phantom: PhantomData,
        }
    }

    pub(crate) fn try_as_internal_mut<'i: 'b>(
        &'i mut self,
    ) -> Option<InternalNode<'b, K, &mut [u8]>> {
        // the unsafe part is actually in Node::from_raw, so at this point we don't care that much
        match self.get_tag() {
            NodeTag::Internal => unsafe {
                Some(InternalNode::from_raw(
                    self.key_buffer_size,
                    &mut self.data.as_mut()[TAG_SIZE..],
                ))
            },
            NodeTag::Leaf => None,
        }
    }

    pub(crate) fn try_as_leaf_mut<'i: 'b>(&'i mut self) -> Option<LeafNode<'b, K, &mut [u8]>> {
        // the unsafe part is actually in Node::from_raw, so at this point we don't care that much
        match self.get_tag() {
            NodeTag::Leaf => unsafe {
                Some(LeafNode::from_raw(
                    self.key_buffer_size,
                    &mut self.data.as_mut()[TAG_SIZE..],
                ))
            },
            NodeTag::Internal => None,
        }
    }

    pub(crate) fn as_internal_mut(&mut self) -> InternalNode<K, &mut [u8]> {
        self.try_as_internal_mut().unwrap()
    }

    pub(crate) fn as_leaf_mut(&mut self) -> LeafNode<K, &mut [u8]> {
        self.try_as_leaf_mut().unwrap()
    }
}

impl<'b, K, T> Node<K, T>
where
    K: Key,
    T: AsRef<[u8]> + 'b,
{
    pub(crate) unsafe fn from_raw(data: T, key_buffer_size: usize) -> Node<K, T> {
        Node {
            data,
            key_buffer_size,
            phantom: PhantomData,
        }
    }

    pub(crate) fn get_tag(&self) -> NodeTag {
        let mut bytes = [0u8; LEN_SIZE];
        bytes.copy_from_slice(&self.data.as_ref()[..LEN_SIZE]);
        match u64::from_le_bytes(bytes) {
            0 => NodeTag::Internal,
            1 => NodeTag::Leaf,
            _ => unreachable!(),
        }
    }

    pub(crate) fn try_as_internal<'i: 'b>(&'i self) -> Option<InternalNode<'b, K, &[u8]>> {
        match self.get_tag() {
            NodeTag::Internal => Some(InternalNode::view(
                self.key_buffer_size,
                &self.data.as_ref()[LEN_SIZE..],
            )),
            NodeTag::Leaf => None,
        }
    }

    pub(crate) fn try_as_leaf<'i: 'b>(&'i self) -> Option<LeafNode<'b, K, &[u8]>> {
        match self.get_tag() {
            NodeTag::Leaf => Some(LeafNode::view(
                self.key_buffer_size,
                &self.data.as_ref()[LEN_SIZE..],
            )),
            NodeTag::Internal => None,
        }
    }

    pub(crate) fn as_leaf(&self) -> LeafNode<K, &[u8]> {
        self.try_as_leaf().unwrap()
    }

    pub(crate) fn as_internal(&self) -> InternalNode<K, &[u8]> {
        self.try_as_internal().unwrap()
    }
}

impl<'b, K> Node<K, crate::mem_page::MemPage>
where
    K: Key,
{
    pub(crate) fn to_page(self) -> crate::mem_page::MemPage {
        self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btreeindex::pages::{
        borrow::{Immutable, Mutable},
        PageHandle, Pages, PagesInitializationParams,
    };
    use crate::btreeindex::PageId;
    use crate::mem_page::MemPage;
    use crate::storage::MmapStorage;
    use crate::tests::U64Key;
    use std::mem::size_of;
    use tempfile::tempfile;

    impl<'a> NodePageRef for PageHandle<'a, Immutable<'a>> {
        fn as_node<K, R>(&self, key_buffer_size: usize, f: impl FnOnce(Node<K, &[u8]>) -> R) -> R
        where
            K: Key,
        {
            self.as_node(key_buffer_size, f)
        }
    }

    impl<'a> NodePageRef for PageHandle<'a, Mutable<'a>> {
        fn as_node<K, R>(&self, key_buffer_size: usize, f: impl FnOnce(Node<K, &[u8]>) -> R) -> R
        where
            K: Key,
        {
            self.as_node(key_buffer_size, f)
        }
    }

    impl<'a> NodePageRefMut for PageHandle<'a, Mutable<'a>> {
        fn as_node_mut<K, R>(
            &mut self,
            key_buffer_size: usize,
            f: impl FnOnce(Node<K, &mut [u8]>) -> R,
        ) -> R
        where
            K: Key,
        {
            self.as_node_mut(key_buffer_size, f)
        }
    }

    pub fn pages() -> Pages {
        let page_size = 8 + 8 + 3 * size_of::<U64Key>() + 5 * size_of::<PageId>() + 4 + 8;
        let storage = MmapStorage::new(tempfile().unwrap()).unwrap();
        let params = PagesInitializationParams {
            storage,
            page_size: dbg!(page_size) as u16,
            key_buffer_size: size_of::<U64Key>() as u32,
        };

        let mut pages = Pages::new(params);
        pages.extend(300).unwrap();
        pages
    }

    pub fn allocate_internal() -> Node<U64Key, MemPage> {
        let page_size = 8 + 8 + 3 * size_of::<U64Key>() + 4 * size_of::<PageId>();
        let page = MemPage::new(page_size);
        Node::new_internal(std::mem::size_of::<U64Key>(), page)
    }

    pub fn internal_page_mut(
        pages: &Pages,
        page_id: PageId,
        keys: Vec<U64Key>,
        children: Vec<u32>,
    ) -> PageHandle<Mutable> {
        assert_eq!(keys.len() + 1, children.len());

        let mut page = pages.mut_page(page_id).unwrap();

        let key_buffer_size = size_of::<U64Key>();
        page.as_slice(|slice| {
            InternalNode::<U64Key, &mut [u8]>::init(key_buffer_size, slice);
        });

        page.as_node_mut(key_buffer_size, |mut node| {
            let mut iter = keys.iter();

            if let Some(first_key) = iter.next() {
                node.as_internal_mut()
                    .insert_first((*first_key).clone(), children[0], children[1]);
            }

            for (k, c) in iter.zip(children[2..].iter()) {
                match node
                    .as_internal_mut()
                    .insert((*k).clone(), *c, &mut allocate_internal)
                {
                    InternalInsertStatus::Ok => (),
                    _ => panic!("insertion shouldn't split"),
                };
            }
        });

        page
    }

    pub fn internal_page(
        pages: &Pages,
        page_id: PageId,
        keys: Vec<U64Key>,
        children: Vec<u32>,
    ) -> PageHandle<Immutable> {
        assert_eq!(keys.len() + 1, children.len());

        {
            internal_page_mut(pages, page_id, keys, children);
        }

        pages.get_page(page_id).unwrap()
    }

    #[test]
    fn insert_internal_with_split_at_first() {
        let insertions = [2u32, 3, 1];
        let mem_size = 8 + 8 + 2 * 8 + 3 * 4;
        internal_insert_with_split(mem_size, &insertions);
    }

    #[test]
    fn insert_internal_with_split_at_middle() {
        let insertions = [1, 2, 3];
        let mem_size = 8 + 8 + 2 * 8 + 3 * 4;
        internal_insert_with_split(mem_size, &insertions);
    }

    #[test]
    fn insert_internal_with_split_at_last() {
        let insertions = [1, 3, 2];
        let mem_size = 8 + 8 + 2 * 8 + 3 * 4;
        internal_insert_with_split(mem_size, &insertions);
    }

    fn internal_insert_with_split(mem_size: usize, insertions: &[u32]) {
        let i1 = insertions[0];
        let i2 = insertions[1];
        let i3 = insertions[2];

        let buffer = MemPage::new(mem_size);
        buffer.as_ref().len();
        let mut node: Node<U64Key, MemPage> =
            Node::new_internal(std::mem::size_of::<U64Key>(), buffer);

        let mut allocate = || {
            let page = MemPage::new(mem_size);
            Node::new_internal(std::mem::size_of::<U64Key>(), page)
        };

        node.as_internal_mut()
            .insert_first(U64Key(i1 as u64), 0u32, i1);
        match node
            .as_internal_mut()
            .insert(U64Key(i2 as u64), i2, &mut allocate)
        {
            InternalInsertStatus::Ok => (),
            _ => panic!("second insertion shouldn't split"),
        };

        match node
            .as_internal_mut()
            .insert(U64Key(i3 as u64), i3, &mut allocate)
        {
            InternalInsertStatus::Split(U64Key(2), new_node) => {
                assert_eq!(new_node.as_internal().keys().len(), 1);
                assert_eq!(new_node.as_internal().keys().get(0), U64Key(3));
                assert_eq!(new_node.as_internal().children().len(), 2);
                assert_eq!(new_node.as_internal().children().get(0), 2);
                assert_eq!(new_node.as_internal().children().get(1), 3);
            }
            _ => {
                panic!("third insertion should split");
            }
        };

        assert_eq!(node.as_internal().keys().len(), 1);
        assert_eq!(node.as_internal().keys().get(0), U64Key(1));
        assert_eq!(node.as_internal().children().len(), 2);
        assert_eq!(node.as_internal().children().get(0), 0u32);
        assert_eq!(node.as_internal().children().get(1), 1u32);
    }

    #[test]
    fn insert_leaf_with_split_at_first() {
        let insertions = [2, 3, 1];
        let mem_size = 8usize + 8 + 2usize * size_of::<PageId>() + 3 * 12;
        leaf_insert_with_split(mem_size, &insertions);
    }

    #[test]
    fn insert_leaf_with_split_at_middle() {
        let insertions = [1, 2, 3];
        let mem_size = 8usize + 8 + 2usize * size_of::<PageId>() + 3 * 12;
        leaf_insert_with_split(mem_size, &insertions);
    }

    #[test]
    fn insert_leaf_with_split_at_last() {
        let insertions = [1, 3, 2];
        let mem_size = 8usize + 8 + 2usize * size_of::<PageId>() + 3 * 12;
        leaf_insert_with_split(mem_size, &insertions);
    }

    fn leaf_insert_with_split(mem_size: usize, insertions: &[u64]) {
        let i1 = insertions[0];
        let i2 = insertions[1];
        let i3 = insertions[2];

        let buffer = MemPage::new(mem_size);
        let mut node: Node<U64Key, MemPage> = Node::new_leaf(std::mem::size_of::<U64Key>(), buffer);

        let mut allocate = || {
            let page = MemPage::new(mem_size);
            Node::new_leaf(std::mem::size_of::<U64Key>(), page)
        };

        match node.as_leaf_mut().insert(U64Key(i1), i1, &mut allocate) {
            LeafInsertStatus::Ok => (),
            _ => panic!("second insertion shouldn't split"),
        };
        match node.as_leaf_mut().insert(U64Key(i2), i2, &mut allocate) {
            LeafInsertStatus::Ok => (),
            _ => panic!("second insertion shouldn't split"),
        };
        match node.as_leaf_mut().insert(U64Key(i3), i3, &mut allocate) {
            LeafInsertStatus::Split(U64Key(2), new_node) => {
                let new_leaf = new_node.as_leaf();
                assert_eq!(new_leaf.keys().len(), 2);
                assert_eq!(new_leaf.keys().get(0), U64Key(2));
                assert_eq!(new_leaf.keys().get(1), U64Key(3));
                assert_eq!(new_leaf.values().len(), 2);
                assert_eq!(new_leaf.values().get(0), 2);
                assert_eq!(new_leaf.values().get(1), 3);
            }
            _ => {
                panic!("third insertion should split");
            }
        };

        assert_eq!(node.as_leaf().keys().len(), 1);
        assert_eq!(node.as_leaf().keys().get(0), U64Key(1));
        assert_eq!(node.as_leaf().values().len(), 1);
        assert_eq!(node.as_leaf().values().get(0), 1);
    }
}
