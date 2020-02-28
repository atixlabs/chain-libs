mod index;

use btree::{BTreeStore, Key as KeyTrait, Storeable};
use chain_core::property::{Block, BlockId, Serialize};
use chain_ser::deser::{Deserialize as _, Serialize as _};
use chain_storage::{
    error::Error,
    store::{BackLink, BlockInfo, BlockStore},
};
use std::marker::PhantomData;
use thiserror::Error;

const BLOCKS_INDEX: &'static str = "blocksdb";
const BLOCK_INFO_INDEX: &'static str = "blocksinfo";
const TAGS_INDEX: &'static str = "tagsindex";
const TAGS_KEY_SIZE: u32 = 16;
const BLOCK_KEY_SIZE: u32 = 32;
const PAGE_SIZE: u16 = 4096;

#[derive(Debug, Clone, Ord, Eq, PartialEq, PartialOrd)]
struct Key([u8; BLOCK_KEY_SIZE as usize]);

impl<'a> Storeable<'a> for Key {
    type Error = std::io::Error;
    type Output = Self;

    fn write(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
        buf.copy_from_slice(&self.0[..]);
        Ok(())
    }

    fn read(mut buf: &'a [u8]) -> Result<Self::Output, Self::Error> {
        use std::io::Read;
        let mut bytes = [0u8; 32];
        buf.read_exact(&mut bytes).expect("deserialize failed");
        Ok(Key(bytes))
    }
}
// TODO: Store a slice?
#[derive(Debug, Clone, Ord, Eq, PartialEq, PartialOrd)]
struct Tag(String);

impl<'a> Storeable<'a> for Tag {
    type Error = std::io::Error;
    type Output = Self;

    fn write(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
        buf.copy_from_slice(&self.0.as_bytes());
        Ok(())
    }

    fn read(mut buf: &'a [u8]) -> Result<Self::Output, Self::Error> {
        Ok(Tag(unsafe { String::from_utf8_unchecked(buf.to_vec()) }))
    }
}

impl<Id: BlockId> From<Id> for Key {
    fn from(id: Id) -> Key {
        let mut buf = [0u8; 32];
        id.serialize(&mut buf[..]).unwrap();
        Key(buf)
    }
}

pub struct BTreeBlockStore<'a, K, B>
where
    K: BlockId,
    B: Block,
{
    block: BTreeStore<Key>,
    block_info: BTreeStore<Key>,
    tags: BTreeStore<Tag>,
    dummy: std::marker::PhantomData<B>,
    marker: std::marker::PhantomData<&'a [K]>,
}

struct BlockInfoStruct {
    depth: u64,
    parent_hash: Option<[u8; BLOCK_KEY_SIZE as usize]>,
    fast_distance: u64,
    fast_hash: Option<[u8; BLOCK_KEY_SIZE as usize]>,
}

impl BlockInfoStruct {
    const fn ser_len() -> usize {
        8 * 2 + BLOCK_KEY_SIZE as usize * 2 as usize
    }

    fn write(self) -> [u8; Self::ser_len()] {
        use chain_ser::mempack::WriteBuf;
        let buf = vec![];
        buf.extend_from_slice(&self.depth.to_le_bytes());
        buf.extend_from_slice(&self.fast_distance.to_le_bytes());

        match self.parent_hash {
            Some(hash) => buf.extend_from_slice(&self.parent_hash),
            None => buf.extend_from_slice([0u8; BLOCK_KEY_SIZE as usize]),
        }

        match self.fast_hash {
            Some(hash) => buf.extend_from_slice(&self.fast_hash),
            None => buf.extend_from_slice([0u8; BLOCK_KEY_SIZE as usize]),
        }

        let mut ser = [0u8; Self::ser_len()];
        ser.copy_from_slice(&buf);
        ser
    }

    fn read(buf: &[u8]) -> Self {
        use std::io::Read;

        let mut depth = [0u8; std::mem::size_of::<u64>()];
        buf.read(&mut depth);
        let depth = u64::from_le_bytes(depth);

        let mut fast_distance = [0u8; std::mem::size_of::<u64>()];
        buf.read(&mut fast_distance);
        let fast_distance = u64::from_le_bytes(fast_distance);

        let mut parent_hash = [0u8; BLOCK_KEY_SIZE as usize];
        buf.read(&mut parent_hash);

        let mut fast_hash = [0u8; BLOCK_KEY_SIZE as usize];
        buf.read(&mut fast_hash);

        let parent_hash = if parent_hash == [0u8; BLOCK_KEY_SIZE as usize] {
            None
        } else {
            Some(parent_hash)
        };

        let fast_hash = if fast_hash == [0u8; BLOCK_KEY_SIZE as usize] {
            None
        } else {
            Some(fast_hash)
        };

        Self {
            depth,
            fast_distance,
            parent_hash,
            fast_hash,
        }
    }
}

impl<'a, K, B> BTreeBlockStore<'a, K, B>
where
    K: BlockId + 'a,
    B: Block,
{
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Self {
        let block: BTreeStore<Key> = BTreeStore::open(BLOCKS_INDEX)
            .or_else(|_| BTreeStore::new(BLOCKS_INDEX, BLOCK_KEY_SIZE, PAGE_SIZE))
            .unwrap();

        let block_info: BTreeStore<Key> = BTreeStore::open(BLOCK_INFO_INDEX)
            .or_else(|_| BTreeStore::new(BLOCK_INFO_INDEX, BLOCK_KEY_SIZE, PAGE_SIZE))
            .unwrap();

        let tags: BTreeStore<Tag> = BTreeStore::open(TAGS_INDEX)
            .or_else(|_| BTreeStore::new(TAGS_INDEX, TAGS_KEY_SIZE, PAGE_SIZE))
            .unwrap();

        Self {
            block,
            block_info,
            tags,
            dummy: PhantomData,
            marker: PhantomData,
        }
    }
}

fn blob_to_hash<Id: BlockId>(blob: Vec<u8>) -> Id {
    Id::deserialize(&blob[..]).unwrap()
}

impl<'a, K, B> BlockStore for BTreeBlockStore<'a, K, B>
where
    K: BlockId,
    B: Block,
{
    type Block = B;

    fn put_block_internal(&mut self, block: &B, block_info: BlockInfo<B::Id>) -> Result<(), Error> {
        self.block
            .insert(
                block_info.block_hash.into(),
                &block.serialize_as_vec().unwrap()[..],
            )
            .map_err(|_| Error::BlockAlreadyPresent)?;

        let parent = block_info
            .back_links
            .iter()
            .find(|x| x.distance == 1)
            .unwrap();

        let (fast_distance, fast_hash) =
            match block_info.back_links.iter().find(|x| x.distance != 1) {
                Some(fast_link) => (Some(fast_link.distance), Some(fast_link)),
                None => (None, None),
            };

        let parent_hash = [0u8; BLOCK_KEY_SIZE as usize];
        let fast_hash = [0u8; BLOCK_KEY_SIZE as usize];
        parent_hash.copy_from_slice(&parent.block_hash.serialize_as_vec().unwrap());
        fast_hash.copy_from_slice(&fast_link.block_hash.serialize_as_vec().unwrap());

        let block_info = BlockInfoStruct {
            depth: block_info.depth,
            parent_hash,
            fast_distance,
            fast_hash,
        };

        self.block_info.insert(
            block_info.block_hash.into(),
            block_info.serialize_as_vec().unwrap(),
        )?;

        // commit

        Ok(())
    }

    fn get_block(&self, block_hash: &B::Id) -> Result<(B, BlockInfo<B::Id>), Error> {
        unimplemented!()
        // let index = self.index.read().unwrap();

        // let row_id = index.get_block(block_hash).ok_or(Error::BlockNotFound)?;

        // let blk = self
        //     .pool
        //     .get()
        //     .map_err(|err| Error::BackendError(Box::new(err)))?
        //     .prepare_cached("select block from Blocks where rowid = ?")
        //     .map_err(|err| Error::BackendError(Box::new(err)))?
        //     .query_row(&[row_id], |row| {
        //         let x: Vec<u8> = row.get(0);
        //         B::deserialize(&x[..]).unwrap()
        //     })
        //     .map_err(|err| Error::BackendError(Box::new(err)))?;

        // let info = self.get_block_info(block_hash)?;

        // Ok((blk, info))
    }

    fn get_block_info(&self, block_hash: &B::Id) -> Result<BlockInfo<B::Id>, Error> {
        unimplemented!()
        // let index = self.index.read().unwrap();

        // let row_id = index
        //     .get_block_info(block_hash)
        //     .ok_or(Error::BlockNotFound)?;

        // self.pool
        //     .get()
        //     .map_err(|err| Error::BackendError(Box::new(err)))?
        //     .prepare_cached(
        //         "select depth, parent, fast_distance, fast_hash from BlockInfo where rowid = ?",
        //     )
        //     .map_err(|err| Error::BackendError(Box::new(err)))?
        //     .query_row(&[row_id], |row| {
        //         let mut back_links = vec![BackLink {
        //             distance: 1,
        //             block_hash: blob_to_hash(row.get(1)),
        //         }];

        //         let fast_distance: Option<i64> = row.get(2);
        //         if let Some(fast_distance) = fast_distance {
        //             back_links.push(BackLink {
        //                 distance: fast_distance as u64,
        //                 block_hash: blob_to_hash(row.get(3)),
        //             });
        //         }

        //         let depth: i64 = row.get(0);

        //         BlockInfo {
        //             block_hash: block_hash.clone(),
        //             depth: depth as u64,
        //             back_links,
        //         }
        //     })
        //     .map_err(|err| Error::BackendError(Box::new(err)))
    }

    fn put_tag(&mut self, tag_name: &str, block_hash: &B::Id) -> Result<(), Error> {
        // let mut index = self.index.write().unwrap();

        // let conn = self
        //     .pool
        //     .get()
        //     .map_err(|err| Error::BackendError(Box::new(err)))?;

        // match index.get_tag(&tag_name.to_owned()) {
        //     Some(row_id) => conn
        //         .prepare_cached("replace into Tags (rowid, name, hash) values(?, ?, ?)")
        //         .map_err(|err| Error::BackendError(Box::new(err)))?
        //         .execute(&[
        //             Value::Integer(*row_id as i64),
        //             Value::Text(tag_name.to_string()),
        //             Value::Blob(block_hash.serialize_as_vec().unwrap()),
        //         ]),
        //     None => {
        //         if index.get_block(block_hash).is_none() {
        //             return Err(Error::BlockNotFound);
        //         }

        //         conn.prepare_cached("insert into Tags (name, hash) values(?, ?)")
        //             .map_err(|err| Error::BackendError(Box::new(err)))?
        //             .execute(&[
        //                 Value::Text(tag_name.to_string()),
        //                 Value::Blob(block_hash.serialize_as_vec().unwrap()),
        //             ])
        //     }
        // }
        // .map_err(|err| Error::BackendError(Box::new(err)))?;

        // conn.prepare_cached("select last_insert_rowid()")
        //     .map_err(|err| Error::BackendError(Box::new(err)))?
        //     .query_row(rusqlite::NO_PARAMS, |row| {
        //         index
        //             .add_tag(tag_name.to_owned(), block_hash, row.get(0))
        //             .map_err(|err| Error::BackendError(Box::new(err)))
        //     })
        //     .map_err(|err| Error::BackendError(Box::new(err)))??;

        // Ok(())
        unimplemented!()
    }

    fn get_tag(&self, tag_name: &str) -> Result<Option<B::Id>, Error> {
        unimplemented!()
        // let index = self.index.read().unwrap();
        // let row_id = match index.get_tag(&tag_name.to_owned()) {
        //     Some(v) => v,
        //     None => return Ok(None),
        // };

        // match self
        //     .pool
        //     .get()
        //     .map_err(|err| Error::BackendError(Box::new(err)))?
        //     .prepare_cached("select hash from Tags where rowid = ?")
        //     .map_err(|err| Error::BackendError(Box::new(err)))?
        //     .query_row(&[row_id], |row| blob_to_hash(row.get(0)))
        // {
        //     Ok(s) => Ok(Some(s)),
        //     Err(err) => Err(Error::BackendError(Box::new(err))),
        // }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chain_storage::store::testing::Block;
    use rand_core::OsRng;

    #[test]
    pub fn put_get() {
        let mut store = BTreeBlockStore::<Block>::new("");
        chain_storage::store::testing::test_put_get(&mut store);
    }

    #[test]
    pub fn nth_ancestor() {
        let mut rng = OsRng;
        let mut store = BTreeBlockStore::<Block>::new("");
        chain_storage::store::testing::test_nth_ancestor(&mut rng, &mut store);
    }

    #[test]
    pub fn iterate_range() {
        let mut rng = OsRng;
        let mut store = BTreeBlockStore::<Block>::new("");
        chain_storage::store::testing::test_iterate_range(&mut rng, &mut store);
    }
}
