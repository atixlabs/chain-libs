use btree::{BTreeStore, Storeable};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tempfile::tempdir;
extern crate rand;
use crate::rand::rngs::StdRng;
use crate::rand::Rng as _;
use crate::rand::SeedableRng;
use byteorder::{ByteOrder, LittleEndian};
use std::convert::TryInto;
use std::sync::{Arc, Barrier};
use std::thread;

static SEED: u64 = 11;

#[derive(Debug, Clone, Ord, Eq, PartialEq, PartialOrd)]
pub struct U64Key(pub u64);

impl<'a> Storeable<'a> for U64Key {
    type Error = std::io::Error;
    type Output = Self;

    fn write(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
        Ok(LittleEndian::write_u64(buf, self.0))
    }
    fn read(buf: &'a [u8]) -> Result<Self::Output, Self::Error> {
        Ok(U64Key(LittleEndian::read_u64(buf)))
    }
}

fn single_key_insertion(c: &mut Criterion) {
    // TODO: Maybe create a temp file somehow?
    let dir_path = "benchmark_single_key_insertion";
    let key_size = std::mem::size_of::<U64Key>();
    let page_size = 4096;

    let tree: BTreeStore<U64Key> =
        BTreeStore::new(dir_path, key_size.try_into().unwrap(), page_size).unwrap();

    let n: u64 = 2000000;

    tree.insert_many(
        (0..n)
            .step_by(2)
            .map(|i| (U64Key(i.clone()), i.to_le_bytes())),
    )
    .expect("Couldn't insert setup values");

    let mut rng = StdRng::seed_from_u64(SEED);

    c.bench_function("insertion", |b| {
        b.iter(|| {
            let r: u64 = rng.gen();
            let key = if r % 2 == 0 { r + 1 } else { r };

            tree.insert(U64Key(key), &key.to_le_bytes()).unwrap_or(());

            assert_eq!(
                LittleEndian::read_u64(
                    tree.get(&U64Key(key))
                        .unwrap()
                        .expect("Key not found")
                        .as_ref()
                ),
                key
            );
        })
    });

    std::fs::remove_dir_all(dir_path).unwrap();
}

fn single_key_search(c: &mut Criterion) {
    let dir_path = "benchmark_single_key_search";
    let key_size = std::mem::size_of::<U64Key>();
    let page_size = 4096;

    let tree: BTreeStore<U64Key> =
        BTreeStore::new(dir_path, key_size.try_into().unwrap(), page_size).unwrap();

    let n: u64 = 2000000;

    tree.insert_many((0..n).map(|i| (U64Key(i.clone()), i.to_le_bytes())))
        .expect("Couldn't insert setup values");

    let mut rng = StdRng::seed_from_u64(SEED);

    c.bench_function("search", |b| {
        b.iter(|| {
            let key: u64 = rng.gen_range(0, n);
            assert_eq!(
                LittleEndian::read_u64(
                    tree.get(&U64Key(key))
                        .unwrap()
                        .expect("Key not found")
                        .as_ref()
                ),
                key
            );
        })
    });

    std::fs::remove_dir_all(dir_path).unwrap();
}

fn multithreaded(c: &mut Criterion) {
    let barrier = Arc::new(Barrier::new(3));
    let key_size = std::mem::size_of::<U64Key>();

    let dir = tempdir().unwrap();
    let page_size = 4096;

    let tree: Arc<BTreeStore<U64Key>> =
        Arc::new(BTreeStore::new(dir.path(), key_size.try_into().unwrap(), page_size).unwrap());

    let n: u64 = 200000;
    let mut rng = StdRng::seed_from_u64(SEED);

    tree.insert_many(
        (0..n)
            .step_by(2)
            .map(|i| (U64Key(i.clone()), random_blob(&mut rng))),
    )
    .unwrap();

    let mut handles = vec![];

    let write_thread = {
        let barrier = barrier.clone();
        let tree = tree.clone();
        let mut rng = StdRng::seed_from_u64(SEED);
        thread::spawn(move || {
            barrier.wait();
            tree.insert_many(
                (1..n)
                    .step_by(2)
                    .map(|i| (U64Key(i.clone()), random_blob(&mut rng))),
            )
            .unwrap();
        })
    };

    handles.push(Some(write_thread));

    let read_thread = {
        let barrier = barrier.clone();
        let tree = tree.clone();
        let mut rng = StdRng::seed_from_u64(SEED);
        thread::spawn(move || {
            barrier.wait();
            for _i in 1..n {
                let r: u64 = rng.gen_range(0, n);
                let key = if r % 2 == 0 { r + 1 } else { r };
                tree.get(&U64Key(key)).unwrap_or(None);
            }
        })
    };

    handles.push(Some(read_thread));

    barrier.wait();
    c.bench_function("multithreaded", move |b| {
        b.iter_custom(|iter| {
            let time = std::time::Instant::now();
            for _i in 0..iter {
                black_box({
                    let r: u64 = rng.gen_range(0, n - 1);
                    let key = if r % 2 == 0 { r } else { r + 1 };
                    tree.get(&U64Key(key)).unwrap().expect("Key not found");
                })
            }

            time.elapsed()
        });
    });

    for handle in handles.iter_mut() {
        handle.take().unwrap().join().unwrap();
    }
}

criterion_group!(
    name = insertion;
    config = Criterion::default().sample_size(10);
    targets = single_key_insertion
);

criterion_group!(
    name = search;
    config = Criterion::default().sample_size(1000);
    targets = single_key_search
);

criterion_group!(
    name = thread_scaling;
    config = Criterion::default().sample_size(100);
    targets = multithreaded
);

criterion_main!(insertion, search, thread_scaling);
