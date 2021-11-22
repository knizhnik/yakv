use anyhow::{bail, Result};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::convert::TryInto;
use std::iter;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use yakv::storage::*;

const RAND_SEED: u64 = 2021;
const N_RECORDS_LARGE: usize = 1000000;
const N_RECORDS_SMALL: usize = 10000;

#[test]
fn test_basic_ops() {
    let store = open_store("test1.dbs", false);
    {
        let mut trans = store.start_transaction();
        trans.put(&v(b"1"), &v(b"one")).unwrap();
        trans.put(&v(b"2"), &v(b"two")).unwrap();
        trans.put(&v(b"3"), &v(b"three")).unwrap();
        trans.put(&v(b"4"), &v(b"four")).unwrap();
        trans.put(&v(b"5"), &v(b"five")).unwrap();
        assert_eq!(trans.get(&v(b"1")).unwrap().unwrap(), v(b"one"));
        trans.commit().unwrap();
    }
    assert_eq!(store.get(&v(b"1")).unwrap().unwrap(), v(b"one"));

    let mut b = b'1';
    for kv in store.iter().flatten() {
        assert_eq!(kv.0, vec![b]);
        b += 1;
    }
    assert_eq!(b, b'6');

    assert_eq!(
        store
            .range(..v(b"3"))
            .flatten()
            .collect::<Vec<(Key, Value)>>(),
        [(v(b"1"), v(b"one")), (v(b"2"), v(b"two"))]
    );
    assert_eq!(
        store
            .range(v(b"3")..v(b"4"))
            .flatten()
            .collect::<Vec<(Key, Value)>>(),
        [(v(b"3"), v(b"three"))]
    );
    assert_eq!(
        store
            .range(v(b"1")..=v(b"2"))
            .flatten()
            .collect::<Vec<(Key, Value)>>(),
        [(v(b"1"), v(b"one")), (v(b"2"), v(b"two"))]
    );
    assert_eq!(
        store
            .range(v(b"5")..)
            .flatten()
            .collect::<Vec<(Key, Value)>>(),
        [(v(b"5"), v(b"five"))]
    );

    {
        let mut it = store.iter();
        assert_eq!(it.next().unwrap().unwrap(), (v(b"1"), v(b"one")));
        assert_eq!(it.next().unwrap().unwrap(), (v(b"2"), v(b"two")));
        assert_eq!(it.next_back().unwrap().unwrap(), (v(b"5"), v(b"five")));
        assert_eq!(it.next_back().unwrap().unwrap(), (v(b"4"), v(b"four")));
    }
    {
        let mut it = store.range(..v(b"4"));
        assert_eq!(it.next_back().unwrap().unwrap(), (v(b"3"), v(b"three")));
        assert_eq!(it.next_back().unwrap().unwrap(), (v(b"2"), v(b"two")));
        assert_eq!(it.next().unwrap().unwrap(), (v(b"1"), v(b"one")));
        assert_eq!(it.next().unwrap().unwrap(), (v(b"2"), v(b"two")));
    }
    {
        let mut it = store.range(v(b"1")..=v(b"2"));
        assert_eq!(it.next().unwrap().unwrap(), (v(b"1"), v(b"one")));
        assert_eq!(it.next().unwrap().unwrap(), (v(b"2"), v(b"two")));
        assert!(it.next().is_none());
        assert_eq!(it.next_back().unwrap().unwrap(), (v(b"2"), v(b"two")));
        assert_eq!(it.next_back().unwrap().unwrap(), (v(b"1"), v(b"one")));
        assert!(it.next_back().is_none());
    }
    store.put(v(b"2"), v(b"two-two")).unwrap();
    assert_eq!(store.get(&v(b"1")).unwrap().unwrap(), v(b"one"));
    assert_eq!(store.get(&v(b"2")).unwrap().unwrap(), v(b"two-two"));
    assert_eq!(store.get(&v(b"3")).unwrap().unwrap(), v(b"three"));

    store.remove(v(b"3")).unwrap();
    assert_eq!(
        store
            .range(v(b"2")..v(b"5"))
            .flatten()
            .collect::<Vec<(Key, Value)>>(),
        [(v(b"2"), v(b"two-two")), (v(b"4"), v(b"four"))]
    );
    drop(store);
    let store = reopen_store("test1.dbs", false);
    assert_eq!(
        store
            .range(v(b"2")..v(b"5"))
            .flatten()
            .collect::<Vec<(Key, Value)>>(),
        [(v(b"2"), v(b"two-two")), (v(b"4"), v(b"four"))]
    );
}

fn seq_benchmark(
    db_path: &str,
    nosync: bool,
    n_records: usize,
    transaction_size: usize,
) -> Result<()> {
    let payload1: Vec<u8> = vec![1u8; 100];
    let payload2: Vec<u8> = vec![2u8; 100];
    {
        let store = open_store(db_path, nosync);
        let mut key: u64 = 0;

        let mut now = Instant::now();
        for _ in 0..n_records / transaction_size {
            store.put_all(
                &mut iter::repeat_with(|| {
                    key += 1;
                    Ok((pack(key), payload1.clone()))
                })
                .take(transaction_size),
            )?;
        }
        println!(
            "Elapsed time for {} inserts: {:?}",
            n_records,
            now.elapsed()
        );

        now = Instant::now();
        for i in 1..=n_records {
            let key = (i as u64).to_be_bytes().to_vec();
            assert_eq!(store.get(&key).unwrap().unwrap(), payload1);
        }
        println!(
            "Elapsed time for {} hot lookups: {:?}",
            n_records,
            now.elapsed()
        );

        now = Instant::now();
        key = 0;
        for _ in 0..n_records / transaction_size {
            let mut trans = store.start_transaction();
            for _ in 0..transaction_size {
                key += 1;
                trans.put(&pack(key), &payload2)?;
            }
            trans.commit()?;
        }
        println!(
            "Elapsed time for {} updates: {:?}",
            n_records,
            now.elapsed()
        );

        for i in 1..=n_records {
            let key = (i as u64).to_be_bytes().to_vec();
            assert_eq!(store.get(&key).unwrap().unwrap(), payload2);
        }
    }
    {
        // reopen database
        let store = reopen_store(db_path, nosync);

        let mut now = Instant::now();
        for i in 1..=n_records {
            let key = (i as u64).to_be_bytes().to_vec();
            assert_eq!(store.get(&key).unwrap().unwrap(), payload2);
        }
        println!(
            "Elapsed time for {} cold lookups: {:?}",
            n_records,
            now.elapsed()
        );

        now = Instant::now();
        let mut key = 0;
        for _ in 0..n_records / transaction_size {
            store.remove_all(
                &mut iter::repeat_with(|| {
                    key += 1;
                    Ok(pack(key))
                })
                .take(transaction_size),
            )?;
        }
        println!(
            "Elapsed time for {} removes: {:?}",
            n_records,
            now.elapsed()
        );
    }
    Ok(())
}

fn rnd_benchmark(
    db_path: &str,
    nosync: bool,
    n_records: usize,
    transaction_size: usize,
) -> Result<()> {
    let payload1: Vec<u8> = vec![1u8; 100];
    let payload2: Vec<u8> = vec![2u8; 100];
    {
        let store = open_store(db_path, nosync);

        let mut rand = StdRng::seed_from_u64(RAND_SEED);
        let mut now = Instant::now();
        for _ in 0..n_records / transaction_size {
            store.put_all(
                &mut iter::repeat_with(|| Ok((rand.gen::<[u8; 8]>().to_vec(), payload1.clone())))
                    .take(transaction_size),
            )?;
        }
        println!(
            "Elapsed time for {} inserts: {:?}",
            n_records,
            now.elapsed()
        );

        now = Instant::now();
        rand = StdRng::seed_from_u64(RAND_SEED);
        for _ in 0..n_records {
            let key = rand.gen::<[u8; 8]>().to_vec();
            assert_eq!(store.get(&key).unwrap().unwrap(), payload1);
        }
        println!(
            "Elapsed time for {} hot lookups: {:?}",
            n_records,
            now.elapsed()
        );

        now = Instant::now();
        rand = StdRng::seed_from_u64(RAND_SEED);
        for _ in 0..n_records / transaction_size {
            let mut trans = store.start_transaction();
            for _ in 0..transaction_size {
                trans.put(&rand.gen::<[u8; 8]>().to_vec(), &payload2)?;
            }
            trans.commit()?;
        }
        println!(
            "Elapsed time for {} updates: {:?}",
            n_records,
            now.elapsed()
        );

        rand = StdRng::seed_from_u64(RAND_SEED);
        for _ in 0..n_records {
            let key = rand.gen::<[u8; 8]>().to_vec();
            assert_eq!(store.get(&key).unwrap().unwrap(), payload2);
        }
    }
    {
        // reopen database
        let store = reopen_store(db_path, nosync);

        let mut now = Instant::now();
        let mut rand = StdRng::seed_from_u64(RAND_SEED);
        for _ in 1..=n_records {
            let key = rand.gen::<[u8; 8]>().to_vec();
            assert_eq!(store.get(&key).unwrap().unwrap(), payload2);
        }
        println!(
            "Elapsed time for {} cold lookups: {:?}",
            n_records,
            now.elapsed()
        );

        now = Instant::now();
        rand = StdRng::seed_from_u64(RAND_SEED);
        for _ in 0..n_records / transaction_size {
            store.remove_all(
                &mut iter::repeat_with(|| Ok(rand.gen::<[u8; 8]>().to_vec()))
                    .take(transaction_size),
            )?;
        }
        println!(
            "Elapsed time for {} removes: {:?}",
            n_records,
            now.elapsed()
        );
    }
    Ok(())
}

#[test]
fn seq_benchmark_sync_large_trans() {
    assert!(seq_benchmark("test2.dbs", false, N_RECORDS_LARGE, 1000,).is_ok());
}

#[test]
fn seq_benchmark_sync_small_trans() {
    assert!(seq_benchmark("test3.dbs", false, N_RECORDS_SMALL, 1,).is_ok());
}

#[test]
fn seq_benchmark_nosync_large_trans() {
    assert!(seq_benchmark("test4.dbs", true, N_RECORDS_LARGE, 1000,).is_ok());
}

#[test]
fn seq_benchmark_nosync_small_trans() {
    assert!(seq_benchmark("test5.dbs", true, N_RECORDS_LARGE, 1,).is_ok());
}

#[test]
fn rnd_benchmark_sync_large_trans() {
    assert!(rnd_benchmark("test6.dbs", false, N_RECORDS_LARGE, 1000,).is_ok());
}

#[test]
fn rnd_benchmark_sync_small_trans() {
    assert!(rnd_benchmark("test7.dbs", false, N_RECORDS_SMALL, 1,).is_ok());
}

#[test]
fn rnd_benchmark_nosync_large_trans() {
    assert!(rnd_benchmark("test8.dbs", true, N_RECORDS_LARGE, 1000,).is_ok());
}

#[test]
fn rnd_benchmark_nosync_small_trans() {
    assert!(rnd_benchmark("test9.dbs", true, N_RECORDS_LARGE, 1,).is_ok());
}

#[test]
fn test_acid() {
    let store = open_store("test10.dbs", false);

    assert!(store
        .put_all(&mut (0..100).map(|key| {
            if key == 50 {
                bail!("Simulate failure")
            } else {
                Ok((pack(key), v(b"hello world!")))
            }
        }))
        .is_err());

    assert_eq!(store.iter().count(), 0);

    assert!(store
        .put_all(&mut (0..100).map(|key| { Ok((pack(key), v(b"hello world!"))) }))
        .is_ok());

    assert!(store
        .put_all(&mut (0..100).map(|key| {
            if key == 50 {
                bail!("Simulate failure")
            } else {
                Ok((pack(key), v(b"good bye!")))
            }
        }))
        .is_err());

    assert_eq!(
        store
            .iter()
            .flatten()
            .map(|kv| assert_eq!(kv.1, v(b"hello world!")))
            .count(),
        100
    );

    assert!(store
        .remove_all(&mut (0..100).map(|key| {
            if key == 50 {
                bail!("Simulate failure")
            } else {
                Ok(pack(key))
            }
        }))
        .is_err());

    assert_eq!(store.iter().count(), 100);
}

#[test]
fn test_recovery() {
    let data_path = "test11.dbs";
    const N_KEYS: u64 = 100000;
    {
        let _ = std::fs::remove_file(&data_path);
        let store = open_store(data_path, false);
        {
            let mut trans = store.start_transaction();
            for key in 0..N_KEYS {
                trans.put(&pack(key), &v(b"first")).unwrap();
            }
            trans.commit().unwrap();
        }
        {
            let mut trans = store.start_transaction();
            for key in 0..N_KEYS {
                trans.put(&pack(key), &v(b"two")).unwrap();
            }
            trans.commit().unwrap();
        }
        {
            let mut trans = store.start_transaction();
            for key in 0..N_KEYS {
                trans.put(&pack(key), &v(b"three")).unwrap();
            }
            // transaction shoud be implicitly aborted
        }
    }
    {
        let store = reopen_store(data_path, false);
        for key in 0..N_KEYS {
            assert_eq!(store.get(&pack(key)).unwrap().unwrap(), v(b"two"));
        }
    }
}

fn do_inserts(s: Arc<Storage>, tid: u32, n_records: u32) -> Result<()> {
    let tid_bytes = tid.to_be_bytes();
    for id in 0..n_records {
        let mut key: Vec<u8> = Vec::new();
        key.extend_from_slice(&id.to_be_bytes());
        key.extend_from_slice(&tid_bytes);
        s.put(key, tid_bytes.to_vec())?;
    }
    Ok(())
}

fn do_selects(s: Arc<Storage>, n_records: usize) {
    while s.iter().count() != n_records {}
}

#[test]
fn test_parallel_access() {
    let store = Arc::new(open_store("test1.dbs", true));
    let n_writers = 10u32;
    let n_records = 10000u32;
    let mut threads = Vec::new();
    for i in 0..n_writers {
        let s = store.clone();
        threads.push(thread::spawn(move || {
            do_inserts(s, i, n_records).unwrap();
        }));
    }
    let s = store.clone();
    threads.push(thread::spawn(move || {
        do_selects(s, n_records as usize * n_writers as usize);
    }));
    for t in threads {
        t.join().expect("Thread crashed");
    }
    let mut id = 0u32;
    let mut tid = 0u32;
    for entry in store.iter() {
        let pair = entry.unwrap();
        let key = pair.0;
        let value = pair.1;
        let curr_id = u32::from_be_bytes(key[0..4].try_into().unwrap());
        let curr_tid = u32::from_be_bytes(key[4..8].try_into().unwrap());
        let curr_value = u32::from_be_bytes(value.try_into().unwrap());
        assert_eq!(curr_id, id);
        assert_eq!(curr_tid, tid);
        assert_eq!(curr_value, tid);
        tid += 1;
        if tid == n_writers {
            tid = 0;
            id += 1;
        }
    }
    assert_eq!(id, n_records);
}

fn v(b: &[u8]) -> Key {
    b.to_vec()
}

fn pack(key: u64) -> Vec<u8> {
    key.to_be_bytes().to_vec()
}

fn open_store(data_file: &str, nosync: bool) -> Storage {
    let data_path = Path::new(data_file);
    let _ = std::fs::remove_file(&data_path);
    let mut cfg = StorageConfig::default();
    cfg.nosync = nosync;
    Storage::open(data_path, cfg).unwrap()
}

fn reopen_store(data_file: &str, nosync: bool) -> Storage {
    let data_path = Path::new(data_file);
    let mut cfg = StorageConfig::default();
    cfg.nosync = nosync;
    Storage::open(data_path, cfg).unwrap()
}
