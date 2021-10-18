use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::io::{Error, ErrorKind, Result};
use std::iter;
use std::path::Path;
use std::time::Instant;

use yakv::storage::*;

const CHECKPOINT_INTERVAL: u64 = 1u64 * 1024 * 1024 * 1024;
const CACHE_SIZE: usize = 128 * 1024; // 1Gb
const RAND_SEED: u64 = 2021;

#[test]
fn test_basic_ops() {
    let store = open_store("test1.dbs", Some("test1.log"));
    store.put(v(b"1"), v(b"one")).unwrap();
    store.put(v(b"2"), v(b"two")).unwrap();
    store.put(v(b"3"), v(b"three")).unwrap();
    store.put(v(b"4"), v(b"four")).unwrap();
    store.put(v(b"5"), v(b"five")).unwrap();

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
}

fn seq_benchmark(
    db_path: &str,
    log_path: Option<&str>,
    n_records: usize,
    transaction_size: usize,
) -> Result<()> {
    let payload1: Vec<u8> = vec![1u8; 100];
    let payload2: Vec<u8> = vec![2u8; 100];
    {
        let store = open_store(db_path, log_path);
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
            store.put_all(
                &mut iter::repeat_with(|| {
                    key += 1;
                    Ok((pack(key), payload2.clone()))
                })
                .take(transaction_size),
            )?;
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
        let store = reopen_store(db_path, log_path);

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
    log_path: Option<&str>,
    n_records: usize,
    transaction_size: usize,
) -> Result<()> {
    let payload1: Vec<u8> = vec![1u8; 100];
    let payload2: Vec<u8> = vec![2u8; 100];
    {
        let store = open_store(db_path, log_path);

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
            store.put_all(
                &mut iter::repeat_with(|| Ok((rand.gen::<[u8; 8]>().to_vec(), payload2.clone())))
                    .take(transaction_size),
            )?;
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
        let store = reopen_store(db_path, log_path);

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
fn seq_benchmark_wal_large_trans() {
    assert!(seq_benchmark("test2.dbs", Some("test2.log"), 1024 * 1024, 1024,).is_ok());
}

#[test]
fn seq_benchmark_wal_small_trans() {
    assert!(seq_benchmark("test3.dbs", Some("test3.log"), 1024 * 1024, 1,).is_ok());
}

#[test]
fn seq_benchmark_nowal_large_trans() {
    assert!(seq_benchmark("test4.dbs", None, 1024 * 1024, 1024,).is_ok());
}

#[test]
fn seq_benchmark_nowal_small_trans() {
    assert!(seq_benchmark("test5.dbs", None, 1024 * 1024, 1,).is_ok());
}

#[test]
fn rnd_benchmark_wal_large_trans() {
    assert!(rnd_benchmark("test6.dbs", Some("test6.log"), 1024 * 1024, 1024,).is_ok());
}

#[test]
fn rnd_benchmark_wal_small_trans() {
    assert!(rnd_benchmark("test7.dbs", Some("test7.log"), 1024 * 1024, 1,).is_ok());
}

#[test]
fn rnd_benchmark_nowal_large_trans() {
    assert!(rnd_benchmark("test8.dbs", None, 1024 * 1024, 1024,).is_ok());
}

#[test]
fn rnd_benchmark_nowal_small_trans() {
    assert!(rnd_benchmark("test9.dbs", None, 1024 * 1024, 1,).is_ok());
}

#[test]
fn test_acid() {
    let store = open_store("test10.dbs", Some("test10.log"));

    assert!(store
        .put_all(&mut (0..100).map(|key| {
            if key == 50 {
                Err(Error::new(ErrorKind::Interrupted, "Simulate failure"))
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
                Err(Error::new(ErrorKind::Interrupted, "Simulate failure"))
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
                Err(Error::new(ErrorKind::Interrupted, "simulate failure"))
            } else {
                Ok(pack(key))
            }
        }))
        .is_err());

    assert_eq!(store.iter().count(), 100);
}

fn v(b: &[u8]) -> Key {
    b.to_vec()
}

fn pack(key: u64) -> Vec<u8> {
    key.to_be_bytes().to_vec()
}

fn open_store(data_file: &str, log_file: Option<&str>) -> Storage {
    let data_path = Path::new(data_file);
    let log_path = log_file.map(|wal| Path::new(wal));
    let _ = std::fs::remove_file(&data_path);
    if let Some(log) = log_path {
        let _ = std::fs::remove_file(&log);
    }
    Storage::open(data_path, log_path, CACHE_SIZE, CHECKPOINT_INTERVAL).unwrap()
}

fn reopen_store(data_file: &str, log_file: Option<&str>) -> Storage {
    let data_path = Path::new(data_file);
    let log_path = log_file.map(|wal| Path::new(wal));
    Storage::open(data_path, log_path, CACHE_SIZE, CHECKPOINT_INTERVAL).unwrap()
}
