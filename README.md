**YAKV** is very simple persistent-key value storage implemented in Rust
using "traditional" architecture: B-Tree, buffer cache, ACID transactions based on copy-on-write.
**YAKV** implements simple MURSIW (multiple-reads-single-writer) access pattern
and is first of all oriented on embedded applications.

It has minimal dependencies from other modules and contains just 2k lines of code.
API of storage is very simple: `put/remove` methods for updating information
and `get/iter/range` range methods for retrieving it.
`put` performs update or insert: it key is not present in the storage, then it is inserted
otherwise associated value is updated. Bulk version of `put/remove` are available which accepts iterators
of pairs/keys. Bulk updates are atomic: i.e. all operations are succeeded or rejected.

Iteration can be done using bidirectional (_double-ended_) iterator and standard Rust ranges, which
allows to simply specify any ranges with open/inclusive/exclusive boundaries. Iterators are not atomic:
i.e. during iteration you can see most recent committed updates of the storage. Moreover,
if concurrent updates delete key at current iterator position, then iteration will stop before processing all results.

Another way of grouping operations is explicit start of transaction.
Transaction has exclusive access to the database and can perform both update and lookup operations.
At the end transaction should be explicitly committed or aborted, if it was not committed before leaving the scope,
then it is implicitly aborted.

**YAKV** supports multi-threaded access to the storage. All threads can share single storage instance (you should use Arc for it).
Storage implementation is thread safe and all methods are immutable, so you can call them concurrently from different threads.
Only one thread can update storage at each moment of time, but multiple threads can read it.
Readers can work concurrently with writer observing database state before start of write transactions.

**YAKV** requires key and value to be a vector of bytes. If you want to store other types, you need to serialize them first.
If you need to preserve natural comparison order for underlying type, then you will have to use proper serializer.
For example for unsigned integer types you need to use _big-endian_ encoding (i.e. `key.to_be_bytes()`)
to make vector of bytes comparison produce the same result as comparison of two numbers.
For signed or floating point types writing such serializer may require more efforts.

**YAKV** uses copy-on-write (COW) mechanism to provide atomic transactions.
To guarantee durability and conscience of committed data, it performs two `fsync` system calls on each commit.
It adds significant performance penalty especially for small transaction (inserting just one pair).
But without fsync database can be corrupted in case of abnormal program termination or power failure.
To disable fsync, set `nosync=true` in config.

COW mechanism allows to eliminate write-ahead-logging (WAL). But commit of transaction requires two syncs: we first need to ensure
that new version of the database is persisted and then atomically update database metadata.
So performance on short transactions will be worse than with WAL. But for long transactions COW mechanism will be more efficient
because it avoid double copying of data. So the most efficient way is to perform changes by means of large (several megabytes) transactions
at application level (`start_transaction()` method). But it is not always possible, because results of updates may be needed by other read-only transactions.
**YAKV** provides two ways of grouping several transactions:

1. Subtransactions. You can finish transaction with `subcommit()` method. It cause subtransaction commit. Other transactions will see results
of this transaction. But this changes becomes durable only once transaction is committed. Rollback aborts all subtransactions.
2. Delayed commit. If transaction is finished with `delay()` method, then changes will not be visible in read-only snapshots.
But them are visible for other write or read-only transactions. Read-only transaction is started by `read_only_transaction()` method and is executed in
MURSIW (multiple-readers-single-writer) mode. Read-only transaction can not be executed concurrently with write transaction.
So copy-on-writes have to be used to provide isolation. This is why MURSIW mode with delayed transactions can be faster than standard mode with snapshots and subtransactions, allowing concurrent
execution of readers and writer.

Below is an example of **YAKV** usage":

```
let store = Storage::open(data_path, StorageConfig::default())?;

// Simple insert/update:
let key = b"Some key".to_vec();
let value = b"Some value".to_vec();
store.put(key, value)?;

// Simple remove:
let key = b"Some key".to_vec();
store.remove(key)?;

// Bulk update
store.put_all(
	&mut iter::repeat_with(|| {
	    let key = rand.gen::<[u8; KEY_LEN]>().to_vec();
	    let value = rand.gen::<[u8; VALUE_LEN]>().to_vec();
        Ok((key, value))
	} ).take(TRANSACTION_SIZE))?;

// Bulk delete
store.remove_all(
    &mut iter::repeat_with(|| Ok(rand.gen::<[u8; 8]>().to_vec()))
       .take(TRANSACTION_SIZE),

// Explicit transaction:
{
    let trans = store.start_transaction();
    trans.put(&1u64.to_be_bytes().to_vec(), &PAYLOAD)?;
    trans.put(&2u64.to_be_bytes().to_vec(), &PAYLOAD)?;
    trans.remove(&2u64.to_be_bytes().to_vec())?;
    trans.commit()?;
}

// Simple lookup
let key = b"Some key".to_vec();
if let Some(value) = store.get(&key)? {
    println!("key={:?}, value={:?}", &key, &value);
}

// Iterate through all records:
for entry in store.iter() {
    let kv = entry?;
	println!("key={:?}, value={:?}", &kv.0, &kv.1);
}

// Range iterator:
let from_key = b"AAA".to_vec();
let till_key = b"ZZZ".to_vec();
for entry in store.range(from_key..till_key) {
    let kv = entry?;
	println!("key={:?}, value={:?}", &kv.0, &kv.1);
}

// Backward iterartion:
let till_key = b"XYZ".to_vec();
let mut it = store.range(..=till_key);
while let Some(entry) = it.next_back() {
    let kv = entry?;
	println!("key={:?}, value={:?}", &kv.0, &kv.1);
}

// Close storage
store.close()?;
```

Performance comparison:

Below are results (msec: smaller is better) of two benchmarks.

SwayDB benchmark: insertion 1M records with 8 byte key and 8 byte value.

| db      |  seq  |  rnd  |
| ------- | ----- | ----- |
| SwayDB  | 5526  | 14849 |
| LevelDB | 1107  | 7969  |
| yakv    |  594  | 1263  |


LMDB benchmark: insert+read of 1M records with 4-bytes key and 100 byte value.

| db        | seq-write | rnd-write | seq-read | rnd-read |
| --------- | --------- | --------- | -------- | -------- |
| Chronicle | 836       | 894       | 613      | 634      |
| LevelDB   | 1962      | 2089      | 2223     | 2476     |
| LMDB      | 144       | 896       | 108      | 634      |
| MapDB     | 8876      | 9304      | 9676     | 9707     |
| MVStore   | 1328      | 1675      | 7757     | 8420     |
| RocksDB   | 1800      | 1850      | 1814     | 2067     |
| Xodus     | 674       | 13981     | 3486     | 4978     |
| kv        | 5626      | 7546      | 742      | 1943     |
| yakv      | 1079      | 1617      | 549      | 1020     |


Performance dependency on transaction size (LMDB vs. YAKV or COW vs. WAL).
This benchmark inserts 1M random keys (as in LMDB benchmark),
but inserts are grouped in transactions (time in msec):

| tx size |  yakv(WAL) |  yakv(COW) |  LMDB  |
| ------- | ---------- | ---------- | ------ |
| 1000000 |       2252 |       1626 |   1853 |
| 100000  |       5860 |       3941 |   4709 |
| 10000   |      29285 |      17989 |  15718 |
| 1000    |      73223 |      54388 |  30050 |
| 100     |     131826 |     123618 |  83233 |

So for large transactions LMDB is slightly faster, for small transactions YAKV with WAL is faster
and for medium size transactions LMDB is about two times faster than YAKV.
