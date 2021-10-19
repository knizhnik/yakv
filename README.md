**YAKV** is very simple persistent-key value storage implemented in Rust
using "traditional" architecture: B-Tree, buffer cache, ACID transaction, write-ahead log.
**YAKV** implements simple MURSIW (multiple-reads-single-writer) access pattern
and is first of all oriented on embedded applications.

It has minimal dependencies from other modules and contains just 1500 lines of code.
API of storage is very simple: `put/remove` methods for updating information
and `get/iter/range` range methods for retrieving it.
`put` performs update or insert: it key is not present in the storage, then it is inserted
otherwise associated value is updated. Bulk version of `put/remove` are available which accepts iterators
of pairs/keys. Bulk updates are atomic: i.e. all operations are succeeded or rejected.

Iteration can be done using bidirectional (_double-ended_) iterator and standard Rust ranges, which
allows to simply specify any ranges with open/inclusive/exclusive boundaries. Iterators are not atomic:
i.e. during iteration you can see most recent committed updates of the storage. Moreover,
if concurrent updates delete key at current iterator position, then iteration will stop before processing all results.

**YAKV** supports multi-threaded access to the storage. All threads can share single storage instance (you should use Arc for it).
Storage implementation is thread safe and all methods are immutable, so you can call them concurrently from different threads.
But only one thread can update database at each moment of time: other readers or writers will be blocked until the end of transaction.
Readers can work in parallel.

**YAKV** requires key and value to be a vector of bytes. If you want to store other types, you need to serialize them first.
If you need to preserve natural comparison order for underlying type, then you will have to use proper serializer.
For example for unsigned integer types you need to use _big-endian_ encoding (i.e. `key.to_be_bytes()`)
to make vector of bytes comparison produce the same result as comparison of two numbers.
For signed or floating point types writing such serializer may require more efforts.

**YAKV** optionally keeps write ahead log (WAL) to provide ACID.
Maintaining WAL requires `fsync` system calls to force persisting data to the non-volatile media.
Add add significant performance penalty especially for small transaction as (inserting just one pair).
But without WAL database can be corrupted in case of abnormal program termination or power failure.
To disable WAL just pass `None` instead of WAL file path.

Below is an example of **YAKV** usage":

```
let store = Storage::open(data_path, Some(log_path), CACHE_SIZE, CHECKPOINT_INTERVAL)?;

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
