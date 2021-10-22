use crc32c::*;
use fs2::FileExt;
use std::cmp::Ordering;
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::Result;
use std::iter;
use std::ops::Bound::*;
use std::ops::{Bound, RangeBounds};
use std::os::unix::prelude::FileExt as UnixFileExt;
use std::path::Path;
use std::sync::{Condvar, Mutex, MutexGuard, RwLock, RwLockWriteGuard};

type PageId = u32; // address of page in the file
type BufferId = u32; // index of page in buffer cache
type LSN = u64; // logical serial number: monotonically increased counter of database state changes
type ItemPointer = usize; // offset within page, actually only 16 bits is enough, but use usize to avoid type casts when used as an index

///
/// Storage key type. If you want to use some other types as a key, you will have to serialize them (for example using serde).
/// As far as vectors are compared using byte-by-byte comparison, you need to take it in account during serialization if
/// you need to preserve order for original type. For example unsigned integer type should be serialized as big-endian (most significant byte first).
///
pub type Key = Vec<u8>;

///
/// Storage value type. All other types should be serialized to vector of bytes.
///
pub type Value = Vec<u8>;

const PAGE_SIZE: usize = 8192;
const MAGIC: u32 = 0xBACE2021;
const VERSION: u32 = 1;
const METADATA_SIZE: usize = 6 * 4;
const PAGE_HEADER_SIZE: usize = 2; // now page header contains just number of items in the page
const MAX_VALUE_LEN: usize = PAGE_SIZE / 4; // assume that pages may fit at least 3 items
const MAX_KEY_LEN: usize = u8::MAX as usize; // should fit in one byte
const N_BUSY_EVENTS: usize = 8; // number of condition variables used for waitingread completion

// Flags for page state
const PAGE_RAW: u16 = 1; // buffer content is uninitialized
const PAGE_BUSY: u16 = 2; // buffer is loaded for the disk
const PAGE_DIRTY: u16 = 4; // buffer was updates
const PAGE_WAIT: u16 = 8; // some thread waits until buffer is loaded

enum LookupOp<'a> {
    First,
    Last,
    Next,
    Prev,
    GreaterOrEqual(&'a Key),
}

#[derive(PartialEq)]
enum DatabaseState {
    InRecovery,
    Opened,
    Closed,
    Corrupted,
}

#[derive(PartialEq)]
enum AccessMode {
    ReadOnly,
    WriteOnly,
}

///
/// Status of transaction
///
#[derive(PartialEq)]
pub enum TransactionStatus {
    InProgress,
    Committed,
    Aborted,
}

///
/// Explicitely started transaction. Storage can be updated in autocommit mode
/// or using explicitly started transaction.
///
pub struct Transaction<'a> {
    pub status: TransactionStatus,
    storage: &'a Storage,
    db: RwLockWriteGuard<'a, Database>,
}

///
/// Status of automatic database recovery after open.
/// In case of start after normal shutdown all fields should be zero.
///
#[derive(Clone, Copy, Default)]
pub struct RecoveryStatus {
    /// number of recovered transactions
    pub recovered_transactions: u64,
    /// size of WAL at the moment of recovery
    pub wal_size: u64,
    /// position of last recovery transaction in WAL
    pub recovery_end: u64,
}

///
/// Abstract storage bidirectional iterator
///
pub struct StorageIterator<'a> {
    storage: &'a Storage,
    trans: Option<&'a Transaction<'a>>,
    from: Bound<Key>,
    till: Bound<Key>,
    left: TreePath,
    right: TreePath,
}

//
// Position in the page saved during tree traversal
//
struct PagePos {
    pid: PageId,
    pos: usize,
}

//
// Path in B-Tree to the current iterator's element
//
struct TreePath {
    curr: Option<(Key, Value)>, // current (key,value) pair if any
    result: Option<Result<(Key, Value)>>,
    stack: Vec<PagePos>, // stack of positions in B-Tree
    lsn: LSN,            // LSN of last operation
}

impl TreePath {
    fn new() -> TreePath {
        TreePath {
            curr: None,
            result: None,
            stack: Vec::new(),
            lsn: 0,
        }
    }
}

impl<'a> StorageIterator<'a> {
    fn next_locked(&mut self, db: &Database) -> Option<<StorageIterator<'a> as Iterator>::Item> {
        if self.left.stack.len() == 0 {
            match &self.from {
                Bound::Included(key) => {
                    self.storage
                        .lookup(db, LookupOp::GreaterOrEqual(key), &mut self.left)
                }
                Bound::Excluded(key) => {
                    self.storage
                        .lookup(db, LookupOp::GreaterOrEqual(key), &mut self.left);
                    if let Some((curr_key, _value)) = &self.left.curr {
                        if curr_key == key {
                            self.storage.lookup(db, LookupOp::Next, &mut self.left);
                        }
                    }
                }
                Bound::Unbounded => self.storage.lookup(db, LookupOp::First, &mut self.left),
            }
        } else {
            self.storage.lookup(db, LookupOp::Next, &mut self.left);
        }
        if let Some((curr_key, _value)) = &self.left.curr {
            match &self.till {
                Bound::Included(key) => {
                    if curr_key > key {
                        return None;
                    }
                }
                Bound::Excluded(key) => {
                    if curr_key >= key {
                        return None;
                    }
                }
                Bound::Unbounded => {}
            }
        }
        self.left.result.take()
    }

    fn next_back_locked(
        &mut self,
        db: &Database,
    ) -> Option<<StorageIterator<'a> as Iterator>::Item> {
        if self.right.stack.len() == 0 {
            match &self.till {
                Bound::Included(key) => {
                    self.storage
                        .lookup(db, LookupOp::GreaterOrEqual(key), &mut self.right);
                    if let Some((curr_key, _value)) = &self.right.curr {
                        if curr_key > key {
                            self.storage.lookup(db, LookupOp::Prev, &mut self.right);
                        }
                    } else {
                        self.storage.lookup(db, LookupOp::Last, &mut self.right);
                    }
                }
                Bound::Excluded(key) => {
                    self.storage
                        .lookup(db, LookupOp::GreaterOrEqual(key), &mut self.right);
                    if let Some((curr_key, _value)) = &self.right.curr {
                        if curr_key >= key {
                            self.storage.lookup(db, LookupOp::Prev, &mut self.right);
                        }
                    } else {
                        self.storage.lookup(db, LookupOp::Last, &mut self.right);
                    }
                }
                Bound::Unbounded => self.storage.lookup(db, LookupOp::Last, &mut self.right),
            }
        } else {
            self.storage.lookup(db, LookupOp::Prev, &mut self.right);
        }
        if let Some((curr_key, _value)) = &self.right.curr {
            match &self.from {
                Bound::Included(key) => {
                    if curr_key < key {
                        return None;
                    }
                }
                Bound::Excluded(key) => {
                    if curr_key <= key {
                        return None;
                    }
                }
                Bound::Unbounded => {}
            }
        }
        self.right.result.take()
    }
}

impl<'a> Iterator for StorageIterator<'a> {
    type Item = Result<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(trans) = self.trans {
            assert!(trans.status == TransactionStatus::InProgress);
            self.next_locked(&trans.db)
        } else {
            let db = self.storage.db.read().unwrap();
            self.next_locked(&db)
        }
    }
}

impl<'a> DoubleEndedIterator for StorageIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(trans) = self.trans {
            assert!(trans.status == TransactionStatus::InProgress);
            self.next_back_locked(&trans.db)
        } else {
            let db = self.storage.db.read().unwrap();
            self.next_back_locked(&db)
        }
    }
}

///
/// Persistent key-value storage implementation
/// Update operations are atomic, select operations are non-atomic and observe most recent database state.
///
pub struct Storage {
    db: RwLock<Database>,
    buf_mgr: Mutex<BufferManager>,
    busy_events: [Condvar; N_BUSY_EVENTS],
    pool: Vec<RwLock<PageData>>,
    checkpoint_interval: u64,
    file: File,
    log: Option<File>,
}

//
// Page header in buffer manager
//
#[derive(Clone, Copy, Default)]
struct PageHeader {
    id: PageId,
    collision: BufferId, // collision chain
    // LRU l2-list
    next: BufferId,
    prev: BufferId,
    access_count: u16,
    state: u16, // bitmask of PAGE_RAW, PAGE_DIRTY, ...
}

impl PageHeader {
    fn new() -> PageHeader {
        Default::default()
    }
}

//
// Database metadata
//
#[derive(Copy, Clone)]
struct Metadata {
    magic: u32,   // storage magic
    version: u32, // storage format version
    free: PageId, // L1 list of free pages
    size: PageId, // size of database (pages)
    root: PageId, // B-Tree root page
    height: u32,  // height of B-Tree
}

impl Metadata {
    fn pack(self) -> [u8; METADATA_SIZE] {
        unsafe { std::mem::transmute::<Metadata, [u8; METADATA_SIZE]>(self) }
    }
    fn unpack(buf: &[u8]) -> Metadata {
        unsafe {
            std::mem::transmute::<[u8; METADATA_SIZE], Metadata>(
                buf[0..METADATA_SIZE].try_into().unwrap(),
            )
        }
    }
}

//
// Database shared state
//
struct Database {
    meta: Metadata,           // cached metadata (stored in root page)
    lsn: LSN,                 // database modification counter
    state: DatabaseState,     // database state
    wal_pos: u64,             // current opsition in log file
    recovery: RecoveryStatus, // status of recovery
}

//
// Buffer manager is using L2-list for LRU cache eviction policy,
// L1 lists for free and dirty pages.
// All modified pages are pinned till the end of transaction.
// Indexes are used instead of pointers to reduce memory footprint and bypass Rust ownership/visibility rules.
// Page with index 0 is reserved in buffer manager for root page. It is not included in any list, so 0 is treated as terminator.
//
struct BufferManager {
    // LRU l2-list
    head: BufferId,
    tail: BufferId,

    free_pages: BufferId,  // L1-list of free pages
    dirty_pages: BufferId, // L1-list of dirty pages
    used: BufferId,        // amount of used pages in the cache

    hash_table: Vec<BufferId>, // array containing indexes of collision chains
    pages: Vec<PageHeader>,    //page data
}

//
// Wrapper class for accessing page data
//
struct PageData {
    data: [u8; PAGE_SIZE],
}

impl PageData {
    fn new() -> PageData {
        PageData {
            data: [0u8; PAGE_SIZE],
        }
    }
}

impl PageData {
    fn get_offs(&self, ip: ItemPointer) -> usize {
        self.get_u16(PAGE_HEADER_SIZE + ip * 2) as usize
    }

    fn set_offs(&mut self, ip: ItemPointer, offs: usize) {
        self.set_u16(PAGE_HEADER_SIZE + ip * 2, offs as u16)
    }

    fn get_child(&self, ip: ItemPointer) -> PageId {
        let offs = self.get_offs(ip);
        let key_len = self.data[offs] as usize;
        self.get_u32(offs + key_len + 1)
    }

    fn get_key(&self, ip: ItemPointer) -> Key {
        let offs = self.get_offs(ip);
        let key_len = self.data[offs] as usize;
        self.data[offs + 1..offs + 1 + key_len].to_vec()
    }

    fn get_last_key(&self) -> Key {
        let n_items = self.get_n_items();
        self.get_key(n_items - 1)
    }

    fn get_item(&self, ip: ItemPointer) -> (Key, Value) {
        let (item_offs, item_len) = self.get_item_offs_len(ip);
        let key_len = self.data[item_offs] as usize;
        (
            self.data[item_offs + 1..item_offs + 1 + key_len].to_vec(),
            self.data[item_offs + 1 + key_len..item_offs + item_len].to_vec(),
        )
    }

    fn get_item_offs_len(&self, ip: ItemPointer) -> (usize, usize) {
        let offs = self.get_offs(ip);
        let next_offs = if ip == 0 {
            PAGE_SIZE
        } else {
            self.get_offs(ip - 1)
        };
        debug_assert!(next_offs > offs);
        (offs, next_offs - offs)
    }

    fn set_u16(&mut self, offs: usize, data: u16) {
        self.copy(offs, &data.to_be_bytes());
    }

    fn set_u32(&mut self, offs: usize, data: u32) {
        self.copy(offs, &data.to_be_bytes());
    }

    fn get_u16(&self, offs: usize) -> u16 {
        u16::from_be_bytes(self.data[offs..offs + 2].try_into().unwrap())
    }

    fn get_u32(&self, offs: usize) -> u32 {
        u32::from_be_bytes(self.data[offs..offs + 4].try_into().unwrap())
    }

    fn get_n_items(&self) -> ItemPointer {
        self.get_u16(0) as ItemPointer
    }

    fn get_size(&self) -> ItemPointer {
        let n_items = self.get_n_items();
        if n_items == 0 {
            0
        } else {
            PAGE_SIZE - self.get_offs(n_items - 1)
        }
    }

    fn set_n_items(&mut self, n_items: ItemPointer) {
        self.set_u16(0, n_items as u16)
    }

    fn copy(&mut self, offs: usize, data: &[u8]) {
        let len = data.len();
        self.data[offs..offs + len].copy_from_slice(&data);
    }

    fn compare_key(&self, ip: ItemPointer, key: &Key) -> Ordering {
        let offs = self.get_offs(ip);
        let key_len = self.data[offs] as usize;
        if key_len == 0 {
            // special handling of +inf in right-most internal nodes
            Ordering::Less
        } else {
            key[..].cmp(&self.data[offs + 1..offs + 1 + key_len])
        }
    }

    fn remove_key(&mut self, ip: ItemPointer) {
        let n_items = self.get_n_items();
        let size = self.get_size();
        let (item_offs, item_len) = self.get_item_offs_len(ip);
        for i in ip + 1..n_items {
            self.set_offs(i - 1, self.get_offs(i) + item_len);
        }
        let items_origin = PAGE_SIZE - size;
        if n_items > 1 && ip + 1 == n_items && self.data[item_offs] == 0 {
            // If we are removing last child of internal page with +inf key,
            // then we should replace key of previous item with +inf
            let prev_key_len = self.data[item_offs + item_len] as usize;
            self.set_offs(ip - 1, item_offs + item_len + prev_key_len);
            self.data[item_offs + item_len + prev_key_len] = 0u8; // zero length means +inf
        } else {
            self.data
                .copy_within(items_origin..item_offs, items_origin + item_len);
        }
        self.set_n_items(n_items - 1);
    }

    //
    // Insert item on the page is there is enough free space, otherwise return false
    //
    fn insert_item(&mut self, ip: ItemPointer, key: &Key, value: &[u8]) -> bool {
        let n_items = self.get_n_items();
        let size = self.get_size();
        let key_len = key.len();
        let item_len = 1 + key_len + value.len();
        if (n_items + 1) * 2 + size + item_len <= PAGE_SIZE - PAGE_HEADER_SIZE {
            // fit in page
            for i in (ip..n_items).rev() {
                self.set_offs(i + 1, self.get_offs(i) - item_len);
            }
            let item_offs = if ip != 0 {
                self.get_offs(ip - 1) - item_len
            } else {
                PAGE_SIZE - item_len
            };
            self.set_offs(ip, item_offs);
            let items_origin = PAGE_SIZE - size;
            self.data
                .copy_within(items_origin..item_offs + item_len, items_origin - item_len);
            self.data[item_offs] = key_len as u8;
            self.data[item_offs + 1..item_offs + 1 + key_len].copy_from_slice(&key);
            self.data[item_offs + 1 + key_len..item_offs + item_len].copy_from_slice(&value);
            self.set_n_items(n_items + 1);
            true
        } else {
            false
        }
    }

    //
    // Split page into two approximately equal parts. Smallest keys are moved to the new page,
    // largest - left on original page.
    // Returns split position
    //
    fn split(&mut self, new_page: &mut PageData) -> ItemPointer {
        let n_items = self.get_n_items();
        let size = self.get_size();
        let margin = PAGE_SIZE - size / 2;
        let mut l: ItemPointer = 0;
        let mut r = n_items;
        while l < r {
            let m = (l + r) >> 1;
            if self.get_offs(m) > margin {
                // items are allocated from right to left
                l = m + 1;
            } else {
                r = m;
            }
        }
        debug_assert!(l == r);
        // Move first r+1 elements to the new page
        let moved_size = PAGE_SIZE - self.get_offs(r);

        // copy item pointers
        new_page.data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + (r + 1) * 2]
            .copy_from_slice(&self.data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + (r + 1) * 2]);
        // copy items
        let dst = PAGE_SIZE - moved_size;
        new_page.data[dst..].copy_from_slice(&self.data[dst..]);

        // Adjust item pointers on old page
        for i in r + 1..n_items {
            self.set_offs(i - r - 1, self.get_offs(i) + moved_size);
        }
        let src = PAGE_SIZE - size;
        self.data.copy_within(src..dst, src + moved_size);
        new_page.set_n_items(r + 1);
        self.set_n_items(n_items - r - 1);
        r
    }
}

impl BufferManager {
    //
    // Link buffer to the head of LRU list (make it acceptable for eviction)
    //
    fn unpin(&mut self, id: BufferId) {
        debug_assert!(self.pages[id as usize].access_count == 1);
        self.pages[id as usize].access_count = 0;
        self.pages[id as usize].next = self.head;
        self.pages[id as usize].prev = 0;
        if self.head != 0 {
            self.pages[self.head as usize].prev = id;
        } else {
            self.tail = id;
        }
        self.head = id;
    }

    //
    // Unlink buffer from LRU list and so pin it in memory (protect from eviction)
    //
    fn pin(&mut self, id: BufferId) {
        debug_assert!(self.pages[id as usize].access_count == 0);
        let prev = self.pages[id as usize].prev as usize;
        if prev == 0 {
            self.head = self.pages[id as usize].next;
        } else {
            self.pages[prev].next = self.pages[id as usize].next;
        }
        let next = self.pages[id as usize].next as usize;
        if next == 0 {
            self.tail = self.pages[id as usize].prev;
        } else {
            self.pages[next].prev = self.pages[id as usize].prev;
        }
    }

    //
    // Insert page in hash table
    //
    fn insert(&mut self, id: BufferId) {
        let h = self.pages[id as usize].id as usize % self.hash_table.len();
        self.pages[id as usize].collision = self.hash_table[h];
        self.hash_table[h] = id;
    }

    //
    // Remove page from hash table
    //
    fn remove(&mut self, id: BufferId) {
        let h = self.pages[id as usize].id as usize % self.hash_table.len();
        let mut p = self.hash_table[h];
        if p == id {
            self.hash_table[h] = self.pages[id as usize].collision;
        } else {
            while self.pages[p as usize].collision != id {
                p = self.pages[p as usize].collision;
            }
            self.pages[p as usize].collision = self.pages[id as usize].collision;
        }
    }

    //
    // Throw away buffer from cache (used by transaction rollback)
    //
    fn throw_buffer(&mut self, id: BufferId) {
        self.remove(id);
        self.pages[id as usize].next = self.free_pages;
        self.free_pages = id;
    }

    //
    // If buffer is not yet marked as dirty then mark it as dirty and pin until the end of transaction
    //
    fn modify_buffer(&mut self, id: BufferId) {
        debug_assert!(self.pages[id as usize].access_count > 0);
        if (self.pages[id as usize].state & PAGE_DIRTY) == 0 {
            self.pages[id as usize].access_count += 1; // pin dirty page in memory
            self.pages[id as usize].state = PAGE_DIRTY;
            self.pages[id as usize].next = self.dirty_pages;
            self.dirty_pages = id;
        }
    }

    //
    // Decrement buffer's access counter and release buffer if it is last reference
    //
    fn release_buffer(&mut self, id: BufferId) {
        debug_assert!(self.pages[id as usize].access_count > 0);
        if self.pages[id as usize].access_count == 1 {
            debug_assert!((self.pages[id as usize].state & PAGE_DIRTY) == 0);
            self.unpin(id);
        } else {
            self.pages[id as usize].access_count -= 1;
        }
    }

    //
    // Find buffer with specified page or allocate new buffer
    //
    fn get_buffer(&mut self, pid: PageId) -> BufferId {
        let hash = pid as usize % self.hash_table.len();
        let mut h = self.hash_table[hash];
        while h != 0 {
            if self.pages[h as usize].id == pid {
                let access_count = self.pages[h as usize].access_count;
                debug_assert!(access_count < u16::MAX - 1);
                if access_count == 0 {
                    self.pin(h);
                }
                self.pages[h as usize].access_count = access_count + 1;
                return h;
            }
            h = self.pages[h as usize].collision;
        }
        // page not found in cache
        h = self.free_pages;
        if h != 0 {
            // has some free pages
            self.free_pages = self.pages[h as usize].next;
        } else {
            h = self.used;
            if (h as usize) < self.hash_table.len() {
                self.used += 1;
            } else {
                // Replace least recently used page
                let victim = self.tail;
                assert!(victim != 0);
                debug_assert!(self.pages[victim as usize].access_count == 0);
                debug_assert!((self.pages[victim as usize].state & PAGE_DIRTY) == 0);
                self.pin(victim);
                self.remove(victim);
                h = victim;
            }
        }
        self.pages[h as usize].access_count = 1;
        self.pages[h as usize].id = pid;
        self.pages[h as usize].state = PAGE_RAW;
        self.insert(h);
        h
    }
}

struct PageGuard<'a> {
    buf: BufferId,
    pid: PageId,
    storage: &'a Storage,
}

impl<'a> Drop for PageGuard<'a> {
    fn drop(&mut self) {
        self.storage.release_page(self.buf);
    }
}

//
// Storage internal methods implementations
//
impl Storage {
    //
    // Unpin page (called by PageGuard)
    //
    fn release_page(&self, buf: BufferId) {
        let mut bm = self.buf_mgr.lock().unwrap();
        bm.release_buffer(buf);
    }

    //
    // Allocate new page in storage and get buffer for it
    //
    fn new_page(&self, db: &mut RwLockWriteGuard<Database>) -> PageGuard<'_> {
        let mut bm = self.buf_mgr.lock().unwrap();
        let free = db.meta.free;
        let buf;
        if free != 0 {
            buf = bm.get_buffer(free);
            let page = self.pool[buf as usize].read().unwrap();
            db.meta.free = page.get_u32(0);
        } else {
            // extend storage
            buf = bm.get_buffer(db.meta.size);
            db.meta.size += 1;
        }
        bm.modify_buffer(buf);
        PageGuard {
            buf,
            pid: bm.pages[buf as usize].id,
            storage: &self,
        }
    }

    //
    // Read page in buffer and return PageGuard with pinned buffer.
    // Buffer will be automatically released on exiting from scope
    //
    fn get_page(&self, pid: PageId, mode: AccessMode) -> Result<PageGuard<'_>> {
        let mut bm = self.buf_mgr.lock().unwrap();
        let buf = bm.get_buffer(pid);
        if (bm.pages[buf as usize].state & PAGE_BUSY) != 0 {
            // Some other thread is loading buffer: just wait until it done
            bm.pages[buf as usize].state |= PAGE_WAIT;
            loop {
                debug_assert!((bm.pages[buf as usize].state & PAGE_WAIT) != 0);
                bm = self.busy_events[buf as usize % N_BUSY_EVENTS]
                    .wait(bm)
                    .unwrap();
                if (bm.pages[buf as usize].state & PAGE_BUSY) == 0 {
                    break;
                }
            }
        } else if (bm.pages[buf as usize].state & PAGE_RAW) != 0 {
            let mut page = self.pool[buf as usize].write().unwrap();
            if mode != AccessMode::WriteOnly {
                // Read buffer if not in write-only mode
                bm.pages[buf as usize].state = PAGE_BUSY;
                drop(bm); // read page without holding lock
                self.file
                    .read_exact_at(&mut page.data, pid as u64 * PAGE_SIZE as u64)?;
                bm = self.buf_mgr.lock().unwrap();
                if (bm.pages[buf as usize].state & PAGE_WAIT) != 0 {
                    // Somebody is waiting for us
                    self.busy_events[buf as usize % N_BUSY_EVENTS].notify_all();
                }
            } else {
                page.data.fill(0u8); // memset
            }
            bm.pages[buf as usize].state = 0;
        }
        if mode != AccessMode::ReadOnly {
            bm.modify_buffer(buf);
        }
        Ok(PageGuard {
            buf,
            pid,
            storage: &self,
        })
    }

    //
    // Mark page as dirty and pin it in-memory until end of transaction
    //
    fn modify_page(&self, buf: BufferId) {
        let mut bm = self.buf_mgr.lock().unwrap();
        bm.modify_buffer(buf);
    }

    pub fn start_transaction(&self) -> Transaction<'_> {
        Transaction {
            status: TransactionStatus::InProgress,
            storage: self,
            db: self.db.write().unwrap(),
        }
    }

    fn commit(&self, db: &mut RwLockWriteGuard<Database>) -> Result<()> {
        let bm = self.buf_mgr.lock().unwrap();

        if let Some(log) = &self.log {
            // Write dirty pages to log file
            let mut dirty = bm.dirty_pages;
            let mut crc = 0u32;
            while dirty != 0 {
                let mut buf = [0u8; PAGE_SIZE + 4];
                let pid = bm.pages[dirty as usize].id;
                let page = self.pool[dirty as usize].read().unwrap();
                buf[0..4].copy_from_slice(&pid.to_be_bytes());
                buf[4..].copy_from_slice(&page.data);
                crc = crc32c_append(crc, &buf);
                log.write_all_at(&buf, db.wal_pos)?;
                db.wal_pos += (4 + PAGE_SIZE) as u64;
                dirty = bm.pages[dirty as usize].next;
            }
            if bm.dirty_pages != 0 {
                let mut buf = [0u8; METADATA_SIZE + 8];
                let meta = db.meta.pack();
                {
                    let mut page = self.pool[0].write().unwrap();
                    page.data[0..METADATA_SIZE].copy_from_slice(&meta);
                }
                buf[4..4 + METADATA_SIZE].copy_from_slice(&meta);
                crc = crc32c_append(crc, &buf[..4 + METADATA_SIZE]);
                buf[4 + METADATA_SIZE..].copy_from_slice(&crc.to_be_bytes());
                log.write_all_at(&buf, db.wal_pos)?;
                db.wal_pos += (8 + METADATA_SIZE) as u64;
                log.sync_all()?;
                db.lsn += 1;

                // Write pages to the data file
                self.flush_buffers(bm)?;

                if db.wal_pos >= self.checkpoint_interval {
                    // Sync data file and restart from the beginning of WAL.
                    // So not truncate WAL to avoid file extension overhead.
                    self.file.sync_all()?;
                    db.wal_pos = 0;
                }
            }
        } else {
            // No WAL mode: just write dirty pages to the disk
            if self.flush_buffers(bm)? {
                let meta = db.meta.pack();
                let mut page = self.pool[0].write().unwrap();
                page.data[0..METADATA_SIZE].copy_from_slice(&meta);
                db.lsn += 1;
            }
        }
        Ok(())
    }

    //
    // Flush dirty pages to the disk. Return true if database is changed.
    //
    fn flush_buffers(&self, mut bm: MutexGuard<BufferManager>) -> Result<bool> {
        let mut dirty = bm.dirty_pages;
        while dirty != 0 {
            let pid = bm.pages[dirty as usize].id;
            let file_offs = pid as u64 * PAGE_SIZE as u64;
            let page = self.pool[dirty as usize].read().unwrap();
            let next = bm.pages[dirty as usize].next;
            self.file.write_all_at(&page.data, file_offs)?;
            debug_assert!(bm.pages[dirty as usize].state == PAGE_DIRTY);
            bm.pages[dirty as usize].state = 0;
            bm.unpin(dirty);
            dirty = next;
        }
        if bm.dirty_pages != 0 {
            // Save metadata page
            bm.dirty_pages = 0;
            let page = self.pool[0].read().unwrap();
            self.file.write_all_at(&page.data, 0)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    //
    // Rollback current transaction
    //
    fn rollback(&self, db: &mut RwLockWriteGuard<Database>) -> Result<()> {
        let mut bm = self.buf_mgr.lock().unwrap();
        let mut dirty = bm.dirty_pages;

        // Just throw away all dirty pages from buffer cache to force reloading of original pages
        while dirty != 0 {
            debug_assert!((bm.pages[dirty as usize].state & PAGE_DIRTY) != 0);
            debug_assert!(bm.pages[dirty as usize].access_count == 1);
            let next = bm.pages[dirty as usize].next;
            bm.throw_buffer(dirty);
            dirty = next;
        }
        if bm.dirty_pages != 0 {
            // reread metadata from disk
            bm.dirty_pages = 0;
            let mut page = self.pool[0].write().unwrap();
            self.file.read_exact_at(&mut page.data, 0)?;
            db.meta = Metadata::unpack(&page.data);
        }
        Ok(())
    }

    ///
    /// Open database storage. If storage file doesn't exist, then it is created.
    /// If path to transaction log is not specified, then WAL (write-ahead-log) is not used.
    /// It will significantly increase performance but can cause database corruption in case of power failure or system crash.
    /// `checkpoint_internal` specifies maximal size of WAL. When it is reached, database file is synced and WAL is rotated (write starts from the beginning)
    ///
    pub fn open(
        db_path: &Path,
        log_path: Option<&Path>,
        cache_size: usize,
        checkpoint_interval: u64,
    ) -> Result<Storage> {
        let mut buf = [0u8; PAGE_SIZE];
        let (file, meta) = if let Ok(file) = OpenOptions::new().write(true).read(true).open(db_path)
        {
            // open existed file
            file.try_lock_exclusive()?;
            file.read_exact_at(&mut buf, 0)?;
            let meta = Metadata::unpack(&buf);
            assert!(meta.magic == MAGIC && meta.version == VERSION && meta.size >= 1);
            (file, meta)
        } else {
            let file = OpenOptions::new()
                .write(true)
                .read(true)
                .create(true)
                .open(db_path)?;
            file.try_lock_exclusive()?;
            // create new file
            let meta = Metadata {
                magic: MAGIC,
                version: VERSION,
                free: 0,
                size: 1,
                root: 0,
                height: 0,
            };
            let metadata = meta.pack();
            buf[0..METADATA_SIZE].copy_from_slice(&metadata);
            file.write_all_at(&mut buf, 0)?;
            (file, meta)
        };
        let log = if let Some(path) = log_path {
            let log = OpenOptions::new()
                .write(true)
                .read(true)
                .create(true)
                .open(path)?;
            log.try_lock_exclusive()?;
            Some(log)
        } else {
            None
        };
        let storage = Storage {
            busy_events: [(); N_BUSY_EVENTS].map(|_| Condvar::new()),
            buf_mgr: Mutex::new(BufferManager {
                head: 0,
                tail: 0,
                free_pages: 0,
                dirty_pages: 0,
                used: 1, // pinned root page
                hash_table: vec![0; cache_size],
                pages: vec![PageHeader::new(); cache_size],
            }),
            pool: iter::repeat_with(|| RwLock::new(PageData::new()))
                .take(cache_size)
                .collect(),
            file,
            log,
            checkpoint_interval,
            db: RwLock::new(Database {
                lsn: 0,
                meta,
                recovery: RecoveryStatus {
                    ..Default::default()
                },
                state: DatabaseState::InRecovery,
                wal_pos: 0,
            }),
        };
        storage.recovery()?;
        Ok(storage)
    }

    //
    // Recover database from WAL (if any)
    //
    fn recovery(&self) -> Result<()> {
        let mut db = self.db.write().unwrap();
        if let Some(log) = &self.log {
            let mut buf = [0u8; 4];
            let mut crc = 0u32;
            let mut wal_pos = 0u64;
            db.recovery.wal_size = log.metadata()?.len();
            loop {
                let len = log.read_at(&mut buf, wal_pos)?;
                if len != 4 {
                    // end of log
                    break;
                }
                wal_pos += 4;
                let pid = PageId::from_be_bytes(buf);
                crc = crc32c_append(crc, &buf);
                if pid != 0 {
                    let pin = self.get_page(pid, AccessMode::WriteOnly)?;
                    let mut page = self.pool[pin.buf as usize].write().unwrap();
                    let len = log.read_at(&mut page.data, wal_pos)?;
                    if len != PAGE_SIZE {
                        break;
                    }
                    wal_pos += len as u64;
                    crc = crc32c_append(crc, &page.data);
                } else {
                    let mut meta_buf = [0u8; METADATA_SIZE];
                    let len = log.read_at(&mut meta_buf, wal_pos)?;
                    if len != PAGE_SIZE {
                        break;
                    }
                    wal_pos += len as u64;
                    crc = crc32c_append(crc, &meta_buf);
                    let len = log.read_at(&mut buf, wal_pos)?;
                    if len != 4 {
                        break;
                    }
                    wal_pos += 4;
                    if u32::from_be_bytes(buf) != crc {
                        // CRC mismatch
                        break;
                    }
                    {
                        let mut page = self.pool[0].write().unwrap();
                        page.data[0..METADATA_SIZE].copy_from_slice(&meta_buf);
                    }
                    let bm = self.buf_mgr.lock().unwrap();
                    self.flush_buffers(bm)?;
                    db.recovery.recovered_transactions += 1;
                    db.recovery.recovery_end = wal_pos;
                }
            }
            self.rollback(&mut db)?;

            // reset WAL
            self.file.sync_all()?;
            db.wal_pos = 0;
            log.set_len(0)?; // truncate log
        }
        // reread metadata
        let mut page = self.pool[0].write().unwrap();
        self.file.read_exact_at(&mut page.data, 0)?;
        db.meta = Metadata::unpack(&page.data);

        db.state = DatabaseState::Opened;

        Ok(())
    }

    //
    // Bulk update
    //
    fn do_updates(
        &self,
        db: &mut RwLockWriteGuard<Database>,
        to_upsert: &mut dyn Iterator<Item = Result<(Key, Value)>>,
        to_remove: &mut dyn Iterator<Item = Result<Key>>,
    ) -> Result<()> {
        for pair in to_upsert {
            let kv = pair?;
            self.do_upsert(db, &kv.0, &kv.1)?;
        }
        for key in to_remove {
            self.do_remove(db, &key?)?;
        }
        Ok(())
    }

    //
    // Allocate new B-Tree leaf page with single (key,value) element
    //
    fn btree_allocate_leaf_page(
        &self,
        db: &mut RwLockWriteGuard<Database>,
        key: &Key,
        value: &Value,
    ) -> PageId {
        let pin = self.new_page(db);
        let mut page = self.pool[pin.buf as usize].write().unwrap();
        page.set_n_items(0);
        page.insert_item(0, key, value);
        pin.pid
    }

    //
    // Allocate new B-Tree internal page referencing two children
    //
    fn btree_allocate_internal_page(
        &self,
        db: &mut RwLockWriteGuard<Database>,
        key: &Key,
        left_child: PageId,
        right_child: PageId,
    ) -> PageId {
        let pin = self.new_page(db);
        let mut page = self.pool[pin.buf as usize].write().unwrap();
        page.set_n_items(0);
        page.insert_item(0, key, &left_child.to_be_bytes().to_vec());
        page.insert_item(1, &vec![], &right_child.to_be_bytes().to_vec());
        pin.pid
    }

    //
    // Insert item at the specified position in B-Tree page.
    // If B-Tree pages is full then split it, evenly distribute items between pages: smaller items moved to new page, larger items left on original page.
    // Value of largest key on new page and its identifiers are returned in case of overflow.
    //
    fn btree_insert_in_page(
        &self,
        db: &mut RwLockWriteGuard<Database>,
        page: &mut PageData,
        ip: ItemPointer,
        key: &Key,
        value: &Value,
    ) -> Option<(Key, PageId)> {
        if !page.insert_item(ip, key, value) {
            // page is full then divide page
            let pin = self.new_page(db);
            let mut new_page = self.pool[pin.buf as usize].write().unwrap();
            let split = page.split(&mut new_page);
            let ok = if ip > split {
                page.insert_item(ip - split - 1, key, value)
            } else {
                new_page.insert_item(ip, key, value)
            };
            assert!(ok);
            Some((new_page.get_last_key(), pin.pid))
        } else {
            None
        }
    }

    //
    // Remove key from B-Tree. Recursively traverse B-Tree and return true in case of underflow.
    // Right now we do not redistribute nodes between pages or merge pages, underflow is reported only if page becomes empty.
    // If key is not found, then nothing is performed and no error is reported.
    //
    fn btree_remove(
        &self,
        db: &mut RwLockWriteGuard<Database>,
        pid: PageId,
        key: &Key,
        height: u32,
    ) -> Result<bool> {
        let pin = self.get_page(pid, AccessMode::ReadOnly)?;
        let mut page = self.pool[pin.buf as usize].write().unwrap();
        let mut l: ItemPointer = 0;
        let n = page.get_n_items();
        let mut r = n;
        while l < r {
            let m = (l + r) >> 1;
            if page.compare_key(m, key) == Ordering::Greater {
                l = m + 1;
            } else {
                r = m;
            }
        }
        debug_assert!(l == r);
        if height == 1 {
            // leaf page
            if r < n && page.compare_key(r, key) == Ordering::Equal {
                self.modify_page(pin.buf);
                page.remove_key(r);
            }
        } else {
            // recurse to next level
            debug_assert!(r < n);
            let underflow = self.btree_remove(db, page.get_child(r), key, height - 1)?;
            if underflow {
                self.modify_page(pin.buf);
                page.remove_key(r);
            }
        }
        if page.get_n_items() == 0 {
            // free page
            page.set_u32(0, db.meta.free);
            db.meta.free = pid;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    //
    // Insert item in B-Tree. Recursively traverse B-Tree and return position of new page in case of overflow.
    //
    fn btree_insert(
        &self,
        db: &mut RwLockWriteGuard<Database>,
        pid: PageId,
        key: &Key,
        value: &Value,
        height: u32,
    ) -> Result<Option<(Key, PageId)>> {
        let pin = self.get_page(pid, AccessMode::ReadOnly)?;
        let mut page = self.pool[pin.buf as usize].write().unwrap();
        let mut l: ItemPointer = 0;
        let n = page.get_n_items();
        let mut r = n;
        while l < r {
            let m = (l + r) >> 1;
            if page.compare_key(m, key) == Ordering::Greater {
                l = m + 1;
            } else {
                r = m;
            }
        }
        debug_assert!(l == r);
        if height == 1 {
            // leaf page
            self.modify_page(pin.buf);
            if r < n && page.compare_key(r, key) == Ordering::Equal {
                // replace old value with new one: just remove old one and reinsert new key-value pair
                page.remove_key(r);
            }
            Ok(self.btree_insert_in_page(db, &mut page, r, key, value))
        } else {
            // recurse to next level
            debug_assert!(r < n);
            let overflow = self.btree_insert(db, page.get_child(r), key, value, height - 1)?;
            if let Some((key, child)) = overflow {
                // insert new page before original
                self.modify_page(pin.buf);
                Ok(
                    self.btree_insert_in_page(
                        db,
                        &mut page,
                        r,
                        &key,
                        &child.to_be_bytes().to_vec(),
                    ),
                )
            } else {
                Ok(None)
            }
        }
    }

    //
    // Insert or update key in the storage
    //
    fn do_upsert(
        &self,
        db: &mut RwLockWriteGuard<Database>,
        key: &Key,
        value: &Value,
    ) -> Result<()> {
        assert!(key.len() != 0 && key.len() <= MAX_KEY_LEN && value.len() <= MAX_VALUE_LEN);
        if db.meta.root == 0 {
            db.meta.root = self.btree_allocate_leaf_page(db, key, value);
            db.meta.height = 1;
        } else if let Some((key, page)) =
            self.btree_insert(db, db.meta.root, key, value, db.meta.height)?
        {
            // overflow
            db.meta.root = self.btree_allocate_internal_page(db, &key, page, db.meta.root);
            db.meta.height += 1;
        }
        Ok(())
    }

    //
    // Remove key from the storage. Does nothing it key not exists.
    //
    fn do_remove(&self, db: &mut RwLockWriteGuard<Database>, key: &Key) -> Result<()> {
        if db.meta.root != 0 {
            let underflow = self.btree_remove(db, db.meta.root, key, db.meta.height)?;
            if underflow {
                db.meta.height = 0;
                db.meta.root = 0;
            }
        }
        Ok(())
    }

    //
    // Perform lookup operation and fill `path` structure with located item and path to it in the tree.
    // If item is not found, then make path empty and current element `None`.
    //
    fn do_lookup(
        &self,
        db: &Database,
        op: LookupOp,
        path: &mut TreePath,
    ) -> Result<Option<(Key, Value)>> {
        match op {
            LookupOp::First => {
                // Locate left-most element in the tree
                let mut pid = db.meta.root;
                if pid != 0 {
                    let mut level = db.meta.height;
                    loop {
                        let pin = self.get_page(pid, AccessMode::ReadOnly)?;
                        let page = self.pool[pin.buf as usize].read().unwrap();
                        path.stack.push(PagePos { pid, pos: 0 });
                        level -= 1;
                        if level == 0 {
                            path.curr = Some(page.get_item(0));
                            path.lsn = db.lsn;
                            break;
                        } else {
                            pid = page.get_child(0)
                        }
                    }
                }
            }
            LookupOp::Last => {
                // Locate right-most element in the tree
                let mut pid = db.meta.root;
                if pid != 0 {
                    let mut level = db.meta.height;
                    loop {
                        let pin = self.get_page(pid, AccessMode::ReadOnly)?;
                        let page = self.pool[pin.buf as usize].read().unwrap();
                        let pos = page.get_n_items() - 1;
                        level -= 1;
                        path.stack.push(PagePos { pid, pos });
                        if level == 0 {
                            path.curr = Some(page.get_item(pos));
                            path.lsn = db.lsn;
                            break;
                        } else {
                            pid = page.get_child(pos)
                        }
                    }
                }
            }
            LookupOp::Next => {
                if path.lsn == db.lsn || self.reconstruct_path(path, db)? {
                    self.move_forward(path, db.meta.height)?;
                }
            }
            LookupOp::Prev => {
                if path.lsn == db.lsn || self.reconstruct_path(path, db)? {
                    self.move_backward(path, db.meta.height)?;
                }
            }
            LookupOp::GreaterOrEqual(key) => {
                if db.meta.root != 0 && self.find(db.meta.root, path, &key, db.meta.height)? {
                    path.lsn = db.lsn;
                }
            }
        }
        Ok(path.curr.clone())
    }

    //
    // Perform lookup in the database. Initialize path in the tree current element or reset path if no element is found or end of set is reached.
    //
    fn lookup(&self, db: &Database, op: LookupOp, path: &mut TreePath) {
        assert!(db.state == DatabaseState::Opened);
        let result = self.do_lookup(db, op, path);
        if result.is_err() {
            path.curr = None;
        }
        path.result = result.transpose();
    }

    //
    // Locate greater or equal key.
    // Returns true and initializes path to this element if such key is found,
    // reset path and returns false otherwise.
    //
    fn find(&self, pid: PageId, path: &mut TreePath, key: &Key, height: u32) -> Result<bool> {
        let pin = self.get_page(pid, AccessMode::ReadOnly)?;
        let page = self.pool[pin.buf as usize].read().unwrap();
        let n = page.get_n_items();
        let mut l: ItemPointer = 0;
        let mut r = n;

        while l < r {
            let m = (l + r) >> 1;
            if page.compare_key(m, key) == Ordering::Greater {
                l = m + 1;
            } else {
                r = m;
            }
        }
        debug_assert!(l == r);
        path.stack.push(PagePos { pid, pos: r });
        if height == 1 {
            // leaf page
            if r < n {
                path.curr = Some(page.get_item(r));
                Ok(true)
            } else {
                path.curr = None;
                path.stack.clear();
                Ok(false)
            }
        } else {
            self.find(page.get_child(r), path, key, height - 1)
        }
    }

    //
    // If storage is updated between iterations then try to reconstruct path by locating current element.
    // Returns true is such element is found and path is successfully reconstructed, false otherwise.
    //
    fn reconstruct_path(&self, path: &mut TreePath, db: &Database) -> Result<bool> {
        if let Some((key, _value)) = &path.curr.clone() {
            if self.find(db.meta.root, path, &key, db.meta.height)? {
                if let Some((ge_key, _value)) = &path.curr {
                    if ge_key == key {
                        path.lsn = db.lsn;
                        return Ok(true);
                    }
                }
            }
        }
        path.curr = None;
        path.stack.clear();
        Ok(false)
    }

    //
    // Move cursor forward
    //
    fn move_forward(&self, path: &mut TreePath, height: u32) -> Result<()> {
        let mut inc: usize = 1;
        path.curr = None;
        while !path.stack.is_empty() {
            let top = path.stack.pop().unwrap();
            let pin = self.get_page(top.pid, AccessMode::ReadOnly)?;
            let page = self.pool[pin.buf as usize].read().unwrap();
            let n_items = page.get_n_items();
            if top.pos + inc < n_items {
                path.stack.push(PagePos {
                    pid: top.pid,
                    pos: top.pos + inc,
                });
                if path.stack.len() == height as usize {
                    let item = page.get_item(top.pos + inc);
                    path.curr = Some(item);
                    break;
                }
                // We have to use this trick with `inc` variable on the way down because
                // Rust will detect overflow if we path -1 as pos
                path.stack.push(PagePos {
                    pid: page.get_child(0),
                    pos: 0,
                });
                inc = 0;
            } else {
                // traverse up
                inc = 1;
            }
        }
        Ok(())
    }

    //
    // Move cursor backward
    //
    fn move_backward(&self, path: &mut TreePath, height: u32) -> Result<()> {
        path.curr = None;
        while !path.stack.is_empty() {
            let top = path.stack.pop().unwrap();
            let pin = self.get_page(top.pid, AccessMode::ReadOnly)?;
            let page = self.pool[pin.buf as usize].read().unwrap();
            if top.pos != 0 {
                path.stack.push(PagePos {
                    pid: top.pid,
                    pos: top.pos - 1,
                });
                if path.stack.len() == height as usize {
                    let item = page.get_item(top.pos - 1);
                    path.curr = Some(item);
                    break;
                }
                let n_items = page.get_n_items();
                path.stack.push(PagePos {
                    pid: page.get_child(n_items - 1),
                    pos: n_items,
                });
            }
        }
        Ok(())
    }
}

//
// Implementation of public methods.
// I have a problems with extracting them in separate treat.
//
impl Storage {
    ///
    /// Perform atomic updates: insert or update `to_upsert` tuples
    /// and remove `to_remove` tuples (if exist)
    /// If all operations are successful, then we commit transaction, otherwise rollback it.
    /// If commit or rollback itself returns error, then database is switched to the corrupted state and no further access to the database is possible.
    /// You will have to close and reopen it.
    ///
    pub fn update(
        &self,
        to_upsert: &mut dyn Iterator<Item = Result<(Key, Value)>>,
        to_remove: &mut dyn Iterator<Item = Result<Key>>,
    ) -> Result<()> {
        let mut db = self.db.write().unwrap(); // prevent concurrent access to the database during update operations (MURSIW)
        assert!(db.state == DatabaseState::Opened);
        let mut result = self.do_updates(&mut db, to_upsert, to_remove);
        if result.is_ok() {
            result = self.commit(&mut db);
            if !result.is_ok() {
                db.state = DatabaseState::Corrupted;
            }
        } else {
            if !self.rollback(&mut db).is_ok() {
                db.state = DatabaseState::Corrupted;
            }
        }
        result
    }

    ///
    /// Put (key,value) pair in the storage, if such key already exist, associated value is updated
    ///
    pub fn put(&self, key: Key, value: Value) -> Result<()> {
        self.update(&mut iter::once(Ok((key, value))), &mut iter::empty())
    }

    ///
    /// Store value for u32 key
    ///
    pub fn put_u32(&self, key: u32, value: Value) -> Result<()> {
        self.put(key.to_be_bytes().to_vec(), value)
    }

    ///
    /// Store value for u64 key
    ///
    pub fn put_u64(&self, key: u64, value: Value) -> Result<()> {
        self.put(key.to_be_bytes().to_vec(), value)
    }

    ///
    /// Put (key,value) pairs in the storage, exited keys are updated
    ///
    pub fn put_all(&self, pairs: &mut dyn Iterator<Item = Result<(Key, Value)>>) -> Result<()> {
        self.update(pairs, &mut iter::empty())
    }

    ///
    /// Remove key from the storage, do nothing if not found
    ///
    pub fn remove(&self, key: Key) -> Result<()> {
        self.update(&mut iter::empty(), &mut iter::once(Ok(key)))
    }

    ///
    /// Remove u32 key from the storage, do nothing if not found
    ///
    pub fn remove_u32(&self, key: u32) -> Result<()> {
        self.remove(key.to_be_bytes().to_vec())
    }

    ///
    /// Remove u64 key from the storage, do nothing if not found
    ///
    pub fn remove_u64(&self, key: u64) -> Result<()> {
        self.remove(key.to_be_bytes().to_vec())
    }

    ///
    /// Remove keys from the storage, do nothing if not found
    ///
    pub fn remove_all(&self, keys: &mut dyn Iterator<Item = Result<Key>>) -> Result<()> {
        self.update(&mut iter::empty(), keys)
    }

    ///
    /// Iterator through pairs in key ascending order.
    /// Byte-wise comparison is used, to it is up to serializer to enforce proper ordering,
    /// for example for unsigned integer type big-endian encoding should be used.
    ///
    pub fn iter(&self) -> StorageIterator<'_> {
        self.range(..)
    }

    ///
    /// Lookup key in the storage.
    ///
    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        let mut iter = self.range((Included(key), Included(key)));
        Ok(iter.next().transpose()?.map(|kv| kv.1))
    }

    ///
    /// Lookup u32 key in the storage.
    ///
    pub fn get_u32(&self, key: u32) -> Result<Option<Value>> {
        self.get(&key.to_be_bytes().to_vec())
    }

    ///
    /// Lookup u64 key in the storage.
    ///
    pub fn get_u64(&self, key: u64) -> Result<Option<Value>> {
        self.get(&key.to_be_bytes().to_vec())
    }

    ///
    /// Returns bidirectional iterator
    ///
    pub fn range<R: RangeBounds<Key>>(&self, range: R) -> StorageIterator<'_> {
        StorageIterator {
            storage: &self,
            trans: None,
            from: range.start_bound().cloned(),
            till: range.end_bound().cloned(),
            left: TreePath::new(),
            right: TreePath::new(),
        }
    }

    ///
    /// Close storage. Close data and WAL files and truncate WAL file.
    ///
    pub fn close(&self) -> Result<()> {
        if let Ok(mut db) = self.db.write() {
            if db.state == DatabaseState::Opened {
                // Sync data file and truncate log in case of normal shutdown
                self.file.sync_all()?;
                if let Some(log) = &self.log {
                    log.set_len(0)?; // truncate WAL
                }
                db.state = DatabaseState::Closed;
            }
        }
        Ok(())
    }

    ///
    /// Get recovery status
    ///
    pub fn get_recovery_status(&self) -> RecoveryStatus {
        let db = self.db.read().unwrap();
        db.recovery
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        self.close().unwrap();
    }
}

impl<'a> Transaction<'_> {
    ///
    /// Commit transaction
    ///
    pub fn commit(&mut self) -> Result<()> {
        assert!(self.status == TransactionStatus::InProgress);
        self.storage.commit(&mut self.db)?;
        self.status = TransactionStatus::Committed;
        Ok(())
    }

    ///
    /// Rollback transaction undoing all changes
    ///
    pub fn rollback(&mut self) -> Result<()> {
        assert!(self.status == TransactionStatus::InProgress);
        self.storage.rollback(&mut self.db)?;
        self.status = TransactionStatus::Aborted;
        Ok(())
    }

    ///
    /// Insert new key in the storage or update existed key as part of this transaction.
    ///
    pub fn put(&mut self, key: &Key, value: &Value) -> Result<()> {
        assert!(self.status == TransactionStatus::InProgress);
        self.storage.do_upsert(&mut self.db, key, value)?;
        Ok(())
    }

    ///
    /// Store value for u32 key
    ///
    pub fn put_u32(&mut self, key: u32, value: &Value) -> Result<()> {
        self.put(&key.to_be_bytes().to_vec(), value)
    }

    ///
    /// Store value for u64 key
    ///
    pub fn put_u64(&mut self, key: u64, value: &Value) -> Result<()> {
        self.put(&key.to_be_bytes().to_vec(), value)
    }

    ///
    /// Remove key from storage as part of this transaction.
    /// Does nothing if key not exist.
    ///
    pub fn remove(&mut self, key: &Key) -> Result<()> {
        assert!(self.status == TransactionStatus::InProgress);
        self.storage.do_remove(&mut self.db, key)?;
        Ok(())
    }

    ///
    /// Remove u32 key from the storage, do nothing if not found
    ///
    pub fn remove_u32(&mut self, key: u32) -> Result<()> {
        self.remove(&key.to_be_bytes().to_vec())
    }

    ///
    /// Remove u64 key from the storage, do nothing if not found
    ///
    pub fn remove_u64(&mut self, key: u64) -> Result<()> {
        self.remove(&key.to_be_bytes().to_vec())
    }

    ///
    /// Iterator through pairs in key ascending order.
    /// Byte-wise comparison is used, to it is up to serializer to enforce proper ordering,
    /// for example for unsigned integer type big-endian encoding should be used.
    ///
    pub fn iter(&self) -> StorageIterator<'_> {
        self.range(..)
    }

    ///
    /// Lookup key in the storage.
    ///
    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        let mut iter = self.range((Included(key), Included(key)));
        Ok(iter.next().transpose()?.map(|kv| kv.1))
    }

    ///
    /// Lookup u32 key in the storage.
    ///
    pub fn get_u32(&self, key: u32) -> Result<Option<Value>> {
        self.get(&key.to_be_bytes().to_vec())
    }

    ///
    /// Lookup u64 key in the storage.
    ///
    pub fn get_u64(&self, key: u64) -> Result<Option<Value>> {
        self.get(&key.to_be_bytes().to_vec())
    }

    ///
    /// Returns bidirectional iterator
    ///
    pub fn range<R: RangeBounds<Key>>(&self, range: R) -> StorageIterator<'_> {
        StorageIterator {
            storage: self.storage,
            trans: Some(&self),
            from: range.start_bound().cloned(),
            till: range.end_bound().cloned(),
            left: TreePath::new(),
            right: TreePath::new(),
        }
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if self.status == TransactionStatus::InProgress {
            self.storage.rollback(&mut self.db).unwrap();
        }
    }
}
