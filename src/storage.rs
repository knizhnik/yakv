use anyhow::{ensure, Result};
use fs2::FileExt;
use parking_lot::{Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::iter;
use std::ops::Bound::*;
use std::ops::{Bound, RangeBounds};
use std::os::unix::prelude::FileExt as UnixFileExt;
use std::path::Path;

type PageId = u32; // address of page in the file
type PageState = u16; // page state bit mask
type BufferId = u32; // index of page in buffer cache
                     // (Sub)transaction identifier: monotonically increased counter of subtransactions
                     // This identifier is used to detect deteriorate state of iterator and maintaining COW map for subtransactions.
                     // We compare XIDs only for equality, so wrap-around is not critical (assuming there are no so deteriorated iterators and > 2^32 subtransactions).
type XID = u32;
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
const N_BITMAP_PAGES: usize = PAGE_SIZE / 4 - 8; // number of memory allocator bitmap pages
const ALLOC_BITMAP_SIZE: usize = PAGE_SIZE * 8; // number of pages mapped by one allocator bitmap page
const PAGE_HEADER_SIZE: usize = 2; // now page header contains just number of items in the page
const MAX_VALUE_LEN: usize = PAGE_SIZE / 4; // assume that pages may fit at least 3 items
const MAX_KEY_LEN: usize = u8::MAX as usize; // should fit in one byte
const N_BUSY_EVENTS: usize = 8; // number of condition variables used for waiting read completion

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

//
// Operations with allocator bitmap
//
enum BitmapOp {
    Prepare,
    Set,
    Clear,
}

///
/// Storage configuration parameters
///
#[derive(Copy, Clone, Debug)]
pub struct StorageConfig {
    /// Buffer pool (pages)
    pub cache_size: usize,
    // Do not sync written pages
    pub nosync: bool,
}

impl StorageConfig {
    pub fn default() -> StorageConfig {
        StorageConfig {
            cache_size: 128 * 1024, // 1Gb
            nosync: false,
        }
    }
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
/// Explicitly started read-write transaction. Storage can be updated in autocommit mode
/// or using explicitly started transaction.
///
pub struct Transaction<'a> {
    pub status: TransactionStatus,
    storage: &'a Storage,
    db: RwLockWriteGuard<'a, Database>,
}

///
/// Class for taking snapshot of the storage: capturing state of the storage
/// preventing concurrent changes
///
pub struct Snapshot<'a> {
    storage: &'a Storage,
    meta: RwLockReadGuard<'a, ShadowMetadata>,
}

///
/// Read-only transactions. Unlike snapshot read-only transaction exclude concurrent execution
/// of write transactions (MURSIW).
///
pub struct ReadOnlyTransaction<'a> {
    storage: &'a Storage,
    db: RwLockReadGuard<'a, Database>,
}

///
/// Read-only queries to the storage
///
pub trait Select {
    ///
    /// Iterator through pairs in key ascending order.
    /// Byte-wise comparison is used, to it is up to serializer to enforce proper ordering,
    /// for example for unsigned integer type big-endian encoding should be used.
    ///
    fn iter(&self) -> StorageIterator<'_> {
        self.range(..)
    }

    ///
    /// Lookup key in the storage.
    ///
    fn get(&self, key: &Key) -> Result<Option<Value>> {
        let mut iter = self.range((Included(key), Included(key)));
        Ok(iter.next().transpose()?.map(|kv| kv.1))
    }

    ///
    /// Lookup u32 key in the storage.
    ///
    fn get_u32(&self, key: u32) -> Result<Option<Value>> {
        self.get(&key.to_be_bytes().to_vec())
    }

    ///
    /// Lookup u64 key in the storage.
    ///
    fn get_u64(&self, key: u64) -> Result<Option<Value>> {
        self.get(&key.to_be_bytes().to_vec())
    }

    ///
    /// Returns bidirectional iterator
    ///
    fn range<R: RangeBounds<Key>>(&self, range: R) -> StorageIterator<'_>;
}

///
/// Database info
///
#[derive(Clone, Copy, Debug)]
pub struct DatabaseInfo {
    /// Height of B-Tree
    pub tree_height: usize,
    /// Size of database file
    pub db_size: u64,
    /// Total size of used pages
    pub db_used: u64,
    /// number of committed transactions in this session
    pub n_committed_transactions: u64,
    /// number of aborted transactions in this session
    pub n_aborted_transactions: u64,
    /// Number of allocated pages since session start
    pub n_alloc_pages: u64,
    /// Number of deallocated pages since session start
    pub n_free_pages: u64,
}

///
/// Buffer cache info
///
#[derive(Clone, Copy, Debug)]
pub struct CacheInfo {
    /// Number of pinned pages in buffer cache
    pub pinned: usize,
    /// Number of dirty pages in buffer cache
    pub dirtied: usize,
    /// Total number of pages in buffer cache
    pub used: usize,
}

///
/// Abstract storage bidirectional iterator
///
pub struct StorageIterator<'a> {
    storage: &'a Storage,
    meta: Option<&'a Metadata>,
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
    xid: XID,            // XID of subtransaction when this path was constructed
}

impl TreePath {
    fn new() -> TreePath {
        TreePath {
            curr: None,
            result: None,
            stack: Vec::new(),
            xid: 0,
        }
    }
}

impl<'a> StorageIterator<'a> {
    fn next_locked(&mut self, meta: &Metadata) -> Option<<StorageIterator<'a> as Iterator>::Item> {
        if self.left.stack.is_empty() {
            match &self.from {
                Bound::Included(key) => {
                    self.storage
                        .lookup(meta, LookupOp::GreaterOrEqual(key), &mut self.left)
                }
                Bound::Excluded(key) => {
                    self.storage
                        .lookup(meta, LookupOp::GreaterOrEqual(key), &mut self.left);
                    if let Some((curr_key, _value)) = &self.left.curr {
                        if curr_key == key {
                            self.storage.lookup(meta, LookupOp::Next, &mut self.left);
                        }
                    }
                }
                Bound::Unbounded => self.storage.lookup(meta, LookupOp::First, &mut self.left),
            }
        } else {
            self.storage.lookup(meta, LookupOp::Next, &mut self.left);
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
        meta: &Metadata,
    ) -> Option<<StorageIterator<'a> as Iterator>::Item> {
        if self.right.stack.is_empty() {
            match &self.till {
                Bound::Included(key) => {
                    self.storage
                        .lookup(meta, LookupOp::GreaterOrEqual(key), &mut self.right);
                    if let Some((curr_key, _value)) = &self.right.curr {
                        if curr_key > key {
                            self.storage.lookup(meta, LookupOp::Prev, &mut self.right);
                        }
                    } else {
                        self.storage.lookup(meta, LookupOp::Last, &mut self.right);
                    }
                }
                Bound::Excluded(key) => {
                    self.storage
                        .lookup(meta, LookupOp::GreaterOrEqual(key), &mut self.right);
                    if let Some((curr_key, _value)) = &self.right.curr {
                        if curr_key >= key {
                            self.storage.lookup(meta, LookupOp::Prev, &mut self.right);
                        }
                    } else {
                        self.storage.lookup(meta, LookupOp::Last, &mut self.right);
                    }
                }
                Bound::Unbounded => self.storage.lookup(meta, LookupOp::Last, &mut self.right),
            }
        } else {
            self.storage.lookup(meta, LookupOp::Prev, &mut self.right);
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
        if let Some(meta) = self.meta {
            self.next_locked(meta)
        } else {
            let meta = self.storage.meta_shadow.read();
            self.next_locked(&meta.snapshot)
        }
    }
}

impl<'a> DoubleEndedIterator for StorageIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(meta) = self.meta {
            self.next_back_locked(meta)
        } else {
            let meta = self.storage.meta_shadow.read();
            self.next_back_locked(&meta.snapshot)
        }
    }
}

struct ShadowMetadata {
    committed: Metadata, // committed metadata state
    snapshot: Metadata,  // current metadata snapshot
}

///
/// Persistent key-value storage implementation
/// Update operations are atomic, select operations are non-atomic and observe most recent database state.
///
pub struct Storage {
    db: RwLock<Database>,
    meta_shadow: RwLock<ShadowMetadata>, // shadow copy of metadata for concurrent read-only access
    buf_mgr: Mutex<BufferManager>,
    busy_events: [Condvar; N_BUSY_EVENTS],
    pool: Vec<RwLock<PageData>>,
    file: File,
    conf: StorageConfig,
}

#[derive(Clone, Copy, Default)]
struct L2List {
    next: BufferId,
    prev: BufferId,
}

//
// Page header in buffer manager
//
#[derive(Clone, Copy, Default)]
struct PageHeader {
    pid: PageId,
    collision: BufferId,   // collision chain
    list: L2List,          // LRU l2-list
    dirty_index: BufferId, // index in dirty_pages vector
    access_count: u16,
    state: PageState, // bitmask of PAGE_RAW, PAGE_DIRTY, ...
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
    magic: u32,                            // storage magic
    version: u32,                          // storage format version
    size: PageId,                          // size of database (pages)
    used: PageId,                          // number of used database pages
    root: PageId,                          // B-Tree root page
    height: u32,                           // height of B-Tree
    xid: XID,                              // current subtransaction identifier
    _reserved: u32,                        // for future use
    alloc_pages: [PageId; N_BITMAP_PAGES], // allocator bitmap pages
}

impl Metadata {
    fn pack(self) -> [u8; PAGE_SIZE] {
        unsafe { std::mem::transmute::<Metadata, [u8; PAGE_SIZE]>(self) }
    }
    fn unpack(buf: &[u8]) -> Metadata {
        unsafe {
            std::mem::transmute::<[u8; PAGE_SIZE], Metadata>(buf[0..PAGE_SIZE].try_into().unwrap())
        }
    }
    // Update metadata fields needed for snapshot operations
    fn update(&mut self, curr: &Metadata) {
        self.root = curr.root;
        self.height = curr.height;
        self.xid = curr.xid;
    }
}

//
// Copy-on-write map entry.
// Key of COW hash map is new pid.
//
struct CowEntry {
    pid: PageId, // original page identifier (or 0 for new page)
    xid: XID,    // XID of subtransaction created this entry
}

//
// Database shared state
//
struct Database {
    meta: Metadata,                     // main copy of database metadata
    alloc_page: usize,                  // index of current allocator internal page
    alloc_page_pos: usize,              // allocator current position in internal page
    cow_map: HashMap<PageId, CowEntry>, // copy-on-write map: we have to store this mapping to avoid creation of redundant copies
    pending_deletes: Vec<PageId>, // we can no immediately delete pages, because deleted page can be reused and can not be stored in `cow` map
    overwritten_pages: Vec<PageId>, // overwritten page copies created by subtransactions
    n_alloc_pages: u64,           // number of allocated pages since session start
    n_free_pages: u64,            // number of deallocated pages since session start
    n_committed_txns: u64,        // number of committed transactions[
    n_aborted_txns: u64,          // number of aborted transactions
    n_overwritten_pages: u64,     // number of copies overwritten by subtransactions
}

impl Database {
    fn is_modified(&self) -> bool {
        !(self.cow_map.is_empty() && self.pending_deletes.is_empty())
    }

    fn get_info(&self) -> DatabaseInfo {
        DatabaseInfo {
            db_size: self.meta.size as u64 * PAGE_SIZE as u64,
            db_used: self.meta.used as u64 * PAGE_SIZE as u64,
            tree_height: self.meta.height as usize,
            n_committed_transactions: self.n_committed_txns,
            n_aborted_transactions: self.n_aborted_txns,
            n_alloc_pages: self.n_alloc_pages,
            n_free_pages: self.n_free_pages,
        }
    }
}

//
// Buffer manager is using L2-list for LRU cache eviction policy,
// L1 lists for free and dirty pages.
// All modified pages are pinned till the end of transaction.
// Indexes are used instead of pointers to reduce memory footprint and bypass Rust ownership/visibility rules.
// Page with index 0 is reserved in buffer manager for root page. It is not included in any list, so 0 is treated as terminator.
//
struct BufferManager {
    lru: L2List,                  // LRU l2-list
    dirty_buffers: Vec<BufferId>, // vector of dirty buffers
    free_buffers: BufferId,       // L1-list of free buffers

    used: BufferId,   // used part of page pool
    pinned: BufferId, // amount of pinned buffers
    cached: BufferId, // amount of cached buffers

    hash_table: Vec<BufferId>, // array containing indexes of collision chains
    buffers: Vec<PageHeader>,  // page data
    overwritten: Vec<PageId>,  // vector of buffers with page copies overwritten in subtransactions
}

//
// Wrapper class for accessing page data
//
#[derive(Clone, Copy)]
struct PageData {
    data: [u8; PAGE_SIZE],
}

impl PageData {
    fn new() -> PageData {
        PageData {
            data: [0u8; PAGE_SIZE],
        }
    }

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

    fn set_child(&mut self, ip: ItemPointer, pid: PageId) {
        let offs = self.get_offs(ip);
        let key_len = self.data[offs] as usize;
        self.set_u32(offs + key_len + 1, pid)
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

    fn test_bit(&self, bit: usize) -> bool {
        (self.data[bit >> 3] & (1 << (bit & 7))) != 0
    }

    fn test_and_set_bit(&mut self, bit: usize) -> bool {
        if (self.data[bit >> 3] & (1 << (bit & 7))) == 0 {
            self.data[bit >> 3] |= 1 << (bit & 7);
            true
        } else {
            false
        }
    }

    fn clear_bit(&mut self, bit: usize) {
        debug_assert!((self.data[bit >> 3] & (1 << (bit & 7))) != 0);
        self.data[bit >> 3] &= !(1 << (bit & 7));
    }

    fn find_first_zero_bit(&self, offs: usize) -> usize {
        let bytes = &self.data;
        for i in offs..PAGE_SIZE {
            if bytes[i] != 0xFFu8 {
                return i * 8 + bytes[i].trailing_ones() as usize;
            }
        }
        usize::MAX
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

    fn remove_key(&mut self, ip: ItemPointer, leaf: bool) {
        let n_items = self.get_n_items();
        let size = self.get_size();
        let (item_offs, item_len) = self.get_item_offs_len(ip);
        for i in ip + 1..n_items {
            self.set_offs(i - 1, self.get_offs(i) + item_len);
        }
        let items_origin = PAGE_SIZE - size;
        if !leaf && n_items > 1 && ip + 1 == n_items {
            // If we are removing last child of internal page then copy it's key to the previous item
            let prev_item_offs = item_offs + item_len;
            let key_len = self.data[item_offs] as usize;
            let prev_key_len = self.data[prev_item_offs] as usize;
            let new_offs = prev_item_offs + prev_key_len - key_len;
            self.set_offs(ip - 1, new_offs);
            self.data
                .copy_within(item_offs..item_offs + prev_key_len + 1, new_offs);
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
    fn split(&mut self, new_page: &mut PageData, ip: ItemPointer) -> ItemPointer {
        let n_items = self.get_n_items();
        let size = self.get_size();
        let mut r = n_items;

        if ip == r {
            // Optimization for insert of sequential keys: move all data to new page,
            // leaving original page empty. It will cause complete filling of B-Tree pages.
            r -= 1;
        } else {
            // Divide page in two approximately equal parts.
            let margin = PAGE_SIZE - size / 2;
            let mut l: ItemPointer = 0;
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
        }
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
    fn get_buffer_pid(&self, id: BufferId) -> PageId {
        self.buffers[id as usize].pid
    }

    fn get_buffer_state(&self, id: BufferId) -> PageState {
        self.buffers[id as usize].state
    }

    fn set_buffer_state(&mut self, id: BufferId, state: PageState) {
        self.buffers[id as usize].state = state
    }

    //
    // Link buffer to the head of LRU list (make it acceptable for eviction)
    //
    fn unpin(&mut self, id: BufferId) {
        debug_assert!(self.buffers[id as usize].access_count == 1);
        self.buffers[id as usize].access_count = 0;
        self.buffers[id as usize].list.next = self.lru.next;
        self.buffers[id as usize].list.prev = 0;
        self.pinned -= 1;
        if self.lru.next != 0 {
            self.buffers[self.lru.next as usize].list.prev = id;
        } else {
            self.lru.prev = id;
        }
        self.lru.next = id;
    }

    //
    // Unlink buffer from LRU list and so pin it in memory (protect from eviction)
    //
    fn pin(&mut self, id: BufferId) {
        debug_assert!(self.buffers[id as usize].access_count == 0);
        let next = self.buffers[id as usize].list.next;
        let prev = self.buffers[id as usize].list.prev;
        if prev == 0 {
            self.lru.next = next;
        } else {
            self.buffers[prev as usize].list.next = next;
        }
        if next == 0 {
            self.lru.prev = prev;
        } else {
            self.buffers[next as usize].list.prev = prev;
        }
        self.pinned += 1;
    }

    //
    // Insert page in hash table
    //
    fn insert(&mut self, id: BufferId) {
        let h = self.buffers[id as usize].pid as usize % self.hash_table.len();
        self.buffers[id as usize].collision = self.hash_table[h];
        self.hash_table[h] = id;
    }

    //
    // Remove page from hash table
    //
    fn remove(&mut self, id: BufferId) {
        let h = self.buffers[id as usize].pid as usize % self.hash_table.len();
        let mut p = self.hash_table[h];
        if p == id {
            self.hash_table[h] = self.buffers[id as usize].collision;
        } else {
            while self.buffers[p as usize].collision != id {
                p = self.buffers[p as usize].collision;
            }
            self.buffers[p as usize].collision = self.buffers[id as usize].collision;
        }
    }

    //
    // Throw away buffer from cache (used by transaction rollback)
    //
    fn throw_buffer(&mut self, id: BufferId) {
        self.pin(id);
        self.remove(id);
        self.buffers[id as usize].list.next = self.free_buffers;
        self.free_buffers = id;
        self.cached -= 1;
        self.pinned -= 1;
    }

    //
    // Try to locate buffer with specified PID and throw away it from cache
    //
    fn forget_buffer(&mut self, pid: PageId) {
        let hash = pid as usize % self.hash_table.len();
        let mut buf = self.hash_table[hash];
        while buf != 0 {
            if self.buffers[buf as usize].pid == pid {
                self.undirty_buffer(buf);
                self.throw_buffer(buf);
                break;
            }
            buf = self.buffers[buf as usize].collision;
        }
    }

    //
    // If buffer is not yet marked as dirty then mark it as dirty and return true
    //
    fn modify_buffer(&mut self, id: BufferId) -> bool {
        debug_assert!(self.buffers[id as usize].access_count > 0);
        if (self.buffers[id as usize].state & PAGE_DIRTY) == 0 {
            self.buffers[id as usize].state = PAGE_DIRTY;
            self.buffers[id as usize].dirty_index = self.dirty_buffers.len() as BufferId;
            self.dirty_buffers.push(id);
            true
        } else {
            false
        }
    }

    //
    // Change pid for copied buffer
    //
    fn copy_on_write(
        &mut self,
        id: BufferId,
        new_pid: PageId,
        try_reuse_buffer: bool,
    ) -> Result<(BufferId, PageId)> {
        if try_reuse_buffer && self.buffers[id as usize].access_count == 1 {
            // If we are the only user of the buffer, we can replace it's PID in place.
            self.remove(id);
            self.forget_buffer(new_pid);
            self.buffers[id as usize].pid = new_pid;
            self.insert(id);
            Ok((id, 0))
        } else {
            // otherwise create new buffer
            self.new_buffer(new_pid)
        }
    }

    //
    // Decrement buffer's access counter and release buffer if it is last reference
    //
    fn release_buffer(&mut self, id: BufferId) {
        debug_assert!(self.buffers[id as usize].access_count > 0);
        if self.buffers[id as usize].access_count == 1 {
            self.unpin(id);
        } else {
            self.buffers[id as usize].access_count -= 1;
        }
    }

    //
    // Find buffer with specified page or allocate new buffer.
    // Returns id of located buffer and dirty evicted page id (or 0 if none)
    //
    fn get_buffer(&mut self, pid: PageId) -> Result<(BufferId, PageId)> {
        let hash = pid as usize % self.hash_table.len();
        let mut buf = self.hash_table[hash];
        while buf != 0 {
            if self.buffers[buf as usize].pid == pid {
                let access_count = self.buffers[buf as usize].access_count;
                debug_assert!(access_count < u16::MAX - 1);
                if access_count == 0 {
                    self.pin(buf);
                }
                self.buffers[buf as usize].access_count = access_count + 1;
                return Ok((buf, 0));
            }
            buf = self.buffers[buf as usize].collision;
        }
        // page not found in cache
        let mut evicted_pid = 0;
        let mut buf = self.free_buffers;
        if buf != 0 {
            // has some free buffers
            self.free_buffers = self.buffers[buf as usize].list.next;
            self.cached += 1;
            self.pinned += 1;
            self.buffers[buf as usize].state = 0;
        } else {
            buf = self.used;
            if (buf as usize) < self.buffers.len() {
                self.used += 1;
                self.cached += 1;
                self.pinned += 1;
            } else {
                // Replace least recently used page
                let victim = self.lru.prev;
                ensure!(victim != 0);
                debug_assert!(self.buffers[victim as usize].access_count == 0);
                let state = self.buffers[victim as usize].state;
                if (state & PAGE_DIRTY) != 0 {
                    // remove page from dirty vector
                    self.undirty_buffer(victim);
                    evicted_pid = self.buffers[victim as usize].pid;
                }
                debug_assert!((state & (PAGE_BUSY | PAGE_RAW)) == 0);
                self.pin(victim);
                self.remove(victim);
                buf = victim;
            }
        }
        self.buffers[buf as usize].access_count = 1;
        self.buffers[buf as usize].pid = pid;
        self.buffers[buf as usize].state = PAGE_RAW;
        self.insert(buf);
        Ok((buf, evicted_pid))
    }

    //
    // Allocate new buffer.
    //
    fn new_buffer(&mut self, pid: PageId) -> Result<(BufferId, PageId)> {
        let (buf, evicted_pid) = self.get_buffer(pid)?;
        debug_assert!(self.buffers[buf as usize].access_count == 1);
        debug_assert!((self.buffers[buf as usize].state & PAGE_DIRTY) == 0);
        self.buffers[buf as usize].state = PAGE_RAW;
        Ok((buf, evicted_pid))
    }

    //
    // Remove dirty mark from the buffer
    //
    fn undirty_buffer(&mut self, buf: BufferId) {
        if (self.buffers[buf as usize].state & PAGE_DIRTY) != 0 {
            let last_dirty = self.dirty_buffers.pop().unwrap();
            if last_dirty != buf {
                // Replace 'buf' in dirty_buffers vector with last element
                let dirty_index = self.buffers[buf as usize].dirty_index;
                self.buffers[last_dirty as usize].dirty_index = dirty_index;
                self.dirty_buffers[dirty_index as usize] = last_dirty;
            }
            self.buffers[buf as usize].state &= !PAGE_DIRTY;
        }
    }
}

struct PageGuard<'a> {
    buf: BufferId,
    pid: PageId,
    storage: &'a Storage,
}

impl<'a> PageGuard<'a> {
    //
    // Copy on write page
    //
    fn modify(&mut self, db: &mut Database, bitmap_page: Option<usize>) -> Result<()> {
        let mut bm = self.storage.buf_mgr.lock();
        let old_buf = self.buf;
        let old_pid = bm.get_buffer_pid(old_buf);
        let just_modified = bm.modify_buffer(old_buf);

        // Check if there is not mapping for this page in this subtransaction,
        // Allocator bitmap pages are not accessed by read-only queries, so there is no need to clone
        // bitmap pages for subtrasactions.
        let orig_pid: PageId;
        match db.cow_map.get(&old_pid) {
            None => {
                if !just_modified {
                    // page already marked as dirty (recusion inside allocator)
                    return Ok(());
                }
                // page was not yet updated by this transactions
                orig_pid = old_pid;
            }
            Some(entry) => {
                // Page was created or updated by this transactions
                if entry.xid == db.meta.xid || bitmap_page.is_some() {
                    // Page is updated by this subtransaction or is bitmap page:
                    // in this case we do not need to create new copy
                    return Ok(());
                }
                // Overwitten page was already copied by some subtransaction.
                // We still have to create copy to provide isolation.
                // But this copy can be removed once this subtransaction is completed.
                bm.overwritten.push(old_pid);
                if entry.pid != 0 {
                    db.pending_deletes.push(entry.pid);
                }
                db.cow_map.remove(&old_pid).unwrap();
                orig_pid = 0;
            }
        }
        drop(bm);

        // Create copy
        let new_pid = self.storage.allocate_page(db)?;
        db.cow_map.insert(
            new_pid,
            CowEntry {
                pid: orig_pid,
                xid: db.meta.xid,
            },
        ); // remember COW mapping

        self.pid = new_pid;
        let mut bm = self.storage.buf_mgr.lock();
        let (new_buf, evicted_pid) = bm.copy_on_write(old_buf, new_pid, just_modified)?;
        if evicted_pid != 0 {
            // dirty page was evicted
            let page = self.storage.pool[new_buf as usize].read();
            self.storage
                .file
                .write_all_at(&page.data, evicted_pid as u64 * PAGE_SIZE as u64)?;
        }
        if new_buf != old_buf {
            // Copy page content to the new buffer
            let mut dst = self.storage.pool[new_buf as usize].write();
            let src = self.storage.pool[old_buf as usize].read();
            *dst = *src;
            debug_assert!(bm.get_buffer_state(new_buf) == PAGE_RAW);
            if just_modified {
                bm.undirty_buffer(old_buf);
            }
            bm.release_buffer(old_buf);
            assert!(bm.modify_buffer(new_buf));
            self.buf = new_buf;
        }
        drop(bm);

        // Prevent infinite recursion
        if let Some(alloc_page) = bitmap_page {
            db.meta.alloc_pages[alloc_page] = new_pid;
        }
        self.storage.prepare_free_page(db, old_pid)?; // prepare deallocation of original page
        Ok(())
    }
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
    // Remove copy-on-write pages overwritten by completed subtransaction.
    //
    fn remove_redundand_copies(&self, db: &mut Database) -> Result<()> {
        let mut overwritten: Vec<PageId> = Vec::new();
        let mut bm = self.buf_mgr.lock();
        std::mem::swap(&mut bm.overwritten, &mut overwritten);
        db.n_overwritten_pages += overwritten.len() as u64;
        for pid in &overwritten {
            bm.forget_buffer(*pid);
        }
        db.overwritten_pages.append(&mut overwritten);
        Ok(())
    }

    //
    // Allocate page
    //
    fn allocate_page(&self, db: &mut Database) -> Result<PageId> {
        // Fast path: try to reuse overwritten page
        if let Some(pid) = db.overwritten_pages.pop() {
            return Ok(pid);
        }
        db.n_alloc_pages += 1;
        //
        // Cyclic scan of bitmap pages. We start with position pointed by (alloc_page,alloc_page_pos)
        // and continue iteration until we hole or reach end of bitmap.
        // In the last case we restart scanning from the beginning but if hole is not found on this pass,
        // then append new bitmap page
        //
        let mut first_pass = true;
        loop {
            // loop through all bitmap pages
            let mut pin;
            let mut bp = db.meta.alloc_pages[db.alloc_page];
            if bp == 0 {
                // bitmap page doesn't exists
                if first_pass && db.alloc_page != 0 {
                    // restart scan from the beginning
                    db.alloc_page = 0;
                    first_pass = false;
                    continue;
                }
                // Allocate bitmap page at the end of database
                bp = db.meta.size;
                db.meta.size += 1;
                pin = self.new_page_at(db, bp)?;
                db.meta.alloc_pages[db.alloc_page] = pin.pid;
                if db.alloc_page == 0 {
                    // Initialize first bitmap page: we need to mark root page as occupied
                    assert!(self.pool[pin.buf as usize].write().test_and_set_bit(0));
                }
                self.mark_page(db, bp)?;
            } else {
                // Open existed bitmap page
                pin = self.get_page(bp)?;
            }
            let bit = self.pool[pin.buf as usize]
                .read()
                .find_first_zero_bit(db.alloc_page_pos);
            if bit != usize::MAX {
                // hole located
                pin.modify(db, Some(db.alloc_page))?;

                // Copying of allocator's pages can occupy hole we have found,
                // so we need first to check if it is still available
                if self.pool[pin.buf as usize].write().test_and_set_bit(bit) {
                    let pid = (db.alloc_page * ALLOC_BITMAP_SIZE + bit) as PageId;
                    if pid >= db.meta.size {
                        db.meta.size = pid + 1;
                    }
                    db.alloc_page_pos = bit / 8;
                    db.meta.used += 1;
                    return Ok(pid);
                }
            } else {
                // No free space in this bitmap page, try next page
                db.alloc_page_pos = 0;
                db.alloc_page += 1;
                if db.alloc_page == N_BITMAP_PAGES {
                    ensure!(first_pass); // OOM detection

                    // Restart scanning from the beginning
                    db.alloc_page = 0;
                    first_pass = false;
                }
            }
        }
    }

    fn test_allocator_bitmap(&self, meta: &Metadata, pid: PageId) -> Result<bool> {
        let alloc_page = pid as usize / ALLOC_BITMAP_SIZE;
        let bit = pid as usize % ALLOC_BITMAP_SIZE;
        let bp = meta.alloc_pages[alloc_page];
        let pin = self.get_page(bp)?;
        Ok(self.pool[pin.buf as usize].read().test_bit(bit))
    }

    fn update_allocator_bitmap(&self, db: &mut Database, pid: PageId, op: BitmapOp) -> Result<()> {
        let alloc_page = pid as usize / ALLOC_BITMAP_SIZE;
        let bit = pid as usize % ALLOC_BITMAP_SIZE;
        let bp = db.meta.alloc_pages[alloc_page];
        let mut pin = self.get_page(bp)?;
        pin.modify(db, Some(alloc_page))?;

        match op {
            BitmapOp::Prepare => {
                // All allocator pages related to this pid are copied: we are ready to perform free operation
            }
            BitmapOp::Set => {
                // Nobody else should use this slot
                assert!(self.pool[pin.buf as usize].write().test_and_set_bit(bit))
            }
            BitmapOp::Clear => {
                // Set allocator's current position if deallocated page is before it
                let alloc_page_pos = bit / 8;
                if alloc_page < db.alloc_page
                    || (alloc_page == db.alloc_page && alloc_page_pos < db.alloc_page_pos)
                {
                    db.alloc_page = alloc_page;
                    db.alloc_page_pos = alloc_page_pos;
                }
                self.pool[pin.buf as usize].write().clear_bit(bit)
            }
        }
        Ok(())
    }

    //
    // When we start deallocation of original pages during commit,
    // we can not perform allocations any more, because allocated page
    // may overwrite original page and we loose consistent state in case
    // of abnormal transaction termination. So we first need to "probe"
    // deallocation, enforcing copying of all affected allocator's pages.
    //
    fn prepare_free_page(&self, db: &mut Database, pid: PageId) -> Result<()> {
        self.update_allocator_bitmap(db, pid, BitmapOp::Prepare)
    }

    fn mark_page(&self, db: &mut Database, pid: PageId) -> Result<()> {
        self.update_allocator_bitmap(db, pid, BitmapOp::Set)
    }

    fn free_page(&self, db: &mut Database, pid: PageId) -> Result<()> {
        self.update_allocator_bitmap(db, pid, BitmapOp::Clear)?;
        db.meta.used -= 1;
        db.n_free_pages += 1;
        Ok(())
    }

    //
    // Unpin page (called by PageGuard)
    //
    fn release_page(&self, buf: BufferId) {
        let mut bm = self.buf_mgr.lock();
        bm.release_buffer(buf);
    }

    //
    // Allocate new page in storage and get buffer for it
    //
    fn new_page(&self, db: &mut Database) -> Result<PageGuard<'_>> {
        let pid = self.allocate_page(db)?;
        self.new_page_at(db, pid)
    }

    //
    // Allocate buffer for new page
    //
    fn new_page_at(&self, db: &mut Database, pid: PageId) -> Result<PageGuard<'_>> {
        db.cow_map.insert(
            pid,
            CowEntry {
                pid: 0,
                xid: db.meta.xid,
            },
        ); // remember new page in COW map to avoid its cloning

        let mut bm = self.buf_mgr.lock();

        let (buf, evicted_pid) = bm.new_buffer(pid)?;
        let mut page = self.pool[buf as usize].write();
        if evicted_pid != 0 {
            // dirty page was evicted
            self.file
                .write_all_at(&mut page.data, evicted_pid as u64 * PAGE_SIZE as u64)?;
        }
        page.data.fill(0u8);
        assert!(bm.modify_buffer(buf));

        Ok(PageGuard {
            buf,
            pid,
            storage: &self,
        })
    }

    //
    // Read page in buffer and return PageGuard with pinned buffer.
    // Buffer will be automatically released on exiting from scope
    //
    fn get_page(&self, pid: PageId) -> Result<PageGuard<'_>> {
        let mut bm = self.buf_mgr.lock();
        let (buf, evicted_pid) = bm.get_buffer(pid)?;
        let state = bm.get_buffer_state(buf);
        if evicted_pid != 0 {
            // dirty page was evicted
            let page = self.pool[buf as usize].read();
            self.file
                .write_all_at(&page.data, evicted_pid as u64 * PAGE_SIZE as u64)?;
        }
        if (state & PAGE_BUSY) != 0 {
            // Some other thread is loading buffer: just wait until it done
            bm.set_buffer_state(buf, state | PAGE_WAIT);
            loop {
                debug_assert!((bm.get_buffer_state(buf) & PAGE_WAIT) != 0);
                self.busy_events[buf as usize % N_BUSY_EVENTS].wait(&mut bm);
                if (bm.get_buffer_state(buf) & PAGE_BUSY) == 0 {
                    break;
                }
            }
        } else if (state & PAGE_RAW) != 0 {
            // Read page
            debug_assert!(state == PAGE_RAW);
            bm.set_buffer_state(buf, PAGE_BUSY);
            drop(bm); // read page without holding lock
            {
                let mut page = self.pool[buf as usize].write();
                self.file
                    .read_exact_at(&mut page.data, pid as u64 * PAGE_SIZE as u64)?;
            }
            bm = self.buf_mgr.lock();
            if (bm.get_buffer_state(buf) & PAGE_WAIT) != 0 {
                // Somebody is waiting for us
                self.busy_events[buf as usize % N_BUSY_EVENTS].notify_all();
            }
            debug_assert!((bm.get_buffer_state(buf) & !(PAGE_BUSY | PAGE_WAIT)) == 0);
            bm.set_buffer_state(buf, 0);
        }
        Ok(PageGuard {
            buf,
            pid,
            storage: &self,
        })
    }

    ///
    /// Take database snapshot for providing repeatable reads.
    /// Snapshot blocks commit of update transaction.
    ///
    pub fn take_snapshot(&self) -> Snapshot<'_> {
        Snapshot {
            storage: self,
            meta: self.meta_shadow.read(),
        }
    }

    ///
    /// Start read-only transaction for MURSIW mode. If you need concurrent execution of readers and writer,
    /// then use take_snapshot()
    ///
    pub fn read_only_transaction(&self) -> ReadOnlyTransaction<'_> {
        ReadOnlyTransaction {
            storage: self,
            db: self.db.read(),
        }
    }

    ///
    /// Start update transaction. Transaction should be either committed,
    /// either aborted. If transaction is not explicitly committed,
    /// then it is implicitly aborted when left out of scope.
    ///
    pub fn start_transaction(&self) -> Transaction<'_> {
        Transaction {
            status: TransactionStatus::InProgress,
            storage: self,
            db: self.db.write(),
        }
    }

    fn commit(&self, db: &mut Database) -> Result<()> {
        let result = self.try_commit(db);
        if result.is_err() {
            self.rollback(db);
        }
        result
    }

    fn try_commit(&self, db: &mut Database) -> Result<()> {
        // Check if something was changed
        if db.is_modified() {
            // First prepare deallocation...
            let mut pending_deletes: Vec<PageId> = Vec::new();
            std::mem::swap(&mut db.pending_deletes, &mut pending_deletes);
            for pg in &pending_deletes {
                self.prepare_free_page(db, *pg)?;
            }
            let mut overwritten_pages: Vec<PageId> = Vec::new();
            std::mem::swap(&mut db.overwritten_pages, &mut overwritten_pages);
            for pg in &overwritten_pages {
                self.prepare_free_page(db, *pg)?;
            }
            // ... then actually do it
            let n_alloc_pages = db.n_alloc_pages;
            for pg in &pending_deletes {
                self.free_page(db, *pg)?;
            }
            for pg in &overwritten_pages {
                self.free_page(db, *pg)?;
            }
            // Delete original pages (them should be already prepared)
            let orig: Vec<PageId> = db
                .cow_map
                .iter()
                .map(|(_new, entry)| entry.pid)
                .filter(|pid| *pid != 0)
                .collect();
            for pid in orig {
                self.free_page(db, pid)?;
            }
            assert!(n_alloc_pages == db.n_alloc_pages); // no allocation should happen during deallocation
            db.cow_map.clear();

            self.flush_buffers()?;
            if !self.conf.nosync {
                self.file.sync_all()?;
            }
            db.meta.xid = db.meta.xid.wrapping_add(1);
            let meta = db.meta.pack();
            self.file.write_all_at(&meta, 0)?;
            if !self.conf.nosync {
                self.file.sync_all()?;
            }
            db.n_committed_txns += 1;

            let mut meta_shadow = self.meta_shadow.write();
            meta_shadow.committed = db.meta;
            meta_shadow.snapshot.update(&db.meta);
        }
        Ok(())
    }

    //
    // Flush dirty pages to the disk.
    //
    fn flush_buffers(&self) -> Result<()> {
        let bm: &mut BufferManager = &mut self.buf_mgr.lock();
        for bp in &bm.dirty_buffers {
            let buf = *bp as usize;
            let pid = bm.buffers[buf].pid;
            let file_offs = pid as u64 * PAGE_SIZE as u64;
            let page = self.pool[buf].read();
            self.file.write_all_at(&page.data, file_offs)?;
            debug_assert!((bm.buffers[buf].state & PAGE_DIRTY) != 0);
            bm.buffers[buf].state = 0;
        }
        bm.dirty_buffers.clear();
        Ok(())
    }

    //
    // Rollback current transaction
    //
    fn rollback(&self, db: &mut Database) {
        let mut bm = self.buf_mgr.lock();
        if db.is_modified() {
            // Just throw away all dirty buffers from buffer cache to force reloading of original pages
            for i in 0..bm.dirty_buffers.len() {
                let buf = bm.dirty_buffers[i];
                debug_assert!((bm.get_buffer_state(buf) & PAGE_DIRTY) != 0);
                bm.throw_buffer(buf);
            }
            bm.dirty_buffers.clear();

            // But it is not enough to throw away just dirty buffers,
            // some modified buffers can already be save on the disk.
            // So we have to do loop through COW map
            let mut cow_map: HashMap<PageId, CowEntry> = HashMap::new();
            std::mem::swap(&mut db.cow_map, &mut cow_map);
            for (new_pid, _entry) in &cow_map {
                bm.forget_buffer(*new_pid);
            }
            // restore old metadata state but preserve current XID
            let mut meta_shadow = self.meta_shadow.write();
            let xid = db.meta.xid;
            db.meta = meta_shadow.committed;
            db.meta.xid = xid;
            meta_shadow.snapshot.update(&db.meta);

            db.n_aborted_txns += 1;
            db.pending_deletes.clear();
            db.overwritten_pages.clear();
        }
    }

    ///
    /// Open database storage. If storage file doesn't exist, then it is created.
    /// If path to transaction log is not specified, then WAL (write-ahead-log) is not used.
    /// It will significantly increase performance but can cause database corruption in case of power failure or system crash.
    ///
    pub fn open(db_path: &Path, conf: StorageConfig) -> Result<Storage> {
        let mut buf = [0u8; PAGE_SIZE];
        let (file, meta) = if let Ok(file) = OpenOptions::new().write(true).read(true).open(db_path)
        {
            // open existed file
            file.try_lock_exclusive()?;
            file.read_exact_at(&mut buf, 0)?;
            let meta = Metadata::unpack(&buf);
            ensure!(meta.magic == MAGIC && meta.version == VERSION && meta.size >= 1);
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
                size: 1,
                used: 1,
                root: 0,
                height: 0,
                xid: 0,
                _reserved: 0,
                alloc_pages: [0u32; N_BITMAP_PAGES],
            };
            let metadata = meta.pack();
            file.write_all_at(&metadata, 0)?;
            (file, meta)
        };
        let storage = Storage {
            busy_events: [(); N_BUSY_EVENTS].map(|_| Condvar::new()),
            buf_mgr: Mutex::new(BufferManager {
                lru: L2List::default(),
                dirty_buffers: Vec::with_capacity(conf.cache_size),
                free_buffers: 0,
                used: 1, // pinned root page
                cached: 1,
                pinned: 1,
                hash_table: vec![0; conf.cache_size],
                buffers: vec![PageHeader::new(); conf.cache_size],
                overwritten: Vec::new(),
            }),
            pool: iter::repeat_with(|| RwLock::new(PageData::new()))
                .take(conf.cache_size)
                .collect(),
            file,
            conf,
            meta_shadow: RwLock::new(ShadowMetadata {
                snapshot: meta,
                committed: meta,
            }),
            db: RwLock::new(Database {
                meta,
                alloc_page: 0,
                alloc_page_pos: 0,
                cow_map: HashMap::new(),
                pending_deletes: Vec::new(),
                overwritten_pages: Vec::new(),
                n_alloc_pages: 0,
                n_free_pages: 0,
                n_committed_txns: 0,
                n_aborted_txns: 0,
                n_overwritten_pages: 0,
            }),
        };
        Ok(storage)
    }

    //
    // Bulk update
    //
    fn do_updates(
        &self,
        db: &mut Database,
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
        db: &mut Database,
        key: &Key,
        value: &Value,
    ) -> Result<PageId> {
        let pin = self.new_page(db)?;
        let mut page = self.pool[pin.buf as usize].write();
        page.set_n_items(0);
        page.insert_item(0, key, value);
        Ok(pin.pid)
    }

    //
    // Allocate new B-Tree internal page referencing two children
    //
    fn btree_allocate_internal_page(
        &self,
        db: &mut Database,
        key: &Key,
        left_child: PageId,
        right_child: PageId,
    ) -> Result<PageId> {
        let pin = self.new_page(db)?;
        let mut page = self.pool[pin.buf as usize].write();
        page.set_n_items(0);
        debug_assert!(left_child != 0);
        debug_assert!(right_child != 0);
        page.insert_item(0, key, &left_child.to_be_bytes().to_vec());
        page.insert_item(1, &vec![], &right_child.to_be_bytes().to_vec());
        Ok(pin.pid)
    }

    //
    // Insert item at the specified position in B-Tree page.
    // If B-Tree pages is full then split it, evenly distribute items between pages: smaller items moved to new page, larger items left on original page.
    // Value of largest key on new page and its identifiers are returned in case of overflow.
    //
    fn btree_insert_in_page(
        &self,
        db: &mut Database,
        page: &mut PageData,
        ip: ItemPointer,
        key: &Key,
        value: &Value,
    ) -> Result<Option<(Key, PageId)>> {
        if !page.insert_item(ip, key, value) {
            // page is full then divide page
            let pin = self.new_page(db)?;
            let mut new_page = self.pool[pin.buf as usize].write();
            let split = page.split(&mut new_page, ip);
            let ok = if ip > split {
                page.insert_item(ip - split - 1, key, value)
            } else {
                new_page.insert_item(ip, key, value)
            };
            ensure!(ok);
            Ok(Some((new_page.get_last_key(), pin.pid)))
        } else {
            Ok(None)
        }
    }

    //
    // Remove key from B-Tree. Returns ID of updated page or 0 in case of underflow
    // Right now we do not redistribute nodes between pages or merge pages, underflow is reported only if page becomes empty.
    // If key is not found, then nothing is performed and no error is reported.
    //
    fn btree_remove(
        &self,
        db: &mut Database,
        pid: PageId,
        key: &Key,
        height: u32,
    ) -> Result<PageId> {
        let mut pin = self.get_page(pid)?;
        let page = self.pool[pin.buf as usize].read();
        let mut l: ItemPointer = 0;
        let n_items = page.get_n_items();
        let mut r = n_items;
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
            if r < n_items && page.compare_key(r, key) == Ordering::Equal {
                drop(page);
                if n_items != 1 {
                    pin.modify(db, None)?;
                    let mut page = self.pool[pin.buf as usize].write();
                    page.remove_key(r, true);
                    Ok(pin.pid)
                } else {
                    // free page
                    db.pending_deletes.push(pid);
                    // avoid redundant write of deleted page modifications to the disk
                    self.buf_mgr.lock().undirty_buffer(pin.buf);
                    Ok(0)
                }
            } else {
                Ok(pid)
            }
        } else {
            // recurse to next level
            debug_assert!(r < n_items);
            let old_child = page.get_child(r);
            drop(page);
            let new_child = self.btree_remove(db, old_child, key, height - 1)?;
            if new_child == 0 {
                // underflow
                if n_items != 1 {
                    pin.modify(db, None)?;
                    let mut page = self.pool[pin.buf as usize].write();
                    page.remove_key(r, false);
                    Ok(pin.pid)
                } else {
                    // free page
                    db.pending_deletes.push(pid);
                    // avoid redundant write of deleted page modifications to the disk
                    self.buf_mgr.lock().undirty_buffer(pin.buf);
                    Ok(0)
                }
            } else {
                pin.modify(db, None)?;
                let mut page = self.pool[pin.buf as usize].write();
                page.set_child(r, new_child);
                Ok(pin.pid)
            }
        }
    }

    //
    // Insert item in B-Tree. Recursively traverse B-Tree and return new page id and position of new page in case of overflow.
    //
    fn btree_insert(
        &self,
        db: &mut Database,
        pid: PageId,
        key: &Key,
        value: &Value,
        height: u32,
    ) -> Result<(PageId, Option<(Key, PageId)>)> {
        let mut pin = self.get_page(pid)?;
        let page = self.pool[pin.buf as usize].read();
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
            drop(page);
            pin.modify(db, None)?;
            let mut page = self.pool[pin.buf as usize].write();
            if r < n && page.compare_key(r, key) == Ordering::Equal {
                // replace old value with new one: just remove old one and reinsert new key-value pair
                page.remove_key(r, true);
            }
            Ok((
                pin.pid,
                self.btree_insert_in_page(db, &mut page, r, key, value)?,
            ))
        } else {
            // recurse to next level
            debug_assert!(r < n);
            let r_child = page.get_child(r);
            drop(page);
            let (child, overflow) = self.btree_insert(db, r_child, key, value, height - 1)?;
            pin.modify(db, None)?;
            let mut page = self.pool[pin.buf as usize].write();
            page.set_child(r, child);
            if let Some((key, child)) = overflow {
                // insert new page before original
                debug_assert!(child != 0);
                Ok((
                    pin.pid,
                    self.btree_insert_in_page(
                        db,
                        &mut page,
                        r,
                        &key,
                        &child.to_be_bytes().to_vec(),
                    )?,
                ))
            } else {
                Ok((pin.pid, None))
            }
        }
    }

    //
    // Insert or update key in the storage
    //
    fn do_upsert(&self, db: &mut Database, key: &Key, value: &Value) -> Result<()> {
        ensure!(key.len() != 0 && key.len() <= MAX_KEY_LEN && value.len() <= MAX_VALUE_LEN);
        if db.meta.root == 0 {
            db.meta.root = self.btree_allocate_leaf_page(db, key, value)?;
            db.meta.height = 1;
        } else {
            let (new_root, overflow) =
                self.btree_insert(db, db.meta.root, key, value, db.meta.height)?;
            if let Some((key, page)) = overflow {
                db.meta.root = self.btree_allocate_internal_page(db, &key, page, new_root)?;
                db.meta.height += 1;
            } else {
                db.meta.root = new_root;
            }
        }
        Ok(())
    }

    //
    // Remove key from the storage. Does nothing it key not exists.
    //
    fn do_remove(&self, db: &mut Database, key: &Key) -> Result<()> {
        if db.meta.root != 0 {
            let new_root = self.btree_remove(db, db.meta.root, key, db.meta.height)?;
            db.meta.root = new_root;
            if new_root == 0 {
                // underflow
                db.meta.height = 0;
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
        meta: &Metadata,
        op: LookupOp,
        path: &mut TreePath,
    ) -> Result<Option<(Key, Value)>> {
        match op {
            LookupOp::First => {
                // Locate left-most element in the tree
                let mut pid = meta.root;
                if pid != 0 {
                    let mut level = meta.height;
                    loop {
                        let pin = self.get_page(pid)?;
                        let page = self.pool[pin.buf as usize].read();
                        path.stack.push(PagePos { pid, pos: 0 });
                        level -= 1;
                        if level == 0 {
                            path.curr = Some(page.get_item(0));
                            path.xid = meta.xid;
                            break;
                        } else {
                            pid = page.get_child(0)
                        }
                    }
                }
            }
            LookupOp::Last => {
                // Locate right-most element in the tree
                let mut pid = meta.root;
                if pid != 0 {
                    let mut level = meta.height;
                    loop {
                        let pin = self.get_page(pid)?;
                        let page = self.pool[pin.buf as usize].read();
                        let pos = page.get_n_items() - 1;
                        level -= 1;
                        path.stack.push(PagePos { pid, pos });
                        if level == 0 {
                            path.curr = Some(page.get_item(pos));
                            path.xid = meta.xid;
                            break;
                        } else {
                            pid = page.get_child(pos)
                        }
                    }
                }
            }
            LookupOp::Next => {
                if path.xid == meta.xid || self.reconstruct_path(path, meta)? {
                    self.move_forward(path, meta.height)?;
                }
            }
            LookupOp::Prev => {
                if path.xid == meta.xid || self.reconstruct_path(path, meta)? {
                    self.move_backward(path, meta.height)?;
                }
            }
            LookupOp::GreaterOrEqual(key) => {
                if meta.root != 0 && self.find(meta.root, path, &key, meta.height)? {
                    path.xid = meta.xid;
                }
            }
        }
        Ok(path.curr.clone())
    }

    //
    // Perform lookup in the database. Initialize path in the tree current element or reset path if no element is found or end of set is reached.
    //
    fn lookup(&self, meta: &Metadata, op: LookupOp, path: &mut TreePath) {
        let result = self.do_lookup(meta, op, path);
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
        let pin = self.get_page(pid)?;
        let page = self.pool[pin.buf as usize].read();
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
                path.stack.pop();
                Ok(false)
            }
        } else {
            debug_assert!(r < n);
            loop {
                debug_assert!(page.get_child(r) != 0);
                if self.find(page.get_child(r), path, key, height - 1)? {
                    return Ok(true);
                }
                path.stack.pop();
                r += 1;
                if r < n {
                    path.stack.push(PagePos { pid, pos: r });
                } else {
                    break;
                }
            }
            Ok(false)
        }
    }

    //
    // If storage is updated between iterations then try to reconstruct path by locating current element.
    // Returns true is such element is found and path is successfully reconstructed, false otherwise.
    //
    fn reconstruct_path(&self, path: &mut TreePath, meta: &Metadata) -> Result<bool> {
        path.stack.clear();
        if let Some((key, _value)) = &path.curr.clone() {
            if self.find(meta.root, path, &key, meta.height)? {
                if let Some((ge_key, _value)) = &path.curr {
                    if ge_key == key {
                        path.xid = meta.xid;
                        debug_assert!(path.stack.len() == meta.height as usize);
                        return Ok(true);
                    }
                }
            }
        }
        path.curr = None;
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
            let pin = self.get_page(top.pid)?;
            let page = self.pool[pin.buf as usize].read();
            let n_items = page.get_n_items();
            let pos = top.pos + inc;
            if pos < n_items {
                path.stack.push(PagePos {
                    pid: top.pid,
                    pos: pos,
                });
                if path.stack.len() == height as usize {
                    let item = page.get_item(pos);
                    path.curr = Some(item);
                    break;
                }
                // We have to use this trick with `inc` variable on the way down because
                // Rust will detect overflow if we path -1 as pos
                debug_assert!(page.get_child(pos) != 0);
                path.stack.push(PagePos {
                    pid: page.get_child(pos),
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
            let pin = self.get_page(top.pid)?;
            let page = self.pool[pin.buf as usize].read();
            let pos = if top.pos == usize::MAX {
                page.get_n_items()
            } else {
                top.pos
            };
            if pos != 0 {
                path.stack.push(PagePos {
                    pid: top.pid,
                    pos: pos - 1,
                });
                if path.stack.len() == height as usize {
                    let item = page.get_item(pos - 1);
                    path.curr = Some(item);
                    break;
                }
                path.stack.push(PagePos {
                    pid: page.get_child(pos - 1),
                    pos: usize::MAX, // start from last element of the page
                });
            }
        }
        Ok(())
    }

    fn traverse(
        &self,
        meta: &Metadata,
        pid: PageId,
        prev_key: &mut Key,
        height: u32,
    ) -> Result<u64> {
        ensure!(self.test_allocator_bitmap(meta, pid)?);
        let pin = self.get_page(pid)?;
        let page = self.pool[pin.buf as usize].read();
        let n_items = page.get_n_items();
        let mut count = 0u64;
        if height == 1 {
            for i in 0..n_items {
                ensure!(page.compare_key(i, prev_key) == Ordering::Less);
                *prev_key = page.get_key(i);
            }
            count += n_items as u64;
        } else {
            for i in 0..n_items {
                count += self.traverse(meta, page.get_child(i), prev_key, height - 1)?;
                let ord = page.compare_key(i, prev_key);
                ensure!(ord == Ordering::Less || ord == Ordering::Equal);
            }
        }
        Ok(count)
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
        let mut db = self.db.write(); // prevent concurrent modification of the database (MURSIW)
        self.do_updates(&mut db, to_upsert, to_remove)?;
        self.commit(&mut db)
    }

    ///
    /// Traverse B-Tree, check B-Tree invariants and return total number of keys in B-Tree
    ///
    pub fn verify(&self) -> Result<u64> {
        let db = self.db.read();
        let meta = &db.meta;
        ensure!(self.test_allocator_bitmap(meta, 0)?);
        for pid in meta.alloc_pages {
            if pid != 0 {
                ensure!(self.test_allocator_bitmap(meta, pid)?);
            }
        }
        if db.meta.root != 0 {
            let mut prev_key = Vec::new();
            self.traverse(meta, db.meta.root, &mut prev_key, db.meta.height)
        } else {
            Ok(0)
        }
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
    /// Get database info
    ///
    pub fn get_database_info(&self) -> DatabaseInfo {
        let db = self.db.read();
        db.get_info()
    }

    ///
    /// Get cache info
    ///
    pub fn get_cache_info(&self) -> CacheInfo {
        let bm = self.buf_mgr.lock();
        CacheInfo {
            used: bm.cached as usize,
            pinned: bm.pinned as usize,
            dirtied: bm.dirty_buffers.len(),
        }
    }
}

impl Select for Storage {
    ///
    /// Returns bidirectional iterator
    ///
    fn range<R: RangeBounds<Key>>(&self, range: R) -> StorageIterator<'_> {
        StorageIterator {
            storage: &self,
            meta: None,
            from: range.start_bound().cloned(),
            till: range.end_bound().cloned(),
            left: TreePath::new(),
            right: TreePath::new(),
        }
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        let mut db = self.db.write();
        // Complete delayed commit
        if db.is_modified() {
            self.commit(&mut db).unwrap();
        }
    }
}

impl<'a> Select for Snapshot<'_> {
    fn range<R: RangeBounds<Key>>(&self, range: R) -> StorageIterator<'_> {
        StorageIterator {
            storage: self.storage,
            meta: Some(&self.meta.snapshot),
            from: range.start_bound().cloned(),
            till: range.end_bound().cloned(),
            left: TreePath::new(),
            right: TreePath::new(),
        }
    }
}

impl<'a> Transaction<'_> {
    ///
    /// Commit transaction
    ///
    pub fn commit(mut self) -> Result<()> {
        self.storage.commit(&mut self.db)?;
        self.status = TransactionStatus::Committed;
        Ok(())
    }

    ///
    /// Commit subtransaction. Allow other transactions to see changes made by this subtransaction.
    /// Changes will be committed only when transaction is committed.
    /// Rollback cause abort of all subtransactins.
    ///
    pub fn subcommit(mut self) -> Result<()> {
        self.db.meta.xid = self.db.meta.xid.wrapping_add(1);
        let mut meta = self.storage.meta_shadow.write();
        meta.snapshot.update(&self.db.meta);
        self.storage.remove_redundand_copies(&mut self.db)?;
        // mark transaction as committed to prevent implicit rollback by destructor
        self.status = TransactionStatus::Committed;
        Ok(())
    }

    ///
    /// Delay transaction commit. This method can be used to group several transactions to reduce commit overhead.
    /// Read-only transactions will see changes done buy this commit. But them are not be visible for selects in snapshot.
    ///
    pub fn delay(mut self) {
        // mark transaction as committed to prevent implicit rollback by destructor
        self.status = TransactionStatus::Committed;
    }

    ///
    /// Rollback transaction undoing all changes
    ///
    pub fn rollback(mut self) -> Result<()> {
        self.storage.rollback(&mut self.db);
        self.status = TransactionStatus::Aborted;
        Ok(())
    }

    ///
    /// Insert new key in the storage or update existed key as part of this transaction.
    ///
    pub fn put(&mut self, key: &Key, value: &Value) -> Result<()> {
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
    /// Get database info
    ///
    pub fn get_database_info(&self) -> DatabaseInfo {
        self.db.get_info()
    }

    ///
    /// Get cache info
    ///
    pub fn get_cache_info(&self) -> CacheInfo {
        self.storage.get_cache_info()
    }
}

impl<'a> Select for Transaction<'_> {
    ///
    /// Returns bidirectional iterator
    ///
    fn range<R: RangeBounds<Key>>(&self, range: R) -> StorageIterator<'_> {
        StorageIterator {
            storage: self.storage,
            meta: Some(&self.db.meta),
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
            self.storage.rollback(&mut self.db);
        }
    }
}

impl<'a> Select for ReadOnlyTransaction<'_> {
    ///
    /// Returns bidirectional iterator
    ///
    fn range<R: RangeBounds<Key>>(&self, range: R) -> StorageIterator<'_> {
        StorageIterator {
            storage: self.storage,
            meta: Some(&self.db.meta),
            from: range.start_bound().cloned(),
            till: range.end_bound().cloned(),
            left: TreePath::new(),
            right: TreePath::new(),
        }
    }
}
