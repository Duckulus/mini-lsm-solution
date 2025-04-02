// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{bail, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{KeySlice, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound_old, map_bound_slice, MemTable};
use crate::mvcc::txn::{Transaction, TxnIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions, path: impl AsRef<Path>) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        let memtable = MemTable::create(0);
        Self {
            memtable: Arc::new(memtable),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(())?;
        let mut handle = self.flush_thread.lock();
        if let Err(e) = handle.take().unwrap().join() {
            bail!("Failed to terminate flush thread");
        }
        self.compaction_notifier.send(())?;
        let mut handle = self.compaction_thread.lock();
        if let Err(e) = handle.take().unwrap().join() {
            bail!("Failed to terminate flush thread");
        }
        if self.inner.options.enable_wal {
            self.sync()?;
            self.inner.sync_dir()?;
        } else {
            self.inner.flush_all_memtables()?;
        }
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

enum MemtableFetchResult {
    Deleted,
    Absent,
    Present(Bytes),
}

impl LsmStorageInner {
    pub fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        std::fs::create_dir_all(path)?;

        let mut state = LsmStorageState::create(&options, path);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let block_cache = Arc::new(BlockCache::new(1024));

        let manifest_path = path.join("MANIFEST");
        let next_sst_id: usize;
        let mut max_ts = 0;
        let manifest = if !manifest_path.exists() {
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    0,
                    Self::path_of_wal_static(path, 0),
                )?)
            }
            let manifest = Manifest::create(manifest_path)?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(0))?;
            next_sst_id = 1;
            manifest
        } else {
            let (manifest, records) = Manifest::recover(manifest_path)?;
            let (new_state, max_id, max_commit_ts) = Self::recover(
                state,
                &compaction_controller,
                block_cache.clone(),
                path.to_path_buf(),
                records,
                options.enable_wal,
            )?;
            max_ts = max_commit_ts;
            state = new_state;
            next_sst_id = max_id + 1;
            manifest
        };

        println!("Next sst_id: {}", next_sst_id);
        println!("Current memtable id: {}", state.memtable.id());

        let mvcc = LsmMvccInner::new(max_ts);

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: Some(mvcc),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    fn recover(
        mut state: LsmStorageState,
        compaction_controller: &CompactionController,
        block_cache: Arc<BlockCache>,
        path: PathBuf,
        records: Vec<ManifestRecord>,
        enable_wal: bool,
    ) -> Result<(LsmStorageState, usize, u64)> {
        let mut memtables = Vec::new();
        let mut max_table_id = 0;
        // apply records to state
        for record in records {
            match record {
                ManifestRecord::Flush(id) => {
                    memtables.remove(
                        memtables
                            .iter()
                            .position(|mid| *mid == id)
                            .expect("Memtable flushed but not inserted"),
                    );
                    if compaction_controller.flush_to_l0() {
                        state.l0_sstables.insert(0, id);
                    } else {
                        state.levels.insert(0, (id, vec![id]));
                    }
                }
                ManifestRecord::Compaction(task, ids) => {
                    let (new_state, to_delete) =
                        compaction_controller.apply_compaction_result(&state, &task, &ids, true);
                    state = new_state;
                }
                ManifestRecord::NewMemtable(id) => {
                    max_table_id = max_table_id.max(id);
                    memtables.insert(0, id);
                }
            }
        }
        // get all sst ids
        let mut sst_ids = Vec::new();
        for sst_id in &state.l0_sstables {
            if *sst_id > max_table_id {
                max_table_id = *sst_id;
            }
            sst_ids.push(*sst_id);
        }
        for level in &state.levels {
            for sst_id in &level.1 {
                if *sst_id > max_table_id {
                    max_table_id = *sst_id;
                }
                sst_ids.push(*sst_id);
            }
        }

        // open all ssts
        for sst_id in &sst_ids {
            let sst = SsTable::open(
                *sst_id,
                Some(block_cache.clone()),
                FileObject::open(&Self::path_of_sst_static(&path, *sst_id))?,
            )?;
            state.sstables.insert(*sst_id, Arc::new(sst));
        }
        println!("{} SSTs opened", sst_ids.len());

        // sort levels
        let sstables = state.sstables.clone();
        for level in &mut state.levels {
            level
                .1
                .sort_by(|a, b| sstables[a].first_key().cmp(sstables[b].first_key()));
        }

        let mut max_commit_ts = sstables
            .iter()
            .map(|entry| entry.1.max_ts())
            .max()
            .unwrap_or(0);

        if enable_wal {
            println!("{} WALs recovered", memtables.len());
            let current_table_id = memtables.remove(0);
            let table = MemTable::recover_from_wal(
                current_table_id,
                Self::path_of_wal_static(path.clone(), current_table_id),
            )?;
            max_commit_ts = max_commit_ts.max(table.max_ts());
            state.memtable = Arc::new(table);
            for table_id in memtables {
                let table = MemTable::recover_from_wal(
                    table_id,
                    Self::path_of_wal_static(path.clone(), table_id),
                )?;
                max_commit_ts = max_commit_ts.max(table.max_ts());
                state.imm_memtables.push(Arc::new(table));
            }
        } else {
            state.memtable = Arc::new(MemTable::create(max_table_id + 1));
            max_table_id += 1;
        }
        Ok((state, max_table_id, max_commit_ts))
    }

    pub fn flush_all_memtables(&self) -> Result<()> {
        {
            let state_lock = self.state_lock.lock();
            if !self.state.read().memtable.is_empty() {
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        loop {
            let guard = self.state.read();
            if guard.imm_memtables.is_empty() {
                break;
            }
            drop(guard);
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let guard = self.state.read();
        guard.memtable.sync_wal()?;
        Ok(())
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    fn may_contain_key(&self, sst: Arc<SsTable>, key: KeySlice) -> bool {
        if !Self::key_within(key, sst.clone()) {
            return false;
        }
        if let Some(bloom) = &sst.bloom {
            if !bloom.may_contain(farmhash::hash32(key.key_ref())) {
                return false;
            }
        }
        true
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(self: &Arc<Self>, _key: &[u8]) -> Result<Option<Bytes>> {
        let txn = self
            .mvcc
            .as_ref()
            .unwrap()
            .new_txn(self.clone(), self.options.serializable);
        self.get_with_ts(_key, txn.read_ts)
    }

    pub fn get_with_ts(&self, _key: &[u8], read_ts: u64) -> Result<Option<Bytes>> {
        let state = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let mut memtable_iters = Vec::new();
        memtable_iters.push(Box::from(state.memtable.scan(
            Bound::Included(KeySlice::from_slice(_key, TS_RANGE_BEGIN)),
            Bound::Included(KeySlice::from_slice(_key, TS_RANGE_END)),
        )));
        for memtable in &state.imm_memtables {
            memtable_iters.push(Box::from(memtable.scan(
                Bound::Included(KeySlice::from_slice(_key, TS_RANGE_BEGIN)),
                Bound::Included(KeySlice::from_slice(_key, TS_RANGE_END)),
            )));
        }
        let memtable_iter = MergeIterator::create(memtable_iters);

        let key_slice = KeySlice::from_slice(_key, TS_RANGE_BEGIN);
        let mut l0_iters = Vec::new();
        for sst_id in &state.l0_sstables {
            let sst = state.sstables[sst_id].clone();
            if self.may_contain_key(sst.clone(), key_slice) {
                l0_iters.push(Box::from(SsTableIterator::create_and_seek_to_key(
                    sst,
                    KeySlice::from_slice(_key, TS_RANGE_BEGIN),
                )?));
            }
        }
        let l0_iter = MergeIterator::create(l0_iters);

        let mut level_iters = Vec::new();
        for level in &state.levels {
            let mut ssts = Vec::new();
            for sst_id in &level.1 {
                let sst = state.sstables[sst_id].clone();
                if self.may_contain_key(sst.clone(), key_slice) {
                    ssts.push(sst);
                }
            }
            let level_iter = SstConcatIterator::create_and_seek_to_key(ssts, key_slice)?;
            level_iters.push(Box::from(level_iter));
        }
        let level_iter = MergeIterator::create(level_iters);

        let iter = LsmIterator::new(
            TwoMergeIterator::create(
                TwoMergeIterator::create(memtable_iter, l0_iter)?,
                level_iter,
            )?,
            Bound::Unbounded,
            read_ts,
        )?;

        if iter.is_valid() && iter.key() == _key {
            let value = iter.value();
            return if value.is_empty() {
                Ok(None)
            } else {
                Ok(Some(Bytes::copy_from_slice(value)))
            };
        }
        Ok(None)
    }

    fn key_within(key: KeySlice, sst: Arc<SsTable>) -> bool {
        let first = sst.first_key().key_ref();
        let last = sst.last_key().key_ref();
        key.key_ref() >= first && key.key_ref() <= last
    }

    fn get_from_memtable(
        &self,
        _key: KeySlice,
        _memtable: Arc<MemTable>,
    ) -> Result<MemtableFetchResult> {
        let value = _memtable.get(_key);
        if let Some(bytes) = value {
            if bytes.is_empty() {
                Ok(MemtableFetchResult::Deleted)
            } else {
                Ok(MemtableFetchResult::Present(bytes))
            }
        } else {
            Ok(MemtableFetchResult::Absent)
        }
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        let guard = self.mvcc.as_ref().unwrap().write_lock.lock();
        let ts = self.mvcc.as_ref().unwrap().latest_commit_ts() + 1;
        for record in _batch {
            match record {
                WriteBatchRecord::Put(k, v) => {
                    self.state
                        .read()
                        .memtable
                        .put(KeySlice::from_slice(k.as_ref(), ts), v.as_ref())?;
                }
                WriteBatchRecord::Del(k) => {
                    self.state
                        .read()
                        .memtable
                        .put(KeySlice::from_slice(k.as_ref(), ts), &[])?;
                }
            }
            if self.state.read().memtable.approximate_size() >= self.options.target_sst_size {
                let state_lock = &self.state_lock.lock();
                // check again with lock to ensure no 2 threads try to freeze at the same time
                if self.state.read().memtable.approximate_size() >= self.options.target_sst_size {
                    self.force_freeze_memtable(state_lock)?;
                }
            }
        }
        self.mvcc.as_ref().unwrap().update_commit_ts(ts);
        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(_key, _value)])?;
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(_key)])?;
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let id = self.next_sst_id();
        let new_memtable = if self.options.enable_wal {
            MemTable::create_with_wal(id, self.path_of_wal(id))
        } else {
            Ok(MemTable::create(id))
        }?;
        {
            if let Some(manifest) = &self.manifest {
                manifest.add_record(_state_lock_observer, ManifestRecord::NewMemtable(id))?;
            }
        }

        let mut guard = self.state.write();
        let mut snapshot = guard.as_ref().clone();
        snapshot.imm_memtables.insert(0, snapshot.memtable.clone());
        snapshot.memtable = Arc::new(new_memtable);

        *guard = Arc::new(snapshot);
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();
        let table = {
            let state = self.state.read();
            let table = state
                .imm_memtables
                .last()
                .expect("Tried flushing imm memtable but there are none");
            table.clone()
        };
        let id = table.id();

        let mut builder = SsTableBuilder::new(self.options.block_size);
        table.flush(&mut builder)?;
        let path = self.path_of_sst(id);
        let sst = builder.build(id, Some(self.block_cache.clone()), path.clone())?;
        if self.options.enable_wal {
            fs::remove_file(self.path_of_wal(table.id()))?;
        }

        self.sync_dir()?;
        File::open(path)?.sync_all()?;

        if let Some(manifest) = &self.manifest {
            manifest.add_record(&state_lock, ManifestRecord::Flush(id))?;
        }

        {
            let mut state = self.state.write();
            let mut snapshot = state.as_ref().clone();
            snapshot.imm_memtables.pop();
            if self.compaction_controller.flush_to_l0() {
                snapshot.l0_sstables.insert(0, id);
            } else {
                let tier = (id, vec![id]);
                snapshot.levels.insert(0, tier);
            }
            println!("flushed {}.sst with size={}", id, sst.table_size());
            snapshot.sstables.insert(id, Arc::new(sst));
            *state = Arc::new(snapshot);
        }
        Ok(())
    }

    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        Ok(self
            .mvcc
            .as_ref()
            .unwrap()
            .new_txn(self.clone(), self.options.serializable))
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        self: &Arc<Self>,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<TxnIterator> {
        let txn = self
            .mvcc
            .as_ref()
            .unwrap()
            .new_txn(self.clone(), self.options.serializable);
        TxnIterator::create(txn.clone(), self.scan_with_ts(_lower, _upper, txn.read_ts)?)
    }

    pub fn scan_with_ts(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
        read_ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        let state = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let mut memtables = Vec::new();
        memtables.push(Box::from(state.memtable.scan(
            map_bound_slice(_lower, TS_RANGE_BEGIN),
            map_bound_slice(_upper, TS_RANGE_END),
        )));
        for memtable in &state.imm_memtables {
            memtables.push(Box::from(memtable.scan(
                map_bound_slice(_lower, TS_RANGE_BEGIN),
                map_bound_slice(_upper, TS_RANGE_END),
            )));
        }
        let merge_iter = MergeIterator::create(memtables);

        let mut l0_ssts = Vec::new();
        for sst_id in &state.l0_sstables {
            let table = state.sstables.get(sst_id).unwrap();
            if !Self::range_overlap(_lower, _upper, table.clone()) {
                continue;
            }
            let mut iter = SsTableIterator::create_and_seek_to_first(table.clone())?;
            match _lower {
                Bound::Included(slice) => {
                    iter.seek_to_key(KeySlice::from_slice(slice, TS_RANGE_BEGIN))?
                }
                Bound::Excluded(slice) => {
                    let key = KeySlice::from_slice(slice, TS_RANGE_BEGIN);
                    iter.seek_to_key(key)?;
                    while iter.is_valid() && iter.key().key_ref() == key.key_ref() {
                        iter.next()?;
                    }
                }
                Bound::Unbounded => {}
            };
            l0_ssts.push(Box::from(iter));
        }
        let l0_sst_iter = MergeIterator::create(l0_ssts);

        let mut level_iters = Vec::new();
        for level in &state.levels {
            let mut ssts = Vec::new();
            for sst_id in &level.1 {
                let table = state.sstables[sst_id].clone();
                if Self::range_overlap(_lower, _upper, table.clone()) {
                    ssts.push(state.sstables.get(sst_id).unwrap().clone());
                }
            }
            let level_iter = match _lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    ssts,
                    KeySlice::from_slice(key, TS_RANGE_BEGIN),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        ssts,
                        KeySlice::from_slice(key, TS_RANGE_BEGIN),
                    )?;
                    while iter.is_valid() && iter.key().key_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(ssts)?,
            };
            level_iters.push(Box::from(level_iter));
        }
        let level_merge_iter = MergeIterator::create(level_iters);

        let combined = TwoMergeIterator::create(merge_iter, l0_sst_iter)?;
        let combined = TwoMergeIterator::create(combined, level_merge_iter)?;
        let lsm_iter = LsmIterator::new(combined, map_bound_old(_upper), read_ts)?;
        Ok(FusedIterator::new(lsm_iter))
    }

    pub fn range_overlap(lower: Bound<&[u8]>, upper: Bound<&[u8]>, sst: Arc<SsTable>) -> bool {
        let first = sst.first_key().as_key_slice().key_ref();
        let last = sst.last_key().as_key_slice().key_ref();

        let lower_before_last = match lower {
            Bound::Included(bound) => bound <= last,
            Bound::Excluded(bound) => bound < last,
            Bound::Unbounded => true,
        };

        let upper_after_first = match upper {
            Bound::Included(bound) => bound >= first,
            Bound::Excluded(bound) => bound > first,
            Bound::Unbounded => true,
        };

        lower_before_last && upper_after_first
    }
}
