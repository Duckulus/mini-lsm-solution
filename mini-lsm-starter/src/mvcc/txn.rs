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

use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::lsm_storage::WriteBatchRecord;
use crate::mem_table::map_bound_old;
use crate::mvcc::CommittedTxnData;
use crate::{
    iterators::StorageIterator,
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::LsmStorageInner,
};
use anyhow::{bail, Result};
use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;
use std::sync::atomic::Ordering;
use std::{
    collections::HashSet,
    ops::Bound,
    sync::{atomic::AtomicBool, Arc},
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(Ordering::Relaxed) {
            panic!("Transaction used after commit")
        }
        if let Some(hashes) = self.key_hashes.as_ref() {
            let mut guard = hashes.lock();
            guard.1.insert(farmhash::hash32(key));
        }

        if let Some(entry) = self.local_storage.get(key) {
            return if entry.value().is_empty() {
                Ok(None)
            } else {
                Ok(Some(Bytes::copy_from_slice(entry.value())))
            };
        }

        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed.load(Ordering::Relaxed) {
            panic!("Transaction used after commit")
        }
        // scan read set
        if let Some(hashes) = self.key_hashes.as_ref() {
            let mut guard = hashes.lock();
            let local_iter = TxnLocalIterator::create(self.clone(), lower, upper);
            let lsm_iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;
            let mut txn_iter = TxnIterator::create(
                self.clone(),
                TwoMergeIterator::create(local_iter, lsm_iter)?,
            )?;
            while txn_iter.is_valid() {
                guard.1.insert(farmhash::hash32(txn_iter.key()));
                txn_iter.next()?;
            }
        }
        let local_iter = TxnLocalIterator::create(self.clone(), lower, upper);
        let lsm_iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;
        TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(local_iter, lsm_iter)?,
        )
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if self.committed.load(Ordering::Relaxed) {
            panic!("Transaction used after commit")
        }
        if let Some(hashes) = self.key_hashes.as_ref() {
            let mut guard = hashes.lock();
            guard.0.insert(farmhash::hash32(key));
        }

        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        self.put(key, &[])
    }

    pub fn commit(&self) -> Result<()> {
        if self.committed.swap(true, Ordering::Relaxed) {
            bail!("Tried Committing a transaction that is already commited");
        }

        let mut records = Vec::with_capacity(self.local_storage.len());
        for entry in self.local_storage.iter() {
            if entry.value().is_empty() {
                records.push(WriteBatchRecord::Del(entry.key().clone()));
            } else {
                records.push(WriteBatchRecord::Put(
                    entry.key().clone(),
                    entry.value().clone(),
                ))
            }
        }
        let guard = self.inner.mvcc().commit_lock.lock();
        if let Some(hashes) = self.key_hashes.as_ref() {
            // serializable verification
            let commited_txns = self.inner.mvcc().committed_txns.lock();
            let expected_commit_ts = self.inner.mvcc().latest_commit_ts() + 1;
            let guard = hashes.lock();
            // no verification necessary if write set is empty
            if !guard.0.is_empty() {
                for commited in commited_txns.range(self.read_ts + 1..expected_commit_ts) {
                    let mut intersect = commited.1.key_hashes.intersection(&guard.1);
                    if intersect.next().is_some() {
                        bail!("Transaction Verification failed: Found intersection between read set and write set of commited txn");
                    }
                }
            }
        }

        let commit_ts = self.inner.clone().write_batch_inner(&records)?;
        if let Some(hashes) = self.key_hashes.as_ref() {
            let mut commited_txns = self.inner.mvcc().committed_txns.lock();
            commited_txns.insert(
                commit_ts,
                CommittedTxnData {
                    key_hashes: hashes.lock().0.clone(),
                    read_ts: self.read_ts,
                    commit_ts,
                },
            );
            commited_txns.retain(|key, _| *key >= self.inner.mvcc().watermark());
        }

        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        let mut ts = self.inner.mvcc().ts.lock();
        ts.1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

fn entry_to_item(entry: Option<Entry<Bytes, Bytes>>) -> (Bytes, Bytes) {
    match entry {
        None => (Bytes::new(), Bytes::new()),
        Some(entry) => (
            Bytes::copy_from_slice(entry.key()),
            Bytes::copy_from_slice(entry.value()),
        ),
    }
}

impl TxnLocalIterator {
    pub fn create(txn: Arc<Transaction>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Self {
        let mut txn_local_iter = TxnLocalIteratorBuilder {
            map: txn.local_storage.clone(),
            item: (Bytes::new(), Bytes::new()),
            iter_builder: |map| map.range((map_bound_old(lower), map_bound_old(upper))),
        }
        .build();
        let first = txn_local_iter.with_iter_mut(|iter| entry_to_item(iter.next()));
        txn_local_iter.with_mut(|iter| *iter.item = first);
        txn_local_iter
    }
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        &self.borrow_item().1
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let item = self.with_iter_mut(|iter| entry_to_item(iter.next()));
        self.with_mut(|iter| *iter.item = item);
        Ok(())
    }
}

pub struct TxnIterator {
    _txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut iter = Self { _txn: txn, iter };
        while iter.is_valid() && iter.value().is_empty() {
            iter.next()?;
        }
        Ok(iter)
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        while self.is_valid() && self.value().is_empty() {
            self.iter.next()?;
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
