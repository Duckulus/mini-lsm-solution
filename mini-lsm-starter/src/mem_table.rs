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

use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::table::SsTableBuilder;
use crate::wal::Wal;
use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use nom::AsBytes;
use ouroboros::self_referencing;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound_old(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub(crate) fn map_bound_slice(bound: Bound<&[u8]>, ts: u64) -> Bound<KeySlice> {
    match bound {
        Bound::Included(x) => Bound::Included(KeySlice::from_slice(x, TS_DEFAULT)),
        Bound::Excluded(x) => Bound::Excluded(KeySlice::from_slice(x, TS_DEFAULT)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub(crate) fn map_bound_key_slice(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
        Bound::Excluded(x) => Bound::Excluded(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn entry_to_item(
    entry: Option<crossbeam_skiplist::map::Entry<KeyBytes, Bytes>>,
) -> (KeyBytes, Bytes) {
    if let Some(entry) = entry {
        (entry.key().clone(), entry.value().clone())
    } else {
        (KeyBytes::new(), Bytes::new())
    }
}

impl MemTable {
    /// Create a new mem-table.cl
    pub fn create(_id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id: _id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            map: Arc::new(SkipMap::new()),
            wal: Some(Wal::create(_path)?),
            id: _id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        let map = SkipMap::new();
        let wal = Wal::recover(_path, &map)?;
        Ok(Self {
            map: Arc::new(map),
            wal: Some(wal),
            id: _id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(KeySlice::from_slice(key, TS_DEFAULT), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(KeySlice::from_slice(key, TS_DEFAULT))
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(
            map_bound_slice(lower, TS_RANGE_BEGIN),
            map_bound_slice(upper, TS_RANGE_END),
        )
    }

    /// Get a value by key.
    pub fn get(&self, _key: KeySlice) -> Option<Bytes> {
        let bytes = KeyBytes::from_bytes_with_ts(
            Bytes::from_static(unsafe {
                std::mem::transmute::<&[u8], &'static [u8]>(_key.key_ref())
            }),
            _key.ts(),
        );
        self.map.get(&bytes).map(|entry| entry.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, _key: KeySlice, _value: &[u8]) -> Result<()> {
        self.put_batch(&[(_key, _value)])
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut size = 0;
        for entry in _data {
            size += entry.0.raw_len() + entry.1.len();
            let key = KeyBytes::from_bytes_with_ts(
                Bytes::copy_from_slice(entry.0.key_ref()),
                entry.0.ts(),
            );
            let value = Bytes::copy_from_slice(entry.1);
            self.map.insert(key, value);
        }
        self.approximate_size.fetch_add(size, Ordering::Relaxed);
        // let new_data: Vec<(&[u8], &[u8])> = _data.iter().map(|entry| (entry.0.key_ref(), entry.1)).collect();
        if let Some(wal) = &self.wal {
            wal.put_batch(_data)?;
        }
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, _lower: Bound<KeySlice>, _upper: Bound<KeySlice>) -> MemTableIterator {
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map: &Arc<SkipMap<KeyBytes, Bytes>>| {
                map.range((map_bound_key_slice(_lower), map_bound_key_slice(_upper)))
            },
            item: (KeyBytes::new(), Bytes::new()),
        }
        .build();
        let first = iter.with_iter_mut(|iter| entry_to_item(iter.next()));
        iter.with_mut(|iter| *iter.item = first);
        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, _builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            _builder.add(
                KeySlice::from_slice(entry.key().key_ref(), entry.key().ts()),
                entry.value().as_bytes(),
            );
        }

        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn max_ts(&self) -> u64 {
        self.map
            .iter()
            .map(|entry| entry.key().ts())
            .max()
            .unwrap_or(0)
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> KeySlice {
        self.borrow_item().0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let next = self.with_iter_mut(|iter| entry_to_item(iter.next()));
        self.with_mut(|iter| *iter.item = next);
        Ok(())
    }
}
