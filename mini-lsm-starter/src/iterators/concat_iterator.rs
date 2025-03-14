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

use std::sync::Arc;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};
use anyhow::Result;

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.is_empty() {
            Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            })
        } else {
            let current = SsTableIterator::create_and_seek_to_first(sstables[0].clone())?;
            Ok(Self {
                current: Some(current),
                next_sst_idx: 1,
                sstables,
            })
        }
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        if sstables.is_empty() {
            Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            })
        } else {
            let id = sstables.partition_point(|table| key > table.last_key().as_key_slice());
            let (current, next_sst_idx) = if id == sstables.iter().len() {
                (None, id)
            } else {
                let table = sstables[id].clone();
                let iter = SsTableIterator::create_and_seek_to_key(table, key)?;
                (Some(iter), id + 1)
            };
            Ok(Self {
                current,
                next_sst_idx,
                sstables,
            })
        }
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        if !self.is_valid() {
            panic!("tried to use invalid iterator");
        }
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("tried to use invalid iterator");
        }
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        if let Some(iter) = &self.current {
            iter.is_valid()
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            panic!("tried to use invalid iterator");
        }
        let iter = self.current.as_mut().unwrap();
        iter.next()?;
        if !iter.is_valid() {
            if self.next_sst_idx < self.sstables.len() {
                let table = self.sstables[self.next_sst_idx].clone();
                let iter = SsTableIterator::create_and_seek_to_first(table)?;
                self.current = Some(iter);
                self.next_sst_idx += 1;
            } else {
                self.current = None;
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
