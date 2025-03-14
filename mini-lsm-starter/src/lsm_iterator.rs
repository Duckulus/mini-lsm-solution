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

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::{KeyBytes, KeySlice};
use crate::table::SsTableIterator;
use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};
use anyhow::{bail, Result};
use bytes::Bytes;
use std::ops::Bound;

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    SstConcatIterator,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    reached_end: bool,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        let mut iter = Self {
            inner: iter,
            end_bound,
            reached_end: false,
        };
        while iter.inner.is_valid() && iter.inner.value().is_empty() {
            iter.next()?;
        }
        Ok(iter)
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        !self.reached_end && self.inner.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().for_testing_key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        while self.inner.is_valid() && self.inner.value().is_empty() {
            self.inner.next()?;
        }

        if self.inner.is_valid() {
            // need to check end bound manually because SsTableIterator does not support ranges
            let end = match &self.end_bound {
                Bound::Included(bytes) => {
                    KeySlice::from_slice(self.key())
                        > KeyBytes::from_bytes(bytes.clone()).as_key_slice()
                }
                Bound::Excluded(bytes) => {
                    KeySlice::from_slice(self.key())
                        >= KeyBytes::from_bytes(bytes.clone()).as_key_slice()
                }
                Bound::Unbounded => false,
            };
            if end {
                self.reached_end = true;
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if !self.is_valid() {
            panic!("Illegal Iterator Access");
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("Illegal Iterator Access");
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("Iterator used after Error");
        } else if self.is_valid() {
            if let Err(e) = self.iter.next() {
                self.has_errored = true;
                return Err(e);
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
