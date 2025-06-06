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
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    reached_end: bool,
    prev_key: Bytes,
    read_ts: u64,
}

impl LsmIterator {
    pub(crate) fn new(
        iter: LsmIteratorInner,
        end_bound: Bound<Bytes>,
        read_ts: u64,
    ) -> Result<Self> {
        let mut iter = Self {
            inner: iter,
            end_bound,
            reached_end: false,
            prev_key: Bytes::new(),
            read_ts,
        };
        Self::skip_invisible_and_deleted_keys(&mut iter)?;
        if iter.inner.is_valid() {
            iter.prev_key = Bytes::copy_from_slice(iter.inner.key().key_ref());
        }
        Self::check_end_bound(&mut iter);
        Ok(iter)
    }

    fn skip_invisible_and_deleted_keys(&mut self) -> Result<()> {
        while self.inner.is_valid() {
            while self.inner.is_valid() && self.inner.key().ts() > self.read_ts {
                self.inner.next()?;
            }
            if self.inner.is_valid() && self.inner.value().is_empty() {
                let key = Bytes::copy_from_slice(self.inner.key().key_ref());
                while self.inner.is_valid() && self.inner.key().key_ref() == key.as_ref() {
                    self.inner.next()?;
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    fn check_end_bound(&mut self) {
        if self.is_valid() {
            // need to check end bound manually because SsTableIterator does not support ranges
            self.reached_end = match &self.end_bound {
                Bound::Included(bytes) => self.key() > bytes.as_ref(),
                Bound::Excluded(bytes) => self.key() >= bytes.as_ref(),
                Bound::Unbounded => false,
            };
        }
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        !self.reached_end && self.inner.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        while self.inner.is_valid() && self.inner.key().key_ref() == self.prev_key.as_ref() {
            self.inner.next()?;
        }

        self.skip_invisible_and_deleted_keys()?;

        if self.inner.is_valid() {
            self.prev_key = Bytes::copy_from_slice(self.inner.key().key_ref());
            self.check_end_bound();
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
