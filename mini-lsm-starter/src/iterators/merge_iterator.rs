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

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(mut iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return MergeIterator {
                iters: BinaryHeap::new(),
                current: None,
            };
        }
        let mut wrappers: Vec<HeapWrapper<I>> = Vec::new();
        for i in 0..iters.len() {
            let iter = iters.remove(0);
            if iter.is_valid() {
                wrappers.push(HeapWrapper(i, iter));
            }
        }
        let mut heap = BinaryHeap::from(wrappers);
        let first = heap.pop();

        MergeIterator {
            iters: heap,
            current: first,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .is_some_and(|wrapper| wrapper.1.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        let current_key = self.current.as_ref().unwrap().1.key();
        while let Some(mut inner) = self.iters.peek_mut() {
            if !inner.1.is_valid() {
                PeekMut::pop(inner);
            } else if inner.1.key() > current_key {
                break;
            } else {
                if let Err(e) = inner.1.next() {
                    PeekMut::pop(inner);
                    return Err(e);
                }
                if !inner.1.is_valid() {
                    PeekMut::pop(inner);
                }
            }
        }

        let mut curr = self.current.take().unwrap();
        curr.1.next()?;
        if curr.1.is_valid() {
            self.iters.push(curr);
        }
        self.current = self.iters.pop();
        Ok(())
    }
}
