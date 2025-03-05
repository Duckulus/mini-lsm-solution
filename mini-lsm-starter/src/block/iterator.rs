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

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block.clone());
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::create_and_seek_to_first(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        self.key.len() > 0
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        self.seek_to_idx(0);
        self.first_key = self.key.clone();
    }

    fn seek_to_idx(&mut self, index: usize) {
        assert!(index < self.block.offsets.len(), "Index out of bounds");
        let offset = self.block.offsets[index] as usize;
        let block = self.block.clone();
        let key_len = Block::u8_to_u16(&block.data[offset..offset + 2]) as usize;
        let key: &[u8] = &block.data[offset + 2..offset + 2 + key_len];
        let value_len =
            Block::u8_to_u16(&block.data[offset + 2 + key_len..offset + 2 + key_len + 2]) as usize;
        self.key = KeyVec::from_vec(key.to_vec());
        self.value_range = (
            (offset + 2 + key_len + 2),
            (offset + 2 + key_len + 2 + value_len),
        );
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx >= self.block.offsets.len() {
            panic!("Tried to call next while iterator is at end");
        }
        if self.idx == self.block.offsets.len() - 1 {
            self.key = KeyVec::new();
        } else {
            self.seek_to_idx(self.idx + 1);
        }

        self.idx += 1;
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.seek_to_first();
        while self.is_valid() && self.key.as_key_slice() < key {
            self.next();
        }
    }
}
