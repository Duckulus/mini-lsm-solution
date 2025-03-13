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

use crate::key::{KeySlice, KeyVec};
use bytes::BufMut;
use std::cmp::min;

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let expected_size = key.len() + value.len() + 2 + 2 + 2;
        if !self.data.is_empty()
            && self.data.len() + expected_size + self.offsets.len() * 2 + 2 > self.block_size
        {
            return false;
        }

        self.offsets.push(self.data.len() as u16);

        let overlap = self.common_prefix_length(key);
        let rest = &key.for_testing_key_ref()[overlap..key.len()];
        assert_eq!(key.len() - overlap, rest.len());
        self.data.put_u16(overlap as u16);
        self.data.put_u16(rest.len() as u16);
        self.data.put_slice(rest);

        self.data.put_u16(value.len() as u16);
        self.data.put_slice(value);

        // first key needs to be set after computing common prefix length
        // or else the key gets lost to compression because it overlaps itself
        if self.first_key.is_empty() {
            self.first_key = KeyVec::from_vec(Vec::from(key.into_inner()));
        }
        true
    }

    fn common_prefix_length(&self, key: KeySlice) -> usize {
        let len = min(self.first_key.len(), key.len());
        for i in 0..len {
            if self.first_key.for_testing_key_ref()[i] != key.for_testing_key_ref()[i] {
                return i;
            }
        }
        len
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
