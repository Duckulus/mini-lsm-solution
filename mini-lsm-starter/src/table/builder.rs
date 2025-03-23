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

use std::path::Path;
use std::sync::Arc;

use super::{BlockMeta, FileObject, SsTable};
use crate::key::KeyVec;
use crate::table::bloom::Bloom;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};
use anyhow::Result;
use bytes::BufMut;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    keys_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            keys_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        self.keys_hashes
            .push(farmhash::fingerprint32(key.into_inner()));
        // set first_key for the very first insert
        if self.data.is_empty() && self.builder.is_empty() {
            self.first_key = key.to_key_vec().into_inner();
        }
        if self.builder.add(key, value) {
            self.last_key = key.to_key_vec().into_inner()
        } else {
            let mut new_builder = BlockBuilder::new(self.block_size);

            // the first insert should always be successful
            assert!(
                new_builder.add(key, value),
                "First insert into block builder failed"
            );

            let old_builder = std::mem::replace(&mut self.builder, new_builder);
            self.write_block(old_builder);

            self.first_key = key.to_key_vec().into_inner();
            self.last_key = key.to_key_vec().into_inner();
        }
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let last_block = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        self.write_block(last_block);

        let mut data = self.data.clone();
        let meta_offset = data.len();
        BlockMeta::encode_block_meta(&self.meta[..], &mut data);
        data.put_u32(meta_offset as u32);

        let bloom = Bloom::build_from_key_hashes(
            &self.keys_hashes,
            Bloom::bloom_bits_per_key(self.keys_hashes.len(), 0.01),
        );
        let bloom_offset = data.len();
        bloom.encode(&mut data);
        data.put_u32(bloom_offset as u32);

        let file_object = FileObject::create(path.as_ref(), data)?;

        let table = SsTable {
            id,
            block_cache,
            first_key: self
                .meta
                .first()
                .map(|meta| meta.first_key.clone())
                .unwrap_or_default(),
            last_key: self
                .meta
                .last()
                .map(|meta| meta.last_key.clone())
                .unwrap_or_default(),
            bloom: Some(bloom),
            file: file_object,
            block_meta: self.meta,
            block_meta_offset: meta_offset,
            max_ts: 0,
        };
        Ok(table)
    }

    fn write_block(&mut self, block: BlockBuilder) {
        let meta = BlockMeta {
            offset: self.data.len(),
            first_key: KeyVec::from_vec(self.first_key.clone()).into_key_bytes(),
            last_key: KeyVec::from_vec(self.last_key.clone()).into_key_bytes(),
        };
        let mut block_data = block.build().encode().to_vec();
        let checksum = crc32fast::hash(&block_data);
        self.data.append(&mut block_data);
        self.data.put_u32(checksum);
        self.meta.push(meta);
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
