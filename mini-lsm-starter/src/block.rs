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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut data = self.data.clone();
        for offset in &self.offsets {
            data.put_u16(*offset);
        }
        data.put_u16(self.offsets.len() as u16);
        Bytes::from(data)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let len = data.len();
        let num_elements = Self::u8_to_u16(&data[len - 2..len]);
        let mut offsets = Vec::with_capacity(num_elements as usize);
        let offsets_start = len - 2 - (num_elements as usize) * 2;
        for slot in (offsets_start..len - 2).step_by(2) {
            offsets.push(Self::u8_to_u16(&data[slot..slot + 2]));
        }
        let data_vec = data[0..offsets_start].to_vec();

        Self {
            data: data_vec,
            offsets,
        }
    }

    fn u8_to_u16(data: &[u8]) -> u16 {
        assert_eq!(data.len(), 2, "Length of u8 slice was not 2");
        ((data[0] as u16) << 8) | (data[1] as u16)
    }
}
