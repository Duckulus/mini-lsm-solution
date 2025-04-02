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

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        self.readers.entry(ts).and_modify(|x| *x += 1).or_insert(1);
    }

    pub fn remove_reader(&mut self, ts: u64) {
        let entry = self.readers.entry(ts);
        match entry {
            Entry::Vacant(_) => panic!("Tried removing reader that wasn't added"),
            Entry::Occupied(mut entry) => match entry.get() {
                1 => entry.remove(),
                2.. => entry.insert(entry.get() - 1),
                _ => unreachable!(),
            },
        };
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.iter().min().map(|entry| *entry.0)
    }
}
