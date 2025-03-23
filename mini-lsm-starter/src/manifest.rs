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

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, BufMut};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(_path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn recover(_path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new().read(true).write(true).open(_path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf = &buf[..];
        let mut records = Vec::new();
        while buf.has_remaining() {
            let len = buf.get_u16() as usize;
            let data = &buf[..len];
            buf.advance(len);
            let checksum = buf.get_u32();
            assert_eq!(crc32fast::hash(data), checksum);
            let record = serde_json::de::from_slice::<ManifestRecord>(data)?;
            records.push(record);
        }
        let file = Arc::new(Mutex::new(file));
        Ok((Self { file }, records))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, _record: ManifestRecord) -> Result<()> {
        let mut buf: Vec<u8> = Vec::new();
        let mut json = serde_json::to_vec(&_record)?;
        let checksum = crc32fast::hash(&json);
        let len = json.len();
        buf.put_u16(len as u16);
        buf.append(&mut json);
        buf.put_u32(checksum);

        let mut guard = self.file.lock();
        guard.write_all(&buf)?;
        guard.sync_all()?;
        Ok(())
    }
}
