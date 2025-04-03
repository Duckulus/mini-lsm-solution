// REMOVE THIS LINE after fully implementing this functionality
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
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::{KeyBytes, KeySlice};
use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use crc32fast::Hasher;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(_path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(_path)?;
        let mut buf = Vec::new();
        let data = file.read_to_end(&mut buf)?;
        let mut data = &buf[..];
        while data.has_remaining() {
            let batch_size = data.get_u32() as usize;
            let mut body = &data[..batch_size];
            let mut hasher = Hasher::new();
            while body.has_remaining() {
                let key_len = body.get_u16() as usize;
                hasher.update(&(key_len as u16).to_be_bytes());
                let key = Bytes::copy_from_slice(&body[..key_len]);
                hasher.update(&body[..key_len]);
                body.advance(key_len);
                let ts = body.get_u64();
                hasher.update(&ts.to_be_bytes());
                let value_len = body.get_u16() as usize;
                hasher.update(&(value_len as u16).to_be_bytes());
                let value = Bytes::copy_from_slice(&body[..value_len]);
                hasher.update(&body[..value_len]);
                body.advance(value_len);
                _skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), value);
            }
            data.advance(batch_size);
            let checksum = data.get_u32();
            assert_eq!(hasher.finalize(), checksum);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, _key: KeySlice, _value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf: Vec<u8> = Vec::with_capacity(_key.raw_len() + _value.len() + 2 + 2);
        buf.put_u16(_key.key_len() as u16);
        buf.put_slice(_key.key_ref());
        buf.put_u64(_key.ts());
        buf.put_u16(_value.len() as u16);
        buf.put_slice(_value);
        let checksum = crc32fast::hash(&buf);
        buf.put_u32(checksum);
        file.write_all(&buf)?;
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut file = self.file.lock();
        let mut body: Vec<u8> = Vec::new();
        for entry in _data {
            body.put_u16(entry.0.key_len() as u16);
            body.put(entry.0.key_ref());
            body.put_u64(entry.0.ts());
            body.put_u16(entry.1.len() as u16);
            body.put(entry.1);
        }
        let checksum = crc32fast::hash(&body);

        file.write_all(&(body.len() as u32).to_be_bytes())?;
        file.write_all(&body)?;
        file.write_all(&checksum.to_be_bytes())?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
