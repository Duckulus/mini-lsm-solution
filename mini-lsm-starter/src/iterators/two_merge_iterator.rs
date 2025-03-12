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

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    next_a: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, mut b: B) -> Result<Self> {
        if a.is_valid() && b.is_valid() && a.key() == b.key() {
            // make sure the invariant a!=b is not violated
            b.next()?;
        }
        let mut next_a = true;
        if a.is_valid() && b.is_valid() {
            next_a = a.key() < b.key();
        } else if a.is_valid() {
            next_a = true;
        } else if b.is_valid() {
            next_a = false;
        }
        Ok(Self { a, b, next_a })
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.next_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.next_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.a.is_valid() && self.b.is_valid() {
            assert!(self.a.key() != self.b.key());
            if self.a.key() < self.b.key() {
                self.a.next()?;
            } else if self.b.key() < self.a.key() {
                self.b.next()?;
            }
            if self.a.is_valid() && self.b.is_valid() {
                if self.a.key() == self.b.key() {
                    self.b.next()?;
                    self.next_a = true;
                } else {
                    self.next_a = self.a.key() < self.b.key();
                }
            } else if self.a.is_valid() {
                self.next_a = true;
            } else if self.b.is_valid() {
                self.next_a = false;
            }
        } else if self.a.is_valid() {
            self.a.next()?;
            self.next_a = true;
        } else if self.b.is_valid() {
            self.b.next()?;
            self.next_a = false;
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
