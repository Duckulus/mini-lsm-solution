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

mod leveled;
mod simple_leveled;
mod tiered;

use std::fs::File;
use std::sync::Arc;
use std::time::Duration;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};
use anyhow::Result;
use bytes::Bytes;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

fn create_merge_iter_from_ids(
    state: &LsmStorageState,
    ids: &Vec<usize>,
) -> Result<MergeIterator<SsTableIterator>> {
    let mut tables = Vec::new();
    for sst_id in ids {
        tables.push(Box::from(SsTableIterator::create_and_seek_to_first(
            state.sstables.get(sst_id).unwrap().clone(),
        )?));
    }
    Ok(MergeIterator::create(tables))
}

fn create_concat_iter_from_ids(
    state: &LsmStorageState,
    ids: &Vec<usize>,
) -> Result<SstConcatIterator> {
    let mut tables = Vec::new();
    for sst_id in ids {
        tables.push(state.sstables.get(sst_id).unwrap().clone());
    }
    SstConcatIterator::create_and_seek_to_first(tables)
}

impl LsmStorageInner {
    fn gen_ssts_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut new_tables = Vec::new();
        let mut current_builder = SsTableBuilder::new(self.options.block_size);
        let mut last_key: Bytes = Bytes::new();
        let mut latest: Vec<u8> = Vec::new();
        while iter.is_valid() {
            if iter.key().ts() <= self.mvcc.as_ref().unwrap().watermark() {
                if latest.as_slice() == iter.key().key_ref() {
                    iter.next()?;
                    continue;
                }
                latest.clear();
                latest.extend(iter.key().key_ref());
            }

            if iter.key().ts() <= self.mvcc.as_ref().unwrap().watermark()
                && iter.value().is_empty()
                && compact_to_bottom_level
            {
                iter.next()?;
                continue;
            }

            if current_builder.estimated_size() > self.options.target_sst_size
                && iter.key().key_ref() != last_key.as_ref()
            {
                let id = self.next_sst_id();
                let builder = std::mem::replace(
                    &mut current_builder,
                    SsTableBuilder::new(self.options.block_size),
                );
                let table =
                    builder.build(id, Some(self.block_cache.clone()), self.path_of_sst(id))?;
                new_tables.push(Arc::new(table));
            }
            let key = iter.key();
            last_key = Bytes::copy_from_slice(key.key_ref());
            current_builder.add(key, iter.value());
            iter.next()?;
        }
        let id = self.next_sst_id();
        let table =
            current_builder.build(id, Some(self.block_cache.clone()), self.path_of_sst(id))?;
        new_tables.push(Arc::new(table));
        Ok(new_tables)
    }

    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let state = self.state.read().as_ref().clone();
        match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let l0_merge_iter = create_merge_iter_from_ids(&state, l0_sstables)?;
                let l1_iter = create_concat_iter_from_ids(&state, l1_sstables)?;
                self.gen_ssts_from_iter(
                    TwoMergeIterator::create(l0_merge_iter, l1_iter)?,
                    _task.compact_to_bottom_level(),
                )
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level_sst_ids,
                ..
            })
            | CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level_sst_ids,
                ..
            }) => {
                if upper_level.is_some() {
                    let upper_iter = create_concat_iter_from_ids(&state, upper_level_sst_ids)?;
                    let lower_iter = create_concat_iter_from_ids(&state, lower_level_sst_ids)?;
                    self.gen_ssts_from_iter(
                        TwoMergeIterator::create(upper_iter, lower_iter)?,
                        _task.compact_to_bottom_level(),
                    )
                } else {
                    let upper_iter = create_merge_iter_from_ids(&state, upper_level_sst_ids)?;
                    let lower_iter = create_concat_iter_from_ids(&state, lower_level_sst_ids)?;
                    self.gen_ssts_from_iter(
                        TwoMergeIterator::create(upper_iter, lower_iter)?,
                        _task.compact_to_bottom_level(),
                    )
                }
            }
            CompactionTask::Tiered(task) => {
                let mut iters = Vec::new();
                for tier in &task.tiers {
                    let iter = create_concat_iter_from_ids(&state, &tier.1)?;
                    iters.push(Box::from(iter));
                }

                self.gen_ssts_from_iter(
                    MergeIterator::create(iters),
                    _task.compact_to_bottom_level(),
                )
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let l0 = snapshot.l0_sstables.clone();
        let l1 = snapshot
            .levels
            .first()
            .expect("first level exists")
            .clone()
            .1;
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0.clone(),
            l1_sstables: l1.clone(),
        };

        let new_ssts = self.compact(&task)?;
        let mut tables_to_delete = Vec::new();
        {
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.write();
            let mut snapshot = state.as_ref().clone();
            for _ in 0..l0.len() {
                snapshot.l0_sstables.pop().unwrap();
            }
            for id in l0 {
                tables_to_delete.push(snapshot.sstables.remove(&id).unwrap());
            }

            snapshot.levels[0] = (
                1,
                new_ssts
                    .iter()
                    .map(|sst| sst.sst_id())
                    .collect::<Vec<usize>>(),
            );
            for id in l1 {
                tables_to_delete.push(snapshot.sstables.remove(&id).unwrap());
            }
            for sst in &new_ssts {
                snapshot.sstables.insert(sst.sst_id(), sst.clone());
            }

            *state = Arc::new(snapshot);
            let sst_ids: Vec<_> = new_ssts.iter().map(|sst| sst.sst_id()).collect();
            for id in &sst_ids {
                File::open(self.path_of_sst(*id))?.sync_all()?;
            }
            self.sync_dir()?;
            if let Some(manifest) = &self.manifest {
                manifest.add_record(&_state_lock, ManifestRecord::Compaction(task, sst_ids))?;
            }
        }
        for table in tables_to_delete {
            std::fs::remove_file(self.path_of_sst(table.sst_id()))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = self.state.read().as_ref().clone();
        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);
        if task.is_none() {
            return Ok(());
        }

        let task = task.unwrap();
        let new_ssts = self.compact(&task)?;

        let mut tables_to_delete = Vec::new();
        {
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.write();
            let mut snapshot = state.as_ref().clone();

            for sst in &new_ssts {
                snapshot.sstables.insert(sst.sst_id(), sst.clone());
            }

            let output: Vec<usize> = new_ssts
                .iter()
                .map(|sst| sst.sst_id())
                .collect::<Vec<usize>>();
            let (mut new_state, del) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output, false);

            for id in del {
                tables_to_delete.push(new_state.sstables.remove(&id).unwrap())
            }

            *state = Arc::new(new_state);

            let sst_ids: Vec<_> = new_ssts.iter().map(|sst| sst.sst_id()).collect();
            for id in &sst_ids {
                File::open(self.path_of_sst(*id))?.sync_all()?;
            }
            self.sync_dir()?;
            if let Some(manifest) = &self.manifest {
                manifest.add_record(&_state_lock, ManifestRecord::Compaction(task, sst_ids))?;
            }
        }

        for table in tables_to_delete {
            std::fs::remove_file(self.path_of_sst(table.sst_id()))?;
        }

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let count = self.state.read().imm_memtables.len() + 1;
        if count > self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
