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

use std::sync::Arc;
use std::time::Duration;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};
use anyhow::Result;
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
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut new_tables = Vec::new();
        let mut current_builder = SsTableBuilder::new(self.options.block_size);
        while iter.is_valid() {
            if iter.value().is_empty() {
                iter.next()?;
                continue;
            }
            if current_builder.estimated_size() > self.options.target_sst_size {
                let id = self.next_sst_id();
                let builder = std::mem::replace(
                    &mut current_builder,
                    SsTableBuilder::new(self.options.block_size),
                );
                let table =
                    builder.build(id, Some(self.block_cache.clone()), self.path_of_sst(id))?;
                new_tables.push(Arc::new(table));
            } else {
                current_builder.add(iter.key(), iter.value());
                iter.next()?;
            }
        }
        let id = self.next_sst_id();
        let table =
            current_builder.build(id, Some(self.block_cache.clone()), self.path_of_sst(id))?;
        new_tables.push(Arc::new(table));
        Ok(new_tables)
    }

    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let state = self.state.read().as_ref().clone();

        if let CompactionTask::ForceFullCompaction {
            l0_sstables,
            l1_sstables,
        } = _task
        {
            let l0_merge_iter = create_merge_iter_from_ids(&state, l0_sstables)?;
            let l1_iter = create_concat_iter_from_ids(&state, l1_sstables)?;
            self.gen_ssts_from_iter(TwoMergeIterator::create(l0_merge_iter, l1_iter)?)
        } else if let CompactionTask::Simple(task) = _task {
            if task.upper_level.is_some() {
                let upper_iter = create_concat_iter_from_ids(&state, &task.upper_level_sst_ids)?;
                let lower_iter = create_concat_iter_from_ids(&state, &task.lower_level_sst_ids)?;
                self.gen_ssts_from_iter(TwoMergeIterator::create(upper_iter, lower_iter)?)
            } else {
                let upper_iter = create_merge_iter_from_ids(&state, &task.upper_level_sst_ids)?;
                let lower_iter = create_concat_iter_from_ids(&state, &task.lower_level_sst_ids)?;
                self.gen_ssts_from_iter(TwoMergeIterator::create(upper_iter, lower_iter)?)
            }
        } else {
            unimplemented!();
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

        let new_tables = self.compact(&task)?;
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
                new_tables
                    .iter()
                    .map(|sst| sst.sst_id())
                    .collect::<Vec<usize>>(),
            );
            for id in l1 {
                tables_to_delete.push(snapshot.sstables.remove(&id).unwrap());
            }
            for sst in new_tables {
                snapshot.sstables.insert(sst.sst_id(), sst.clone());
            }

            *state = Arc::new(snapshot);
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
