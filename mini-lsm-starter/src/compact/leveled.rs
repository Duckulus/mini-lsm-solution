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

use crate::key::KeyBytes;
use crate::lsm_storage::LsmStorageState;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::cmp::max;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

fn level_size_mb(snapshot: &LsmStorageState, level_idx: usize) -> usize {
    let mut size = 0;
    for sst_id in &snapshot.levels[level_idx].1 {
        let sst = snapshot.sstables[sst_id].clone();
        size += sst.table_size();
    }
    (size / (1024 * 1024)) as usize
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn compute_target_sizes(&self, snapshot: &LsmStorageState) -> Vec<usize> {
        assert_eq!(snapshot.levels.len(), self.options.max_levels);
        let mut target_size = vec![0; self.options.max_levels];
        let mut idx = self.options.max_levels - 1;
        let size = level_size_mb(snapshot, idx);
        target_size[idx] = max(self.options.base_level_size_mb, size);
        if size > self.options.base_level_size_mb {
            while idx > 0 && target_size[idx] > self.options.base_level_size_mb {
                target_size[idx - 1] = target_size[idx] / self.options.level_size_multiplier;
                idx -= 1;
            }
        }
        target_size
    }

    fn find_oldest(&self, snapshot: &LsmStorageState, level: usize) -> usize {
        *snapshot.levels[level - 1].1.iter().min().unwrap()
    }

    fn find_overlapping_ssts(
        &self,
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize,
    ) -> Vec<usize> {
        let mut overlapping = Vec::new();
        let mut first_key = KeyBytes::from_bytes(Bytes::new());
        let mut last_key = KeyBytes::from_bytes(Bytes::new());
        for sst_id in _sst_ids {
            let sst = _snapshot.sstables[sst_id].clone();
            if first_key.is_empty() || sst.first_key() < &first_key {
                first_key = sst.first_key().clone();
            }
            if last_key.is_empty() || sst.last_key() > &last_key {
                last_key = sst.last_key().clone();
            }
        }
        for sst_id in &_snapshot.levels[_in_level - 1].1 {
            let sst = _snapshot.sstables[sst_id].clone();
            if sst.first_key() <= &last_key && sst.last_key() >= &first_key {
                overlapping.push(sst.sst_id());
            }
        }
        overlapping
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let target_sizes = self.compute_target_sizes(_snapshot);
        let base_level = target_sizes.iter().position(|size| *size != 0).unwrap() + 1;
        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let overlapping =
                self.find_overlapping_ssts(_snapshot, &_snapshot.l0_sstables, base_level);
            println!("flush L0 SST to base level {}", base_level);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: overlapping,
                is_lower_level_bottom_level: base_level == _snapshot.levels.len(),
            });
        }

        let mut priorities: Vec<(f64, usize)> = (0..self.options.max_levels - 1)
            .filter_map(|idx| {
                let prio = if target_sizes[idx] == 0 {
                    0.
                } else {
                    level_size_mb(_snapshot, idx) as f64 / target_sizes[idx] as f64
                };
                if prio > 1.0 {
                    Some((prio, idx + 1))
                } else {
                    None
                }
            })
            .collect();
        priorities.sort_by(|a, b| a.partial_cmp(b).unwrap().reverse());
        let highest = priorities.first();

        if let Some((_, upper)) = highest {
            println!(
                "target level sizes: {:?}, real level sizes: {:?}, base_level: {}",
                target_sizes
                    .iter()
                    .map(|size| format!("{:.3}MB", *size as f64))
                    .collect::<Vec<_>>(),
                (0..self.options.max_levels)
                    .map(|idx| format!("{:.3}MB", level_size_mb(_snapshot, idx) as f64))
                    .collect::<Vec<_>>(),
                base_level
            );

            let oldest = self.find_oldest(_snapshot, *upper);

            println!(
                "compaction triggered by priority: {} out of {:?}, select {oldest} for compaction",
                upper, priorities
            );

            let overlapping = self.find_overlapping_ssts(_snapshot, &[oldest], upper + 1);
            return Some(LeveledCompactionTask {
                upper_level: Some(*upper),
                upper_level_sst_ids: vec![oldest],
                lower_level: upper + 1,
                lower_level_sst_ids: overlapping,
                is_lower_level_bottom_level: upper + 1 == self.options.max_levels,
            });
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut ssts_to_delete = Vec::new();

        let (head, tail) = snapshot.levels.split_at_mut(_task.lower_level - 1);
        let upper = match _task.upper_level {
            None => &mut snapshot.l0_sstables,
            Some(level) => &mut head[level - 1].1,
        };
        let lower = &mut tail[0].1;
        for id in &_task.upper_level_sst_ids {
            let idx = upper.iter().position(|sst_id| sst_id == id);
            assert!(idx.is_some());
            ssts_to_delete.push(*id);
            upper.remove(idx.unwrap());
        }
        for id in &_task.lower_level_sst_ids {
            let idx = lower.iter().position(|sst_id| sst_id == id);
            assert!(idx.is_some());
            ssts_to_delete.push(*id);
            lower.remove(idx.unwrap());
        }
        lower.extend_from_slice(_output);
        lower.sort_by(|a, b| {
            snapshot.sstables[a]
                .first_key()
                .cmp(snapshot.sstables[b].first_key())
        });

        (snapshot, ssts_to_delete)
    }
}
