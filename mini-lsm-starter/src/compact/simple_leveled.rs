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

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!(
                "compaction triggered at level 0 because L0 has {} SSTs >= {}",
                _snapshot.l0_sstables.len(),
                self.options.level0_file_num_compaction_trigger
            );
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                lower_level: 1,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level_sst_ids: _snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: self.options.max_levels == 1,
            });
        } else {
            for upper in 1..self.options.max_levels {
                let lower = upper + 1;
                let upper_len = _snapshot.levels[upper - 1].1.len();
                if upper_len == 0 {
                    continue;
                }
                let lower_len = _snapshot.levels[lower - 1].1.len();
                let size_ratio = lower_len as f64 / upper_len as f64;
                if ((size_ratio * 100.) as usize) < self.options.size_ratio_percent {
                    println!(
                        "compaction triggered at level {} and {} with size ratio {}",
                        upper, lower, size_ratio
                    );
                    return Some(SimpleLeveledCompactionTask {
                        upper_level: Some(upper),
                        lower_level: lower,
                        upper_level_sst_ids: _snapshot.levels[upper - 1].1.clone(),
                        lower_level_sst_ids: _snapshot.levels[lower - 1].1.clone(),
                        is_lower_level_bottom_level: self.options.max_levels == lower,
                    });
                }
            }
        }
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let levels = &mut snapshot.levels;
        let (head, tail) = levels.split_at_mut(_task.lower_level - 1);
        let upper = match _task.upper_level {
            None => &mut snapshot.l0_sstables,
            Some(level) => &mut head[level - 1].1,
        };
        let lower = &mut tail[0].1;

        let mut del = Vec::new();
        upper.retain(|id| {
            if _task.upper_level_sst_ids.contains(id) {
                del.push(*id);
                false
            } else {
                true
            }
        });
        lower.retain(|id| {
            if _task.lower_level_sst_ids.contains(id) {
                del.push(*id);
                false
            } else {
                true
            }
        });
        assert_eq!(
            del.len(),
            _task.upper_level_sst_ids.len() + _task.lower_level_sst_ids.len()
        );
        assert!(lower.is_empty());
        lower.extend_from_slice(_output);
        (snapshot, del)
    }
}
