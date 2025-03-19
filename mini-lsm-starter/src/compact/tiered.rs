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

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if _snapshot.levels.len() < self.options.num_tiers {
            return None;
        }
        let all_levels_size: usize = _snapshot.levels.iter().map(|lvl| lvl.1.len()).sum();
        let last_level_size = _snapshot.levels[_snapshot.levels.len() - 1].1.len();
        let ratio = (all_levels_size - last_level_size) as f64 / last_level_size as f64 * 100.;
        if ratio >= self.options.max_size_amplification_percent as f64 {
            println!(
                "compaction triggered by space amplification ratio: {}",
                ratio
            );
            return Some(TieredCompactionTask {
                tiers: _snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }
        for tier in self.options.min_merge_width.._snapshot.levels.len() {
            let this_tier = _snapshot.levels[tier].1.len();
            let previous_tiers: usize = _snapshot.levels[0..tier]
                .iter()
                .map(|level| level.1.len())
                .sum();
            let ratio = this_tier as f64 / previous_tiers as f64 * 100.;
            if ratio > (100 + self.options.size_ratio) as f64 {
                println!(
                    "compaction triggered by size ratio: {} > {}",
                    ratio,
                    100 + self.options.size_ratio
                );
                let tiers = _snapshot.levels[0..tier].to_vec();
                return Some(TieredCompactionTask {
                    tiers,
                    bottom_tier_included: false,
                });
            }
        }
        if _snapshot.levels.len() >= self.options.num_tiers {
            println!("compaction triggered by reducing sorted runs");
            return Some(TieredCompactionTask {
                tiers: _snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let levels_to_delete: Vec<usize> = _task.tiers.iter().map(|lvl| lvl.0).collect();
        let mut sst_to_delete = Vec::new();
        // this is the index of the first tier at the time the compaction task was created
        // in the meantime new tiers could have been inserted so we need to pay attention to that
        let start = snapshot
            .levels
            .iter()
            .position(|level| level.0 == _task.tiers[0].0)
            .unwrap();
        for i in start..start + _task.tiers.len() {
            assert_eq!(snapshot.levels[i].0, _task.tiers[i - start].0);
            for sst_id in &snapshot.levels[i].1 {
                sst_to_delete.push(*sst_id)
            }
        }
        let new_level = (_output[0], _output.to_vec());
        snapshot
            .levels
            .retain(|level| !levels_to_delete.contains(&level.0));
        snapshot.levels.insert(start, new_level);
        (snapshot, sst_to_delete)
    }
}
