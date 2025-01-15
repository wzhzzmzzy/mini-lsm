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

    fn over_size_ratio(&self, lower_len: usize, upper_len: usize) -> bool {
        (lower_len as f64 / upper_len as f64) < self.options.size_ratio_percent as f64 / 100.0
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        let l0_sst = &snapshot.l0_sstables;
        let l1_sst = &snapshot.levels[0].1;

        if l0_sst.len() >= self.options.level0_file_num_compaction_trigger
            && self.over_size_ratio(l1_sst.len(), l0_sst.len())
        {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: l0_sst.clone(),
                lower_level: 1,
                lower_level_sst_ids: l1_sst.clone(),
                is_lower_level_bottom_level: self.options.max_levels == 1,
            });
        }
        for level in 1..self.options.max_levels {
            let upper_level_sst_ids = &snapshot.levels[level - 1].1;
            if upper_level_sst_ids.is_empty() {
                continue;
            }
            let lower_level_sst_ids = &snapshot.levels[level].1;
            if lower_level_sst_ids.is_empty()
                || self.over_size_ratio(lower_level_sst_ids.len(), upper_level_sst_ids.len())
            {
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(level),
                    upper_level_sst_ids: upper_level_sst_ids.clone(),
                    lower_level: level + 1,
                    lower_level_sst_ids: lower_level_sst_ids.clone(),
                    is_lower_level_bottom_level: self.options.max_levels == level + 1,
                });
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
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut levels = snapshot.levels.clone();
        levels[task.lower_level - 1] = (task.lower_level, Vec::from(output));
        if let Some(upper_level) = task.upper_level {
            let mut t = levels[upper_level - 1].1.clone();
            t.retain_mut(|i| !task.upper_level_sst_ids.contains(i));
            levels[upper_level - 1] = (upper_level, t);
        }
        let new_state = LsmStorageState {
            memtable: snapshot.memtable.clone(),
            imm_memtables: snapshot.imm_memtables.clone(),
            levels,
            sstables: snapshot.sstables.clone(),
            l0_sstables: if task.upper_level.is_none() {
                snapshot
                    .l0_sstables
                    .iter()
                    .filter(|id| !task.upper_level_sst_ids.contains(*id))
                    .cloned()
                    .collect()
            } else {
                snapshot.l0_sstables.clone()
            },
        };

        let mut need_delete = snapshot.levels[task.lower_level - 1].1.clone();
        need_delete.extend(task.upper_level_sst_ids.iter());

        (new_state, need_delete)
    }
}
