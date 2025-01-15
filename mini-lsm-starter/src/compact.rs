#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{KeySlice, KeyVec};
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

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

type CompactionIterator = TwoMergeIterator<SstConcatIterator, SstConcatIterator>;

impl LsmStorageInner {
    fn compact_by_iterator(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut sst_next_level: Vec<Arc<SsTable>> = vec![];
        let mut builder = SsTableBuilder::new(self.options.block_size);
        // flag, 表示当前 SsTableBuilder 中是否存在 KV 对
        let mut builder_empty = true;

        while iter.is_valid() {
            if compact_to_bottom_level && iter.value().is_empty() {
                iter.next()?;
                continue;
            }
            builder.add(iter.key(), iter.value());
            if builder_empty {
                builder_empty = false;
            }

            if builder.estimated_size() >= self.options.target_sst_size {
                // 使用 mem::replace 更新数据
                let old_builder =
                    std::mem::replace(&mut builder, SsTableBuilder::new(self.options.block_size));
                builder_empty = true;

                let sst_id = self.next_sst_id();
                sst_next_level.push(Arc::new(old_builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id).clone(),
                )?));
            }

            iter.next()?;
        }

        // 最后一个 builder 需要 flush
        if !builder_empty {
            let sst_id = self.next_sst_id();
            sst_next_level.push(Arc::new(builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id).clone(),
            )?));
        }

        Ok(sst_next_level)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                // get sstables from self.state
                let l0_sstables: Result<Vec<_>> = l0_sstables
                    .iter()
                    .filter_map(|id| self.state.read().sstables.get(id).cloned())
                    .map(SsTableIterator::create_and_seek_to_first)
                    .map(|sst_iter_res| sst_iter_res.map(Box::new))
                    .collect();
                let l1_sstables: Vec<_> = l1_sstables
                    .iter()
                    .filter_map(|id| self.state.read().sstables.get(id).cloned())
                    .collect();

                self.compact_by_iterator(
                    TwoMergeIterator::create(
                        MergeIterator::create(l0_sstables?),
                        SstConcatIterator::create_and_seek_to_first(l1_sstables)?,
                    )?,
                    task.compact_to_bottom_level(),
                )
            }
            CompactionTask::Simple(task_inner) => {
                // get sstables from self.state
                let upper_sstables = task_inner
                    .upper_level_sst_ids
                    .iter()
                    .filter_map(|id| self.state.read().sstables.get(id).cloned());
                let lower_sstables: Vec<_> = task_inner
                    .lower_level_sst_ids
                    .iter()
                    .filter_map(|id| self.state.read().sstables.get(id).cloned())
                    .collect();

                if task_inner.upper_level.is_none() {
                    self.compact_by_iterator(
                        TwoMergeIterator::create(
                            MergeIterator::create(
                                upper_sstables
                                    .map(SsTableIterator::create_and_seek_to_first)
                                    .map(|sst_iter_res| sst_iter_res.map(Box::new))
                                    .collect::<Result<Vec<_>>>()?,
                            ),
                            SstConcatIterator::create_and_seek_to_first(lower_sstables)?,
                        )?,
                        task.compact_to_bottom_level(),
                    )
                } else {
                    self.compact_by_iterator(
                        TwoMergeIterator::create(
                            SstConcatIterator::create_and_seek_to_first(upper_sstables.collect())?,
                            SstConcatIterator::create_and_seek_to_first(lower_sstables)?,
                        )?,
                        task.compact_to_bottom_level(),
                    )
                }
            }
            _ => Ok(vec![]),
        }
    }

    fn select_sst_by_level(&self, level: usize) -> Vec<usize> {
        if level == 0 {
            self.state.read().l0_sstables.clone()
        } else {
            self.state.read().levels[level - 1].1.clone()
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let l0_sstables = self.select_sst_by_level(0);
        let l1_sstables = self.select_sst_by_level(1);

        let sst_next_level = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        })?;

        // 使用压缩后的 SST 更新 L1 SST
        let _state_guard = self.state_lock.lock();
        let mut write_guard = self.state.write();
        let mut write_ref = write_guard.as_ref().clone();

        // 更新 levels，由于 L0 有单独的字段存储，所以 levels 中 0 代表 L1
        write_ref.levels[0] = (
            1,
            sst_next_level
                .iter()
                .map(|sst| sst.sst_id())
                .collect::<Vec<_>>(),
        );
        for sst in sst_next_level.iter() {
            write_ref.sstables.insert(sst.sst_id(), sst.clone());
        }

        // 清除不需要的历史数据
        for id in l0_sstables.iter() {
            write_ref.l0_sstables.pop();
            std::fs::remove_file(self.path_of_sst(*id))?;
        }

        for id in l1_sstables.iter() {
            write_ref.sstables.remove(id);
            std::fs::remove_file(self.path_of_sst(*id))?;
        }

        *write_guard = Arc::new(write_ref);

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        match self.options.compaction_options {
            CompactionOptions::Simple(_) => {
                let snapshot = self.state.read().clone();
                let has_task = self
                    .compaction_controller
                    .generate_compaction_task(&snapshot);

                if has_task.is_none() {
                    return Ok(());
                }

                let task = has_task.unwrap();
                let output_sst = self.compact(&task)?;
                let output_sst_ids: Vec<_> = output_sst.iter().map(|sst| sst.sst_id()).collect();

                let _state_guard = self.state_lock.lock();
                let mut snapshot = self.state.read().as_ref().clone();

                output_sst.into_iter().for_each(|sst| {
                    snapshot.sstables.insert(sst.sst_id(), sst);
                });
                let (mut new_state, del) = self.compaction_controller.apply_compaction_result(
                    &snapshot,
                    &task,
                    &output_sst_ids[..],
                    false,
                );
                let mut file_to_remove = Vec::with_capacity(del.len());
                for del_sst_id in &del {
                    new_state.sstables.remove(del_sst_id);
                    file_to_remove.push(del_sst_id);
                }

                *self.state.write() = Arc::new(new_state);
                drop(snapshot);
                self.sync_dir()?;

                for sst_id in file_to_remove {
                    std::fs::remove_file(self.path_of_sst(*sst_id))?;
                }
                self.sync_dir()?;

                Ok(())
            }
            _ => Ok(()),
        }
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
        if self.options.num_memtable_limit <= self.state.read().imm_memtables.len() {
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
