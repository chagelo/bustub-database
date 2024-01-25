//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <memory>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

enum class LockSetAction { Insert, Delete };

// modify table lock set
template <LockSetAction action>
void ModifyTableLockSet(Transaction *txn, const std::shared_ptr<LockManager::LockRequest> &lock_request) {
  txn->LockTxn();
  switch (lock_request->lock_mode_) {
    case LockManager::LockMode::SHARED:
      if constexpr (action == LockSetAction::Insert) {
        txn->GetSharedTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockManager::LockMode::EXCLUSIVE:
      if constexpr (action == LockSetAction::Insert) {
        txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockManager::LockMode::INTENTION_SHARED:
      if constexpr (action == LockSetAction::Insert) {
        txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetIntentionSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockManager::LockMode::INTENTION_EXCLUSIVE:
      if constexpr (action == LockSetAction::Insert) {
        txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      if constexpr (action == LockSetAction::Insert) {
        txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
  }
  txn->UnlockTxn();
}

// modify row table lock set
template <LockSetAction action>
void ModifyRowLockSet(Transaction *txn, const std::shared_ptr<LockManager::LockRequest> &lock_request) {
  txn->LockTxn();
  switch (lock_request->lock_mode_) {
    case LockManager::LockMode::SHARED:
      if constexpr (action == LockSetAction::Insert) {
        (*txn->GetSharedRowLockSet())[lock_request->oid_].insert(lock_request->rid_);
      } else {
        (*txn->GetSharedRowLockSet())[lock_request->oid_].erase(lock_request->rid_);
      }
      break;
    case LockManager::LockMode::EXCLUSIVE:
      if constexpr (action == LockSetAction::Insert) {
        (*txn->GetExclusiveRowLockSet())[lock_request->oid_].insert(lock_request->rid_);
      } else {
        (*txn->GetExclusiveRowLockSet())[lock_request->oid_].erase(lock_request->rid_);
      }
      break;
    case LockManager::LockMode::INTENTION_SHARED:
    case LockManager::LockMode::INTENTION_EXCLUSIVE:
    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
  txn->UnlockTxn();
}

auto LockManager::CanLockUpgrade(LockManager::LockMode curr_lock_mode, LockManager::LockMode requested_lock_mode)
    -> bool {
  if (curr_lock_mode == LockManager::LockMode::INTENTION_SHARED) {  // IS->[S, X, IX, SIX]
    return requested_lock_mode == LockManager::LockMode::SHARED ||
           requested_lock_mode == LockManager::LockMode::EXCLUSIVE ||
           requested_lock_mode == LockManager::LockMode::INTENTION_EXCLUSIVE ||
           requested_lock_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE;
  }

  if (curr_lock_mode == LockManager::LockMode::SHARED) {  // S -> [X, SIX]
    return requested_lock_mode == LockManager::LockMode::EXCLUSIVE ||
           requested_lock_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE;
  }

  if (curr_lock_mode == LockManager::LockMode::INTENTION_EXCLUSIVE) {  // IX -> [X, SIX]
    return requested_lock_mode == LockManager::LockMode::EXCLUSIVE ||
           requested_lock_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE;
  }

  if (curr_lock_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {  // SIX -> [X]
    return requested_lock_mode == LockManager::LockMode::EXCLUSIVE;
  }
  return false;
}

auto CheckIsolation(Transaction *txn, LockManager::LockMode lock_mode) -> bool {
  if (txn == nullptr) {
    return false;
  }

  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode == LockManager::LockMode::SHARED || lock_mode == LockManager::LockMode::INTENTION_SHARED ||
          lock_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if (txn->GetState() == TransactionState::SHRINKING &&
          (lock_mode == LockManager::LockMode::EXCLUSIVE || lock_mode == LockManager::LockMode::INTENTION_EXCLUSIVE)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockManager::LockMode::SHARED &&
          lock_mode != LockManager::LockMode::INTENTION_SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
  }
  return true;
}

auto LockManager::AreLocksCompatible(LockManager::LockMode l1, LockManager::LockMode l2) -> bool {
  if (l1 == LockManager::LockMode::INTENTION_SHARED) {
    return l2 == LockManager::LockMode::INTENTION_SHARED || l2 == LockManager::LockMode::INTENTION_EXCLUSIVE ||
           l2 == LockManager::LockMode::SHARED || l2 == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  if (l1 == LockManager::LockMode::INTENTION_EXCLUSIVE) {
    return l2 == LockManager::LockMode::INTENTION_SHARED || l2 == LockManager::LockMode::INTENTION_EXCLUSIVE;
  }
  if (l1 == LockManager::LockMode::SHARED) {
    return l2 == LockManager::LockMode::INTENTION_SHARED || l2 == LockManager::LockMode::SHARED;
  }
  if (l1 == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return l2 == LockManager::LockMode::INTENTION_SHARED;
  }

  return false;
}

/**
 * should put the upgrade request in front of the first request not granted
 */

// can satisfy lock request? should be highest priority request
auto LockManager::CanGrantLock(const std::shared_ptr<LockRequest> &lock_request,
                               const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  // current lock request should be compatible with all granted request in  the request queue
  for (const auto &lr : lock_request_queue->request_queue_) {
    if (lr->granted_ && !AreLocksCompatible(lock_request->lock_mode_, lr->lock_mode_)) {
      return false;
    }
  }

  // current lock request is the priorisited request
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    if (lock_request_queue->upgrading_ == lock_request->txn_id_) {
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      lock_request->granted_ = true;
      return true;
    }
    // if there exists upgrading request, but no the current request
    return false;
  }

  // granted and compatiable
  // not granted and compatiable
  // not granted and incompatiable
  // if no upgrading request, find the highest priority request followed by fifo
  for (auto &lr : lock_request_queue->request_queue_) {
    if (lr->txn_id_ == lock_request->txn_id_) {
      lr->granted_ = true;
      break;
    }
    // X(waiting) S(wating), the X(waiting) have the highest priority
    // S(granted) X(waiting) S(waiting),
    if (!lr->granted_ && !AreLocksCompatible(lock_request->lock_mode_, lr->lock_mode_)) {
      return false;
    }
    // lr->granted_ = true;?
    // (1)S(granted) X(waiting) S(waiting), which should be granted, both ok?
    // (2)S(granted) S(waiting)(1) S(waiting)(2)(cur), which should be granted, both ok!
    // for (2), if find S(waiting)(1), should not do noting
  }
  return true;
}

// check lock on a row, intention lock should not appear
void CheckLockRow(Transaction *txn, LockManager::LockMode lock_mode, const table_oid_t &oid) {
  switch (lock_mode) {
    case LockManager::LockMode::EXCLUSIVE:
      if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
          !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
      }
      break;
    case LockManager::LockMode::SHARED:
      break;
    case LockManager::LockMode::INTENTION_EXCLUSIVE:
    case LockManager::LockMode::INTENTION_SHARED:
    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
      break;
  }
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // txn related lock request
  if (!CheckIsolation(txn, lock_mode)) {
    return false;
  }

  // the target lockrequest not exist, just create
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto lrq = table_lock_map_.at(oid);
  std::unique_lock lock(lrq->latch_);
  table_lock_map_latch_.unlock();

  // is exist a request with same transaction id, check if need upgrade lock
  for (auto it = lrq->request_queue_.begin(); it != lrq->request_queue_.end(); it++) {
    auto lr = *it;
    if (lr->txn_id_ == txn->GetTransactionId()) {
      // same lock mode, it means a repeat lock request, return
      if (lr->lock_mode_ == lock_mode) {
        return true;
      }
      // cur lock request is a lock upgrade request
      // UPGRADE_CONFLICT, only one transaction is allowed to upgrade its lock on a given source
      if (lrq->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      if (!CanLockUpgrade(lr->lock_mode_, lock_mode)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      lrq->upgrading_ = txn->GetTransactionId();
      lrq->request_queue_.erase(it);
      ModifyTableLockSet<LockSetAction::Delete>(txn, lr);
      break;
    }
  }

  // add current lock request to request queue
  auto nlr = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lrq->request_queue_.push_back(nlr);
  while (!CanGrantLock(nlr, lrq)) {
    lrq->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      if (lrq->upgrading_ == txn->GetTransactionId()) {
        lrq->upgrading_ = INVALID_TXN_ID;
      }
      lrq->request_queue_.remove(nlr);
      lrq->cv_.notify_all();
      return false;
    }
  }
  ModifyTableLockSet<LockSetAction::Insert>(txn, nlr);
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  if (txn == nullptr) {
    return false;
  }

  // unlock row before unlock table
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  if (!(s_row_lock_set->find(oid) == s_row_lock_set->end() || s_row_lock_set->at(oid).empty()) ||
      !(x_row_lock_set->find(oid) == x_row_lock_set->end() || x_row_lock_set->at(oid).empty())) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  // there no lock request related to this table
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto lrq = table_lock_map_.at(oid);
  std::unique_lock lock(lrq->latch_);
  table_lock_map_latch_.unlock();

  for (auto it = lrq->request_queue_.begin(); it != lrq->request_queue_.end(); it++) {
    auto lr = *it;
    if (lr->granted_ && lr->txn_id_ == txn->GetTransactionId()) {
      switch (txn->GetIsolationLevel()) {
        case IsolationLevel::REPEATABLE_READ:
          if (lr->lock_mode_ == LockMode::SHARED || lr->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
          break;
        case IsolationLevel::READ_COMMITTED:
        case IsolationLevel::READ_UNCOMMITTED:
          if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
          break;
      }
      ModifyTableLockSet<LockSetAction::Delete>(txn, lr);
      lrq->request_queue_.erase(it);
      lrq->cv_.notify_all();
      return true;
    }
  }
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  // check row lock isolation
  CheckIsolation(txn, lock_mode);
  // check lock on a row, intention lock should not appear
  CheckLockRow(txn, lock_mode, oid);

  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }

  auto lrq = row_lock_map_.at(rid);
  std::unique_lock lock(lrq->latch_);
  row_lock_map_latch_.unlock();

  std::shared_ptr<LockRequest> nlr{nullptr};
  for (auto it = lrq->request_queue_.begin(); it != lrq->request_queue_.end(); ++it) {
    auto lr = *it;
    if (lr->txn_id_ == txn->GetTransactionId()) {
      // repeat request
      if (lr->lock_mode_ == lock_mode) {
        return true;
      }
      // multiple upgrade request, in this bustub is not allowed
      if (lrq->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      // upgrade request
      if (!CanLockUpgrade(lr->lock_mode_, lock_mode)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      // mark there is a upgrade request in the queue
      lrq->upgrading_ = txn->GetTransactionId();
      lrq->request_queue_.erase(it);
      ModifyRowLockSet<LockSetAction::Delete>(txn, lr);
      nlr = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
      break;
    }
  }

  // if no upgrading request, just general request
  if (!nlr) {
    nlr = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  }
  lrq->request_queue_.push_back(nlr);

  // grant lock follow by priority
  while (!CanGrantLock(nlr, lrq)) {
    lrq->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      if (lrq->upgrading_ == txn->GetTransactionId()) {
        lrq->upgrading_ = INVALID_TXN_ID;
      }
      lrq->request_queue_.remove(nlr);
      lrq->cv_.notify_all();
      return false;
    }
  }

  ModifyRowLockSet<LockSetAction::Insert>(txn, nlr);
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  row_lock_map_latch_.lock();
  // unlock but no lock
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto lrq = row_lock_map_.at(rid);
  std::unique_lock lock(lrq->latch_);
  row_lock_map_latch_.unlock();

  for (auto it = lrq->request_queue_.begin(); it != lrq->request_queue_.end(); ++it) {
    auto lr = *it;
    if (lr->granted_ && lr->txn_id_ == txn->GetTransactionId()) {
      if (!force) {
        switch (txn->GetIsolationLevel()) {
          case IsolationLevel::REPEATABLE_READ:
            if (lr->lock_mode_ == LockMode::SHARED || lr->lock_mode_ == LockMode::EXCLUSIVE) {
              txn->SetState(TransactionState::SHRINKING);
            }
            break;
          case IsolationLevel::READ_COMMITTED:
          case IsolationLevel::READ_UNCOMMITTED:
            if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
              txn->SetState(TransactionState::SHRINKING);
            }
            break;
        }
      }
      ModifyRowLockSet<LockSetAction::Delete>(txn, lr);
      lrq->request_queue_.erase(it);
      lrq->cv_.notify_all();
      return true;
    }
  }
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::BuildGraph() {
  {
    std::lock_guard lock(table_lock_map_latch_);
    for (const auto &[_, lrq] : table_lock_map_) {
      std::vector<txn_id_t> granted;
      std::vector<txn_id_t> waited;
      {
        std::lock_guard lrq_lock(lrq->latch_);
        for (const auto &lr : lrq->request_queue_) {
          auto txn = txn_manager_->GetTransaction(lr->txn_id_);
          if (txn != nullptr && txn->GetState() != TransactionState::ABORTED) {
            if (lr->granted_) {
              granted.push_back(lr->txn_id_);
            } else {
              waited.push_back(lr->txn_id_);
            }
          }
        }
      }
      for (const auto &u : granted) {
        for (const auto &v : waited) {
          AddEdge(u, v);
        }
      }
    }
  }

  {
    std::lock_guard lock(row_lock_map_latch_);
    for (const auto &[_, lrq] : row_lock_map_) {
      std::vector<txn_id_t> granted;
      std::vector<txn_id_t> waited;
      {
        std::lock_guard lrq_lock(lrq->latch_);
        for (const auto &lr : lrq->request_queue_) {
          auto txn = txn_manager_->GetTransaction(lr->txn_id_);
          if (txn != nullptr && txn->GetState() != TransactionState::ABORTED) {
            if (lr->granted_) {
              granted.push_back(lr->txn_id_);
            } else {
              waited.push_back(lr->txn_id_);
            }
          }
        }
      }
      for (const auto &u : granted) {
        for (const auto &v : waited) {
          AddEdge(u, v);
        }
      }
    }
  }
}

void LockManager::PrintGraph() {
  if (waits_for_.empty()) {
    return;
  }
  std::cout << "==========\n";
  for (const auto &[t1, t2s] : waits_for_) {
    std::cout << t1 << " -> ";
    for (const auto &t2 : t2s) {
      std::cout << t2 << " ";
    }
    std::cout << "\n";
  }
}

void LockManager::RemoveAllAboutAbortTxn(txn_id_t tid) {
  // std::cout << "RemoveEdge " << tid << "\n";
  waits_for_.erase(tid);
  for (auto &[_, waits] : waits_for_) {
    waits.erase(tid);
  }
}

void LockManager::WakeAbortedTxn(txn_id_t tid) {
  {
    bool is_find = false;
    std::lock_guard lock(table_lock_map_latch_);
    for (auto &[_, lrq] : table_lock_map_) {
      for (const auto &lr : lrq->request_queue_) {
        if (lr->txn_id_ == tid && !lr->granted_) {
          lrq->cv_.notify_all();
          is_find = true;
          break;
        }
      }
    }
    if (is_find) {
      return;
    }
  }
  std::lock_guard lock(row_lock_map_latch_);
  for (auto &[_, lrq] : row_lock_map_) {
    for (const auto &lr : lrq->request_queue_) {
      if (lr->txn_id_ == tid && !lr->granted_) {
        lrq->cv_.notify_all();
        break;
      }
    }
  }
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].insert(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].erase(t2); }

auto LockManager::DFS(txn_id_t txn_id) -> bool {
  if (searched_set_.count(txn_id) > 0) {
    return false;
  }
  if (cycle_set_.find(txn_id) != cycle_set_.end()) {
    return true;
  }
  cycle_set_.emplace(txn_id);
  auto &targets = waits_for_[txn_id];
  for (const auto &target : targets) {
    if (DFS(target)) {
      return true;
    }
  }
  searched_set_.emplace(txn_id);
  cycle_set_.erase(txn_id);
  return false;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  for (const auto &edge : waits_for_) {
    txn_sort_list_.emplace_back(edge.first);
  }

  std::sort(txn_sort_list_.begin(), txn_sort_list_.end(),
            [](const txn_id_t &t1, const txn_id_t &t2) { return t1 < t2; });
  for (const auto &begin_txn : txn_sort_list_) {
    cycle_set_.clear();
    if (DFS(begin_txn)) {
      auto youngest_txn = *cycle_set_.begin();
      for (auto cycle_node : cycle_set_) {
        youngest_txn = std::max(youngest_txn, cycle_node);
      }
      *txn_id = youngest_txn;
      return true;
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  // std::lock_guard lock(waits_for_latch_);
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &[t1, t2s] : waits_for_) {
    for (const auto &t2 : t2s) {
      edges.emplace_back(t1, t2);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      waits_for_.clear();
      BuildGraph();
      while (true) {
        txn_sort_list_.clear();
        searched_set_.clear();
        cycle_set_.clear();
        txn_id_t tid;
        if (HasCycle(&tid)) {
          auto txn = txn_manager_->GetTransaction(tid);
          txn->SetState(TransactionState::ABORTED);
          RemoveAllAboutAbortTxn(tid);
          WakeAbortedTxn(tid);
        } else {
          break;
        }
      }
    }
  }
}

}  // namespace bustub
