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

auto LockManager::CanLockUpgrade(LockManager::LockMode curr_lock_mode, LockManager::LockMode requested_lock_mode)
    -> bool {
  if (curr_lock_mode == LockManager::LockMode::INTENTION_SHARED) {  // IS->[S, X, IX, SIX]
    return requested_lock_mode == LockManager::LockMode::SHARED ||
           requested_lock_mode == LockManager::LockMode::SHARED ||
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

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { return true; }

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
