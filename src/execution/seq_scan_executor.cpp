//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>
#include "common/exception.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "recovery/log_manager.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())) {}

void SeqScanExecutor::Init() {
  try {
    if (exec_ctx_->IsDelete()) {
      // delete operation intend to delete one or servel tuples
      // so lockmode is ix
      if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
                                                  LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_)) {
        throw ExecutionException("Lock Table FAILED");
      }
    } else if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
               !exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(table_info_->oid_)) {
      if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,
                                                  table_info_->oid_)) {
        throw ExecutionException("Lock Table FAILED");
      }
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("SeqExecutor::Init " + e.GetInfo());
  }

  iter_ = std::make_shared<TableIterator>(table_info_->table_->MakeEagerIterator());
  is_false_ = Check(plan_->filter_predicate_);
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_false_) {
    return false;
  }

  for (; !iter_->IsEnd();) {
    *rid = iter_->GetRID();
    try {
      if (exec_ctx_->IsDelete()) {
        // Delete, We have IX, now X for row
        if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                  table_info_->oid_, *rid)) {
          throw ExecutionException("Lock Row Failed");
        }

        // for rc and rr, if now we have no this row's X lock
      } else if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
                 !exec_ctx_->GetTransaction()->IsRowExclusiveLocked(table_info_->oid_, *rid)) {
        // we now lock this row successfully
        if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                  table_info_->oid_, *rid)) {
          // else throw error
          throw ExecutionException("Lock Row Failed");
        }
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("SeqExecutor::Next " + e.GetInfo());
    }

    auto cur_tuple = iter_->GetTuple();
    bool ok = false;
    if (!cur_tuple.first.is_deleted_) {
      if (plan_->filter_predicate_ == nullptr) {
        *tuple = std::move(cur_tuple.second);
        *rid = iter_->GetRID();
        ok = true;
      } else {
        // filter seqscan
        auto value = plan_->filter_predicate_->Evaluate(&cur_tuple.second, plan_->OutputSchema());

        if (!value.IsNull() && value.GetAs<bool>()) {
          *tuple = std::move(cur_tuple.second);
          *rid = iter_->GetRID();
          ok = true;
        }
      }
    }
    ++(*iter_);

    // unlock row
    // sucessful get the next tuple
    if (ok) {
      // for RC, read and release the S lock, but for rr, unlock S in SHRINKING phase
      if (!exec_ctx_->IsDelete() &&
          exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        try {
          if (!exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), table_info_->oid_, *rid)) {
            throw ExecutionException("UnLock Row Failed");
          }
        } catch (TransactionAbortException &e) {
          throw ExecutionException("SeqExecutor::Next " + e.GetInfo());
        }
      }

      return true;
    }

    // unlock row
    // only when read operation and isolation level is ru, we add no lock, otherwise, here we need to release the cur
    // lock
    if (exec_ctx_->IsDelete() || exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      try {
        if (!exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), table_info_->oid_, *rid, true)) {
          throw ExecutionException("UnLock Row Failed");
        }
      } catch (TransactionAbortException &e) {
        throw ExecutionException("SeqExecutor::Next " + e.GetInfo());
      }
    }
  }

  // unlock table
  // now, there is the end of the table, so we need to release the table lock
  // for rr we unlock in SHRINKING phase, for ru read we don't apply any lock
  //        ru   rc
  //  read  no   yes
  //  write no   yes
  //  here, for write operation, because the delete operation is not work on, now just seqcan the table
  if (!exec_ctx_->IsDelete() && exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    try {
      if (!exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), table_info_->oid_)) {
        throw ExecutionException("Unlock Table Failed");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("SeqExecutor::Next " + e.GetInfo());
    }
  }

  return false;
}

auto SeqScanExecutor::Check(const AbstractExpressionRef &expr) -> bool {
  if (expr == nullptr) {
    return false;
  }

  for (auto &child : expr->GetChildren()) {
    auto is_false = Check(child);
    if (is_false) {
      return true;
    }
  }
  if (const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(expr.get()); comp_expr != nullptr) {
    if (const auto *left_expr = dynamic_cast<const ConstantValueExpression *>(comp_expr->children_[0].get());
        left_expr != nullptr) {
      if (const auto *right_expr = dynamic_cast<const ConstantValueExpression *>(comp_expr->children_[1].get());
          right_expr != nullptr) {
        if (comp_expr->comp_type_ == ComparisonType::Equal) {
          if (left_expr->val_.CompareEquals(right_expr->val_) == CmpBool::CmpFalse) {
            return true;
          }
        } else if (comp_expr->comp_type_ == ComparisonType::NotEqual) {
          if (left_expr->val_.CompareNotEquals(right_expr->val_) == CmpBool::CmpFalse) {
            return true;
          }
        } else if (comp_expr->comp_type_ == ComparisonType::LessThan) {
          if (left_expr->val_.CompareLessThan(right_expr->val_) == CmpBool::CmpFalse) {
            return true;
          }
        }
        if (comp_expr->comp_type_ == ComparisonType::LessThanOrEqual) {
          if (left_expr->val_.CompareLessThanEquals(right_expr->val_) == CmpBool::CmpFalse) {
            return true;
          }
        } else if (comp_expr->comp_type_ == ComparisonType::GreaterThan) {
          if (left_expr->val_.CompareGreaterThan(right_expr->val_) == CmpBool::CmpFalse) {
            return true;
          }
        } else if (comp_expr->comp_type_ == ComparisonType::GreaterThanOrEqual) {
          if (left_expr->val_.CompareGreaterThanEquals(right_expr->val_) == CmpBool::CmpFalse) {
            return true;
          }
        }
      }
    }
  }
  return false;
}
}  // namespace bustub
