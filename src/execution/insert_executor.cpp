//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>

#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/executors/insert_executor.h"
#include "storage/index/index.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  try {
    if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                                table_info_->oid_)) {
      throw ExecutionException("Lock Table FAILED");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("InsertExecutor::Init " + e.GetInfo());
  }
}

// If a transaction aborts, you will need to undo its previous write operations;
// to achieve this, you will need to maintain the write set in each transaction,
// which is used by the Abort() method of the transaction manager.
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_ok_) {
    return false;
  }

  // schema column not match
  if (child_executor_->GetOutputSchema().GetColumnCount() != table_info_->schema_.GetColumnCount()) {
    return false;
  }
  auto child_columns = child_executor_->GetOutputSchema().GetColumns();
  auto table_columns = table_info_->schema_.GetColumns();

  auto count = 0;
  TupleMeta tuple_meta{.insert_txn_id_ = INVALID_TXN_ID, .delete_txn_id_ = INVALID_TXN_ID, .is_deleted_ = false};
  while (child_executor_->Next(tuple, rid)) {  // value executor
    // it means the tuple is in a page, it no need to insert again
    // if (rid->GetPageId() != INVALID_PAGE_ID) {
    //   continue;
    // }

    // compare the type of each couple column
    bool no_match = false;
    for (uint32_t i = 0; i < child_columns.size(); ++i) {
      if (table_columns[i].GetType() != child_columns[i].GetType() ||
          table_columns[i].GetOffset() != child_columns[i].GetOffset()) {
        no_match = true;
        break;
      }
    }
    // type or the offset of the column differ
    if (no_match) {
      continue;
    }

    // insert
    tuple_meta.is_deleted_ = false;
    auto insert_rid = table_info_->table_->InsertTuple(tuple_meta, *tuple);
    if (insert_rid == std::nullopt) {
      continue;
    }
    count++;

    try {
      if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                table_info_->oid_, insert_rid.value())) {
        exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
        throw ExecutionException("Lock Row FAILED");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("InsertExecutor::Next " + e.GetInfo());
    }

    // when aborted, we need undo the previous wirte operation, so here we need to record the write operation
    auto record = TableWriteRecord(table_info_->oid_, insert_rid.value(), table_info_->table_.get());
    record.wtype_ = WType::INSERT;
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(record);

    // for this tuple, iterate each index, and insert index
    for (auto &index_info : index_infos_) {
      // new tuple select server column from the given tuple and tuple schema and key schema
      auto key = tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, insert_rid.value(), nullptr);

      // when aborted, we need undo the previous wirte operation, so here we need to record the write operation
      exec_ctx_->GetTransaction()->AppendIndexWriteRecord(IndexWriteRecord(
          insert_rid.value(), table_info_->oid_, WType::INSERT, key, index_info->index_oid_, exec_ctx_->GetCatalog()));
    }
  }

  *tuple = Tuple{std::vector<Value>{{TypeId::INTEGER, count}}, &GetOutputSchema()};
  is_ok_ = true;
  return true;
}

}  // namespace bustub
