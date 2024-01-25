//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>

#include "common/config.h"
#include "execution/executors/delete_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  try {
    if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                                table_info_->oid_)) {
      throw ExecutionException("Lock Table FAILED");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("InsertExecutor::Init " + e.GetInfo());
  }
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_ok_) {
    return false;
  }

  if (table_info_->schema_.GetColumnCount() != child_executor_->GetOutputSchema().GetColumnCount()) {
    return false;
  }

  auto count = 0;
  while (child_executor_->Next(tuple, rid)) {
    if (rid->GetPageId() == INVALID_PAGE_ID) {
      continue;
    }

    try {
      if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                table_info_->oid_, *rid)) {
        exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
        throw ExecutionException("Lock Row FAILED");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("InsertExecutor::Next " + e.GetInfo());
    }

    auto tuple_meta = table_info_->table_->GetTupleMeta(*rid);
    tuple_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(tuple_meta, *rid);

    // when aborted, we need to undo the previous write operation, so here is a record for undo
    auto record = TableWriteRecord(table_info_->oid_, *rid, table_info_->table_.get());
    record.wtype_ = WType::DELETE;
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(record);

    for (auto &index : index_info_) {
      auto delete_key = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(delete_key, *rid, nullptr);

      // when aborted, we need to undo the previous write operation, so here is a record for undo
      exec_ctx_->GetTransaction()->AppendIndexWriteRecord(IndexWriteRecord(
          *rid, table_info_->oid_, WType::DELETE, delete_key, index->index_oid_, exec_ctx_->GetCatalog()));
    }

    count++;
  }
  is_ok_ = true;
  auto shema = GetOutputSchema();
  *tuple = Tuple{std::vector<Value>{{TypeId::INTEGER, count}}, &shema};
  return true;
}

}  // namespace bustub
