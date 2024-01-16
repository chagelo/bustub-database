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

    auto tuple_meta = table_info_->table_->GetTupleMeta(*rid);
    tuple_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(tuple_meta, *rid);

    for (auto &index : index_info_) {
      auto delete_key = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(delete_key, *rid, nullptr);
    }

    count++;
  }
  is_ok_ = true;
  auto shema = GetOutputSchema();
  *tuple = Tuple{std::vector<Value>{{TypeId::INTEGER, count}}, &shema};
  return true;
}

}  // namespace bustub
