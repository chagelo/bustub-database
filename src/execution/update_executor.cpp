//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <optional>

#include "common/logger.h"
#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  is_ok_ = false;
}
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // table_info maybe nullptr
  int count = 0;
  if (is_ok_) {
    return false;
  }

  TupleMeta tuple_meta{.insert_txn_id_ = INVALID_TXN_ID, .delete_txn_id_ = INVALID_TXN_ID, .is_deleted_ = false};
  auto &tuple_schema = child_executor_->GetOutputSchema();
  
  while (child_executor_->Next(tuple, rid)) {
    std::vector<Value> values{};
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(tuple, tuple_schema));
    }

    auto new_tuple = Tuple{values, &tuple_schema};

    tuple_meta.is_deleted_ = false;
    auto insert_ok = table_info_->table_->InsertTuple(tuple_meta, new_tuple);

    if (insert_ok != std::nullopt) {
      tuple_meta.is_deleted_ = true;
      table_info_->table_->UpdateTupleMeta(tuple_meta, *rid);

      // first construct the key of the index of this tuple for each index
      // first delete the old key, than index new key
      for (auto &index_info : index_info_) {
        std::cout << "321111111111111111" << std::endl;
        auto delet_key = tuple->KeyFromTuple(tuple_schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(delet_key, *rid, nullptr);
        auto insert_key =
            new_tuple.KeyFromTuple(tuple_schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(insert_key, *rid, nullptr);
      }

      count++;
    }
  }
  *tuple = Tuple{std::vector<Value>{{TypeId::INTEGER, count}}, &GetOutputSchema()};
  is_ok_ = true;
  return true;
}

}  // namespace bustub
