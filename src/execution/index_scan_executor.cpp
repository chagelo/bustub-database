//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <algorithm>

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_info_(exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())),
      table_info_(exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)),
      tree_(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get())),
      iter_(tree_->GetBeginIterator()),
      ed_(tree_->GetEndIterator()) {}

void IndexScanExecutor::Init() {
  iter_ = tree_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  for (; !iter_.IsEnd();) {
    *rid = (*iter_).second;
    ++iter_;
    auto [tuple_meta, tmp_tuple] = table_info_->table_->GetTuple(*rid);
    if (!tuple_meta.is_deleted_) {
      *tuple = tmp_tuple;
      return true;
    }
  }
  return false;
}

}  // namespace bustub