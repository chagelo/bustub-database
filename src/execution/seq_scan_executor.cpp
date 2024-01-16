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
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan), iter_(exec_ctx_->GetCatalog()->GetTable(plan_->table_name_)->table_->MakeIterator()){}

void SeqScanExecutor::Init() {
  iter_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_name_)->table_->MakeIterator();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  for (; !iter_.IsEnd();) {
    auto cur_tuple = iter_.GetTuple();
    if (!cur_tuple.first.is_deleted_) {
      *tuple = std::move(cur_tuple.second);
      *rid = iter_.GetRID();
      ++iter_;
      return true;
    }
    ++iter_;
  }
  return false;
}

}  // namespace bustub
