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
    : AbstractExecutor(exec_ctx), plan_(plan) {}

SeqScanExecutor::~SeqScanExecutor() { delete iter_; }

void SeqScanExecutor::Init() {
  auto metadata_catalog = exec_ctx_->GetCatalog();
  auto tableinfo = metadata_catalog->GetTable(plan_->GetTableOid());
  iter_ = new TableIterator(tableinfo->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  for (; !iter_->IsEnd();) {
    auto cur_tuple = iter_->GetTuple();
    if (!cur_tuple.first.is_deleted_) {
      *tuple = std::move(cur_tuple.second);
      *rid = iter_->GetRID();
      ++(*iter_);
      return true;
    }
    ++(*iter_);
  }
  return false;
}

}  // namespace bustub
