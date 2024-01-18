#include "execution/executors/sort_executor.h"
#include <algorithm>
#include "binder/bound_order_by.h"
#include "execution/expressions/comparison_expression.h"
#include "storage/table/tuple.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {}

void SortExecutor::Init() {
  child_executor_->Init();
  index_ = 0;
  Tuple tuple{};
  RID rid{};

  if (!sorted_tuples_.empty()) {
    return;    
  }

  while (child_executor_->Next(&tuple, &rid)) {
    sorted_tuples_.emplace_back(std::move(tuple));
  }
  if (sorted_tuples_.empty()) {
    return;
  }

  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), [this](const Tuple &a, const Tuple &b) {
    for (auto [order_by_type, expr] : plan_->GetOrderBy()) {
      bool default_order_by = (order_by_type == OrderByType::DEFAULT || order_by_type == OrderByType::ASC);
      if (expr->Evaluate(&a, child_executor_->GetOutputSchema())
              .CompareLessThan(expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
        return default_order_by;
      }
      if (expr->Evaluate(&a, child_executor_->GetOutputSchema())
              .CompareGreaterThan(expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
        return !default_order_by;
      }
    }
    return true;
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sorted_tuples_.empty()) {
    return false;
  }

  if (index_ == sorted_tuples_.size()) {
    return false;
  }

  *tuple = sorted_tuples_[index_++];
  return true;
}

}  // namespace bustub
