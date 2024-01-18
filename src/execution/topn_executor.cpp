#include "execution/executors/topn_executor.h"
#include <algorithm>
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  index_ = 0;

  Tuple tuple{};
  RID rid{};
  uint32_t count = 0;

  // when parent is join, may use this sorted_tuple many times
  if (sorted_tuples_.size() == plan_->GetN()) {
    return;
  }

  while (child_executor_->Next(&tuple, &rid)) {
    sorted_tuples_.emplace_back(std::move(tuple));
    count++;

    // every time insert elements count = 1000, sort
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
    while (sorted_tuples_.size() > plan_->GetN()) {
      sorted_tuples_.pop_back();
    }
    count = 0;
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::cout << GetNumInHeap() << std::endl;
  if (sorted_tuples_.empty()) {
    return false;
  }

  if (index_ >= sorted_tuples_.size()) {
    return false;
  }

  *tuple = sorted_tuples_[index_++];
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return sorted_tuples_.size(); }

// priority_queue
// TopNExecutor::Compare::Compare(const std::vector<std::pair<OrderByType, AbstractExpressionRef>> *order_bys,
//                                const Schema *schema)
//     : order_bys_(order_bys), schema_(schema) {}

// auto TopNExecutor::Compare::operator()(const Tuple &a, const Tuple &b) -> bool {
//   for (auto [order_by_type, expr] : *order_bys_) {
//     bool default_order_by = (order_by_type == OrderByType::DEFAULT || order_by_type == OrderByType::ASC);
//     if (expr->Evaluate(&a, *schema_).CompareLessThan(expr->Evaluate(&b, *schema_)) == CmpBool::CmpTrue) {
//       return default_order_by;
//     }
//     if (expr->Evaluate(&a, *schema_).CompareGreaterThan(expr->Evaluate(&b, *schema_)) == CmpBool::CmpTrue) {
//       return !default_order_by;
//     }
//   }
//   return true;
// }

}  // namespace bustub
