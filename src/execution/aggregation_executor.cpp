//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstdint>
#include <memory>
#include <vector>

#include "common/rid.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(child.release()),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  // Add this line
  child_->Init();
  Tuple child_tuple{};
  RID child_rid{};

  while (child_->Next(&child_tuple, &child_rid)) {
    std::vector<Value> values{};
    for (auto &expr : plan_->GetGroupBys()) {
      values.push_back(expr->Evaluate(&child_tuple, child_->GetOutputSchema()));
    }

    auto group_by = AggregateKey{values};

    values.clear();
    for (auto &expr : plan_->GetAggregates()) {
      values.push_back(expr->Evaluate(&child_tuple, child_->GetOutputSchema()));
    }
    auto aggregate = AggregateValue{values};
    aht_.InsertCombine(group_by, aggregate, false);
  }
  aht_iterator_ = aht_.Begin();
  if (aht_iterator_ == aht_.End()) {
    aht_.InsertCombine(AggregateKey{}, AggregateValue{}, true);
    aht_iterator_ = aht_.Begin();
    is_empty_ = true;
  }
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!plan_->GetGroupBys().empty() && is_empty_) {
    return false;
  }

  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  auto group = aht_iterator_.Key();
  auto aggre = aht_iterator_.Val();

  std::vector<Value> values{};
  values.reserve(plan_->GetGroupBys().size() + aggre.aggregates_.size());
  if (!plan_->GetGroupBys().empty()) {
    for (auto &gp : group.group_bys_) {
      values.emplace_back(std::move(gp));
    }
  }
  
  for (auto &agg : aggre.aggregates_) {
    values.emplace_back(std::move(agg));
  }
  
  *tuple = Tuple{values, &GetOutputSchema()};
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
