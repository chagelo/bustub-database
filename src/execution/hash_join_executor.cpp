//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <sys/types.h>
#include <cassert>
#include <cstdint>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/rid.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_child_(left_child.release()), right_child_(right_child.release()) {}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();

  HashJoinBuild();
  LeftNext();
  assert(GetOutputSchema().GetColumnCount() ==
         left_child_->GetOutputSchema().GetColumnCount() + right_child_->GetOutputSchema().GetColumnCount());
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (left_not_end_) {
    if (range_.first == range_.second) {
      if (plan_->GetJoinType() == JoinType::INNER) {
        LeftNext();
      } else {  // null
        ConstructOutPut(tuple, &left_tuple_, &left_child_->GetOutputSchema(), nullptr,
                        &right_child_->GetOutputSchema());
        LeftNext();
        return true;
      }
    } else {
      if (ht_iterator_ == range_.second) {
        LeftNext();
      } else {
        ConstructOutPut(tuple, &left_tuple_, &left_child_->GetOutputSchema(), &ht_iterator_->second,
                        &right_child_->GetOutputSchema());
        ++ht_iterator_;
        return true;
      }
    }
  }
  return false;
}

void HashJoinExecutor::ConstructOutPut(Tuple *tuple, Tuple *left_tuple, const Schema *left_schema, Tuple *right_tuple,
                                       const Schema *right_schema) {
  std::vector<Value> values{};
  values.reserve(left_schema->GetColumnCount() + right_schema->GetColumnCount());
  for (uint32_t i = 0; i < left_schema->GetColumnCount(); ++i) {
    values.emplace_back(left_tuple->GetValue(left_schema, i));
  }

  if (right_tuple != nullptr) {
    for (uint32_t i = 0; i < right_schema->GetColumnCount(); ++i) {
      values.emplace_back(right_tuple->GetValue(right_schema, i));
    }
  } else {
    for (uint32_t i = 0; i < right_schema->GetColumnCount(); ++i) {
      values.emplace_back(ValueFactory::GetNullValueByType(right_schema->GetColumn(i).GetType()));
    }
  }
  *tuple = Tuple{values, &GetOutputSchema()};
}

void HashJoinExecutor::HashJoinBuild() {
  Tuple tuple{};
  RID rid{};

  // Hash Join Buidl
  while (right_child_->Next(&tuple, &rid)) {
    std::vector<Value> values{};
    values.reserve(plan_->RightJoinKeyExpressions().size());
    for (auto &expr : plan_->RightJoinKeyExpressions()) {
      values.push_back(expr->Evaluate(&tuple, right_child_->GetOutputSchema()));
    }

    auto hash_key = HashKey{values};
    ht_.insert({hash_key, {tuple}});
  }
}

void HashJoinExecutor::LeftNext() {
  RID rid{};
  left_not_end_ = left_child_->Next(&left_tuple_, &rid);
  if (left_not_end_) {
    std::vector<Value> values{};
    values.reserve(plan_->LeftJoinKeyExpressions().size());

    for (auto &expr : plan_->LeftJoinKeyExpressions()) {
      values.push_back(expr->Evaluate(&left_tuple_, left_child_->GetOutputSchema()));
    }

    range_ = ht_.equal_range(HashKey{values});
    ht_iterator_ = range_.first;
  }
}

}  // namespace bustub
