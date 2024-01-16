//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstdint>
#include <utility>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(left_executor.release()),
      right_executor_(right_executor.release()) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  RID rid{};
  outer_not_end_ = left_executor_->Next(&left_tuple_, &rid);
  inner_not_end_ = right_executor_->Next(&right_tuple_, &rid);
  // std::cout << plan_->OutputSchema().ToString() << std::endl;
  std::cout << plan_->ToString() << std::endl;
  // std::cout << left_tuple_.ToString(&left_executor_->GetOutputSchema()) << " "
  //           << right_tuple_.ToString(&right_executor_->GetOutputSchema()) << std::endl;
  // std::cout << "hello world" << std::endl;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (outer_not_end_ || inner_not_end_) {
    // if (outer_not_end_) {
    //   std::cout << left_tuple_.ToString(&left_executor_->GetOutputSchema()) << " left_tuple \n";
    // }
    // if (inner_not_end_) {
    //   std::cout << right_tuple_.ToString(&right_executor_->GetOutputSchema()) << " right_tuple \n";
    // }
    if (outer_not_end_ && inner_not_end_ &&
        (plan_->Predicate() == nullptr || plan_->Predicate()
                                              ->EvaluateJoin(&left_tuple_, plan_->GetLeftPlan()->OutputSchema(),
                                                             &right_tuple_, plan_->GetRightPlan()->OutputSchema())
                                              .GetAs<bool>())) {
      // std::cout << left_tuple_.ToString(&left_executor_->GetOutputSchema()) << "world"
                // << right_tuple_.ToString(&right_executor_->GetOutputSchema()) << std::endl;

      // construct tuple
      std::vector<Value> values{};
      auto left_shema = left_executor_->GetOutputSchema();
      auto right_shema = right_executor_->GetOutputSchema();
      values.reserve(left_shema.GetColumnCount() + right_shema.GetColumnCount());
      
      
      for (uint32_t i = 0; i < left_shema.GetColumnCount(); ++i) {
        values.emplace_back(left_tuple_.GetValue(&left_shema, i));
      }


      // std::cout << left_tuple_.ToString(&left_shema) << std::endl;
      for (uint32_t i = 0; i < right_shema.GetColumnCount(); ++i) {
        values.emplace_back(right_tuple_.GetValue(&right_shema, i));
      }
      *tuple = Tuple{values, &plan_->OutputSchema()};
      find_ = true;
      inner_not_end_ = right_executor_->Next(&right_tuple_, rid);
      return true;
    }
    if (inner_not_end_) {
      inner_not_end_ = right_executor_->Next(&right_tuple_, rid);
    } else {
      // outer not end, inner end, find no the matched inner tuple, return null
      if (!find_ && plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values{};
        auto left_shema = left_executor_->GetOutputSchema();
        auto right_shema = right_executor_->GetOutputSchema();
        values.reserve(left_shema.GetColumnCount() + left_shema.GetColumnCount());

        for (uint32_t i = 0; i < left_shema.GetColumnCount(); ++i) {
          values.emplace_back(left_tuple_.GetValue(&left_shema, i));
        }
        // all null
        for (auto &col : right_shema.GetColumns()) {
          values.emplace_back(ValueFactory::GetNullValueByType(col.GetType()));
        }
        *tuple = Tuple{values, &plan_->OutputSchema()};

        outer_not_end_ = left_executor_->Next(&left_tuple_, rid);
        if (outer_not_end_) {
          right_executor_->Init();
          inner_not_end_ = right_executor_->Next(&right_tuple_, rid);
        }
        return true;
      }

      outer_not_end_ = left_executor_->Next(&left_tuple_, rid);
      find_ = false;
      if (outer_not_end_) {
        right_executor_->Init();
        inner_not_end_ = right_executor_->Next(&right_tuple_, rid);
      }
    }
  }
  return false;
}

}  // namespace bustub
