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
#include <memory>
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  iter_ =
      std::make_shared<TableIterator>(exec_ctx_->GetCatalog()->GetTable(plan_->table_name_)->table_->MakeIterator());
  is_false_ = Check(plan_->filter_predicate_);
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_false_) {
    return false;
  }

  for (; !iter_->IsEnd();) {
    auto cur_tuple = iter_->GetTuple();
    if (!cur_tuple.first.is_deleted_) {
      if (plan_->filter_predicate_ == nullptr) {
        *tuple = std::move(cur_tuple.second);
        *rid = iter_->GetRID();
        ++(*iter_);
        return true;
      }

      // filter seqscan

      auto value = plan_->filter_predicate_->Evaluate(&cur_tuple.second, plan_->OutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        *tuple = std::move(cur_tuple.second);
        *rid = iter_->GetRID();
        ++(*iter_);
        return true;
      }
    }
    ++(*iter_);
  }
  return false;
}

auto SeqScanExecutor::Check(const AbstractExpressionRef &expr) -> bool {
  if (expr == nullptr) {
    return false;
  }

  for (auto &child : expr->GetChildren()) {
    auto is_false = Check(child);
    if (is_false) {
      return true;
    }
  }
  if (const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(expr.get()); comp_expr != nullptr) {
    if (const auto *left_expr = dynamic_cast<const ConstantValueExpression *>(comp_expr->children_[0].get());
        left_expr != nullptr) {
      if (const auto *right_expr = dynamic_cast<const ConstantValueExpression *>(comp_expr->children_[1].get());
          right_expr != nullptr) {
        if (comp_expr->comp_type_ == ComparisonType::Equal) {
          if (left_expr->val_.CompareEquals(right_expr->val_) == CmpBool::CmpFalse) {
            return true;
          }
        } else if (comp_expr->comp_type_ == ComparisonType::NotEqual) {
          if (left_expr->val_.CompareNotEquals(right_expr->val_) == CmpBool::CmpFalse) {
            return true;
          }
        } else if (comp_expr->comp_type_ == ComparisonType::LessThan) {
          if (left_expr->val_.CompareLessThan(right_expr->val_) == CmpBool::CmpFalse) {
            return true;
          }
        }
        if (comp_expr->comp_type_ == ComparisonType::LessThanOrEqual) {
          if (left_expr->val_.CompareLessThanEquals(right_expr->val_) == CmpBool::CmpFalse) {
            return true;
          }
        } else if (comp_expr->comp_type_ == ComparisonType::GreaterThan) {
          if (left_expr->val_.CompareGreaterThan(right_expr->val_) == CmpBool::CmpFalse) {
            return true;
          }
        } else if (comp_expr->comp_type_ == ComparisonType::GreaterThanOrEqual) {
          if (left_expr->val_.CompareGreaterThanEquals(right_expr->val_) == CmpBool::CmpFalse) {
            return true;
          }
        }
      }
    }
  }
  return false;
}
}  // namespace bustub
