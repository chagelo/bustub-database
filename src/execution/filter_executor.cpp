#include "execution/executors/filter_executor.h"
#include "common/exception.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

FilterExecutor::FilterExecutor(ExecutorContext *exec_ctx, const FilterPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void FilterExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();
  is_false_ = Check(plan_->predicate_);
}

auto FilterExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_false_) {
    return false;
  }

  auto filter_expr = plan_->GetPredicate();

  while (true) {
    // Get the next tuple
    const auto status = child_executor_->Next(tuple, rid);

    if (!status) {
      return false;
    }

    auto value = filter_expr->Evaluate(tuple, child_executor_->GetOutputSchema());
    if (!value.IsNull() && value.GetAs<bool>()) {
      return true;
    }
  }
}

auto FilterExecutor::Check(const AbstractExpressionRef &expr) -> bool {
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
