#include <algorithm>
#include <cstdint>
#include <memory>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    // Check if expr is equal condition where one is for the left table, and one is for the right table.
    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->comp_type_ == ComparisonType::Equal) {
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
              right_expr != nullptr) {
            auto left_schema = nlj_plan.GetLeftPlan()->OutputSchema();
            auto right_schema = nlj_plan.GetRightPlan()->OutputSchema();

            auto left_expr_tuple = std::make_shared<ColumnValueExpression>(
                left_expr->GetTupleIdx(), left_expr->GetColIdx(), left_expr->GetReturnType());
            auto right_expr_tuple = std::make_shared<ColumnValueExpression>(
                right_expr->GetTupleIdx(), right_expr->GetColIdx(), right_expr->GetReturnType());

            std::vector<AbstractExpressionRef> left_expres{};
            std::vector<AbstractExpressionRef> right_expres{};
            if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
              if (left_schema.GetColumn(left_expr->GetColIdx()).GetType() ==
                  right_schema.GetColumn(right_expr->GetColIdx()).GetType()) {
                left_expres.push_back(AbstractExpressionRef{std::move(left_expr_tuple)});
                right_expres.push_back(AbstractExpressionRef{std::move(right_expr_tuple)});
              }
            } else if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
              if (left_schema.GetColumn(right_expr->GetColIdx()).GetType() ==
                  right_schema.GetColumn(left_expr->GetColIdx()).GetType()) {
                left_expres.push_back(AbstractExpressionRef{std::move(right_expr_tuple)});
                right_expres.push_back(AbstractExpressionRef{std::move(left_expr_tuple)});
              }
            } else {
              return optimized_plan;
            }

            return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                      nlj_plan.GetRightPlan(), std::move(left_expres),
                                                      std::move(right_expres), nlj_plan.GetJoinType());
          }
        }
      }
    } else if (const auto *expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->logic_type_ == LogicType::And) {
        std::vector<AbstractExpressionRef> left_expres{};
        std::vector<AbstractExpressionRef> right_expres{};
        for (int i = 0; i < 2; ++i) {
          const auto *cexpr = dynamic_cast<const ComparisonExpression *>(expr->children_[i].get());
          if (cexpr == nullptr || cexpr->comp_type_ != ComparisonType::Equal) {
            return optimized_plan;
          }
          const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(cexpr->children_[0].get());
          if (left_expr == nullptr) {
            return optimized_plan;
          }

          const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(cexpr->children_[1].get());
          if (right_expr == nullptr) {
            return optimized_plan;
          }

          auto left_schema = nlj_plan.GetLeftPlan()->OutputSchema();
          auto right_schema = nlj_plan.GetRightPlan()->OutputSchema();

          auto left_expr_tuple = std::make_shared<ColumnValueExpression>(
              left_expr->GetTupleIdx(), left_expr->GetColIdx(), left_expr->GetReturnType());
          auto right_expr_tuple = std::make_shared<ColumnValueExpression>(
              right_expr->GetTupleIdx(), right_expr->GetColIdx(), right_expr->GetReturnType());

          if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
            if (left_schema.GetColumn(left_expr->GetColIdx()).GetType() ==
                right_schema.GetColumn(right_expr->GetColIdx()).GetType()) {
              left_expres.push_back(AbstractExpressionRef{std::move(left_expr_tuple)});
              right_expres.push_back(AbstractExpressionRef{std::move(right_expr_tuple)});
            }
          } else if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
            if (left_schema.GetColumn(right_expr->GetColIdx()).GetType() ==
                right_schema.GetColumn(left_expr->GetColIdx()).GetType()) {
              left_expres.push_back(AbstractExpressionRef{std::move(right_expr_tuple)});
              right_expres.push_back(AbstractExpressionRef{std::move(left_expr_tuple)});
            }
          } else {
            return optimized_plan;
          }
        }

        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), left_expres, right_expres,
                                                  nlj_plan.GetJoinType());
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
