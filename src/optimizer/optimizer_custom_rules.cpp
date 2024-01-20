#include <memory>
#include "common/macros.h"
#include "common/util/string_util.h"
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
#include "optimizer/optimizer_internal.h"

// Note for 2023 Spring: You can add all optimizer rule implementations and apply the rules as you want in this file.
// Note that for some test cases, we force using starter rules, so that the configuration here won't take effects.
// Starter rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);
  // p = OptimizeMergeFilterNLJ(p);
  p = OptimizePushDownFilter(p);
  p = OptimizeMergeFilterScan(p);
  p = OptimizeNLJAsHashJoin(p);
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  return p;
}

auto Optimizer::RewriteExpressionForFilter(const AbstractExpressionRef &expr) -> std::vector<AbstractExpressionRef> {
  std::vector<std::vector<AbstractExpressionRef>> total{{}, {}, {}};

  for (const auto &child : expr->GetChildren()) {
    auto childrens = RewriteExpressionForFilter(child);
    for (int i = 0; i < 3; ++i) {
      if (childrens[i] != nullptr) {
        total[i].emplace_back(childrens[i]);
      }
    }
  }

  if (const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(expr.get()); comp_expr != nullptr) {
    BUSTUB_ENSURE(comp_expr->GetChildren().size() == 2, "comparsion expression must have two childen");
    if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(comp_expr->children_[0].get());
        left_expr != nullptr) {
      if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(comp_expr->children_[1].get());
          right_expr != nullptr) {
        if ((left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) ||
            (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0)) {
          total[2].emplace_back(expr);
        } else if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 0) {
          total[0].emplace_back(expr);
        } else if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 1) {
          total[1].emplace_back(expr);
        }
      } else if (const auto *right_expr = dynamic_cast<const ConstantValueExpression *>(comp_expr->children_[1].get());
                 right_expr != nullptr) {
        if (left_expr->GetTupleIdx() == 0) {
          total[0].emplace_back(expr);
        } else if (left_expr->GetTupleIdx() == 1) {
          total[1].emplace_back(expr);
        }
      }
    } else if (const auto *left_expr = dynamic_cast<const ConstantValueExpression *>(comp_expr->children_[0].get());
               left_expr != nullptr) {
      if (const auto *right_expr = dynamic_cast<const ConstantValueExpression *>(comp_expr->children_[1].get());
          right_expr != nullptr) {
        for (int i = 0; i < 3; ++i) {
          if (comp_expr->comp_type_ == ComparisonType::Equal) {
            if (left_expr->val_.CompareEquals(right_expr->val_) == CmpBool::CmpFalse) {
              total[i].emplace_back(
                  std::make_shared<ConstantValueExpression>(ValueFactory::GetNullValueByType(TypeId::INTEGER)));
            }
          } else if (comp_expr->comp_type_ == ComparisonType::NotEqual) {
            if (left_expr->val_.CompareNotEquals(right_expr->val_) == CmpBool::CmpFalse) {
              total[i].emplace_back(
                  std::make_shared<ConstantValueExpression>(ValueFactory::GetNullValueByType(TypeId::INTEGER)));
            }
          } else if (comp_expr->comp_type_ == ComparisonType::LessThan) {
            if (left_expr->val_.CompareLessThan(right_expr->val_) == CmpBool::CmpFalse) {
              total[i].emplace_back(
                  std::make_shared<ConstantValueExpression>(ValueFactory::GetNullValueByType(TypeId::INTEGER)));
            }
          }
          if (comp_expr->comp_type_ == ComparisonType::LessThanOrEqual) {
            if (left_expr->val_.CompareLessThanEquals(right_expr->val_) == CmpBool::CmpFalse) {
              total[i].emplace_back(
                  std::make_shared<ConstantValueExpression>(ValueFactory::GetNullValueByType(TypeId::INTEGER)));
            }
          } else if (comp_expr->comp_type_ == ComparisonType::GreaterThan) {
            if (left_expr->val_.CompareGreaterThan(right_expr->val_) == CmpBool::CmpFalse) {
              total[i].emplace_back(
                  std::make_shared<ConstantValueExpression>(ValueFactory::GetNullValueByType(TypeId::INTEGER)));
            }
          } else if (comp_expr->comp_type_ == ComparisonType::GreaterThanOrEqual) {
            if (left_expr->val_.CompareGreaterThanEquals(right_expr->val_) == CmpBool::CmpFalse) {
              total[i].emplace_back(
                  std::make_shared<ConstantValueExpression>(ValueFactory::GetNullValueByType(TypeId::INTEGER)));
            }
          }
        }
      } else if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(comp_expr->children_[1].get());
                 right_expr != nullptr) {
        if (right_expr->GetTupleIdx() == 0) {
          total[0].emplace_back(expr);
        } else if (right_expr->GetTupleIdx() == 1) {
          total[1].emplace_back(expr);
        }
      }
    } else {
      total[2].emplace_back(expr);
    }
  }

  std::vector<AbstractExpressionRef> res;
  for (int i = 0; i < 3; ++i) {
    if (total[i].empty()) {
      res.emplace_back(nullptr);
    } else if (total[i].size() == 1) {
      res.emplace_back(total[i][0]);
    } else if (total[i].size() >= 2) {
      // check is there any const compare const comaprsion is always false
      AbstractExpressionRef false_expr;
      for (auto &total_expr : total[i]) {
        if (const auto *c_expr = dynamic_cast<const ConstantValueExpression *>(total_expr.get()); c_expr != nullptr) {
          if (c_expr->val_.CompareEquals(ValueFactory::GetNullValueByType(TypeId::INTEGER)) == CmpBool::CmpTrue) {
            false_expr = total_expr;
            break;
          }
        }
      }

      if (false_expr != nullptr) {
        res.emplace_back(false_expr);
        continue;
      }

      auto temp = std::make_shared<LogicExpression>(total[i][0], total[i][1], LogicType::And);
      AbstractExpressionRef log_combine = temp;
      for (uint32_t j = 2; j < total[i].size(); ++j) {
        log_combine = std::make_shared<LogicExpression>(log_combine, total[i][j], LogicType::And);
      }
      res.emplace_back(log_combine);
    }
  }

  return res;
}

auto Optimizer::RewriteExpressionForFilterProjection(const AbstractExpressionRef &plan,
                                                     const ProjectionPlanNode &proj_plan) -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(RewriteExpressionForFilterProjection(child, proj_plan));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(plan.get());
      column_value_expr != nullptr) {
    BUSTUB_ENSURE(column_value_expr->GetTupleIdx() == 0, "tuple index must be 0");
    auto col_idx = column_value_expr->GetColIdx();
    return proj_plan.GetExpressions()[col_idx];
  }
  return optimized_plan;
}

auto Optimizer::OptimizePushDownFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  AbstractPlanNodeRef down_child;
  bool ok = false;
  if (plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*plan);
    // Has exactly one child
    BUSTUB_ENSURE(plan->children_.size() == 1, "Filter with multiple children?? Impossible!");
    // filter, projection -> projection, filter
    const auto &child_plan = plan->GetChildAt(0);
    if (child_plan->GetType() == PlanType::Projection) {
      const auto &proj_plan = dynamic_cast<const ProjectionPlanNode &>(*child_plan);
      BUSTUB_ENSURE(child_plan->children_.size() == 1, "Projection with multiple children?? Impossible!");

      auto new_filter_predicte = RewriteExpressionForFilterProjection(filter_plan.GetPredicate(), proj_plan);
      auto new_filter_plan =
          std::make_shared<FilterPlanNode>(plan->output_schema_, new_filter_predicte, proj_plan.GetChildPlan());
      down_child =
          std::make_shared<ProjectionPlanNode>(proj_plan.output_schema_, proj_plan.GetExpressions(), new_filter_plan);
      ok = true;
    }

    if (child_plan->GetType() == PlanType::NestedLoopJoin) {
      const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*child_plan);
      BUSTUB_ENSURE(child_plan->children_.size() == 2, "NestedLoopJoin has more than two children?? Impossible!");
      std::vector<AbstractPlanNodeRef> vector;
      auto new_expr =
          RewriteExpressionForJoin(filter_plan.GetPredicate(), nlj_plan.GetLeftPlan()->OutputSchema().GetColumnCount(),
                                   nlj_plan.GetRightPlan()->OutputSchema().GetColumnCount());
      auto expres = RewriteExpressionForFilter(new_expr);

      AbstractPlanNodeRef left_child = nlj_plan.GetLeftPlan();
      AbstractPlanNodeRef right_child = nlj_plan.GetRightPlan();
      AbstractExpressionRef predicte = nlj_plan.Predicate();
      if (expres[0] != nullptr) {
        left_child =
            std::make_shared<FilterPlanNode>(nlj_plan.GetLeftPlan()->output_schema_, expres[0], nlj_plan.GetLeftPlan());
      }

      if (expres[1] != nullptr) {
        right_child = std::make_shared<FilterPlanNode>(nlj_plan.GetRightPlan()->output_schema_, expres[1],
                                                       nlj_plan.GetRightPlan());
      }

      if (expres[2] != nullptr) {
        predicte = std::make_shared<LogicExpression>(predicte, expres[2], LogicType::And);
      }

      down_child = std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, left_child, right_child, predicte,
                                                            nlj_plan.GetJoinType());
      ok = true;
    }

    if (child_plan->GetType() == PlanType::HashJoin) {
      const auto &hj_plan = dynamic_cast<const HashJoinPlanNode &>(*child_plan);
      BUSTUB_ENSURE(child_plan->children_.size() == 2, "HashJoin has more than two children?? Impossible!");
      auto new_expr =
          RewriteExpressionForJoin(filter_plan.GetPredicate(), hj_plan.GetLeftPlan()->OutputSchema().GetColumnCount(),
                                   hj_plan.GetRightPlan()->OutputSchema().GetColumnCount());
      auto expres = RewriteExpressionForFilter(new_expr);

      AbstractPlanNodeRef left_child = hj_plan.GetLeftPlan();
      AbstractPlanNodeRef right_child = hj_plan.GetRightPlan();
      if (expres[0] != nullptr) {
        left_child =
            std::make_shared<FilterPlanNode>(hj_plan.GetLeftPlan()->output_schema_, expres[0], hj_plan.GetLeftPlan());
      }

      if (expres[1] != nullptr) {
        right_child =
            std::make_shared<FilterPlanNode>(hj_plan.GetRightPlan()->output_schema_, expres[1], hj_plan.GetRightPlan());
      }
      down_child = std::make_shared<HashJoinPlanNode>(hj_plan.output_schema_, left_child, right_child,
                                                      hj_plan.LeftJoinKeyExpressions(),
                                                      hj_plan.RightJoinKeyExpressions(), hj_plan.GetJoinType());
      ok = true;
    }

    // if (child_plan->GetType() == PlanType::MockScan) {
    //   const auto &mc_plan = dynamic_cast<const MockScanPlanNode &>(*child_plan);
    //   down_child =
    //       std::make_shared<SeqScanPlanNode>(mc_plan.output_schema_, -1, mc_plan.GetTable(),
    //       filter_plan.GetPredicate());
    //   ok = true;
    // }

    if (child_plan->GetType() == PlanType::Filter) {
      const auto &child_fl_plan = dynamic_cast<const FilterPlanNode &>(*child_plan);
      AbstractExpressionRef new_predicate =
          std::make_shared<LogicExpression>(child_fl_plan.GetPredicate(), filter_plan.GetPredicate(), LogicType::And);
      down_child =
          std::make_shared<FilterPlanNode>(child_fl_plan.output_schema_, new_predicate, child_fl_plan.GetChildPlan());
      ok = true;
    }
  }

  if (ok) {
    std::vector<AbstractPlanNodeRef> children;
    for (const auto &child : down_child->GetChildren()) {
      children.emplace_back(OptimizePushDownFilter(child));
    }

    return down_child->CloneWithChildren(std::move(children));
  }

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizePushDownFilter(child));
  }

  if (ok) {
    return down_child;
  }

  return plan->CloneWithChildren(std::move(children));
}

}  // namespace bustub
