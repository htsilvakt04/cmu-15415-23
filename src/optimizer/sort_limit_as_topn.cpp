#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"
namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    const auto &limit = limit_plan.GetLimit();
    auto child = limit_plan.GetChildAt(0);
    BUSTUB_ASSERT(limit_plan.GetChildren().size() == 1, "Limit plan must have only 1 child.");
    if (child->GetType() == PlanType::Sort) {
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*child);
      const auto &order_bys = sort_plan.GetOrderBy();
      // convert the plan to top-n
      return std::make_shared<TopNPlanNode>(limit_plan.output_schema_, child, order_bys, limit);
    }
  }

  return optimized_plan;
}

}  // namespace bustub
