#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  // fetch all the child tuples (with fit in memory assumption)
  while (child_executor_->Next(&tuple, &rid)) {
    child_tuples_.push_back(tuple);
  }
  // sort them
  auto schema = child_executor_->GetOutputSchema();
  auto order_by_conditions = plan_->GetOrderBy();
  std::sort(child_tuples_.begin(), child_tuples_.end(), [schema, order_by_conditions](const Tuple &a, const Tuple &b) {
    // for each two tuples, consider a set of comparisons
    for (const auto &order_by : order_by_conditions) {
      auto order_type = order_by.first;
      auto expression = order_by.second;

      // is a < b ?
      auto cmp_result = expression->Evaluate(&a, schema).CompareLessThan(expression->Evaluate(&b, schema));
      if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
        // is a < b
        if (cmp_result == CmpBool::CmpTrue) {
          return true;
        }
        // is a > b
        if (expression->Evaluate(&b, schema).CompareLessThan(expression->Evaluate(&a, schema)) == CmpBool::CmpTrue) {
          return false;
        }
      } else if (order_type == OrderByType::DESC) {
        // is a > b
        if (expression->Evaluate(&a, schema).CompareGreaterThan(expression->Evaluate(&b, schema)) == CmpBool::CmpTrue) {
          return true;
        }
        // is a < b
        if (cmp_result == CmpBool::CmpTrue) {
          return false;
        }
      }
    }              // end for
    return false;  // Default case if all comparisons are equal
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (idx_ < child_tuples_.size()) {
    std::vector<Value> values;
    for (unsigned i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
      values.push_back(child_tuples_[idx_].GetValue(&child_executor_->GetOutputSchema(), i));
    }
    *tuple = Tuple{values, &GetOutputSchema()};
    idx_++;
    return true;
  }
  return false;
}
}  // namespace bustub
