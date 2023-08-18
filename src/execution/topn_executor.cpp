#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  // init child
  child_executor_->Init();
  // make comparator
  auto schema = child_executor_->GetOutputSchema();
  auto order_by_conditions = plan_->GetOrderBy();
  auto cmp = [schema, order_by_conditions](const Tuple &a, const Tuple &b) {
    for (const auto &order_by : order_by_conditions) {
      auto order_type = order_by.first;
      auto expression = order_by.second;

      // is a < b ?
      auto cmp_result = expression->Evaluate(&a, schema).CompareLessThan(expression->Evaluate(&b, schema));
      if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT || order_type == OrderByType::INVALID) {
        // is a < b
        if (cmp_result == CmpBool::CmpTrue) {
          return true;
        }
        // is a > b
        if (expression->Evaluate(&a, schema).CompareGreaterThan(expression->Evaluate(&b, schema)) == CmpBool::CmpTrue) {
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
  };
  Tuple child_tuple;
  RID child_rid;
  // init a queue
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> max_heap(cmp);
  unsigned size = plan_->GetN();
  // iterate overs all the tuples from child
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    max_heap.push(child_tuple);
    if (max_heap.size() > size) {
      max_heap.pop();
    }
  }
  // add to the final list
  while (!max_heap.empty()) {
    queue_.push_back(max_heap.top());
    max_heap.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!queue_.empty()) {
    auto elem = queue_.back();
    *tuple = elem;
    *rid = elem.GetRid();
    // remove elem from queue
    queue_.pop_back();
    return true;
  }
  return false;
}
}  // namespace bustub
