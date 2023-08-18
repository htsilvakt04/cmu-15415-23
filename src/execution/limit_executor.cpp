//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() { child_executor_->Init(); }

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple;
  RID child_rid;
  auto n = plan_->GetLimit();
  std::vector<Value> values{};
  while (n_ < n && child_executor_->Next(&child_tuple, &child_rid)) {
    // output the outer
    for (unsigned j = 0; j < child_executor_->GetOutputSchema().GetColumnCount(); ++j) {
      values.push_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), j));
    }
    *rid = child_rid;
    *tuple = Tuple{values, &GetOutputSchema()};
    n_++;
    return true;
  }

  return false;
}
}  // namespace bustub
