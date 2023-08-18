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
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"
namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple r_tup;
  RID r_rid;
  // fetch all the right tuples into memory [This is too much for the memory!!!]
  while (right_executor_->Next(&r_tup, &r_rid)) {
    right_tuples_.push_back(r_tup);
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }
  RID l_rid;
  std::vector<Value> values{};
  while ((right_idx_ != -1 && right_idx_ < static_cast<int>(right_tuples_.size())) ||
         left_executor_->Next(&left_tuple_, &l_rid)) {
    // [INNER JOIN]
    right_idx_ = (right_idx_ == -1 || right_idx_ >= static_cast<int>(right_tuples_.size())) ? 0 : right_idx_;
    // start from right_idx to the end
    for (unsigned i = right_idx_; i < right_tuples_.size(); ++i, ++right_idx_) {
      auto &r_tup = right_tuples_[i];
      if (!IsMatch(&left_tuple_, &r_tup)) {
        continue;
      }

      // turn on the flag
      found_ = true;
      // retrieve all columns from the left
      CollectColumns(&values, left_executor_, &left_tuple_);
      // retrieve all columns from the right
      CollectColumns(&values, right_executor_, &r_tup);
      // the output
      *tuple = Tuple{values, &GetOutputSchema()};
      // advance the pointers [because we return, so we need to manually increase this]
      right_idx_++;
      // [edge case] reach the end, we need to reset the found flag, otherwise
      // when the next call to Next() on this instance, we will fetch the next left_tuple
      // but the found flag still not yet reset.
      if (right_idx_ >= static_cast<int>(right_tuples_.size())) {
        found_ = false;
      }
      return true;
    }  // end for

    // [LEFT JOIN] Where the predicate doesn't match,
    // we need to include the left tuple and the right tuple as nulls
    if (plan_->GetJoinType() == JoinType::LEFT && !found_) {
      // retrieve all columns from the left
      CollectColumns(&values, left_executor_, &left_tuple_);
      // nulls for the right columns
      for (unsigned j = 0; j < right_executor_->GetOutputSchema().GetColumnCount(); ++j) {
        auto col = right_executor_->GetOutputSchema().GetColumn(j);
        values.push_back(ValueFactory::GetNullValueByType(col.GetType()));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      right_idx_ = -1;  // -1 means: we need to fetch the next left child
      found_ = false;
      return true;
    }

    // reset
    right_idx_ = -1;  // -1 means: we need to fetch the next left child
    found_ = false;
  }  // end while()

  done_ = true;
  return false;
}

auto NestedLoopJoinExecutor::IsMatch(const Tuple *left_tuple, const Tuple *right_tuple) -> bool {
  auto res = plan_->predicate_->EvaluateJoin(left_tuple, left_executor_->GetOutputSchema(), right_tuple,
                                             right_executor_->GetOutputSchema());

  return !res.IsNull() && res.GetAs<bool>();
}
void NestedLoopJoinExecutor::CollectColumns(std::vector<Value> *values, std::unique_ptr<AbstractExecutor> &executor,
                                            Tuple *tuple) {
  for (unsigned j = 0; j < executor->GetOutputSchema().GetColumnCount(); ++j) {
    values->push_back(tuple->GetValue(&executor->GetOutputSchema(), j));
  }
}
}  // namespace bustub
