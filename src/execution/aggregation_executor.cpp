//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)) {}

void AggregationExecutor::Init() {
  // init the child
  child_->Init();
  /* Build the hash table 'aht_' */
  Tuple child_tuple;
  RID child_rid;
  // retrieve the tuples
  while (child_->Next(&child_tuple, &child_rid)) {
    // convert the tuple into keys & values
    auto k = MakeAggregateKey(&child_tuple);
    auto v = MakeAggregateValue(&child_tuple);
    // insert into the hash table
    aht_.InsertCombine(k, v);
  }  // end while

  // we need to support the case when there is no entries the in child AND
  // there is only 1 column output. That is: select count(*) from t1; when t1 is empty
  if(aht_.Size() == 0 && GetOutputSchema().GetColumnCount() == 1) {
    aht_.InsertEmpty();
  }

  // reset the iterator, this is critical.
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  std::vector<Value> values;
  // iterate over the hash table
  auto keys = aht_iterator_.Key().group_bys_;   // a list of group by keys
  auto vals = aht_iterator_.Val().aggregates_;  // a list of aggregates keys
  // the output [group_by column, aggregation column]
  values.reserve(keys.size() + vals.size());
  for (auto &key : keys) {
    values.emplace_back(key);
  }
  for (auto &val : vals) {
    values.emplace_back(val);
  }

  // output
  *tuple = Tuple{values, &GetOutputSchema()};
  // advance
  ++aht_iterator_;
  return true;
}
auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
