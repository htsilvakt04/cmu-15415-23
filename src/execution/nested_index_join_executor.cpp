//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"
namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }

  auto cat = exec_ctx_->GetCatalog();
  // get the index of the inner table (in the plan_)
  auto index = cat->GetIndex(plan_->GetIndexOid());
  auto table = cat->GetTable(index->table_name_);
  std::vector<Value> values{};
  Tuple outer_tuple;
  Tuple inner_tuple;
  RID outer_rid;
  std::vector<RID> rids;
  auto tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index->index_.get());

  // for each tuple from the outer table (the child_executor_)
  while (child_executor_->Next(&outer_tuple, &outer_rid)) {
    auto index_probe_key = plan_->KeyPredicate()->Evaluate(&outer_tuple, child_executor_->GetOutputSchema());
    // probe this tuple into the index to retrieve the corresponding inner row RID
    tree->ScanKey(Tuple{{index_probe_key}, index->index_->GetKeySchema()}, &rids, exec_ctx_->GetTransaction());

    if (!rids.empty()) {
      // retrieve the inner tuple from the table
      table->table_->GetTuple(rids[0], &inner_tuple, exec_ctx_->GetTransaction());
      // output the outer
      for (unsigned j = 0; j < child_executor_->GetOutputSchema().GetColumnCount(); ++j) {
        values.push_back(outer_tuple.GetValue(&child_executor_->GetOutputSchema(), j));
      }
      // output the inner
      for (unsigned j = 0; j < plan_->InnerTableSchema().GetColumnCount(); ++j) {
        values.push_back(inner_tuple.GetValue(&plan_->InnerTableSchema(), j));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
    // [LEFT JOIN]
    if (plan_->GetJoinType() == JoinType::LEFT) {
      // output the outer
      for (unsigned j = 0; j < child_executor_->GetOutputSchema().GetColumnCount(); ++j) {
        values.push_back(outer_tuple.GetValue(&child_executor_->GetOutputSchema(), j));
      }
      // output the inner as nulls
      for (unsigned j = 0; j < plan_->InnerTableSchema().GetColumnCount(); ++j) {
        auto col = plan_->InnerTableSchema().GetColumn(j);
        values.push_back(ValueFactory::GetNullValueByType(col.GetType()));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
  }  // end while()

  done_ = true;
  return false;
}
}  // namespace bustub
