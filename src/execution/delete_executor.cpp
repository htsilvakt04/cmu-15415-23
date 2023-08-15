//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }

  int c = 0;
  auto cat = exec_ctx_->GetCatalog();
  auto table = cat->GetTable(plan_->TableOid());
  Tuple child_tuple{};
  RID child_rid;
  while (true) {
    const auto status = child_executor_->Next(&child_tuple, &child_rid);
    if (!status) {
      break;
    }

    // mark as delete
    table->table_->MarkDelete(child_rid, exec_ctx_->GetTransaction());
    // modify the indexes
    for (auto &index : cat->GetTableIndexes(table->name_)) {
      index->index_->DeleteEntry(
          child_tuple.KeyFromTuple(table->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
          child_rid,
          exec_ctx_->GetTransaction());
    }
    // increment the count
    c++;
  }  // end while

  // report back # of deleted rows
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, c);
  *tuple = Tuple{values, &GetOutputSchema()};
  done_ = true;
  return true;
}
}  // namespace bustub
