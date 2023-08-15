//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(
                exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get())
                ->GetBeginIterator()) {}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto cat = exec_ctx_->GetCatalog();
  auto index = cat->GetIndex(plan_->GetIndexOid());
  auto table = cat->GetTable(index->table_name_);
  auto tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index->index_.get());

  if (iter_ == tree->GetEndIterator()) {
    return false;
  }

  *rid = (*iter_).second;
  // use this 'RID' to get the actual tuple in the table
  table->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  // advance
  ++iter_;

  return true;
}
}  // namespace bustub
