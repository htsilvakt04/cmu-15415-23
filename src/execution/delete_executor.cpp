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

void DeleteExecutor::LockTable() {
  auto catalog = exec_ctx_->GetCatalog();
  auto oid = catalog->GetTable(plan_->TableOid())->oid_;
  auto txn = exec_ctx_->GetTransaction();
  // acquire the table lock in IS mode
  try {
    // READ_UNCOMMITTED is allowed to take only X/IX locks
    if(txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      // if not yet hold the lock on the table
      if (!txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableExclusiveLocked(oid)) {
        bool res = exec_ctx_->GetLockManager()->LockTable(
            txn, LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
        if (!res) {
          throw ExecutionException("DeleteExecutor failed to acquire table lock.");
        }
      }
    }
    else {
      // if not yet hold SIX, then acquire IX
      if (!txn->IsTableSharedIntentionExclusiveLocked(oid)) {
        bool res = exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
        if (!res) {
          throw ExecutionException("DeleteExecutor failed to acquire table lock.");
        }
      }
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("DeleteExecutor failed to acquire table lock " + e.GetInfo());
  }
}

void DeleteExecutor::LockRow(RID rid) {
  auto catalog = exec_ctx_->GetCatalog();
  auto table = catalog->GetTable(plan_->TableOid());
  auto oid = table->oid_;

  try {
    bool res = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(),
                                                    LockManager::LockMode::EXCLUSIVE, oid,
                                                    rid);
    if (!res) {
      throw ExecutionException("DeleteExecutor failed to acquire row lock.");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("DeleteExecutor failed to acquire row lock " + e.GetInfo());
  }
}
void DeleteExecutor::Init() {
  child_executor_->Init();
  LockTable();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }

  int c = 0;
  auto cat = exec_ctx_->GetCatalog();
  auto table = cat->GetTable(plan_->TableOid());
  auto txn = exec_ctx_->GetTransaction();
  Tuple child_tuple{};
  RID child_rid;
  while (true) {
    const auto status = child_executor_->Next(&child_tuple, &child_rid);
    if (!status) {
      break;
    }

    // mark as delete
    table->table_->MarkDelete(child_rid, exec_ctx_->GetTransaction());
    LockRow(child_rid);
    // modify the indexes
    for (auto &index : cat->GetTableIndexes(table->name_)) {
      index->index_->DeleteEntry(
          child_tuple.KeyFromTuple(table->schema_, index->key_schema_, index->index_->GetKeyAttrs()), child_rid,
          exec_ctx_->GetTransaction());
      // add into the index write set
      txn->GetIndexWriteSet()->
          emplace_back(child_rid, table->oid_, WType::DELETE, child_tuple, index->index_oid_, cat);
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
