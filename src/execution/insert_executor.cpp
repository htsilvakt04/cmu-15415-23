//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}


void InsertExecutor::LockTable() {
  auto catalog = exec_ctx_->GetCatalog();
  auto oid = catalog->GetTable(plan_->TableOid())->oid_;
  auto txn = exec_ctx_->GetTransaction();
  // acquire the table lock in IS mode
  try {
    // READ_UNCOMMITTED is allowed to take only X/IX locks
    if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      // if not yet hold the lock on the table
      if (!txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableExclusiveLocked(oid)) {
        bool res = exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
        if (!res) {
          throw ExecutionException("InsertExecutor failed to acquire table lock.");
        }
      }
    }
    else {
      // if not yet hold SIX, then acquire IX
      if (!txn->IsTableSharedIntentionExclusiveLocked(oid)) {
        bool res = exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
        if (!res) {
          throw ExecutionException("InsertExecutor failed to acquire table lock.");
        }
      }
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("InsertExecutor failed to acquire table lock " + e.GetInfo());
  }
}

void InsertExecutor::LockRow(RID rid) {
  auto catalog = exec_ctx_->GetCatalog();
  auto table = catalog->GetTable(plan_->TableOid());
  auto oid = table->oid_;

  try {
    bool res = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(),
                                                    LockManager::LockMode::EXCLUSIVE, oid,
                                                    rid);
    if (!res) {
      throw ExecutionException("InsertExecutor failed to acquire row lock.");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("InsertExecutor failed to acquire row lock " + e.GetInfo());
  }
}
void InsertExecutor::Init() {
  child_executor_->Init();
  LockTable();
}
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }
  int c = 0;
  auto cat = exec_ctx_->GetCatalog();
  auto table = cat->GetTable(plan_->TableOid());
  auto txn = exec_ctx_->GetTransaction();
  // get child tuple
  Tuple child_tuple;
  RID child_rid;
  while (true) {
    const auto status = child_executor_->Next(&child_tuple, &child_rid);
    if (!status) {
      break;
    }

    // insert into table [already record the write set for table]
    if (table->table_->InsertTuple(child_tuple, &child_rid, exec_ctx_->GetTransaction())) {
      // after create a tuple, we need to hold its lock
      LockRow(child_rid);
      // insert into index
      for (auto &index : cat->GetTableIndexes(table->name_)) {
        index->index_->InsertEntry(
            child_tuple.KeyFromTuple(table->schema_, index->key_schema_, index->index_->GetKeyAttrs()), child_rid,
            exec_ctx_->GetTransaction());
        // add into the index write set
        txn->GetIndexWriteSet()->
            emplace_back(child_rid, table->oid_, WType::INSERT, child_tuple, index->index_oid_, cat);
      }
      // increment the count
      c++;
    }
  }  // end while

  // report back the # of inserted rows
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, c);
  *tuple = Tuple{values, &GetOutputSchema()};
  done_ = true;
  return true;
}
}  // namespace bustub
