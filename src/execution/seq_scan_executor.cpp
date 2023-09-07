//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::LockTable() {
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    auto catalog = exec_ctx_->GetCatalog();
    auto oid = catalog->GetTable(plan_->GetTableOid())->oid_;
    auto txn = exec_ctx_->GetTransaction();
    // acquire the table lock in IS mode
    try {
      // if not yet hold the lock on the table
      if (!txn->IsTableIntentionSharedLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid)) {
        bool res = exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, oid);
        if (!res) {
          throw ExecutionException("SeqScanExecutor failed to acquire table lock.");
        }
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("SeqScanExecutor failed to acquire table lock " + e.GetInfo());
    }
  }
}
void SeqScanExecutor::LockRow() {
  auto catalog = exec_ctx_->GetCatalog();
  auto table = catalog->GetTable(plan_->GetTableOid());
  auto oid = table->oid_;
  auto rid = iter_->GetRid();
  auto txn = exec_ctx_->GetTransaction();
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      // if not yet hold the lock
      if(!txn->IsRowSharedLocked(oid, rid) && !txn->IsRowExclusiveLocked(oid, rid)) {
        bool res = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(),
                                                        LockManager::LockMode::SHARED, oid,
                                                        rid);
        if (!res) {
          throw ExecutionException("SeqScanExecutor failed to acquire row lock.");
        }
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("SeqScanExecutor failed to acquire row lock " + e.GetInfo());
    }
  }
}
void SeqScanExecutor::UnlockRow() {
  auto catalog = exec_ctx_->GetCatalog();
  auto table = catalog->GetTable(plan_->GetTableOid());

  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    auto oid = table->oid_;
    try {
      bool res = exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), oid, iter_->GetRid());
      if (!res) {
        throw ExecutionException("SeqScanExecutor failed to release row lock.");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("SeqScanExecutor failed to release row lock " + e.GetInfo());
    }
  }
}
void SeqScanExecutor::UnlockTable() {
  auto catalog = exec_ctx_->GetCatalog();
  auto table = catalog->GetTable(plan_->GetTableOid());
  auto oid = table->oid_;
  auto txn = exec_ctx_->GetTransaction();
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    try {
      if(txn->IsTableIntentionSharedLocked(oid)) {
        bool res = exec_ctx_->GetLockManager()->UnlockTable(
            exec_ctx_->GetTransaction(), oid);
        if (!res) {
          throw ExecutionException("SeqScanExecutor failed to release table lock.");
        }
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("SeqScanExecutor failed to release table lock " + e.GetInfo());
    }
  }
}

void SeqScanExecutor::Init() {
  LockTable();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto catalog = exec_ctx_->GetCatalog();
  auto table = catalog->GetTable(plan_->GetTableOid());
  // iterate over the current page and emit the tuple with predicate check
  while (iter_ != table->table_->End()) {
    LockRow();
    *tuple = *iter_;
    *rid = iter_->GetRid();;
    UnlockRow();
    iter_++;
    return true;
  }  // end while

  // done, release the table lock
  UnlockTable();
  return false;
}
}  // namespace bustub
