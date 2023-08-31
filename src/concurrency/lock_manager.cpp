//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <random>
#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
namespace bustub {
void TxnRemoveTableLock(Transaction *txn, const table_oid_t &oid, LockManager::LockMode lock_mode);

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // acquire the lock of the table
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  table_lock_map_latch_.unlock();
  // check abort condition
  CheckAbortCond(txn, oid, lock_mode);
  // hold the lock on the queue
  std::unique_lock queue_lock(table_lock_map_[oid]->latch_);
  // check if we already hold the lock with the same lock mode
  bool is_abort = false;
  if (IsHeldLock(txn, lock_mode, oid, queue_lock, is_abort)) {
    return true;
  }
  if (is_abort) {
    return false;
  }

  auto l_req = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  // add request to the queue
  table_lock_map_[oid]->request_queue_.push_back(l_req);

  /* We want to acquire the lock and put this thread to sleep if the lock is not free */
  while (!LockIsFree(txn, lock_mode, oid)) {
    table_lock_map_[oid]->cv_.wait(queue_lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      table_lock_map_[oid]->request_queue_.remove(l_req);
      table_lock_map_[oid]->cv_.notify_all();
      return false;
    }
  }

  // add the granted lock to the txn
  TxnAddTableLock(txn, oid, lock_mode);
  l_req->granted_ = true;
  // the lock is implicitly release
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { return true; }

// logic is basically the same as with Table lock, just have some abort conditions are different.
auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  CheckAbortCond(txn, oid, lock_mode, true);
  // init the queue if it is empty
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  row_lock_map_latch_.unlock();

  // hold the lock on the queue
  std::unique_lock queue_lock(row_lock_map_[rid]->latch_);
  // check if we already hold the lock with the same lock mode
  bool is_abort = false;
  if (IsHeldLockRow(txn, lock_mode, oid, rid, queue_lock, is_abort)) {
    return true;
  }
  if (is_abort) {
    return false;
  }

  auto l_req = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  // add request to the queue
  row_lock_map_[rid]->request_queue_.push_back(l_req);

  /* We want to acquire the lock and put this thread to sleep if the lock is not free */
  while (!RowLockIsFree(txn, lock_mode, oid, rid)) {
    row_lock_map_[rid]->cv_.wait(queue_lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      row_lock_map_[rid]->request_queue_.remove(l_req);
      row_lock_map_[rid]->cv_.notify_all();
      return false;
    }
  }

  // add the granted lock to the txn
  TxnAddRowLock(txn, lock_mode, oid, rid);
  l_req->granted_ = true;
  // the lock is implicitly release
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { return true; }

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}
void TxnRemoveTableLock(Transaction *txn, const table_oid_t &oid, LockManager::LockMode lock_mode) {
  // acquire lock on the transaction
  std::lock_guard<std::mutex> lock(txn->latch_);

  switch (lock_mode) {
    case LockManager::LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockManager::LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockManager::LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockManager::LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  }
}

void TxnRemoveRowLock(Transaction *txn, const table_oid_t &oid, const RID &rid, LockManager::LockMode lock_mode) {
  // acquire lock on the transaction
  std::lock_guard<std::mutex> lock(txn->latch_);

  if (lock_mode == LockManager::LockMode::SHARED) {
    auto table = txn->GetSharedRowLockSet()->find(oid);
    table->second.erase(rid);
  } else if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    auto table = txn->GetExclusiveRowLockSet()->find(oid);
    table->second.erase(rid);
  }
}
// add the table lock to the txn
void LockManager::TxnAddTableLock(Transaction *txn, const table_oid_t &oid, LockMode lock_mode) {
  // acquire lock on the transaction
  std::lock_guard<std::mutex> lock(txn->latch_);
  // add to the table set
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
  }
}

void LockManager::TxnAddRowLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  // acquire lock on the transaction
  std::lock_guard<std::mutex> lock(txn->latch_);
  // add to the row set
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedRowLockSet()->find(oid)->second.insert(rid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveRowLockSet()->find(oid)->second.insert(rid);
      break;
    default:
      break;
  }
}
auto LockManager::IsHeldLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid,
                             std::unique_lock<std::mutex> &queue_lock, bool &is_abort) -> bool {
  for (const auto &request : table_lock_map_[oid]->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      // same lock and same mode
      if (request->lock_mode_ == lock_mode) {
        return true;
      }
      // otherwise, we want to upgrade the lock
      // multiple attempts to upgrade lock
      if (table_lock_map_[oid]->upgrading_ != INVALID_TXN_ID) {
        txn->LockTxn();
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      // check abort conditions
      CheckSatisfyTransitionCond(txn, request, lock_mode);

      // drop the current lock
      table_lock_map_[oid]->request_queue_.remove(request);
      // drop the lock from the txn
      TxnRemoveTableLock(txn, oid, lock_mode);
      auto upgrade_req = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
      // reserve the upgrade position
      table_lock_map_[oid]->request_queue_.push_back(upgrade_req);
      // set the upgrading
      table_lock_map_[oid]->upgrading_ = txn->GetTransactionId();
      // wait to get new lock granted
      while (!LockIsFree(txn, lock_mode, oid)) {
        table_lock_map_[oid]->cv_.wait(queue_lock);
        // if the txt was aborted, take action
        if (txn->GetState() == TransactionState::ABORTED) {
          table_lock_map_[oid]->request_queue_.remove(upgrade_req);
          is_abort = true;
          table_lock_map_[oid]->upgrading_ = INVALID_TXN_ID;
          table_lock_map_[oid]->cv_.notify_all();
          return false;
        }
      }

      // set the lock to granted
      upgrade_req->granted_ = true;
      // clear the upgrade_req
      table_lock_map_[oid]->upgrading_ = INVALID_TXN_ID;
      // add the granted lock to the txn
      TxnAddTableLock(txn, oid, lock_mode);
      return true;
    }  // end if
  }    // end for

  return false;
}
void LockManager::CheckSatisfyTransitionCond(Transaction *txn, const std::shared_ptr<LockRequest> &request,
                                             LockMode upgrade_lock_mode) {
  std::unique_lock lock(txn->latch_);
  //     While upgrading, only the following transitions should be allowed:
  //  IS -> [S, X, IX, SIX]
  //  S -> [X, SIX]
  //  IX -> [X, SIX]
  //  SIX -> [X]
  if (request->lock_mode_ == LockMode::SHARED) {
    if (upgrade_lock_mode != LockMode::EXCLUSIVE && upgrade_lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(request->txn_id_, AbortReason::INCOMPATIBLE_UPGRADE);
    }
  }

  if (request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
    if (upgrade_lock_mode != LockMode::EXCLUSIVE && upgrade_lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(request->txn_id_, AbortReason::INCOMPATIBLE_UPGRADE);
    }
  }

  if (request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    if (upgrade_lock_mode != LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(request->txn_id_, AbortReason::INCOMPATIBLE_UPGRADE);
    }
  }
}
void LockManager::CheckSatisfyRowTransitionCond(Transaction *txn, const std::shared_ptr<LockRequest> &request,
                                                LockMode row_lock_mode) {
  std::unique_lock lock(txn->latch_);
  //     While upgrading, only the following transitions should be allowed:
  //  IS -> [S, X, IX, SIX]
  //  S -> [X, SIX]
  //  IX -> [X, SIX]
  //  SIX -> [X]
  if (request->lock_mode_ == LockMode::SHARED) {
    if (row_lock_mode != LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(request->txn_id_, AbortReason::INCOMPATIBLE_UPGRADE);
    }
  } else if (request->lock_mode_ == LockMode::EXCLUSIVE) {
    if (row_lock_mode == LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(request->txn_id_, AbortReason::INCOMPATIBLE_UPGRADE);
    }
  }
}
/// Check if there is any conflict 'mode' with some granted requests in the queue
/// \param txn
/// \param mode
/// \param oid
/// \return true if no conflict, false otherwise.
auto LockManager::LockIsFree(Transaction *txn, LockMode mode, const table_oid_t &oid) -> bool {
  // stop other threads from running before the upgrade request
  if (table_lock_map_[oid]->upgrading_ != INVALID_TXN_ID &&
      txn->GetTransactionId() != table_lock_map_[oid]->upgrading_) {
    return false;
  }
  // given the current set of granted locks, does this 'mode' compatible with all of them?
  return std::all_of(table_lock_map_[oid]->request_queue_.begin(), table_lock_map_[oid]->request_queue_.end(),
                     [mode](const std::shared_ptr<LockRequest> &request) { return !IsConflictMode(request, mode); });
}

/// Check if there is any conflict 'mode' with some granted requests in the queue
/// \param txn
/// \param mode
/// \param oid
/// \return true if no conflict, false otherwise.
auto LockManager::RowLockIsFree(Transaction *txn, LockMode mode, const table_oid_t &oid, const RID &rid) -> bool {
  // stop other threads from running before the upgrade request
  if (row_lock_map_[rid]->upgrading_ != INVALID_TXN_ID && txn->GetTransactionId() != row_lock_map_[rid]->upgrading_) {
    return false;
  }
  // given the current set of granted locks, does this 'mode' compatible with all of them?
  return std::all_of(row_lock_map_[rid]->request_queue_.begin(), row_lock_map_[rid]->request_queue_.end(),
                     [mode](const std::shared_ptr<LockRequest> &request) { return !IsConflictRowMode(request, mode); });
}

auto LockManager::IsConflictRowMode(const std::shared_ptr<LockRequest> &request, LockMode mode) -> bool {
  if (!request->granted_) {
    return true;
  }
  if (mode == LockMode::SHARED) {
    return request->lock_mode_ != LockMode::EXCLUSIVE;
  }

  return false;
}

auto LockManager::IsConflictMode(const std::shared_ptr<LockRequest> &request, LockMode mode) -> bool {
  if (!request->granted_) {
    return true;
  }

  switch (mode) {
    case LockMode::INTENTION_SHARED:
      return request->lock_mode_ != LockMode::EXCLUSIVE;
    case LockMode::INTENTION_EXCLUSIVE:
      return request->lock_mode_ == LockMode::INTENTION_SHARED || request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE;
    case LockMode::SHARED:
      return request->lock_mode_ == LockMode::INTENTION_SHARED || request->lock_mode_ == LockMode::SHARED;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return request->lock_mode_ == LockMode::INTENTION_SHARED;
    case LockMode::EXCLUSIVE:
      return false;
  }
}

// before attempting to acquire the lock, need to check abort conditions
void LockManager::CheckAbortCond(Transaction *txn, const table_oid_t &oid, LockMode mode, bool is_lock_row) {
  std::unique_lock lock(txn->latch_);

  //    *    Depending on the ISOLATION LEVEL, a transaction should attempt to take locks:
  //   *    - Only if required, AND
  //   *    - Only if allowed
  //   *
  //   *    For instance S/IS/SIX locks are not required under READ_UNCOMMITTED, and any such attempt should set the
  //   *    TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
  //   *
  //   *    Similarly, X/IX locks on rows are not allowed if the the Transaction State is SHRINKING, and any such
  //   attempt
  //   *    should set the TransactionState as ABORTED and throw a TransactionAbortException (LOCK_ON_SHRINKING).
  //   *
  //   *    REPEATABLE_READ:
  //   *        The transaction is required to take all locks.
  //   *        All locks are allowed in the GROWING state
  //   *        No locks are allowed in the SHRINKING state
  //   *
  //   *    READ_COMMITTED:
  //   *        The transaction is required to take all locks.
  //   *        All locks are allowed in the GROWING state
  //   *        Only IS, S locks are allowed in the SHRINKING state
  //   *
  //   *    READ_UNCOMMITTED:
  //   *        The transaction is required to take only IX, X locks.
  //   *        IX, X locks are allowed in the GROWING state.
  //   *        S, IS, SIX locks are never allowed
  if (is_lock_row) {
    // for row lock request, there is no request if it is under SHRINKING phrase.
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    // request lock mode for row must not be 'Intention'.
    if (mode == LockMode::INTENTION_SHARED || mode == LockMode::INTENTION_EXCLUSIVE ||
        mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    }

    CheckRowTableCompatible(txn, oid, mode);
  }

  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn->GetState() == TransactionState::SHRINKING && mode != LockMode::INTENTION_SHARED &&
          mode != LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if (txn->GetState() != TransactionState::GROWING ||
          (mode != LockMode::INTENTION_EXCLUSIVE && mode != LockMode::EXCLUSIVE)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      break;
  }
}
/// Check weather the parent (the table) has been acquired accordingly
/// \param txn
/// \param oid the table oid
/// \param row_mode the mode of the row
void LockManager::CheckRowTableCompatible(Transaction *txn, const table_oid_t &oid, LockMode row_mode) {
  table_lock_map_latch_.lock();
  std::unique_lock lock(table_lock_map_[oid]->latch_);
  table_lock_map_latch_.unlock();
  //    *    While locking rows, Lock() should ensure that the transaction has an appropriate lock on the table which
  //    the row
  //   *    belongs to. For instance, if an exclusive lock is attempted on a row, the transaction must hold either
  //   *    X, IX, or SIX on the table. If such a lock does not exist on the table, Lock() should set the
  //   TransactionState
  //   *    as ABORTED and throw a TransactionAbortException (TABLE_LOCK_NOT_PRESENT)
  auto table = table_lock_map_[oid];
  // lookup the parent table lock
  auto table_itr = std::find_if(
      table->request_queue_.begin(), table->request_queue_.end(),
      [txn](const std::shared_ptr<LockRequest> &request) { return request->txn_id_ == txn->GetTransactionId(); });
  auto table_request = (*table_itr);

  // S, IS, SIX for the parent
  if (row_mode == LockMode::SHARED) {
    if (table_request->lock_mode_ != LockMode::SHARED && table_request->lock_mode_ != LockMode::INTENTION_SHARED &&
        table_request->lock_mode_ != LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  } else if (row_mode == LockMode::EXCLUSIVE) {  // X, IX, SIX for the parent
    if (table_request->lock_mode_ != LockMode::EXCLUSIVE &&
        table_request->lock_mode_ != LockMode::INTENTION_EXCLUSIVE &&
        table_request->lock_mode_ != LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
}
auto LockManager::IsHeldLockRow(Transaction *txn, LockMode row_lock_mode, const table_oid_t &oid, const RID &rid,
                                std::unique_lock<std::mutex> &queue_lock, bool &is_abort) -> bool {
  for (const auto &request : row_lock_map_[rid]->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      // same lock and same mode
      if (request->lock_mode_ == row_lock_mode) {
        return true;
      }
      // otherwise, we want to upgrade the lock
      // multiple attempts to upgrade lock
      if (row_lock_map_[rid]->upgrading_ != INVALID_TXN_ID) {
        txn->LockTxn();
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      // check abort conditions
      CheckSatisfyRowTransitionCond(txn, request, row_lock_mode);

      // drop the current lock
      row_lock_map_[rid]->request_queue_.remove(request);
      // drop the lock from the txn
      TxnRemoveRowLock(txn, oid, rid, row_lock_mode);

      auto row_upgrade_req = std::make_shared<LockRequest>(txn->GetTransactionId(), row_lock_mode, oid, rid);
      // reserve the upgrade position
      row_lock_map_[rid]->request_queue_.push_back(row_upgrade_req);
      // set the upgrading
      row_lock_map_[rid]->upgrading_ = txn->GetTransactionId();
      // wait to get new lock granted
      while (!RowLockIsFree(txn, row_lock_mode, oid, rid)) {
        row_lock_map_[rid]->cv_.wait(queue_lock);
        // if the txt was aborted, take action
        if (txn->GetState() == TransactionState::ABORTED) {
          row_lock_map_[rid]->request_queue_.remove(row_upgrade_req);
          is_abort = true;
          row_lock_map_[rid]->upgrading_ = INVALID_TXN_ID;
          row_lock_map_[rid]->cv_.notify_all();
          return false;
        }
      }

      // set the lock to be granted
      row_upgrade_req->granted_ = true;
      // clear the upgrade_req
      row_lock_map_[rid]->upgrading_ = INVALID_TXN_ID;
      // add the granted lock to the txn
      TxnAddRowLock(txn, row_lock_mode, oid, rid);
      return true;
    }  // end if
  }    // end for

  return false;
}
}  // namespace bustub
