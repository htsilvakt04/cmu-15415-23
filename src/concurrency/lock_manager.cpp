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
void TxnRemoveRowLock(Transaction *txn, const table_oid_t &oid, const RID &rid, LockManager::LockMode lock_mode);

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  std::list<std::shared_ptr<LockRequest>>::iterator table_iter;
  // acquire the lock
  table_lock_map_latch_.lock();
  // make sure we do have the entry in the map
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    txn->SetState(TransactionState::ABORTED);
    table_lock_map_latch_.unlock();
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  table_lock_map_[oid]->latch_.lock();
  auto table = table_lock_map_[oid];
  table_lock_map_latch_.unlock();

  CheckTableUnlockAbortCond(txn, oid, table, table_iter);
  auto table_lock_req = *table_iter;
  auto mode = table_lock_req->lock_mode_;
  // remove table lock request
  table->request_queue_.remove(table_lock_req);

  SetTxnState(txn, mode);
  table->latch_.unlock();
  // remove from the txn
  TxnRemoveTableLock(txn, oid, mode);
  // wakes up other threads
  table->cv_.notify_all();
  return true;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  CheckAbortCond(txn, oid, lock_mode);
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  table_lock_map_[oid]->latch_.lock();
  auto table = table_lock_map_[oid];

  // release the global latch for the map
  table_lock_map_latch_.unlock();

  bool is_abort = false;
  // check if we already hold the lock with the same lock mode
  if (IsHeldLock(txn, lock_mode, oid, table, table->latch_, is_abort)) {
    return true;
  }

  if (is_abort) {
    return false;
  }

  auto l_req = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  // add request to the queue
  table->request_queue_.push_back(l_req);
  // reacquire the lock, so that we can use cv on it
  std::unique_lock<std::mutex> lock(table->latch_, std::adopt_lock);
  /* We want to acquire the lock and put this thread to sleep if the lock is not free */
  while (!LockIsFree(txn, lock_mode, table)) {
    table->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      table->request_queue_.remove(l_req);
      table->cv_.notify_all();
      return false;
    }
  }

  // add the granted lock to the txn
  TxnAddTableLock(txn, oid, lock_mode);
  l_req->granted_ = true;
  // the lock is implicitly release
  return true;
}

// logic is basically the same as with Table lock, just have some abort conditions are different.
auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  CheckAbortCond(txn, oid, lock_mode, true);
  // check if the txn has held the table lock in compatible mode with the lock_mode
  CheckRowTableCompatible(txn, oid, lock_mode);
  // init the queue if it is empty
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  // hold the lock on the queue
  row_lock_map_[rid]->latch_.lock();
  auto row = row_lock_map_[rid];
  row_lock_map_latch_.unlock();

  // check if we already hold the lock with the same lock mode
  bool is_abort = false;
  if (IsHeldLockRow(txn, lock_mode, oid, rid, row, is_abort)) {
    return true;
  }

  if (is_abort) {
    return false;
  }

  auto l_req = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  // add request to the waiting queue
  row->request_queue_.push_back(l_req);

  // reacquire the lock to used with cv
  std::unique_lock lock(row->latch_, std::adopt_lock);
  /* We want to acquire the lock and put this thread to sleep if the lock is not free */
  while (!RowLockIsFree(txn, lock_mode, row)) {
    row->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      row->request_queue_.remove(l_req);
      row->cv_.notify_all();
      return false;
    }
  }
  // set the request lock to be granted
  l_req->granted_ = true;
  // add the granted lock to the txn
  TxnAddRowLock(txn, lock_mode, oid, rid);
  // the lock is implicitly release
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  std::list<std::shared_ptr<LockRequest>>::iterator row_iter;
  row_lock_map_latch_.lock();
  // make sure we do have the entry in the map
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    txn->SetState(TransactionState::ABORTED);
    row_lock_map_latch_.unlock();
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // acquire the lock
  row_lock_map_[rid]->latch_.lock();
  auto table = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  CheckRowUnlockAbortCond(txn, table, row_iter);
  auto row_lock_req = *row_iter;
  auto mode = row_lock_req->lock_mode_;
  SetTxnState(txn, mode);

  // remove table lock request
  table->request_queue_.remove(row_lock_req);
  // remove from the txn
  TxnRemoveRowLock(txn, oid, rid, mode);
  // wakes up other threads
  table->latch_.unlock();
  table->cv_.notify_all();
  return true;
}
void LockManager::CheckRowUnlockAbortCond(Transaction *txn, const std::shared_ptr<LockRequestQueue> &table,
                                          std::list<std::shared_ptr<LockRequest>>::iterator &row_iterator) {
  //    * GENERAL BEHAVIOUR:
  //   *    Both UnlockTable() and UnlockRow() should release the lock on the resource and return.
  //   *    Both should ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
  //   *    If not, LockManager should set the TransactionState as ABORTED and throw
  //   *    a TransactionAbortException (ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD)
  //   *
  //   *    Additionally, unlocking a table should only be allowed if the transaction does not hold locks on any
  //   *    row on that table. If the transaction holds locks on rows of the table, Unlock should set the Transaction
  //   State
  //   *    as ABORTED and throw a TransactionAbortException (TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS).
  //   *
  //   *    Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
  // lookup the row lock
  auto row_itr = std::find_if(table->request_queue_.begin(), table->request_queue_.end(),
                              [txn](const std::shared_ptr<LockRequest> &request) {
                                return request->txn_id_ == txn->GetTransactionId() && request->granted_;
                              });
  // not found
  if (row_itr == table->request_queue_.end()) {
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    table->latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  // return the row_lock iterator
  row_iterator = row_itr;
}

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
  txn->LockTxn();
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
  txn->UnlockTxn();
}

void TxnRemoveRowLock(Transaction *txn, const table_oid_t &oid, const RID &rid, LockManager::LockMode lock_mode) {
  // acquire lock on the transaction
  txn->LockTxn();
  if (lock_mode == LockManager::LockMode::SHARED) {
    txn->GetSharedRowLockSet()->find(oid)->second.erase(rid);
  } else if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    txn->GetExclusiveRowLockSet()->find(oid)->second.erase(rid);
  }
  txn->UnlockTxn();
}

// add the table lock to the txn
void LockManager::TxnAddTableLock(Transaction *txn, const table_oid_t &oid, LockMode lock_mode) {
  // acquire lock on the transaction
  txn->LockTxn();
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
  txn->UnlockTxn();
}

void LockManager::TxnAddRowLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  // acquire lock on the transaction
  txn->LockTxn();
  // add to the row set
  if (lock_mode == LockMode::SHARED) {
    auto s_row_lock_set = txn->GetSharedRowLockSet();
    if (!s_row_lock_set) {
      s_row_lock_set = std::make_shared<std::unordered_map<table_oid_t, std::unordered_set<RID>>>();
    }
    // insert it
    (*s_row_lock_set)[oid].insert(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    auto ex_row_lock_set = txn->GetExclusiveRowLockSet();
    if (!ex_row_lock_set) {
      ex_row_lock_set = std::make_shared<std::unordered_map<table_oid_t, std::unordered_set<RID>>>();
    }
    (*ex_row_lock_set)[oid].insert(rid);
  }
  txn->UnlockTxn();
}
/// If hold the lock already and has the same mode => return true immediately. Otherwise, check the upgrading condition
/// \param txn
/// \param lock_mode
/// \param oid
/// \param queue_lock
/// \param is_abort indicator for the caller, so that they will just return false, instead of continue working with the
/// locking request. \return false if there is no upgrade request or this txn not yet hold the table lock. return true
/// otherwise.
auto LockManager::IsHeldLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid,
                             std::shared_ptr<LockRequestQueue> &table, std::mutex &queue_lock, bool &is_abort) -> bool {
  for (const auto &request : table->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      // request the lock for this table before
      if (!request->granted_) {
        txn->LockTxn();
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        queue_lock.unlock();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      // same lock and same mode
      if (request->lock_mode_ == lock_mode) {
        queue_lock.unlock();
        return true;
      }
      // otherwise, we want to upgrade the lock
      // multiple attempts to upgrade lock
      if (table->upgrading_ != INVALID_TXN_ID) {
        txn->LockTxn();
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        queue_lock.unlock();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      // check abort conditions
      CheckSatisfyTransitionCond(txn, request, lock_mode, queue_lock);

      // drop the lock from the txn
      TxnRemoveTableLock(txn, oid, request->lock_mode_);
      // drop the current lock
      table->request_queue_.remove(request);
      auto upgrade_req = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
      // reserve the upgrade position
      table->request_queue_.push_back(upgrade_req);
      // set the upgrading
      table->upgrading_ = txn->GetTransactionId();
      std::unique_lock lock(queue_lock, std::adopt_lock);
      // wait to get new lock granted
      while (!LockIsFree(txn, lock_mode, table)) {
        table->cv_.wait(lock);
        // if the txt was aborted, take action
        if (txn->GetState() == TransactionState::ABORTED) {
          table->request_queue_.remove(upgrade_req);
          is_abort = true;
          table->upgrading_ = INVALID_TXN_ID;
          table->cv_.notify_all();
          return false;
        }
      }

      // set the lock to granted
      upgrade_req->granted_ = true;
      // clear the upgrade_req
      table->upgrading_ = INVALID_TXN_ID;
      // add the granted lock to the txn
      TxnAddTableLock(txn, oid, lock_mode);
      return true;
    }  // end if
  }    // end for

  return false;
}
void LockManager::CheckSatisfyTransitionCond(Transaction *txn, const std::shared_ptr<LockRequest> &request,
                                             LockMode upgrade_lock_mode, std::mutex &queue_lock) {
  txn->LockTxn();
  auto lock_mode = upgrade_lock_mode;
  //     While upgrading, only the following transitions should be allowed:
  //  IS -> [S, X, IX, SIX]
  //  S -> [X, SIX]
  //  IX -> [X, SIX]
  //  SIX -> [X]
  bool cond1 = request->lock_mode_ == LockMode::SHARED &&
               (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE);
  bool cond2 = request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE &&
               (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE);
  bool cond3 = request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE;

  if (cond1 || cond2 || cond3 || request->lock_mode_ == LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    queue_lock.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
  }
  txn->UnlockTxn();
}
///
/// \param txn
/// \param request is the lock request this txn already granted
/// \param row_lock_mode
/// \param queue_lock
void LockManager::CheckSatisfyRowTransitionCond(Transaction *txn, const std::shared_ptr<LockRequest> &request,
                                                LockMode row_lock_mode, std::mutex &queue_lock) {
  txn->LockTxn();
  //     While upgrading, only the following transitions should be allowed:
  //  IS -> [S, X, IX, SIX]
  //  S -> [X, SIX]
  //  IX -> [X, SIX]
  //  SIX -> [X]
  if (request->lock_mode_ == LockMode::SHARED) {
    if (row_lock_mode != LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      queue_lock.unlock();
      throw TransactionAbortException(request->txn_id_, AbortReason::INCOMPATIBLE_UPGRADE);
    }
  } else if (request->lock_mode_ == LockMode::EXCLUSIVE) {
    // X wants to upgrade to X
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    queue_lock.unlock();
    throw TransactionAbortException(request->txn_id_, AbortReason::INCOMPATIBLE_UPGRADE);
  }
  txn->UnlockTxn();
}

/// Check if there is any conflict 'mode' with some granted requests in the queue
/// \param txn
/// \param mode
/// \param oid
/// \return true if no conflict, false otherwise.
auto LockManager::LockIsFree(Transaction *txn, LockMode mode, const std::shared_ptr<LockRequestQueue> &table) -> bool {
  // stop other threads from running before the upgrade request
  if (table->upgrading_ != INVALID_TXN_ID && txn->GetTransactionId() != table->upgrading_) {
    return false;
  }

  // given the current set of granted locks, does this 'mode' compatible with all of them?
  for (const auto &request : table->request_queue_) {
    if (!NotConflictMode(request, mode, txn)) {
      return false;
    }
  }

  return true;
}

/// Check if there is any conflict 'mode' with some granted requests in the queue
/// \param txn
/// \param mode the mode we want to achieve
/// \param oid
/// \return true if no conflict, false otherwise.
auto LockManager::RowLockIsFree(Transaction *txn, LockMode mode, const std::shared_ptr<LockRequestQueue> &table) -> bool {
  // stop other threads from running before the upgrading request
  if (table->upgrading_ != INVALID_TXN_ID && txn->GetTransactionId() != table->upgrading_) {
    return false;
  }

  // given the current set of granted locks, does this 'mode' compatible with all of them?
  return std::all_of(
      table->request_queue_.begin(), table->request_queue_.end(),
      [txn, mode](const std::shared_ptr<LockRequest> &request) { return NotConflictRowMode(request, mode, txn); });
}
// is 'mode' conflicts with request->lock_mode_ ?
auto LockManager::NotConflictRowMode(const std::shared_ptr<LockRequest> &request, LockMode mode, Transaction *txn)
    -> bool {
  // not yet grant, so there is no conflict
  if (!request->granted_ || request->txn_id_ == txn->GetTransactionId()) {
    return true;
  }
  if (mode == LockMode::SHARED) {
    return request->lock_mode_ == LockMode::SHARED;
  }

  return false;
}
auto LockManager::NotConflictMode(const std::shared_ptr<LockRequest> &request, LockMode mode, Transaction *txn)
    -> bool {
  if (!request->granted_ || request->txn_id_ == txn->GetTransactionId()) {
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
  txn->LockTxn();
  if (is_lock_row) {
    // Row locking should not support Intention locks
    if (mode == LockMode::INTENTION_EXCLUSIVE || mode == LockMode::INTENTION_SHARED ||
        mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    }
    // X/IX locks on rows are not allowed if the Transaction State is SHRINKING
    if (txn->GetState() == TransactionState::SHRINKING && mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
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
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn->GetState() == TransactionState::SHRINKING
          && mode != LockMode::INTENTION_SHARED
          && mode != LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if (mode == LockMode::SHARED || mode == LockMode::INTENTION_SHARED ||
          mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if (txn->GetState() == TransactionState::SHRINKING &&
          (mode == LockMode::EXCLUSIVE || mode == LockMode::INTENTION_EXCLUSIVE)) {
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
  }

  txn->UnlockTxn();
}
/// Check weather the parent (the table) has been acquired accordingly
/// \param txn
/// \param oid the table oid
/// \param row_mode the mode of the row
void LockManager::CheckRowTableCompatible(Transaction *txn, const table_oid_t &oid, LockMode row_mode) {
  txn->LockTxn();
  // IS, S, IX, SIX for the parent
  if (row_mode == LockMode::EXCLUSIVE) {  // X, IX, SIX for the parent
    if (!txn->IsTableExclusiveLocked(oid)
        && !txn->IsTableIntentionExclusiveLocked(oid)
        && !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  txn->UnlockTxn();
}
auto LockManager::IsHeldLockRow(Transaction *txn, LockMode row_lock_mode, const table_oid_t &oid, const RID &rid,
                                const std::shared_ptr<LockRequestQueue> &row, bool &is_abort) -> bool {
  for (const auto &request : row->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      // request the lock before
      if (!request->granted_) {
        txn->LockTxn();
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        row->latch_.unlock();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }

      // same lock and same mode
      if (request->lock_mode_ == row_lock_mode) {
        row->latch_.unlock();
        return true;
      }
      // otherwise, we want to upgrade the lock
      // multiple attempts to upgrade lock
      if (row->upgrading_ != INVALID_TXN_ID) {
        txn->LockTxn();
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        row->latch_.unlock();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      // check abort conditions
      CheckSatisfyRowTransitionCond(txn, request, row_lock_mode, row->latch_);
      // drop the lock from the txn
      TxnRemoveRowLock(txn, oid, rid, request->lock_mode_);
      // drop the current lock
      row->request_queue_.remove(request);
      auto row_upgrade_req = std::make_shared<LockRequest>(txn->GetTransactionId(), row_lock_mode, oid, rid);
      // reserve the upgrade position
      row->request_queue_.push_back(row_upgrade_req);
      // set the upgrading
      row->upgrading_ = txn->GetTransactionId();
      std::unique_lock lock(row->latch_, std::adopt_lock);
      // wait to get new lock granted
      while (!RowLockIsFree(txn, row_lock_mode, row)) {
        row->cv_.wait(lock);
        // if the txt was aborted, take action
        if (txn->GetState() == TransactionState::ABORTED) {
          row->request_queue_.remove(row_upgrade_req);
          is_abort = true;
          row->upgrading_ = INVALID_TXN_ID;
          row->cv_.notify_all();
          return false;
        }
      }

      // set the lock to be granted
      row_upgrade_req->granted_ = true;
      // clear the upgrade_req
      row->upgrading_ = INVALID_TXN_ID;
      // add the granted lock to the txn
      TxnAddRowLock(txn, row_lock_mode, oid, rid);
      return true;
    }  // end if
  }    // end for

  return false;
}

void LockManager::SetTxnState(Transaction *txn, LockMode mode) {
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED ||
      (mode != LockMode::SHARED && mode != LockMode::EXCLUSIVE)) {
    return;
  }
  //    * TRANSACTION STATE UPDATE
  //   *    Unlock should update the transaction state appropriately (depending upon the ISOLATION LEVEL)
  //   *    Only unlocking S or X locks changes transaction state.
  //   *
  //   *    REPEATABLE_READ:
  //   *        Unlocking S/X locks should set the transaction state to SHRINKING
  //   *
  //   *    READ_COMMITTED:
  //   *        Unlocking X locks should set the transaction state to SHRINKING.
  //   *        Unlocking S locks does not affect transaction state.
  //   *
  //   *   READ_UNCOMMITTED:
  //   *        Unlocking X locks should set the transaction state to SHRINKING.
  //   *        S locks are not permitted under READ_UNCOMMITTED.
  //   *            The behaviour upon unlocking an S lock under this isolation level is undefined.
  txn->LockTxn();
  auto iso_level = txn->GetIsolationLevel();
  if (iso_level == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  } else if (iso_level == IsolationLevel::READ_COMMITTED || iso_level == IsolationLevel::READ_UNCOMMITTED) {
    if (mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }

  txn->UnlockTxn();
}

void LockManager::CheckTableUnlockAbortCond(Transaction *txn, const table_oid_t &oid,
                                            const std::shared_ptr<LockRequestQueue> &table,
                                            std::list<std::shared_ptr<LockRequest>>::iterator &table_iterator) {
  //    * GENERAL BEHAVIOUR:
  //   *    Both UnlockTable() and UnlockRow() should release the lock on the resource and return.
  //   *    Both should ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
  //   *    If not, LockManager should set the TransactionState as ABORTED and throw
  //   *    a TransactionAbortException (ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD)
  //   *
  //   *    Additionally, unlocking a table should only be allowed if the transaction does not hold locks on any
  //   *    row on that table. If the transaction holds locks on rows of the table, Unlock should set the Transaction
  //   State
  //   *    as ABORTED and throw a TransactionAbortException (TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS).
  //   *
  //   *    Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
  // acquire the lock on txn
  txn->LockTxn();
  // check if the txn hold any lock on row level
  if (IsTxnHoldRowLock(txn, oid)) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    table->latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  // lookup the table lock
  auto table_itr = std::find_if(table->request_queue_.begin(), table->request_queue_.end(),
                                [txn](const std::shared_ptr<LockRequest> &request) {
                                  return request->txn_id_ == txn->GetTransactionId() && request->granted_;
                                });

  // not found
  if (table_itr == table->request_queue_.end()) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    table->latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  // return the table_lock iterator
  table_iterator = table_itr;

  txn->UnlockTxn();
}
auto LockManager::IsTxnHoldRowLock(Transaction *txn, const table_oid_t &oid) const -> bool {
  bool has_shared = false;
  bool has_exclusive = false;
  auto shared_rows = txn->GetSharedRowLockSet()->find(oid);

  if (shared_rows != txn->GetSharedRowLockSet()->end()) {
    has_shared = !shared_rows->second.empty();
  }

  auto exclusive_rows = txn->GetExclusiveRowLockSet()->find(oid);
  if (exclusive_rows != txn->GetExclusiveRowLockSet()->end()) {
    has_exclusive = !exclusive_rows->second.empty();
  }
  return has_shared || has_exclusive;
}
}  // namespace bustub
