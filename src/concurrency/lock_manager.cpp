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

  // create a lock request
  auto l_req = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);

  /* Otherwise, we want to acquire the lock and put this thread to sleep if the lock is not free */
  while (!LockIsFree(lock_mode, oid, l_req)) {
    table_lock_map_[oid]->cv_.wait(queue_lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      table_lock_map_[oid]->cv_.notify_all();
      return false;
    }
  }
  // add the granted lock to the txn
  TxnAddTableLock(txn, oid, lock_mode);
  l_req->granted_ = true;
  table_lock_map_[oid]->request_queue_.push_back(l_req);
  // the lock is implicitly release
  return true;
}
auto LockManager::IsHeldLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid,
                             std::unique_lock<std::mutex>& queue_lock, bool& is_abort) -> bool {
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
      table_lock_map_[oid]->upgrade_req_ = upgrade_req;
      // reserve the upgrade position
      table_lock_map_[oid]->request_queue_.push_back(upgrade_req);

      // wait to get new lock granted
      while (!LockIsFree(lock_mode, oid, upgrade_req)) {
        table_lock_map_[oid]->cv_.wait(queue_lock);
        // if the txt was aborted, take action
        if (txn->GetState() == TransactionState::ABORTED) {
          table_lock_map_[oid]->request_queue_.remove(upgrade_req);
          table_lock_map_[oid]->cv_.notify_all();
          is_abort = true;
          table_lock_map_[oid]->upgrade_req_ = nullptr;
          return false;
        }
      }

      // set the lock to granted
      upgrade_req->granted_ = true;
      // set the upgrading txn to the current txn id
      table_lock_map_[oid]->upgrading_ = txn->GetTransactionId();
      // clear the upgrade_req
      table_lock_map_[oid]->upgrade_req_ = nullptr;
      // add the granted lock to the txn
      TxnAddTableLock(txn, oid, lock_mode);
      return true;
    }  // end if
  }    // end for

  return false;
}
auto LockManager::CheckSatisfyTransitionCond(Transaction *txn, const std::shared_ptr<LockRequest> &request,
                                             LockManager::LockMode upgrade_lock_mode) -> bool {
  std::unique_lock lock(txn->latch_);
  //     While upgrading, only the following transitions should be allowed:
  //  IS -> [S, X, IX, SIX]
  //  S -> [X, SIX]
  //  IX -> [X, SIX]
  //  SIX -> [X]
  switch (request->lock_mode_) {
    case LockMode::SHARED:
      if (upgrade_lock_mode != LockMode::EXCLUSIVE && upgrade_lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(request->txn_id_, AbortReason::INCOMPATIBLE_UPGRADE);
      }
      return true;
    case LockMode::INTENTION_SHARED:
      return upgrade_lock_mode != LockMode::INTENTION_SHARED;
    case LockMode::EXCLUSIVE:
      return false;
    case LockMode::INTENTION_EXCLUSIVE:
      if (upgrade_lock_mode != LockMode::EXCLUSIVE && upgrade_lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(request->txn_id_, AbortReason::INCOMPATIBLE_UPGRADE);
      }
      return true;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (upgrade_lock_mode != LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(request->txn_id_, AbortReason::INCOMPATIBLE_UPGRADE);
      }
      return true;
  }
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { return true; }

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
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
// add the table lock to the txn
void LockManager::TxnAddTableLock(Transaction *txn, const table_oid_t &oid, LockMode lock_mode) {
  // acquire lock on the transaction
  std::lock_guard<std::mutex> lock(txn->latch_);
  /* Check abort conditions */
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }  // end if

  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }  // end if

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

/// Check if there is any conflict 'mode' with some granted requests in the queue
/// \param txn
/// \param mode
/// \param oid
/// \return true if no conflict, false otherwise.
auto LockManager::LockIsFree(LockMode mode, const table_oid_t &oid, const std::shared_ptr<LockRequest> &req) -> bool {
  // stop other threads from running before the upgrade request
  if (table_lock_map_[oid]->upgrade_req_ != nullptr && req != table_lock_map_[oid]->upgrade_req_) {
    return false;
  }

  return std::all_of(table_lock_map_[oid]->request_queue_.begin(), table_lock_map_[oid]->request_queue_.end(),
                     [mode](const std::shared_ptr<LockRequest> &request) { return !IsConflictMode(request, mode); });
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
}  // namespace bustub
