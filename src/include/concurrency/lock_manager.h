//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
#include <stack>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
 public:
  enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };


  class DirectedCycle {
   public:
    // keeps track of which vertex has been processed.
    std::set<txn_id_t> marked_;
    // edge_to[v] = w means v comes to w.
    std::unordered_map<txn_id_t, txn_id_t> edge_to_;
    // keeps track of which vertices the search path have gone through
    std::set<txn_id_t> on_stack_;
    // Directed cycle (Null if no such cycle)
    std::vector<txn_id_t> cycle_;

    std::unordered_map<txn_id_t, std::vector<txn_id_t>> *waits_for_graph_;

    explicit DirectedCycle(std::unordered_map<txn_id_t, std::vector<txn_id_t>> *waits_for)
        : waits_for_graph_(waits_for){};

    void BreakCycle(Transaction * txn) {
      auto id = txn->GetTransactionId();
      // set txn state to abort
      txn->SetState(TransactionState::ABORTED);
      // remove all the edges from this node
      waits_for_graph_->erase(id);
      // remove all the edges that linked to this node
      for (auto& pair : *waits_for_graph_) {
        std::vector<txn_id_t>& vector = pair.second;
        auto it = vector.begin();

        while (it != vector.end()) {
          if (*it == id) {
            // Remove the element from the vector
            it = vector.erase(it);
          } else {
            ++it;
          }
        }
      }
    }

    bool HasCycle(const std::vector<txn_id_t> &vector, txn_id_t *txn_id) {
      // do the search
      for (const auto &tran_id : vector) {
        // not yet marked
        if (marked_.count(tran_id) == 0) {
          Dfs(tran_id);
        }
      }

      // return result
      if(!cycle_.empty()) {
        txn_id_t max = cycle_[0];
        for(const auto& tran_id : cycle_) {
          if (tran_id > max) {
            max = tran_id;
          }
        }
        // set the txn_id to the lowest
        *txn_id = max;
      }

      return !cycle_.empty();
    }

   private:
    void Dfs(txn_id_t v) {
      on_stack_.insert(v);
      marked_.insert(v);
      if(waits_for_graph_->find(v) != waits_for_graph_->end()) {
        for (const auto& w : waits_for_graph_->find(v)->second) {
          // short circuit if directed cycle found
          if (!cycle_.empty()) {
            return;
          }
          // found new vertex, so recur
          if (marked_.count(w) == 0) {
            edge_to_.emplace(w, v);
            Dfs(w);
          }
          // trace back directed cycle
          else if (on_stack_.count(w) == 1) {
            for (int x = v; x != w; x = edge_to_[x]) {
              cycle_.push_back(x);
            }
            cycle_.push_back(w);
            cycle_.push_back(v);
          }
        } // end for
      }
      // uncheck
      on_stack_.erase(v);
    } // end Dfs
  };

  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() {
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
  }

  ~LockManager() {
    enable_cycle_detection_ = false;
    cycle_detection_thread_->join();
    delete cycle_detection_thread_;
  }

  /**
   * Structure to hold a lock request.
   * This could be a lock request on a table OR a row.
   * For table lock requests, the rid_ attribute would be unused.
   */
  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid) /** Table lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid) {}
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid, RID rid) /** Row lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid), rid_(rid) {}

    /** Txn_id of the txn requesting the lock */
    txn_id_t txn_id_;
    /** Locking mode of the requested lock */
    LockMode lock_mode_;
    /** Oid of the table for a table lock; oid of the table the row belong to for a row lock */
    table_oid_t oid_;
    /** Rid of the row for a row lock; unused for table locks */
    RID rid_;
    /** Whether the lock has been granted or not */
    bool granted_{false};
  };

  class LockRequestQueue {
   public:
    /** List of lock requests for the same resource (table or row) */
    std::list<std::shared_ptr<LockRequest>> request_queue_;
    /** For notifying blocked transactions on this rid */
    std::condition_variable cv_;
    /** txn_id of an upgrading transaction. Unset when there is no upgrading request. */
    txn_id_t upgrading_ = INVALID_TXN_ID;
    /** coordination */
    std::mutex latch_;
  };

  /**
   * [LOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both LockTable() and LockRow() are blocking methods; they should wait till the lock is granted and then return.
   *    If the transaction was aborted in the meantime, do not grant the lock and return false.
   *
   *
   * MULTIPLE TRANSACTIONS:
   *    LockManager should maintain a queue for each resource; locks should be granted to transactions in a FIFO manner.
   *    If there are multiple compatible lock requests, all should be granted at the same time
   *    as long as FIFO is honoured.
   *
   * SUPPORTED LOCK MODES:
   *    Table locking should support all lock modes.
   *    Row locking should not support Intention locks. Attempting this should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (ATTEMPTED_INTENTION_LOCK_ON_ROW)
   *
   *
   * ISOLATION LEVEL:
   *    Depending on the ISOLATION LEVEL, a transaction should attempt to take locks:
   *    - Only if required, AND
   *    - Only if allowed
   *
   *    For instance S/IS/SIX locks are not required under READ_UNCOMMITTED, and any such attempt should set the
   *    TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
   *
   *    Similarly, X/IX locks on rows are not allowed if the the Transaction State is SHRINKING, and any such attempt
   *    should set the TransactionState as ABORTED and throw a TransactionAbortException (LOCK_ON_SHRINKING).
   *
   *    REPEATABLE_READ:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        No locks are allowed in the SHRINKING state
   *
   *    READ_COMMITTED:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        Only IS, S locks are allowed in the SHRINKING state
   *
   *    READ_UNCOMMITTED:
   *        The transaction is required to take only IX, X locks.
   *        IX, X locks are allowed in the GROWING state.
   *        S, IS, SIX locks are never allowed
   *
   *
   * MULTILEVEL LOCKING:
   *    While locking rows, Lock() should ensure that the transaction has an appropriate lock on the table which the row
   *    belongs to. For instance, if an exclusive lock is attempted on a row, the transaction must hold either
   *    X, IX, or SIX on the table. If such a lock does not exist on the table, Lock() should set the TransactionState
   *    as ABORTED and throw a TransactionAbortException (TABLE_LOCK_NOT_PRESENT)
   *
   *
   * LOCK UPGRADE:
   *    Calling Lock() on a resource that is already locked should have the following behaviour:
   *    - If requested lock mode is the same as that of the lock presently held,
   *      Lock() should return true since it already has the lock.
   *    - If requested lock mode is different, Lock() should upgrade the lock held by the transaction.
   *
   *    A lock request being upgraded should be prioritised over other waiting lock requests on the same resource.
   *
   *    While upgrading, only the following transitions should be allowed:
   *        IS -> [S, X, IX, SIX]
   *        S -> [X, SIX]
   *        IX -> [X, SIX]
   *        SIX -> [X]
   *    Any other upgrade is considered incompatible, and such an attempt should set the TransactionState as ABORTED
   *    and throw a TransactionAbortException (INCOMPATIBLE_UPGRADE)
   *
   *    Furthermore, only one transaction should be allowed to upgrade its lock on a given resource.
   *    Multiple concurrent lock upgrades on the same resource should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (UPGRADE_CONFLICT).
   *
   *
   * BOOK KEEPING:
   *    If a lock is granted to a transaction, lock manager should update the transaction's lock sets
   *    appropriately (check transaction.h)
   */

  /**
   * [UNLOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both UnlockTable() and UnlockRow() should release the lock on the resource and return.
   *    Both should ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
   *    If not, LockManager should set the TransactionState as ABORTED and throw
   *    a TransactionAbortException (ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD)
   *
   *    Additionally, unlocking a table should only be allowed if the transaction does not hold locks on any
   *    row on that table. If the transaction holds locks on rows of the table, Unlock should set the Transaction State
   *    as ABORTED and throw a TransactionAbortException (TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS).
   *
   *    Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
   *
   * TRANSACTION STATE UPDATE
   *    Unlock should update the transaction state appropriately (depending upon the ISOLATION LEVEL)
   *    Only unlocking S or X locks changes transaction state.
   *
   *    REPEATABLE_READ:
   *        Unlocking S/X locks should set the transaction state to SHRINKING
   *
   *    READ_COMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        Unlocking S locks does not affect transaction state.
   *
   *   READ_UNCOMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        S locks are not permitted under READ_UNCOMMITTED.
   *            The behaviour upon unlocking an S lock under this isolation level is undefined.
   *
   *
   * BOOK KEEPING:
   *    After a resource is unlocked, lock manager should update the transaction's lock sets
   *    appropriately (check transaction.h)
   */

  /**
   * Acquire a lock on table_oid_t in the given lock_mode.
   * If the transaction already holds a lock on the table, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table to be locked in lock_mode
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) noexcept(false) -> bool;

  /**
   * Release the lock held on a table by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param oid the table_oid_t of the table to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool;

  /**
   * Acquire a lock on rid in the given lock_mode.
   * If the transaction already holds a lock on the row, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be locked
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool;

  /**
   * Release the lock held on a row by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param rid the RID that is locked by the transaction
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool;

  /*** Graph API ***/

  /**
   * Adds an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto AddEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Removes an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto RemoveEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  auto HasCycle(txn_id_t *txn_id) -> bool;

  /**
   * @return all edges in current waits_for graph
   */
  auto GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>>;

  /**
   * Runs cycle detection in the background.
   */
  auto RunCycleDetection() -> void;

  static void TxnAddTableLock(Transaction *p_transaction, const table_oid_t &oid, LockMode mode);
  static void TxnAddRowLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid);
  auto LockIsFree(Transaction *txn, LockMode mode, const std::shared_ptr<LockRequestQueue> &table) -> bool;
  auto IsHeldLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid,
                  std::shared_ptr<LockRequestQueue> &table, std::mutex &queue_lock, bool &is_abort) -> bool;
  void CheckSatisfyTransitionCond(Transaction *txn, const std::shared_ptr<LockRequest> &request,
                                  LockMode upgrade_lock_mode, std::mutex &queue_lock);
  void CheckSatisfyRowTransitionCond(Transaction *txn, const std::shared_ptr<LockRequest> &request,
                                     LockMode row_lock_mode, std::mutex &queue_lock);
  static auto NotConflictMode(const std::shared_ptr<LockRequest> &request, LockMode mode, Transaction *txn) -> bool;
  void CheckAbortCond(Transaction *txn, const table_oid_t &oid, LockMode mode, bool is_lock_row = false);
  void CheckRowTableCompatible(Transaction *txn, const table_oid_t &oid, LockMode row_mode);
  auto IsHeldLockRow(Transaction *txn, LockMode row_lock_mode, const table_oid_t &oid, const RID &rid,
                     const std::shared_ptr<LockRequestQueue> &row, bool &is_abort) -> bool;
  auto static NotConflictRowMode(const std::shared_ptr<LockRequest> &request, LockMode mode, Transaction *txn) -> bool;
  void CheckTableUnlockAbortCond(Transaction *txn, const table_oid_t &oid,
                                 const std::shared_ptr<LockRequestQueue> &table,
                                 std::list<std::shared_ptr<LockRequest>>::iterator &table_iterator);
  void CheckRowUnlockAbortCond(Transaction *txn, const std::shared_ptr<LockRequestQueue> &table,
                               std::list<std::shared_ptr<LockRequest>>::iterator &row_iterator);
  auto IsTxnHoldRowLock(Transaction *txn, const table_oid_t &oid) const -> bool;
  static void SetTxnState(Transaction *txn, LockMode mode);
  auto RowLockIsFree(Transaction *txn, LockMode mode, const std::shared_ptr<LockRequestQueue> &table) -> bool;
  void BuildGraph();
  void ProcessTableResources();
  void ProcessRowResources();
 private:
  /** Fall 2022 */
  /** Structure that holds lock requests for a given table oid */
  std::unordered_map<table_oid_t, std::shared_ptr<LockRequestQueue>> table_lock_map_;
  /** Coordination */
  std::mutex table_lock_map_latch_;

  /** Structure that holds lock requests for a given RID */
  std::unordered_map<RID, std::shared_ptr<LockRequestQueue>> row_lock_map_;
  /** Coordination */
  std::mutex row_lock_map_latch_;

  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;
  std::mutex waits_for_latch_;
  /** Waits-for graph representation. */
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
  std::unique_ptr<DirectedCycle> directed_cycle_{nullptr};
  std::unordered_map<txn_id_t , table_oid_t> waits_for_table_;
  std::unordered_map<txn_id_t , RID> waits_for_row_;
};

}  // namespace bustub
