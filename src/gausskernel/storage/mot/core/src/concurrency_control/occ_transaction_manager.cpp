/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * occ_transaction_manager.cpp
 *    Optimistic Concurrency Control (OCC) implementation
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/concurrency_control/occ_transaction_manager.h
 *
 * -------------------------------------------------------------------------
 */

#include "occ_transaction_manager.h"
#include "../utils/utilities.h"
#include "cycles.h"
#include "mot_engine.h"
#include "row.h"
#include "row_header.h"
#include "txn.h"
#include "txn_access.h"
#include "checkpoint_manager.h"
#include "mm_session_api.h"
#include "mot_error.h"
#include <pthread.h>
#include "../../../fdw_adapter/src/mot_internal.h"//ADDBY NEU
namespace MOT {
DECLARE_LOGGER(OccTransactionManager, ConcurrenyControl);

OccTransactionManager::OccTransactionManager()
    : m_txnCounter(0),
      m_abortsCounter(0),
      m_writeSetSize(0),
      m_rowsSetSize(0),
      m_deleteSetSize(0),
      m_insertSetSize(0),
      m_dynamicSleep(100),
      m_rowsLocked(false),
      m_preAbort(true),
      m_validationNoWait(true)
{}

OccTransactionManager::~OccTransactionManager()
{}

bool OccTransactionManager::Init()
{
    bool result = true;
    return result;
}

bool OccTransactionManager::CheckVersion(const Access* access)
{
    // We always validate on committed rows!
    const Row* row = access->GetRowFromHeader();
    return (row->m_rowHeader.GetCSN() == access->m_tid);
}

bool OccTransactionManager::QuickHeaderValidation(const Access* access)
{
    if (access->m_type != INS) {
        // For WR/DEL/RD_FOR_UPDATE lets verify CSN
        return CheckVersion(access);
    } else {
        // Lets verify the inserts
        // For upgrade we verify  the row
        // csn has not changed!
        Sentinel* sent = access->m_origSentinel;
        if (access->m_params.IsUpgradeInsert()) {
            if (access->m_params.IsDummyDeletedRow()) {
                // Check is sentinel is deleted and CSN is VALID -  ABA problem
                if (sent->IsCommited() == false) {
                    if (sent->GetData()->GetCommitSequenceNumber() != access->m_tid) {
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                // We deleted internally!, we only need to check version
                if (sent->GetData()->GetCommitSequenceNumber() != access->m_tid) {
                    return false;
                }
            }
        } else {
            // If the sent is committed or inserted-deleted we abort!
            if (sent->IsCommited() or sent->GetData() != nullptr) {
                return false;
            }
        }
    }

    return true;
}

bool OccTransactionManager::ValidateReadSet(TxnManager* txMan)
{
    TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
    for (const auto& raPair : orderedSet) {
        const Access* ac = raPair.second;
        if (ac->m_type != RD) {
            continue;
        }
        if (!ac->GetRowFromHeader()->m_rowHeader.ValidateRead(ac->m_tid)) {
            return false;
        }
    }

    return true;
}

bool OccTransactionManager::ValidateWriteSet(TxnManager* txMan)
{
    TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
    for (const auto& raPair : orderedSet) {
        const Access* ac = raPair.second;
        if (ac->m_type == RD) {
            continue;
        }

        if (!QuickHeaderValidation(ac)) {
            return false;
        }
    }
    return true;
}

RC OccTransactionManager::LockRows(TxnManager* txMan, uint32_t& numRowsLock)
{
    RC rc = RC_OK;
    TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
    numRowsLock = 0;
    for (const auto& raPair : orderedSet) {
        const Access* ac = raPair.second;
        if (ac->m_type == RD) {
            continue;
        }
        if (ac->m_params.IsPrimarySentinel()) {
            Row* row = ac->GetRowFromHeader();
            row->m_rowHeader.Lock();
            numRowsLock++;
            MOT_ASSERT(row->GetPrimarySentinel()->IsLocked() == true);
        }
    }

    return rc;
}

bool OccTransactionManager::LockHeadersNoWait(TxnManager* txMan, uint32_t& numSentinelsLock)
{
    uint64_t sleepTime = 1;
    uint64_t thdId = txMan->GetThdId();
    TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
    numSentinelsLock = 0;
    while (numSentinelsLock != m_writeSetSize) {
        for (const auto& raPair : orderedSet) {
            const Access* ac = raPair.second;
            if (ac->m_type == RD) {
                continue;
            }
            Sentinel* sent = ac->m_origSentinel;
            if (!sent->TryLock(thdId)) {
                break;
            }
            numSentinelsLock++;
            if (ac->m_params.IsPrimaryUpgrade()) {
                ac->m_auxRow->m_rowHeader.Lock();
            }
            // New insert row is already committed!
            // Check if row has changed in sentinel
            if (!QuickHeaderValidation(ac)) {
                return false;
            }
        }

        if (numSentinelsLock != m_writeSetSize) {
            ReleaseHeaderLocks(txMan, numSentinelsLock);
            numSentinelsLock = 0;
            if (m_preAbort) {
                for (const auto& acPair : orderedSet) {
                    const Access* ac = acPair.second;
                    if (!QuickHeaderValidation(ac)) {
                        return false;
                    }
                }
            }
            if (sleepTime > LOCK_TIME_OUT) {
                return false;
            } else {
                if (IsHighContention() == false) {
                    CpuCyclesLevelTime::Sleep(5);
                } else {
                    usleep(m_dynamicSleep);
                }
                sleepTime = sleepTime << 1;
            }
        }
    }

    return true;
}

RC OccTransactionManager::LockHeaders(TxnManager* txMan, uint32_t& numSentinelsLock)
{
    RC rc = RC_OK;
    uint64_t thdId = txMan->GetThdId();
    TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
    numSentinelsLock = 0;
    if (m_validationNoWait) {
        if (!LockHeadersNoWait(txMan, numSentinelsLock)) {
            rc = RC_ABORT;
            goto final;
        }
    } else {
        for (const auto& raPair : orderedSet) {
            const Access* ac = raPair.second;
            if (ac->m_type == RD) {
                continue;
            }
            Sentinel* sent = ac->m_origSentinel;
            sent->Lock(thdId);
            numSentinelsLock++;
            if (ac->m_params.IsPrimaryUpgrade()) {
                ac->m_auxRow->m_rowHeader.Lock();
            }
            // New insert row is already committed!
            // Check if row has chained in sentinel
            if (!QuickHeaderValidation(ac)) {
                rc = RC_ABORT;
                goto final;
            }
        }
    }
final:
    return rc;
}

bool OccTransactionManager::PreAllocStableRow(TxnManager* txMan)
{
    if (GetGlobalConfiguration().m_enableCheckpoint) {
        GetCheckpointManager()->BeginCommit(txMan);

        TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
        for (const auto& raPair : orderedSet) {
            const Access* access = raPair.second;
            if (access->m_type == RD) {
                continue;
            }
            if (access->m_params.IsPrimarySentinel()) {
                if (!GetCheckpointManager()->PreAllocStableRow(txMan, access->GetRowFromHeader(), access->m_type)) {
                    GetCheckpointManager()->FreePreAllocStableRows(txMan);
                    GetCheckpointManager()->EndCommit(txMan);
                    return false;
                }
            }
        }
    }
    return true;
}

bool OccTransactionManager::QuickVersionCheck(TxnManager* txMan, uint32_t& readSetSize)
{
    int isolationLevel = txMan->GetTxnIsoLevel();
    TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
    readSetSize = 0;
    for (const auto& raPair : orderedSet) {
        const Access* ac = raPair.second;
        if (ac->m_params.IsPrimarySentinel()) {
            m_rowsSetSize++;
        }
        switch (ac->m_type) {
            case RD_FOR_UPDATE:
            case WR:
                m_writeSetSize++;
                break;
            case DEL:
                m_writeSetSize++;
                m_deleteSetSize++;
                break;
            case INS:
                m_insertSetSize++;
                m_writeSetSize++;
                break;
            case RD:
                if (isolationLevel > READ_COMMITED) {
                    readSetSize++;
                } else {
                    continue;
                }
                break;
            default:
                break;
        }

        if (m_preAbort) {
            if (!QuickHeaderValidation(ac)) {
                return false;
            }
        }
    }
    return true;
}

RC OccTransactionManager::ValidateOcc(TxnManager* txMan)
{
    uint32_t numSentinelLock = 0;
    m_rowsLocked = false;
    TxnAccess* tx = txMan->m_accessMgr.Get();
    RC rc = RC_OK;
    const uint32_t rowCount = tx->m_rowCnt;

    m_writeSetSize = 0;
    m_rowsSetSize = 0;
    m_deleteSetSize = 0;
    m_insertSetSize = 0;
    m_txnCounter++;

    if (rowCount == 0) {
        // READONLY
        return rc;
    }

    uint32_t readSetSize = 0;
    TxnOrderedSet_t& orderedSet = tx->GetOrderedRowSet();
    MOT_ASSERT(rowCount == orderedSet.size());

    /* Perform Quick Version check */
    if (!QuickVersionCheck(txMan, readSetSize)) {
        rc = RC_ABORT;
        goto final;
    }

    MOT_LOG_DEBUG("Validate OCC rowCnt=%u RD=%u WR=%u\n", tx->m_rowCnt, tx->m_rowCnt - m_writeSetSize, m_writeSetSize);
    rc = LockHeaders(txMan, numSentinelLock);
    if (rc != RC_OK) {
        goto final;
    }

    // Validate rows in the read set and write set
    if (readSetSize > 0) {
        if (!ValidateReadSet(txMan)) {
            rc = RC_ABORT;
            goto final;
        }
    }

    if (!ValidateWriteSet(txMan)) {
        rc = RC_ABORT;
        goto final;
    }

    // Pre-allocate stable row according to the checkpoint state.
    if (!PreAllocStableRow(txMan)) {
        rc = RC_MEMORY_ALLOCATION_ERROR;
        goto final;
    }

final:
    if (likely(rc == RC_OK)) {
        MOT_ASSERT(numSentinelLock == m_writeSetSize);
        m_rowsLocked = true;
    } else {
        ReleaseHeaderLocks(txMan, numSentinelLock);
        if (likely(rc == RC_ABORT)) {
            m_abortsCounter++;
        }
    }

    return rc;
}

void OccTransactionManager::RollbackInserts(TxnManager* txMan)
{
    return txMan->UndoInserts();
}

void OccTransactionManager::ApplyWrite(TxnManager* txMan)
{
    if (GetGlobalConfiguration().m_enableCheckpoint) {
        TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
        for (const auto& raPair : orderedSet) {
            const Access* access = raPair.second;
            if (access->m_type == RD) {
                continue;
            }
            if (access->m_params.IsPrimarySentinel()) {
                // Pass the actual global row (access->GetRowFromHeader()), so that the stable row will have the
                // same CSN, rowid, etc as the original row before the modifications are applied.
                GetCheckpointManager()->ApplyWrite(txMan, access->GetRowFromHeader(), access->m_type);
            }
        }
    }
}

void OccTransactionManager::WriteChanges(TxnManager* txMan)
{
    if (m_writeSetSize == 0 && m_insertSetSize == 0) {
        return;
    }
    //ADDBY NEU
    // LockRows(txMan, m_rowsSetSize);
    TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
    if(is_full_async_exec == false)
        for (const auto& raPair : orderedSet) {
            const Access* access = raPair.second;
            Row* row = access->GetRowFromHeader();
            AccessType type = access->m_type;
            if (type == RD) {
                continue;
            }
            row->GetRowHeader()->Lock();
            row->GetRowHeader()->LockStable();
        }

    // Stable rows for checkpoint needs to be created (copied from original row) before modifying the global rows.
    ApplyWrite(txMan);

    // Update CSN with all relevant information on global rows
    // For deletes invalidate sentinels - rows still locked!
    for (const auto& raPair : orderedSet) {
        const Access* access = raPair.second;
        access->GetRowFromHeader()->m_rowHeader.WriteChangesToRow(access, txMan->GetCommitSequenceNumber());
    }

    // Treat Inserts
    if (m_insertSetSize > 0) {
        for (const auto& raPair : orderedSet) {
            Access* access = raPair.second;
            if (access->m_type != INS) {
                continue;
            }
            MOT_ASSERT(access->m_origSentinel->IsLocked() == true);
            if (access->m_params.IsUpgradeInsert() == false) {
                if (access->m_params.IsPrimarySentinel()) {
                    MOT_ASSERT(access->m_origSentinel->IsDirty() == true);
                    // Connect row and sentinel, row is set to absent and locked
                    access->m_origSentinel->SetNextPtr(access->GetRowFromHeader());
                    // Current state: row is set to absent,sentinel is locked and not dirty
                    // Readers will not see the row
                    access->GetTxnRow()->GetTable()->UpdateRowCount(1);
                } else {
                    // We only set the in the secondary sentinel!
                    access->m_origSentinel->SetNextPtr(access->GetRowFromHeader()->GetPrimarySentinel());
                }
            } else {
                MOT_ASSERT(access->m_params.IsUniqueIndex() == true);
                // Rows are locked and marked as deleted
                if (access->m_params.IsPrimarySentinel()) {
                    /* Switch the locked row's in the sentinel
                     * The old row is locked and marked deleted
                     * The new row is locked
                     * Save previous row in the access!
                     * We need it for the row release!
                     */
                    Row* row = access->GetRowFromHeader();
                    access->m_localInsertRow = row;
                    access->m_origSentinel->SetNextPtr(access->m_auxRow);
                    // Add row to GC!
                    txMan->GetGcSession()->GcRecordObject(row->GetTable()->GetPrimaryIndex()->GetIndexId(),
                        row,
                        nullptr,
                        Row::RowDtor,
                        ROW_SIZE_FROM_POOL(row->GetTable()));
                } else {
                    // Set Sentinel for
                    access->m_origSentinel->SetNextPtr(access->m_auxRow->GetPrimarySentinel());
                }
                // upgrade should not change the reference count!
                if (access->m_origSentinel->IsCommited()) {
                    access->m_origSentinel->SetUpgradeCounter();
                }
            }
        }
    }

    // Treat Inserts
    if (m_insertSetSize > 0) {
        for (const auto& raPair : orderedSet) {
            const Access* access = raPair.second;
            if (access->m_type != INS) {
                continue;
            }
            access->m_origSentinel->UnSetDirty();
        }
    }

    CleanRowsFromIndexes(txMan);

    if(is_full_async_exec == false) {
         for (const auto& raPair : orderedSet) {
            const Access* access = raPair.second;
            Row* row = access->GetRowFromHeader();
            AccessType type = access->m_type;
            if (type == RD) {
                continue;
            }
            row->GetRowHeader()->ReleaseStable();
            row->GetRowHeader()->Release();
        }
    }
    else {
        auto csn = txMan->GetCommitSequenceNumber();
        for (const auto& raPair : orderedSet) {
            const Access* access = raPair.second;
            Row* row = access->GetRowFromHeader();
            AccessType type = access->m_type;
            if (type == RD) {
                continue;
            }
            if(row->GetRowHeader()->GetCSN() == csn && row->GetRowHeader()->GetServerId() == local_ip_index) {
                row->GetRowHeader()->ReleaseStable();
                row->GetRowHeader()->Release();
            }
        }
    }

}

void OccTransactionManager::CleanRowsFromIndexes(TxnManager* txMan)
{
    if (m_deleteSetSize == 0) {
        return;
    }

    TxnAccess* tx = txMan->m_accessMgr.Get();
    TxnOrderedSet_t& orderedSet = tx->GetOrderedRowSet();
    uint32_t numOfDeletes = m_deleteSetSize;
    // use local counter to optimize
    for (const auto& raPair : orderedSet) {
        const Access* access = raPair.second;
        if (access->m_type == DEL) {
            numOfDeletes--;
            access->GetTxnRow()->GetTable()->UpdateRowCount(-1);
            MOT_ASSERT(access->m_params.IsUpgradeInsert() == false);
            // Use Txn Row as row may change INSERT after DELETE leaves residue
            txMan->RemoveKeyFromIndex(access->GetTxnRow(), access->m_origSentinel);
        }
        if (!numOfDeletes) {
            break;
        }
    }
}

void OccTransactionManager::ReleaseHeaderLocks(TxnManager* txMan, uint32_t numOfLocks)
{
    if (numOfLocks == 0) {
        return;
    }

    TxnAccess* tx = txMan->m_accessMgr.Get();
    TxnOrderedSet_t& orderedSet = tx->GetOrderedRowSet();
    // use local counter to optimize
    for (const auto& raPair : orderedSet) {
        const Access* access = raPair.second;
        if (access->m_type == RD) {
            continue;
        } else {
            numOfLocks--;
            access->m_origSentinel->Release();
        }
        if (!numOfLocks) {
            break;
        }
    }
}

void OccTransactionManager::ReleaseRowsLocks(TxnManager* txMan, uint32_t numOfLocks)
{
    if (numOfLocks == 0) {
        return;
    }

    TxnAccess* tx = txMan->m_accessMgr.Get();
    TxnOrderedSet_t& orderedSet = tx->GetOrderedRowSet();

    // use local counter to optimize
    for (const auto& raPair : orderedSet) {
        const Access* access = raPair.second;
        if (access->m_type == RD) {
            continue;
        }

        if (access->m_params.IsPrimarySentinel()) {
            numOfLocks--;
            access->GetRowFromHeader()->m_rowHeader.Release();
            if (access->m_params.IsUpgradeInsert()) {
                // This is the global row that we switched!
                // Currently it's in the gc!
                access->m_localInsertRow->m_rowHeader.Release();
            }
        }
        if (!numOfLocks) {
            break;
        }
    }
}

void OccTransactionManager::CleanUp()
{
    m_writeSetSize = 0;
    m_insertSetSize = 0;
    m_rowsSetSize = 0;
}


//ADDBY NEU

bool OccTransactionManager::ValidateReadInMerge(TxnManager * txMan, uint32_t server_id){
    if(txMan->GetStartLogicalEpoch() == MOTAdaptor::GetLogicalEpoch() && is_full_async_exec == false) return true;
    TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
    bool result = true;
    for (const auto &raPair : orderedSet)
    {
        const Access *ac = raPair.second;
        if (ac->m_type == RD)
        {
            // if (!ac->GetRowFromHeader()->m_rowHeader.ValidateRead(ac->m_cts))
            if (!ac->GetRowFromHeader()->m_rowHeader.ValidateReadI(ac->m_cts, ac->m_server_id))
            {
                return false;
            }
        }
    }
    return result;
}
bool OccTransactionManager::ValidateReadInMergeForSnap(TxnManager * txMan, uint32_t server_id){
    auto start_logical_epoch = txMan->GetStartLogicalEpoch(); 
    if(start_logical_epoch == MOTAdaptor::GetLogicalEpoch() && is_full_async_exec == false) return true;
    TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
    bool result = true;
    for (const auto &raPair : orderedSet)
    {
        const Access *ac = raPair.second;
        if (ac->m_type == RD)
        {
            // if (!ac->GetRowFromHeader()->m_rowHeader.ValidateRead(ac->m_cts))
            if (!ac->GetRowFromHeader()->m_rowHeader.ValidateReadForSnap(ac->m_cts, start_logical_epoch, ac->m_server_id))
            {
                return false;
            }
        }
    }
    return result;
}



void OccTransactionManager::recoverRowHeader(TxnManager * txMan, uint32_t server_id){
    TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
    std::string table_name, key, key_temp, csn_temp, csn_result;
    uint64_t currentCSN;
    MOT::Key* key_ptr;
    MOT::Row* row;
    void* buf;
    currentCSN = txMan->GetCommitSequenceNumber();
    csn_temp = std::to_string(currentCSN) + ":" + std::to_string(server_id);
    for (const auto &raPair : orderedSet){
        const Access *ac = raPair.second;
        if (ac->m_type == RD){
            continue;
        }
        if(ac->m_type == INS){
            table_name = ac->m_localInsertRow->GetTable()->GetLongTableName();
            key_ptr = ac->m_localInsertRow->GetTable()->BuildKeyByRow(ac->m_localInsertRow, txMan, buf);
            if(key_ptr == nullptr) assert(false);
            key = key_ptr->GetKeyStr();
            key_temp = table_name + key;
            MOTAdaptor::insertSet.remove(key_temp, csn_temp);
            MOT::MemSessionFree(buf);
            continue;
        }
        if (!ac->GetRowFromHeader()->m_rowHeader.GetCSN() != txMan->GetCommitSequenceNumber()){
            continue; //already modify by other txn , no need to recover
        }
        else{
            ac->GetRowFromHeader()->m_rowHeader.RecoverToStable(); 
        }
    }
}

RC OccTransactionManager::CommitPhase(TxnManager *txMan, uint32_t server_id)
{
    bool result = ValidateAndSetWriteSet(txMan, server_id);
    if (result) {
        return RC::RC_OK;
    }
    //是否需要进行recovery
    // recoverRowHeader(txMan,server_id);//txn abort, need to recover the RowHeader
    return RC::RC_ABORT;
}

bool OccTransactionManager::ValidateAndSetWriteSet(TxnManager *txMan, uint32_t server_id)//Commit Phase set csn
{
    TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
    bool result = true;
    std::string table_name, key, key_temp, csn_temp, csn_result;
    uint64_t currentCSN;
    MOT::Row* row;
    currentCSN = txMan->GetCommitSequenceNumber();
    csn_temp = std::to_string(currentCSN) + ":" + std::to_string(server_id);
    for (const auto &raPair : orderedSet){
        const Access *ac = raPair.second;
        if (ac->m_type == RD){
            continue;
        }
        else if(ac->m_type == INS) { //已经生成了localInsertRow 问题在于先后插入，先插入的已经完成后将dirty置为true
            if(ac->m_localInsertRow->GetTable() == nullptr){
                result = false;
                continue;
            }
            table_name = ac->m_localInsertRow->GetTable()->GetLongTableName();
            void* buf;
            MOT::Key* key_ptr = ac->m_localInsertRow->GetTable()->BuildKeyByRow(ac->m_localInsertRow, txMan, buf);
            // MOT::Key* key_ptr = txMan->GetTxnKey(ac->m_localInsertRow->GetTable()->GetPrimaryIndex());
            if(key_ptr == nullptr) assert(false);
            key = key_ptr->GetKeyStr();
            row = ac->GetSentinel()->GetData();
            if (!(row == nullptr || row->IsAbsentRow())) {
                result = false;
            }
            key_temp = table_name + key;
            if(!MOTAdaptor::insertSetForCommit.insert(key_temp, csn_temp, &csn_result)){
                result = false;
            }
            MOTAdaptor::abort_transcation_csn_set.insert(csn_result, csn_result);
            // MOT::MemSessionFree(buf);   
        }
        else{ // update or delete
            if (ac->m_localRow->GetTable() == nullptr){
                result = false;
                continue;
            }
            if (ac->m_origSentinel == nullptr || ac->m_origSentinel->IsDirty()) {
                result = false;
                continue;
            }
            if(!ac->GetRowFromHeader()->m_rowHeader.ValidateAndSetWriteForCommit(txMan->GetCommitSequenceNumber(), txMan->GetStartEpoch(), txMan->GetCommitEpoch(), server_id)){
                result = false;
            }
        }
    }
    if(result == false){
        MOTAdaptor::abort_transcation_csn_set.insert(csn_temp, csn_temp);
    } 
    return result;
}

RC OccTransactionManager::CommitCheck(TxnManager *txMan, uint32_t server_id){ //not use
    bool result = ValidateWriteSetForCommit(txMan, server_id);
    if (result){
        return RC::RC_OK;
    }
    return RC::RC_ABORT;
}

bool OccTransactionManager::ValidateWriteSetForCommit(TxnManager *txMan, uint32_t server_id){ //not use
    std::string table_name, key, key_temp, csn_temp;
    MOT::Key* key_ptr;
    void* buf;
    csn_temp = std::to_string(txMan->GetCommitSequenceNumber())+ ":" + std::to_string(server_id);
    TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
    for (const auto &raPair : orderedSet)
    {
        const Access *ac = raPair.second;
        if (ac->m_type == RD or !ac->m_params.IsPrimarySentinel()){
            continue;
        }
        else if(ac->m_type == INS){
            table_name = ac->m_localInsertRow->GetTable()->GetLongTableName();
            key_ptr = ac->m_localInsertRow->GetTable()->BuildKeyByRow(ac->m_localInsertRow, txMan, buf);
            if(key_ptr == nullptr) assert(false);
            key_temp = table_name + key_ptr->GetKeyStr();
            // MOT::MemSessionFree(buf);
            if(MOTAdaptor::insertSetForCommit.contain(key_temp, csn_temp) == false){
                return false;
            }
        }
        else{
            MOT::RowHeader& row_header = ac->GetRowFromHeader()->m_rowHeader;
            if(row_header.GetStableCSN() != txMan->GetCommitSequenceNumber()  || row_header.GetStableServerId() != server_id)
                return false;
        }
    }
    return true;
}

void OccTransactionManager::updateInsertSetSize(TxnManager * txMan){
    TxnAccess *tx = txMan->m_accessMgr.Get();
    const uint32_t rowCount = tx->m_rowCnt;

    m_writeSetSize = 0;
    m_rowsSetSize = 0;
    m_deleteSetSize = 0;
    m_insertSetSize = 0;
    m_txnCounter++;

    TxnOrderedSet_t &orderedSet = tx->GetOrderedRowSet();
    MOT_ASSERT(rowCount == orderedSet.size());

    for (const auto &raPair : orderedSet)
    {
        const Access *ac = raPair.second;
        if (ac->m_params.IsPrimarySentinel())
        {
            m_rowsSetSize++;
        }
        switch (ac->m_type)
        {
            case WR:
                m_writeSetSize++;
                break;
            case DEL:
                m_writeSetSize++;
                m_deleteSetSize++;
                break;
            case INS:
                m_insertSetSize++;
                m_writeSetSize++;
                break;
            case RD: /// now only support the "READ-COMMITTED" isolation
                break;
            default:
                break;
        }
    }
}

bool OccTransactionManager::IsReadOnly(TxnManager * txMan){
    // return txMan->m_accessMgr.Get()->m_rowCnt == 0;
    return m_writeSetSize == 0;
}

}  // namespace MOT
