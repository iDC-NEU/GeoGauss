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
 * mot_internal.cpp
 *    MOT Foreign Data Wrapper internal interfaces to the MOT engine.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/fdw_adapter/src/mot_internal.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <google/protobuf/io/gzip_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include <ostream>
#include <istream>
#include <iomanip>
#include <pthread.h>
#include <cstring>
#include "message.pb.h"
#include "postgres.h"
#include "access/dfs/dfs_query.h"
#include "access/sysattr.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "utils/syscache.h"
#include "executor/executor.h"
#include "storage/ipc.h"
#include "commands/dbcommands.h"
#include "knl/knl_session.h"

#include "mot_internal.h"
#include "row.h"
#include "log_statistics.h"
#include "spin_lock.h"
#include "txn.h"
#include "table.h"
#include "utilities.h"
#include "mot_engine.h"
#include "sentinel.h"
#include "txn.h"
#include "txn_access.h"
#include "index_factory.h"
#include "column.h"
#include "mm_raw_chunk_store.h"
#include "ext_config_loader.h"
#include "config_manager.h"
#include "mot_error.h"
#include "utilities.h"
#include "jit_context.h"
#include "mm_cfg.h"
#include "jit_statistics.h"
#include "gaussdb_config_loader.h"
#include <cstdio> 
#include <stdio.h>
#include <iostream>
#include "string"
#include "sstream"
#include <fstream>
#include <atomic>
#include <sys/time.h>
// #include "neu_concurrency_tools/blockingconcurrentqueue.h"
// #include "neu_concurrency_tools/blocking_mpmc_queue.h"
// #include "neu_concurrency_tools/ThreadPool.h"
// #include "epoch_merge.h"
#include "zmq.hpp"
#include "zmq.h"
#include <sched.h>
#include "utils/timestamp.h"
#include "postmaster/postmaster.h"

using merge::MergeRequest;

#define IS_CHAR_TYPE(oid) (oid == VARCHAROID || oid == BPCHAROID || oid == TEXTOID || oid == CLOBOID || oid == BYTEAOID)
#define IS_INT_TYPE(oid)                                                                                           \
    (oid == BOOLOID || oid == CHAROID || oid == INT8OID || oid == INT2OID || oid == INT4OID || oid == FLOAT4OID || \
        oid == FLOAT8OID || oid == INT1OID || oid == DATEOID || oid == TIMEOID || oid == TIMESTAMPOID ||           \
        oid == TIMESTAMPTZOID)

MOT::MOTEngine* MOTAdaptor::m_engine = nullptr;
static XLOGLogger xlogger;

// enable MOT Engine logging facilities
DECLARE_LOGGER(InternalExecutor, FDW)

/** @brief on_proc_exit() callback for cleaning up current thread - only when thread pool is ENABLED. */
static void MOTCleanupThread(int status, Datum ptr);

/** @brief Helper for cleaning up all JIT context objects stored in all CachedPlanSource of the current session. */
static void DestroySessionJitContexts();

// in a thread-pooled environment we need to ensure thread-locals are initialized properly
static inline void EnsureSafeThreadAccessInline()
{
    if (MOTCurrThreadId == INVALID_THREAD_ID) {
        MOT_LOG_DEBUG("Initializing safe thread access for current thread");
        MOT::AllocThreadId();
        // register for cleanup only once - not having a current thread id is the safe indicator we never registered
        // proc-exit callback for this thread
        if (g_instance.attr.attr_common.enable_thread_pool) {
            on_proc_exit(MOTCleanupThread, PointerGetDatum(nullptr));
            MOT_LOG_DEBUG("Registered current thread for proc-exit callback for thread %p", (void*)pthread_self());
        }
    }
    if (MOTCurrentNumaNodeId == MEM_INVALID_NODE) {
        MOT::InitCurrentNumaNodeId();
    }
    MOT::InitMasstreeThreadinfo();
}

extern void EnsureSafeThreadAccess()
{
    EnsureSafeThreadAccessInline();
}

static void DestroySession(MOT::SessionContext* sessionContext)
{
    MOT_ASSERT(MOTAdaptor::m_engine);
    MOT_LOG_DEBUG("Destroying session context %p, connection_id %u", sessionContext, sessionContext->GetConnectionId());

    if (u_sess->mot_cxt.jit_session_context_pool) {
        JitExec::FreeSessionJitContextPool(u_sess->mot_cxt.jit_session_context_pool);
    }
    MOT::GetSessionManager()->DestroySessionContext(sessionContext);
}

// Global map of PG session identification (required for session statistics)
// This approach is safer than saving information in the session context
static pthread_spinlock_t sessionDetailsLock;
typedef std::map<MOT::SessionId, pair<::ThreadId, pg_time_t>> SessionDetailsMap;
static SessionDetailsMap sessionDetailsMap;

static void InitSessionDetailsMap()
{
    pthread_spin_init(&sessionDetailsLock, 0);
}

static void DestroySessionDetailsMap()
{
    pthread_spin_destroy(&sessionDetailsLock);
}

static void RecordSessionDetails()
{
    MOT::SessionId sessionId = u_sess->mot_cxt.session_id;
    if (sessionId != INVALID_SESSION_ID) {
        pthread_spin_lock(&sessionDetailsLock);
        sessionDetailsMap.emplace(sessionId, std::make_pair(t_thrd.proc->pid, t_thrd.proc->myStartTime));
        pthread_spin_unlock(&sessionDetailsLock);
    }
}

static void ClearSessionDetails(MOT::SessionId sessionId)
{
    if (sessionId != INVALID_SESSION_ID) {
        pthread_spin_lock(&sessionDetailsLock);
        SessionDetailsMap::iterator itr = sessionDetailsMap.find(sessionId);
        if (itr != sessionDetailsMap.end()) {
            sessionDetailsMap.erase(itr);
        }
        pthread_spin_unlock(&sessionDetailsLock);
    }
}

inline void ClearCurrentSessionDetails()
{
    ClearSessionDetails(u_sess->mot_cxt.session_id);
}

static void GetSessionDetails(MOT::SessionId sessionId, ::ThreadId* gaussSessionId, pg_time_t* sessionStartTime)
{
    // although we have the PGPROC in the user data of the session context, we prefer not to use
    // it due to safety (in some unknown constellation we might hold an invalid pointer)
    // it is much safer to save a copy of the two required fields
    pthread_spin_lock(&sessionDetailsLock);
    SessionDetailsMap::iterator itr = sessionDetailsMap.find(sessionId);
    if (itr != sessionDetailsMap.end()) {
        *gaussSessionId = itr->second.first;
        *sessionStartTime = itr->second.second;
    }
    pthread_spin_unlock(&sessionDetailsLock);
}

// provide safe session auto-cleanup in case of missing session closure
// This mechanism relies on the fact that when a session ends, eventually its thread is terminated
// ATTENTION: in thread-pooled envelopes this assumption no longer holds true, since the container thread keeps
// running after the session ends, and a session might run each time on a different thread, so we
// disable this feature, instead we use this mechanism to generate thread-ended event into the MOT Engine
static pthread_key_t sessionCleanupKey;

static void SessionCleanup(void* key)
{
    MOT_ASSERT(!g_instance.attr.attr_common.enable_thread_pool);

    // in order to ensure session-id cleanup for session 0 we use positive values
    MOT::SessionId sessionId = (MOT::SessionId)(((uint64_t)key) - 1);
    if (sessionId != INVALID_SESSION_ID) {
        MOT_LOG_WARN("Encountered unclosed session %u (missing call to DestroyTxn()?)", (unsigned)sessionId);
        ClearSessionDetails(sessionId);
        MOT_LOG_DEBUG("SessionCleanup(): Calling DestroySessionJitContext()");
        DestroySessionJitContexts();
        if (MOTAdaptor::m_engine) {
            MOT::SessionContext* sessionContext = MOT::GetSessionManager()->GetSessionContext(sessionId);
            if (sessionContext != nullptr) {
                DestroySession(sessionContext);
            }
            // since a call to on_proc_exit(destroyTxn) was probably missing, we should also cleanup thread-locals
            // pay attention that if we got here it means the thread pool is disabled, so we must ensure thread-locals
            // are cleaned up right now. Due to these complexities, onCurrentThreadEnding() was designed to be proof
            // for repeated calls.
            MOTAdaptor::m_engine->OnCurrentThreadEnding();
        }
    }
}

static void InitSessionCleanup()
{
    pthread_key_create(&sessionCleanupKey, SessionCleanup);
}

static void DestroySessionCleanup()
{
    pthread_key_delete(sessionCleanupKey);
}

static void ScheduleSessionCleanup()
{
    pthread_setspecific(sessionCleanupKey, (const void*)(uint64_t)(u_sess->mot_cxt.session_id + 1));
}

static void CancelSessionCleanup()
{
    pthread_setspecific(sessionCleanupKey, nullptr);
}

static GaussdbConfigLoader* gaussdbConfigLoader = nullptr;

bool MOTAdaptor::m_initialized = false;
bool MOTAdaptor::m_callbacks_initialized = false;

static void WakeupWalWriter()
{
    if (g_instance.proc_base->walwriterLatch != nullptr) {
        SetLatch(g_instance.proc_base->walwriterLatch);
    }
}

void MOTAdaptor::Init()
{
    if (m_initialized) {
        // This is highly unexpected, and should especially be guarded in scenario of switch-over to standby.
        elog(FATAL, "Double attempt to initialize MOT engine, it is already initialized");
    }

    m_engine = MOT::MOTEngine::CreateInstanceNoInit(g_instance.attr.attr_common.MOTConfigFileName, 0, nullptr);
    if (m_engine == nullptr) {
        elog(FATAL, "Failed to create MOT engine");
    }

    MOT::MOTConfiguration& motCfg = MOT::GetGlobalConfiguration();
    motCfg.SetTotalMemoryMb(g_instance.attr.attr_memory.max_process_memory / KILO_BYTE);

    gaussdbConfigLoader = new (std::nothrow) GaussdbConfigLoader();
    if (gaussdbConfigLoader == nullptr) {
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to allocate memory for GaussDB/MOTEngine configuration loader.");
    }
    MOT_LOG_TRACE("Adding external configuration loader for GaussDB");
    if (!m_engine->AddConfigLoader(gaussdbConfigLoader)) {
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to add GaussDB/MOTEngine configuration loader");
    }

    if (!m_engine->LoadConfig()) {
        m_engine->RemoveConfigLoader(gaussdbConfigLoader);
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to load configuration for MOT engine.");
    }

    // Check max process memory here - we do it anyway to protect ourselves from miscalculations.
    // Attention: the following values are configured during the call to MOTEngine::LoadConfig() just above
    uint64_t globalMemoryKb = MOT::g_memGlobalCfg.m_maxGlobalMemoryMb * KILO_BYTE;
    uint64_t localMemoryKb = MOT::g_memGlobalCfg.m_maxLocalMemoryMb * KILO_BYTE;
    uint64_t maxReserveMemoryKb = globalMemoryKb + localMemoryKb;

    // check whether the 2GB gap between MOT and envelope is still kept
    if ((g_instance.attr.attr_memory.max_process_memory < (int32)maxReserveMemoryKb) ||
        ((g_instance.attr.attr_memory.max_process_memory - maxReserveMemoryKb) < MIN_DYNAMIC_PROCESS_MEMORY)) {
        // we allow one extreme case: GaussDB is configured to its limit, and zero memory is left for us
        if (maxReserveMemoryKb <= motCfg.MOT_MIN_MEMORY_USAGE_MB * KILO_BYTE) {
            MOT_LOG_WARN("Allowing MOT to work in minimal memory mode");
        } else {
            m_engine->RemoveConfigLoader(gaussdbConfigLoader);
            delete gaussdbConfigLoader;
            gaussdbConfigLoader = nullptr;
            MOT::MOTEngine::DestroyInstance();
            elog(FATAL,
                "The value of pre-reserved memory for MOT engine is not reasonable: "
                "Request for a maximum of %" PRIu64 " KB global memory, and %" PRIu64
                " KB session memory (total of %" PRIu64 " KB) is invalid since max_process_memory is %u KB",
                globalMemoryKb,
                localMemoryKb,
                maxReserveMemoryKb,
                g_instance.attr.attr_memory.max_process_memory);
        }
    }

    if (!m_engine->Initialize()) {
        m_engine->RemoveConfigLoader(gaussdbConfigLoader);
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to initialize MOT engine.");
    }

    if (!JitExec::JitStatisticsProvider::CreateInstance()) {
        m_engine->RemoveConfigLoader(gaussdbConfigLoader);
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to initialize JIT statistics.");
    }

    // make sure current thread is cleaned up properly when thread pool is enabled
    EnsureSafeThreadAccessInline();

    if (motCfg.m_enableRedoLog && motCfg.m_loggerType == MOT::LoggerType::EXTERNAL_LOGGER) {
        m_engine->GetRedoLogHandler()->SetLogger(&xlogger);
        m_engine->GetRedoLogHandler()->SetWalWakeupFunc(WakeupWalWriter);
    }

    InitSessionDetailsMap();
    if (!g_instance.attr.attr_common.enable_thread_pool) {
        InitSessionCleanup();
    }
    InitDataNodeId();
    InitKeyOperStateMachine();
    m_initialized = true;
}

void MOTAdaptor::NotifyConfigChange()
{
    if (gaussdbConfigLoader != nullptr) {
        gaussdbConfigLoader->MarkChanged();
    }
}

void MOTAdaptor::InitDataNodeId()
{
    MOT::GetGlobalConfiguration().SetPgNodes(1, 1);
}

void MOTAdaptor::Destroy()
{
    if (!m_initialized) {
        return;
    }

    JitExec::JitStatisticsProvider::DestroyInstance();
    if (!g_instance.attr.attr_common.enable_thread_pool) {
        DestroySessionCleanup();
    }
    DestroySessionDetailsMap();
    if (gaussdbConfigLoader != nullptr) {
        m_engine->RemoveConfigLoader(gaussdbConfigLoader);
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
    }

    EnsureSafeThreadAccessInline();
    MOT::MOTEngine::DestroyInstance();
    m_engine = nullptr;
    knl_thread_mot_init();  // reset all thread-locals, mandatory for standby switch-over
    m_initialized = false;
}

MOT::TxnManager* MOTAdaptor::InitTxnManager(
    const char* callerSrc, MOT::ConnectionId connection_id /* = INVALID_CONNECTION_ID */)
{
    if (!u_sess->mot_cxt.txn_manager) {
        bool attachCleanFunc =
            (MOTCurrThreadId == INVALID_THREAD_ID ? true : !g_instance.attr.attr_common.enable_thread_pool);

        // First time we handle this connection
        if (m_engine == nullptr) {
            elog(ERROR, "initTxnManager: MOT engine is not initialized");
            return nullptr;
        }

        // create new session context
        MOT::SessionContext* session_ctx =
            MOT::GetSessionManager()->CreateSessionContext(IS_PGXC_COORDINATOR, 0, nullptr, connection_id);
        if (session_ctx == nullptr) {
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "Session Initialization", "Failed to create session context in %s", callerSrc);
            ereport(ERROR, (errmsg("Session startup: failed to create session context.")));
            return nullptr;
        }
        MOT_ASSERT(u_sess->mot_cxt.session_context == session_ctx);
        MOT_ASSERT(u_sess->mot_cxt.session_id == session_ctx->GetSessionId());
        MOT_ASSERT(u_sess->mot_cxt.connection_id == session_ctx->GetConnectionId());

        // make sure we cleanup leftovers from other session
        u_sess->mot_cxt.jit_context_count = 0;

        // record session details for statistics report
        RecordSessionDetails();

        if (attachCleanFunc) {
            // schedule session cleanup when thread pool is not used
            if (!g_instance.attr.attr_common.enable_thread_pool) {
                on_proc_exit(DestroyTxn, PointerGetDatum(session_ctx));
                ScheduleSessionCleanup();
            } else {
                on_proc_exit(MOTCleanupThread, PointerGetDatum(nullptr));
                MOT_LOG_DEBUG("Registered current thread for proc-exit callback for thread %p", (void*)pthread_self());
            }
        }

        u_sess->mot_cxt.txn_manager = session_ctx->GetTxnManager();
        elog(DEBUG1, "Init TXN_MAN for thread %u", MOTCurrThreadId);
    }

    return u_sess->mot_cxt.txn_manager;
}

static void DestroySessionJitContexts()
{
    // we must release all JIT context objects associated with this session now.
    // it seems that when thread pool is disabled, all cached plan sources for the session are not
    // released explicitly, but rather implicitly as part of the release of the memory context of the session.
    // in any case, we guard against repeated destruction of the JIT context by nullifying it
    MOT_LOG_DEBUG("Cleaning up all JIT context objects for current session");
    CachedPlanSource* psrc = u_sess->pcache_cxt.first_saved_plan;
    while (psrc != nullptr) {
        if (psrc->mot_jit_context != nullptr) {
            MOT_LOG_DEBUG("DestroySessionJitContexts(): Calling DestroyJitContext(%p)", psrc->mot_jit_context);
            JitExec::DestroyJitContext(psrc->mot_jit_context);
            psrc->mot_jit_context = nullptr;
        }
        psrc = psrc->next_saved;
    }
    MOT_LOG_DEBUG("DONE Cleaning up all JIT context objects for current session");
}

/** @brief Notification from thread pool that a session ended (only when thread pool is ENABLED). */
extern void MOTOnSessionClose()
{
    MOT_LOG_TRACE("Received session close notification (current session id: %u, current connection id: %u)",
        u_sess->mot_cxt.session_id,
        u_sess->mot_cxt.connection_id);
    if (u_sess->mot_cxt.session_id != INVALID_SESSION_ID) {
        ClearCurrentSessionDetails();
        MOT_LOG_DEBUG("MOTOnSessionClose(): Calling DestroySessionJitContexts()");
        DestroySessionJitContexts();
        if (!MOTAdaptor::m_engine) {
            MOT_LOG_ERROR("MOTOnSessionClose(): MOT engine is not initialized");
        } else {
            EnsureSafeThreadAccessInline();  // this is ok, it wil be cleaned up when thread exits
            MOT::SessionContext* sessionContext = u_sess->mot_cxt.session_context;
            if (sessionContext == nullptr) {
                MOT_LOG_WARN("Received session close notification, but no current session is found. Current session id "
                             "is %u. Request ignored.",
                    u_sess->mot_cxt.session_id);
            } else {
                DestroySession(sessionContext);
                MOT_ASSERT(u_sess->mot_cxt.session_id == INVALID_SESSION_ID);
            }
        }
    }
}

/** @brief Notification from thread pool that a pooled thread ended (only when thread pool is ENABLED). */
static void MOTOnThreadShutdown()
{
    if (!MOTAdaptor::m_initialized) {
        return;
    }

    MOT_LOG_TRACE("Received thread shutdown notification");
    if (!MOTAdaptor::m_engine) {
        MOT_LOG_ERROR("MOTOnThreadShutdown(): MOT engine is not initialized");
    } else {
        MOTAdaptor::m_engine->OnCurrentThreadEnding();
    }
    knl_thread_mot_init();  // reset all thread locals
}

/**
 * @brief on_proc_exit() callback to handle thread-cleanup - regardless of whether thread pool is enabled or not.
 * registration to on_proc_exit() is triggered by first call to EnsureSafeThreadAccessInline().
 */
static void MOTCleanupThread(int status, Datum ptr)
{
    MOT_ASSERT(g_instance.attr.attr_common.enable_thread_pool);
    MOT_LOG_TRACE("Received thread cleanup notification (thread-pool ON)");

    // when thread pool is used we just cleanup current thread
    // this might be a duplicate because thread pool also calls MOTOnThreadShutdown() - this is still ok
    // because we guard against repeated calls in MOTEngine::onCurrentThreadEnding()
    MOTOnThreadShutdown();
}

void MOTAdaptor::DestroyTxn(int status, Datum ptr)
{
    MOT_ASSERT(!g_instance.attr.attr_common.enable_thread_pool);

    // cleanup session
    if (!g_instance.attr.attr_common.enable_thread_pool) {
        CancelSessionCleanup();
    }
    ClearCurrentSessionDetails();
    MOT_LOG_DEBUG("DestroyTxn(): Calling DestroySessionJitContexts()");
    DestroySessionJitContexts();
    MOT::SessionContext* session = (MOT::SessionContext*)DatumGetPointer(ptr);
    if (m_engine == nullptr) {
        elog(ERROR, "destroyTxn: MOT engine is not initialized");
    }

    if (session != MOT_GET_CURRENT_SESSION_CONTEXT()) {
        MOT_LOG_WARN("Ignoring request to delete session context: already deleted");
    } else if (session != nullptr) {
        elog(DEBUG1, "Destroy SessionContext, connection_id = %u \n", session->GetConnectionId());
        EnsureSafeThreadAccessInline();  // may be accessed from new thread pool worker
        MOT::GcManager* gc = MOT_GET_CURRENT_SESSION_CONTEXT()->GetTxnManager()->GetGcSession();
        if (gc != nullptr) {
            gc->GcEndTxn();
        }
        DestroySession(session);
    }

    // clean up thread
    MOTOnThreadShutdown();
}

MOT::RC MOTAdaptor::ValidateCommit()
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (!IS_PGXC_COORDINATOR) {
        // return txn->ValidateCommit();//ADDBY NEU change to Commit
        return txn->Commit();
    } else {
        // Nothing to do in coordinator
        return MOT::RC_OK;
    }
}

void MOTAdaptor::RecordCommit(uint64_t csn)
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetCommitSequenceNumber(csn);
    if (!IS_PGXC_COORDINATOR) {
        txn->RecordCommit();
    } else {
        txn->LiteCommit();
    }
}

MOT::RC MOTAdaptor::Commit(uint64_t csn)
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetCommitSequenceNumber(csn);
    if (!IS_PGXC_COORDINATOR) {
        return txn->Commit();
    } else {
        txn->LiteCommit();
        return MOT::RC_OK;
    }
}

void MOTAdaptor::EndTransaction()
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    // Nothing to do in coordinator
    if (!IS_PGXC_COORDINATOR) {
        txn->EndTransaction();
    }
}

void MOTAdaptor::Rollback()
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (!IS_PGXC_COORDINATOR) {
        txn->Rollback();
    } else {
        txn->LiteRollback();
    }
}

MOT::RC MOTAdaptor::Prepare()
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (!IS_PGXC_COORDINATOR) {
        return txn->Prepare();
    } else {
        txn->LitePrepare();
        return MOT::RC_OK;
    }
}

void MOTAdaptor::CommitPrepared(uint64_t csn)
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetCommitSequenceNumber(csn);
    if (!IS_PGXC_COORDINATOR) {
        txn->CommitPrepared();
    } else {
        txn->LiteCommitPrepared();
    }
}

void MOTAdaptor::RollbackPrepared()
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (!IS_PGXC_COORDINATOR) {
        txn->RollbackPrepared();
    } else {
        txn->LiteRollbackPrepared();
    }
}

MOT::RC MOTAdaptor::InsertRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot)
{
    EnsureSafeThreadAccessInline();
    uint8_t* newRowData = nullptr;
    fdwState->m_currTxn->SetTransactionId(fdwState->m_txnId);
    MOT::Table* table = fdwState->m_table;
    MOT::Row* row = table->CreateNewRow();
    if (row == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Insert Row", "Failed to create new row for table %s", table->GetLongTableName().c_str());
        return MOT::RC_MEMORY_ALLOCATION_ERROR;
    }
    newRowData = const_cast<uint8_t*>(row->GetData());
    PackRow(slot, table, fdwState->m_attrsUsed, newRowData);

    MOT::RC res = table->InsertRow(row, fdwState->m_currTxn);
    if ((res != MOT::RC_OK) && (res != MOT::RC_UNIQUE_VIOLATION)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Insert Row", "Failed to insert new row for table %s", table->GetLongTableName().c_str());
    }
    return res;
}

MOT::RC MOTAdaptor::UpdateRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot, MOT::Row* currRow)
{
    EnsureSafeThreadAccessInline();
    MOT::RC rc;

    do {
        fdwState->m_currTxn->SetTransactionId(fdwState->m_txnId);
        rc = fdwState->m_currTxn->UpdateLastRowState(MOT::AccessType::WR);
        if (rc != MOT::RC::RC_OK) {
            break;
        }
        uint8_t* rowData = const_cast<uint8_t*>(currRow->GetData());
        PackUpdateRow(slot, fdwState->m_table, fdwState->m_attrsModified, rowData);
        MOT::BitmapSet modified_columns(fdwState->m_attrsModified, fdwState->m_table->GetFieldCount() - 1);

        rc = fdwState->m_currTxn->OverwriteRow(currRow, modified_columns);
    } while (0);

    return rc;
}

MOT::RC MOTAdaptor::DeleteRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot)
{
    EnsureSafeThreadAccessInline();
    fdwState->m_currTxn->SetTransactionId(fdwState->m_txnId);
    MOT::RC rc = fdwState->m_currTxn->DeleteLastRow();
    return rc;
}

// NOTE: colId starts from 1
bool MOTAdaptor::SetMatchingExpr(
    MOTFdwStateSt* state, MatchIndexArr* marr, int16_t colId, KEY_OPER op, Expr* expr, Expr* parent, bool set_local)
{
    bool res = false;
    uint16_t numIx = state->m_table->GetNumIndexes();

    for (uint16_t i = 0; i < numIx; i++) {
        MOT::Index* ix = state->m_table->GetIndex(i);
        if (ix != nullptr && ix->IsFieldPresent(colId)) {
            if (marr->m_idx[i] == nullptr) {
                marr->m_idx[i] = (MatchIndex*)palloc0(sizeof(MatchIndex));
                marr->m_idx[i]->Init();
                marr->m_idx[i]->m_ix = ix;
            }

            res |= marr->m_idx[i]->SetIndexColumn(state, colId, op, expr, parent, set_local);
        }
    }

    return res;
}

MatchIndex* MOTAdaptor::GetBestMatchIndex(MOTFdwStateSt* festate, MatchIndexArr* marr, int numClauses, bool setLocal)
{
    MatchIndex* best = nullptr;
    double bestCost = INT_MAX;
    uint16_t numIx = festate->m_table->GetNumIndexes();
    uint16_t bestI = (uint16_t)-1;

    for (uint16_t i = 0; i < numIx; i++) {
        if (marr->m_idx[i] != nullptr && marr->m_idx[i]->IsUsable()) {
            double cost = marr->m_idx[i]->GetCost(numClauses);
            if (cost < bestCost) {
                if (bestI < MAX_NUM_INDEXES) {
                    if (marr->m_idx[i]->GetNumMatchedCols() < marr->m_idx[bestI]->GetNumMatchedCols())
                        continue;
                }
                bestCost = cost;
                bestI = i;
            }
        }
    }

    if (bestI < MAX_NUM_INDEXES) {
        best = marr->m_idx[bestI];
        for (int k = 0; k < 2; k++) {
            for (int j = 0; j < best->m_ix->GetNumFields(); j++) {
                if (best->m_colMatch[k][j]) {
                    if (best->m_opers[k][j] < KEY_OPER::READ_INVALID) {
                        best->m_params[k][j] = AddParam(&best->m_remoteConds, best->m_colMatch[k][j]);
                        if (!list_member(best->m_remoteCondsOrig, best->m_parentColMatch[k][j])) {
                            best->m_remoteCondsOrig = lappend(best->m_remoteCondsOrig, best->m_parentColMatch[k][j]);
                        }

                        if (j > 0 && best->m_opers[k][j - 1] != KEY_OPER::READ_KEY_EXACT &&
                            !list_member(festate->m_localConds, best->m_parentColMatch[k][j])) {
                            if (setLocal)
                                festate->m_localConds = lappend(festate->m_localConds, best->m_parentColMatch[k][j]);
                        }
                    } else if (!list_member(festate->m_localConds, best->m_parentColMatch[k][j]) &&
                               !list_member(best->m_remoteCondsOrig, best->m_parentColMatch[k][j])) {
                        if (setLocal)
                            festate->m_localConds = lappend(festate->m_localConds, best->m_parentColMatch[k][j]);
                        best->m_colMatch[k][j] = nullptr;
                        best->m_parentColMatch[k][j] = nullptr;
                    }
                }
            }
        }
    }

    for (uint16_t i = 0; i < numIx; i++) {
        if (marr->m_idx[i] != nullptr) {
            MatchIndex* mix = marr->m_idx[i];
            if (i != bestI) {
                if (setLocal) {
                    for (int k = 0; k < 2; k++) {
                        for (int j = 0; j < mix->m_ix->GetNumFields(); j++) {
                            if (mix->m_colMatch[k][j] &&
                                !list_member(festate->m_localConds, mix->m_parentColMatch[k][j]) &&
                                !(best != nullptr &&
                                    list_member(best->m_remoteCondsOrig, mix->m_parentColMatch[k][j]))) {
                                festate->m_localConds = lappend(festate->m_localConds, mix->m_parentColMatch[k][j]);
                            }
                        }
                    }
                }
                pfree(mix);
                marr->m_idx[i] = nullptr;
            }
        }
    }
    if (best != nullptr && best->m_ix != nullptr) {
        for (uint16_t i = 0; i < numIx; i++) {
            if (best->m_ix == festate->m_table->GetIndex(i)) {
                best->m_ixPosition = i;
                break;
            }
        }
    }

    return best;
}

void MOTAdaptor::OpenCursor(Relation rel, MOTFdwStateSt* festate)
{
    bool matchKey = true;
    bool forwardDirection = true;
    bool found = false;

    EnsureSafeThreadAccessInline();

    // GetTableByExternalId cannot return nullptr at this stage, because it is protected by envelope's table lock.
    festate->m_table = festate->m_currTxn->GetTableByExternalId(rel->rd_id);

    do {
        // this scan all keys case
        // we need to open both cursors on start and end to prevent
        // infinite scan in case "insert into table A ... as select * from table A ...
        if (festate->m_bestIx == nullptr) {
            int fIx, bIx;
            uint8_t* buf = nullptr;
            // assumption that primary index cannot be changed, can take it from
            // table and not look on ddl_access
            MOT::Index* ix = festate->m_table->GetPrimaryIndex();
            uint16_t keyLength = ix->GetKeyLength();

            if (festate->m_order == SORTDIR_ENUM::SORTDIR_ASC) {
                fIx = 0;
                bIx = 1;
                festate->m_forwardDirectionScan = true;
            } else {
                fIx = 1;
                bIx = 0;
                festate->m_forwardDirectionScan = false;
            }

            festate->m_cursor[fIx] = festate->m_table->Begin(festate->m_currTxn->GetThdId());

            festate->m_stateKey[bIx].InitKey(keyLength);
            buf = festate->m_stateKey[bIx].GetKeyBuf();
            errno_t erc = memset_s(buf, keyLength, 0xff, keyLength);
            securec_check(erc, "\0", "\0");
            festate->m_cursor[bIx] =
                ix->Search(&festate->m_stateKey[bIx], false, false, festate->m_currTxn->GetThdId(), found);
            break;
        }

        for (int i = 0; i < 2; i++) {
            if (i == 1 && festate->m_bestIx->m_end < 0) {
                if (festate->m_forwardDirectionScan) {
                    uint8_t* buf = nullptr;
                    MOT::Index* ix = festate->m_bestIx->m_ix;
                    uint16_t keyLength = ix->GetKeyLength();

                    festate->m_stateKey[1].InitKey(keyLength);
                    buf = festate->m_stateKey[1].GetKeyBuf();
                    errno_t erc = memset_s(buf, keyLength, 0xff, keyLength);
                    securec_check(erc, "\0", "\0");
                    festate->m_cursor[1] =
                        ix->Search(&festate->m_stateKey[1], false, false, festate->m_currTxn->GetThdId(), found);
                } else {
                    festate->m_cursor[1] = festate->m_bestIx->m_ix->Begin(festate->m_currTxn->GetThdId());
                }
                break;
            }

            KEY_OPER oper = (i == 0 ? festate->m_bestIx->m_ixOpers[0] : festate->m_bestIx->m_ixOpers[1]);

            forwardDirection = ((oper & ~KEY_OPER_PREFIX_BITMASK) < KEY_OPER::READ_KEY_OR_PREV);

            CreateKeyBuffer(rel, festate, i);

            if (i == 0) {
                festate->m_forwardDirectionScan = forwardDirection;
            }

            switch (oper) {
                case KEY_OPER::READ_KEY_EXACT:
                case KEY_OPER::READ_KEY_OR_NEXT:
                case KEY_OPER::READ_KEY_LIKE:
                case KEY_OPER::READ_PREFIX_LIKE:
                case KEY_OPER::READ_PREFIX:
                case KEY_OPER::READ_PREFIX_OR_NEXT:
                    matchKey = true;
                    forwardDirection = true;
                    break;

                case KEY_OPER::READ_KEY_AFTER:
                case KEY_OPER::READ_PREFIX_AFTER:
                    matchKey = false;
                    forwardDirection = true;
                    break;

                case KEY_OPER::READ_KEY_OR_PREV:
                case KEY_OPER::READ_PREFIX_OR_PREV:
                    matchKey = true;
                    forwardDirection = false;
                    break;

                case KEY_OPER::READ_KEY_BEFORE:
                case KEY_OPER::READ_PREFIX_BEFORE:
                    matchKey = false;
                    forwardDirection = false;
                    break;

                default:
                    elog(INFO, "Invalid key operation: %u", oper);
                    break;
            }

            festate->m_cursor[i] = festate->m_bestIx->m_ix->Search(
                &festate->m_stateKey[i], matchKey, forwardDirection, festate->m_currTxn->GetThdId(), found);

            if (!found && oper == KEY_OPER::READ_KEY_EXACT && festate->m_bestIx->m_ix->GetUnique()) {
                festate->m_cursor[i]->Invalidate();
                festate->m_cursor[i]->Destroy();
                delete festate->m_cursor[i];
                festate->m_cursor[i] = nullptr;
            }
        }
    } while (0);
}

static void VarLenFieldType(
    Form_pg_type typeDesc, Oid typoid, int32_t colLen, int16* typeLen, bool& isBlob, MOT::RC& res)
{
    isBlob = false;
    res = MOT::RC_OK;
    if (typeDesc->typlen < 0) {
        *typeLen = colLen;
        switch (typeDesc->typstorage) {
            case 'p':
                break;
            case 'x':
            case 'm':
                if (typoid == NUMERICOID) {
                    *typeLen = DECIMAL_MAX_SIZE;
                    break;
                }
                /* fall through */
            case 'e':
#ifdef USE_ASSERT_CHECKING
                if (typoid == TEXTOID)
                    *typeLen = colLen = MAX_VARCHAR_LEN;
#endif
                if (colLen > MAX_VARCHAR_LEN || colLen < 0) {
                    res = MOT::RC_COL_SIZE_INVALID;
                } else {
                    isBlob = true;
                }
                break;
            default:
                break;
        }
    }
}

static MOT::RC TableFieldType(
    const ColumnDef* colDef, MOT::MOT_CATALOG_FIELD_TYPES& type, int16* typeLen, Oid& typoid, bool& isBlob)
{
    MOT::RC res = MOT::RC_OK;
    Type tup;
    Form_pg_type typeDesc;
    int32_t colLen;

    if (colDef->typname->arrayBounds != nullptr) {
        return MOT::RC_UNSUPPORTED_COL_TYPE_ARR;
    }

    tup = typenameType(nullptr, colDef->typname, &colLen);
    typeDesc = ((Form_pg_type)GETSTRUCT(tup));
    typoid = HeapTupleGetOid(tup);
    *typeLen = typeDesc->typlen;

    // Get variable-length field length.
    VarLenFieldType(typeDesc, typoid, colLen, typeLen, isBlob, res);

    switch (typoid) {
        case CHAROID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_CHAR;
            break;
        case INT1OID:
        case BOOLOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TINY;
            break;
        case INT2OID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_SHORT;
            break;
        case INT4OID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_INT;
            break;
        case INT8OID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG;
            break;
        case DATEOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DATE;
            break;
        case TIMEOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TIME;
            break;
        case TIMESTAMPOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TIMESTAMP;
            break;
        case TIMESTAMPTZOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TIMESTAMPTZ;
            break;
        case INTERVALOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_INTERVAL;
            break;
        case TINTERVALOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TINTERVAL;
            break;
        case TIMETZOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TIMETZ;
            break;
        case FLOAT4OID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_FLOAT;
            break;
        case FLOAT8OID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DOUBLE;
            break;
        case NUMERICOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DECIMAL;
            break;
        case VARCHAROID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR;
            break;
        case BPCHAROID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR;
            break;
        case TEXTOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR;
            break;
        case CLOBOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_BLOB;
            break;
        case BYTEAOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR;
            break;
        default:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_UNKNOWN;
            res = MOT::RC_UNSUPPORTED_COL_TYPE;
    }

    if (tup) {
        ReleaseSysCache(tup);
    }

    return res;
}

void MOTAdaptor::ValidateCreateIndex(IndexStmt* stmt, MOT::Table* table, MOT::TxnManager* txn)
{
    if (stmt->primary) {
        if (!table->IsTableEmpty(txn->GetThdId())) {
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_FDW_ERROR),
                    errmsg(
                        "Table %s is not empty, create primary index is not allowed", table->GetTableName().c_str())));
            return;
        }
    } else if (table->GetNumIndexes() == MAX_NUM_INDEXES) {
        ereport(ERROR,
            (errmodule(MOD_MOT),
                errcode(ERRCODE_FDW_TOO_MANY_INDEXES),
                errmsg("Can not create index, max number of indexes %u reached", MAX_NUM_INDEXES)));
        return;
    }

    if (strcmp(stmt->accessMethod, "btree") != 0) {
        ereport(ERROR, (errmodule(MOD_MOT), errmsg("MOT supports indexes of type BTREE only (btree or btree_art)")));
        return;
    }

    if (list_length(stmt->indexParams) > (int)MAX_KEY_COLUMNS) {
        ereport(ERROR,
            (errmodule(MOD_MOT),
                errcode(ERRCODE_FDW_TOO_MANY_INDEX_COLUMNS),
                errmsg("Can't create index"),
                errdetail(
                    "Number of columns exceeds %d max allowed %u", list_length(stmt->indexParams), MAX_KEY_COLUMNS)));
        return;
    }
}

MOT::RC MOTAdaptor::CreateIndex(IndexStmt* stmt, ::TransactionId tid)
{
    MOT::RC res;
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetTransactionId(tid);
    MOT::Table* table = txn->GetTableByExternalId(stmt->relation->foreignOid);

    if (table == nullptr) {
        ereport(ERROR,
            (errmodule(MOD_MOT),
                errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("Table not found for oid %u", stmt->relation->foreignOid)));
        return MOT::RC_ERROR;
    }

    ValidateCreateIndex(stmt, table, txn);

    elog(LOG,
        "creating %s index %s (OID: %u), for table: %s",
        (stmt->primary ? "PRIMARY" : "SECONDARY"),
        stmt->idxname,
        stmt->indexOid,
        stmt->relation->relname);
    uint64_t keyLength = 0;
    MOT::Index* index = nullptr;
    MOT::IndexOrder index_order = MOT::IndexOrder::INDEX_ORDER_SECONDARY;

    // Use the default index tree flavor from configuration file
    MOT::IndexingMethod indexing_method = MOT::IndexingMethod::INDEXING_METHOD_TREE;
    MOT::IndexTreeFlavor flavor = MOT::GetGlobalConfiguration().m_indexTreeFlavor;

    // check if we have primary and delete previous definition
    if (stmt->primary) {
        index_order = MOT::IndexOrder::INDEX_ORDER_PRIMARY;
    }

    index = MOT::IndexFactory::CreateIndex(index_order, indexing_method, flavor);
    if (index == nullptr) {
        report_pg_error(MOT::RC_ABORT);
        return MOT::RC_ABORT;
    }
    index->SetExtId(stmt->indexOid);
    index->SetNumTableFields((uint32_t)table->GetFieldCount());
    int count = 0;

    ListCell* lc = nullptr;
    foreach (lc, stmt->indexParams) {
        IndexElem* ielem = (IndexElem*)lfirst(lc);

        uint64_t colid = table->GetFieldId((ielem->name != nullptr ? ielem->name : ielem->indexcolname));
        if (colid == (uint64_t)-1) {  // invalid column
            delete index;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
                    errmsg("Can't create index on field"),
                    errdetail("Specified column not found in table definition")));
            return MOT::RC_ERROR;
        }

        MOT::Column* col = table->GetField(colid);

        // Temp solution for NULLs, do not allow index creation on column that does not carry not null flag
        if (!MOT::GetGlobalConfiguration().m_allowIndexOnNullableColumn && !col->m_isNotNull) {
            delete index;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_FDW_INDEX_ON_NULLABLE_COLUMN_NOT_ALLOWED),
                    errmsg("Can't create index on nullable columns"),
                    errdetail("Column %s is nullable", col->m_name)));
            return MOT::RC_ERROR;
        }

        // Temp solution, we have to support DECIMAL and NUMERIC indexes as well
        if (col->m_type == MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DECIMAL) {
            delete index;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Can't create index on field"),
                    errdetail("INDEX on NUMERIC or DECIMAL fields not supported yet")));
            return MOT::RC_ERROR;
        }
        if (col->m_keySize > MAX_KEY_SIZE) {
            delete index;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
                    errmsg("Can't create index on field"),
                    errdetail("Column size is greater than maximum index size")));
            return MOT::RC_ERROR;
        }
        keyLength += col->m_keySize;

        index->SetLenghtKeyFields(count, colid, col->m_keySize);
        count++;
    }

    index->SetNumIndexFields(count);

    if ((res = index->IndexInit(keyLength, stmt->unique, stmt->idxname, nullptr)) != MOT::RC_OK) {
        delete index;
        report_pg_error(res);
        return res;
    }

    res = txn->CreateIndex(table, index, stmt->primary);
    if (res != MOT::RC_OK) {
        delete index;
        if (res == MOT::RC_TABLE_EXCEEDS_MAX_INDEXES) {
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_FDW_TOO_MANY_INDEXES),
                    errmsg("Can not create index, max number of indexes %u reached", MAX_NUM_INDEXES)));
            return MOT::RC_TABLE_EXCEEDS_MAX_INDEXES;
        } else {
            report_pg_error(txn->m_err, stmt->idxname, txn->m_errMsgBuf);
            return MOT::RC_UNIQUE_VIOLATION;
        }
    }

    return MOT::RC_OK;
}

void MOTAdaptor::AddTableColumns(MOT::Table* table, List *tableElts, bool& hasBlob)
{
    hasBlob = false;
    ListCell* cell = nullptr;
    foreach (cell, tableElts) {
        int16 typeLen = 0;
        bool isBlob = false;
        MOT::MOT_CATALOG_FIELD_TYPES colType;
        ColumnDef* colDef = (ColumnDef*)lfirst(cell);

        if (colDef == nullptr || colDef->typname == nullptr) {
            delete table;
            table = nullptr;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
                    errmsg("Column definition is not complete"),
                    errdetail("target table is a foreign table")));
            break;
        }

        Oid typoid = InvalidOid;
        MOT::RC res = TableFieldType(colDef, colType, &typeLen, typoid, isBlob);
        if (res != MOT::RC_OK) {
            delete table;
            table = nullptr;
            report_pg_error(res, colDef, (void*)(int64)typeLen);
            break;
        }
        hasBlob |= isBlob;

        if (colType == MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DECIMAL) {
            if (list_length(colDef->typname->typmods) > 0) {
                bool canMakeShort = true;
                int precision = 0;
                int scale = 0;
                int count = 0;

                ListCell* c = nullptr;
                foreach (c, colDef->typname->typmods) {
                    Node* d = (Node*)lfirst(c);
                    if (!IsA(d, A_Const)) {
                        canMakeShort = false;
                        break;
                    }
                    A_Const* ac = (A_Const*)d;

                    if (ac->val.type != T_Integer) {
                        canMakeShort = false;
                        break;
                    }

                    if (count == 0) {
                        precision = ac->val.val.ival;
                    } else {
                        scale = ac->val.val.ival;
                    }

                    count++;
                }

                if (canMakeShort) {
                    int len = 0;

                    len += scale / DEC_DIGITS;
                    len += (scale % DEC_DIGITS > 0 ? 1 : 0);

                    precision -= scale;

                    len += precision / DEC_DIGITS;
                    len += (precision % DEC_DIGITS > 0 ? 1 : 0);

                    typeLen = sizeof(MOT::DecimalSt) + len * sizeof(NumericDigit);
                }
            }
        }

        res = table->AddColumn(colDef->colname, typeLen, colType, colDef->is_not_null, typoid);
        if (res != MOT::RC_OK) {
            delete table;
            table = nullptr;
            report_pg_error(res, colDef, (void*)(int64)typeLen);
            break;
        }
    }
}

MOT::RC MOTAdaptor::CreateTable(CreateForeignTableStmt* stmt, ::TransactionId tid)
{
    bool hasBlob = false;
    MOT::Index* primaryIdx = nullptr;
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__, tid);
    MOT::Table* table = nullptr;
    MOT::RC res = MOT::RC_ERROR;
    std::string tname("");
    char* dbname = NULL;

    do {
        table = new (std::nothrow) MOT::Table();
        if (table == nullptr) {
            ereport(ERROR,
                (errmodule(MOD_MOT), errcode(ERRCODE_OUT_OF_MEMORY), errmsg("Allocation of table metadata failed")));
            break;
        }

        uint32_t columnCount = list_length(stmt->base.tableElts);

        // once the columns have been counted, we add one more for the nullable columns
        ++columnCount;

        // prepare table name
        dbname = get_database_name(u_sess->proc_cxt.MyDatabaseId);
        if (dbname == nullptr) {
            delete table;
            table = nullptr;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_UNDEFINED_DATABASE),
                    errmsg("database with OID %u does not exist", u_sess->proc_cxt.MyDatabaseId)));
            break;
        }
        tname.append(dbname);
        tname.append("_");
        if (stmt->base.relation->schemaname != nullptr) {
            tname.append(stmt->base.relation->schemaname);
        } else {
            tname.append("#");
        }

        tname.append("_");
        tname.append(stmt->base.relation->relname);

        if (!table->Init(
                stmt->base.relation->relname, tname.c_str(), columnCount, stmt->base.relation->foreignOid)) {
            delete table;
            table = nullptr;
            report_pg_error(MOT::RC_MEMORY_ALLOCATION_ERROR);
            break;
        }

        // the null fields are copied verbatim because we have to give them back at some point
        res = table->AddColumn(
            "null_bytes", BITMAPLEN(columnCount - 1), MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_NULLBYTES);
        if (res != MOT::RC_OK) {
            delete table;
            table = nullptr;
            report_pg_error(MOT::RC_MEMORY_ALLOCATION_ERROR);
            break;
        }

        /*
         * Add all the columns.
         * NOTE: On failure, table object will be deleted and ereport will be done in AddTableColumns.
         */
        AddTableColumns(table, stmt->base.tableElts, hasBlob);

        table->SetFixedLengthRow(!hasBlob);

        uint32_t tupleSize = table->GetTupleSize();
        if (tupleSize > (unsigned int)MAX_TUPLE_SIZE) {
            delete table;
            table = nullptr;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                    errdetail("MOT: Table %s tuple size %u exceeds MAX_TUPLE_SIZE=%u !!!",
                        stmt->base.relation->relname,
                        tupleSize,
                        (unsigned int)MAX_TUPLE_SIZE)));
        }

        if (!table->InitRowPool()) {
            delete table;
            table = nullptr;
            report_pg_error(MOT::RC_MEMORY_ALLOCATION_ERROR);
            break;
        }

        elog(LOG,
            "creating table %s (OID: %u), num columns: %u, tuple: %u",
            table->GetLongTableName().c_str(),
            stmt->base.relation->foreignOid,
            columnCount,
            tupleSize);

        res = txn->CreateTable(table);
        if (res != MOT::RC_OK) {
            delete table;
            table = nullptr;
            report_pg_error(res);
            break;
        }

        // add default PK index
        primaryIdx = MOT::IndexFactory::CreatePrimaryIndexEx(MOT::IndexingMethod::INDEXING_METHOD_TREE,
            DEFAULT_TREE_FLAVOR,
            8,
            table->GetLongTableName(),
            res,
            nullptr);
        if (res != MOT::RC_OK) {
            txn->DropTable(table);
            report_pg_error(res);
            break;
        }
        primaryIdx->SetExtId(stmt->base.relation->foreignOid + 1);
        primaryIdx->SetNumTableFields(columnCount);
        primaryIdx->SetNumIndexFields(1);
        primaryIdx->SetLenghtKeyFields(0, -1, 8);
        primaryIdx->SetFakePrimary(true);

        // Add default primary index
        res = txn->CreateIndex(table, primaryIdx, true);
    } while (0);

    if (res != MOT::RC_OK) {
        if (table != nullptr) {
            txn->DropTable(table);
        }
        if (primaryIdx != nullptr) {
            delete primaryIdx;
        }
    }

    return res;
}

MOT::RC MOTAdaptor::DropIndex(DropForeignStmt* stmt, ::TransactionId tid)
{
    MOT::RC res = MOT::RC_OK;
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetTransactionId(tid);

    elog(LOG, "dropping index %s, ixoid: %u, taboid: %u", stmt->name, stmt->indexoid, stmt->reloid);

    // get table
    do {
        MOT::Index* index = txn->GetIndexByExternalId(stmt->reloid, stmt->indexoid);
        if (index == nullptr) {
            elog(LOG,
                "Drop index %s error, index oid %u of table oid %u not found.",
                stmt->name,
                stmt->indexoid,
                stmt->reloid);
            res = MOT::RC_INDEX_NOT_FOUND;
        } else if (index->IsPrimaryKey()) {
            elog(LOG, "Drop primary index is not supported, failed to drop index: %s", stmt->name);
        } else {
            MOT::Table* table = index->GetTable();
            uint64_t table_relid = table->GetTableExId();
            JitExec::PurgeJitSourceCache(table_relid, false);
            table->WrLock();
            res = txn->DropIndex(index);
            table->Unlock();
        }
    } while (0);

    return res;
}

MOT::RC MOTAdaptor::DropTable(DropForeignStmt* stmt, ::TransactionId tid)
{
    MOT::RC res = MOT::RC_OK;
    MOT::Table* tab = nullptr;
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetTransactionId(tid);

    elog(LOG, "dropping table %s, oid: %u", stmt->name, stmt->reloid);
    do {
        tab = txn->GetTableByExternalId(stmt->reloid);
        if (tab == nullptr) {
            res = MOT::RC_TABLE_NOT_FOUND;
            elog(LOG, "Drop table %s error, table oid %u not found.", stmt->name, stmt->reloid);
        } else {
            uint64_t table_relid = tab->GetTableExId();
            JitExec::PurgeJitSourceCache(table_relid, false);
            res = txn->DropTable(tab);
        }
    } while (0);

    return res;
}

MOT::RC MOTAdaptor::TruncateTable(Relation rel, ::TransactionId tid)
{
    MOT::RC res = MOT::RC_OK;
    MOT::Table* tab = nullptr;

    EnsureSafeThreadAccessInline();

    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetTransactionId(tid);

    elog(LOG, "truncating table %s, oid: %u", NameStr(rel->rd_rel->relname), rel->rd_id);
    do {
        tab = txn->GetTableByExternalId(rel->rd_id);
        if (tab == nullptr) {
            elog(LOG, "Truncate table %s error, table oid %u not found.", NameStr(rel->rd_rel->relname), rel->rd_id);
            break;
        }

        JitExec::PurgeJitSourceCache(rel->rd_id, true);
        tab->WrLock();
        res = txn->TruncateTable(tab);
        tab->Unlock();
    } while (0);

    return res;
}

MOT::RC MOTAdaptor::VacuumTable(Relation rel, ::TransactionId tid)
{
    MOT::RC res = MOT::RC_OK;
    MOT::Table* tab = nullptr;
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetTransactionId(tid);

    elog(LOG, "vacuuming table %s, oid: %u", NameStr(rel->rd_rel->relname), rel->rd_id);
    do {
        tab = MOT::GetTableManager()->GetTableSafeByExId(rel->rd_id);
        if (tab == nullptr) {
            elog(LOG, "Vacuum table %s error, table oid %u not found.", NameStr(rel->rd_rel->relname), rel->rd_id);
            break;
        }

        tab->Compact(txn);
        tab->Unlock();
    } while (0);
    return res;
}

uint64_t MOTAdaptor::GetTableIndexSize(uint64_t tabId, uint64_t ixId)
{
    uint64_t res = 0;
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    MOT::Table* tab = nullptr;
    MOT::Index* ix = nullptr;

    do {
        tab = txn->GetTableByExternalId(tabId);
        if (tab == nullptr) {
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_FDW_TABLE_NOT_FOUND),
                    errmsg("Get table size error, table oid %lu not found.", tabId)));
            break;
        }

        if (ixId > 0) {
            ix = tab->GetIndexByExtId(ixId);
            if (ix == nullptr) {
                ereport(ERROR,
                    (errmodule(MOD_MOT),
                        errcode(ERRCODE_FDW_TABLE_NOT_FOUND),
                        errmsg("Get index size error, index oid %lu for table oid %lu not found.", ixId, tabId)));
                break;
            }
            res = ix->GetIndexSize();
        } else
            res = tab->GetTableSize();
    } while (0);

    return res;
}

MotMemoryDetail* MOTAdaptor::GetMemSize(uint32_t* nodeCount, bool isGlobal)
{
    EnsureSafeThreadAccessInline();
    MotMemoryDetail* result = nullptr;
    *nodeCount = 0;

    /* We allocate an array of size (m_nodeCount + 1) to accommodate one aggregated entry of all global pools. */
    uint32_t statsArraySize = MOT::g_memGlobalCfg.m_nodeCount + 1;
    MOT::MemRawChunkPoolStats* chunkPoolStatsArray =
        (MOT::MemRawChunkPoolStats*)palloc(statsArraySize * sizeof(MOT::MemRawChunkPoolStats));
    if (chunkPoolStatsArray != nullptr) {
        errno_t erc = memset_s(chunkPoolStatsArray,
            statsArraySize * sizeof(MOT::MemRawChunkPoolStats),
            0,
            statsArraySize * sizeof(MOT::MemRawChunkPoolStats));
        securec_check(erc, "\0", "\0");

        uint32_t realStatsEntries;
        if (isGlobal) {
            realStatsEntries = MOT::MemRawChunkStoreGetGlobalStats(chunkPoolStatsArray, statsArraySize);
        } else {
            realStatsEntries = MOT::MemRawChunkStoreGetLocalStats(chunkPoolStatsArray, statsArraySize);
        }

        MOT_ASSERT(realStatsEntries <= statsArraySize);
        if (realStatsEntries > 0) {
            result = (MotMemoryDetail*)palloc(realStatsEntries * sizeof(MotMemoryDetail));
            if (result != nullptr) {
                for (uint32_t node = 0; node < realStatsEntries; ++node) {
                    result[node].numaNode = chunkPoolStatsArray[node].m_node;
                    result[node].reservedMemory = chunkPoolStatsArray[node].m_reservedBytes;
                    result[node].usedMemory = chunkPoolStatsArray[node].m_usedBytes;
                }
                *nodeCount = realStatsEntries;
            }
        }
        pfree(chunkPoolStatsArray);
    }

    return result;
}

MotSessionMemoryDetail* MOTAdaptor::GetSessionMemSize(uint32_t* sessionCount)
{
    EnsureSafeThreadAccessInline();
    MotSessionMemoryDetail* result = nullptr;
    *sessionCount = 0;

    uint32_t session_count = MOT::g_memGlobalCfg.m_maxThreadCount;
    MOT::MemSessionAllocatorStats* session_stats_array =
        (MOT::MemSessionAllocatorStats*)palloc(session_count * sizeof(MOT::MemSessionAllocatorStats));
    if (session_stats_array != nullptr) {
        uint32_t real_session_count = MOT::MemSessionGetAllStats(session_stats_array, session_count);
        if (real_session_count > 0) {
            result = (MotSessionMemoryDetail*)palloc(real_session_count * sizeof(MotSessionMemoryDetail));
            if (result != nullptr) {
                for (uint32_t session_index = 0; session_index < real_session_count; ++session_index) {
                    GetSessionDetails(session_stats_array[session_index].m_sessionId,
                        &result[session_index].threadid,
                        &result[session_index].threadStartTime);
                    result[session_index].totalSize = session_stats_array[session_index].m_reservedSize;
                    result[session_index].usedSize = session_stats_array[session_index].m_usedSize;
                    result[session_index].freeSize = result[session_index].totalSize - result[session_index].usedSize;
                }
                *sessionCount = real_session_count;
            }
        }
        pfree(session_stats_array);
    }

    return result;
}

void MOTAdaptor::CreateKeyBuffer(Relation rel, MOTFdwStateSt* festate, int start)
{
    uint8_t* buf = nullptr;
    uint8_t pattern = 0x00;
    EnsureSafeThreadAccessInline();
    int16_t num = festate->m_bestIx->m_ix->GetNumFields();
    const uint16_t* fieldLengths = festate->m_bestIx->m_ix->GetLengthKeyFields();
    const int16_t* orgCols = festate->m_bestIx->m_ix->GetColumnKeyFields();
    TupleDesc desc = rel->rd_att;
    uint16_t offset = 0;
    int32_t* exprs = nullptr;
    KEY_OPER* opers = nullptr;
    uint16_t keyLength;
    KEY_OPER oper;

    if (start == 0) {
        exprs = festate->m_bestIx->m_params[festate->m_bestIx->m_start];
        opers = festate->m_bestIx->m_opers[festate->m_bestIx->m_start];
        oper = festate->m_bestIx->m_ixOpers[0];
    } else {
        exprs = festate->m_bestIx->m_params[festate->m_bestIx->m_end];
        opers = festate->m_bestIx->m_opers[festate->m_bestIx->m_end];
        // end may be equal start but the operation maybe different, look at getCost
        oper = festate->m_bestIx->m_ixOpers[1];
    }

    keyLength = festate->m_bestIx->m_ix->GetKeyLength();
    festate->m_stateKey[start].InitKey(keyLength);
    buf = festate->m_stateKey[start].GetKeyBuf();

    switch (oper) {
        case KEY_OPER::READ_KEY_EXACT:
        case KEY_OPER::READ_KEY_OR_NEXT:
        case KEY_OPER::READ_KEY_BEFORE:
        case KEY_OPER::READ_KEY_LIKE:
        case KEY_OPER::READ_PREFIX:
        case KEY_OPER::READ_PREFIX_LIKE:
        case KEY_OPER::READ_PREFIX_OR_NEXT:
        case KEY_OPER::READ_PREFIX_BEFORE:
            pattern = 0x00;
            break;

        case KEY_OPER::READ_KEY_OR_PREV:
        case KEY_OPER::READ_PREFIX_AFTER:
        case KEY_OPER::READ_PREFIX_OR_PREV:
        case KEY_OPER::READ_KEY_AFTER:
            pattern = 0xff;
            break;

        default:
            elog(LOG, "Invalid key operation: %u", oper);
            break;
    }

    for (int i = 0; i < num; i++) {
        if (opers[i] < KEY_OPER::READ_INVALID) {
            bool is_null = false;
            ExprState* expr = (ExprState*)list_nth(festate->m_execExprs, exprs[i] - 1);
            Datum val = ExecEvalExpr((ExprState*)(expr), festate->m_econtext, &is_null, nullptr);
            if (is_null) {
                MOT_ASSERT((offset + fieldLengths[i]) <= keyLength);
                errno_t erc = memset_s(buf + offset, fieldLengths[i], 0x00, fieldLengths[i]);
                securec_check(erc, "\0", "\0");
            } else {
                MOT::Column* col = festate->m_table->GetField(orgCols[i]);
                uint8_t fill = 0x00;

                // in case of like fill rest of the key with appropriate to direction values
                if (opers[i] == KEY_OPER::READ_KEY_LIKE) {
                    switch (oper) {
                        case KEY_OPER::READ_KEY_LIKE:
                        case KEY_OPER::READ_KEY_OR_NEXT:
                        case KEY_OPER::READ_KEY_AFTER:
                        case KEY_OPER::READ_PREFIX:
                        case KEY_OPER::READ_PREFIX_LIKE:
                        case KEY_OPER::READ_PREFIX_OR_NEXT:
                        case KEY_OPER::READ_PREFIX_AFTER:
                            break;

                        case KEY_OPER::READ_PREFIX_BEFORE:
                        case KEY_OPER::READ_PREFIX_OR_PREV:
                        case KEY_OPER::READ_KEY_BEFORE:
                        case KEY_OPER::READ_KEY_OR_PREV:
                            fill = 0xff;
                            break;

                        case KEY_OPER::READ_KEY_EXACT:
                        default:
                            elog(LOG, "Invalid key operation: %u", oper);
                            break;
                    }
                }

                DatumToMOTKey(col,
                    expr->resultType,
                    val,
                    desc->attrs[orgCols[i] - 1]->atttypid,
                    buf + offset,
                    fieldLengths[i],
                    opers[i],
                    fill);
            }
        } else {
            MOT_ASSERT((offset + fieldLengths[i]) <= keyLength);
            festate->m_stateKey[start].FillPattern(pattern, fieldLengths[i], offset);
        }

        offset += fieldLengths[i];
    }

    festate->m_bestIx->m_ix->AdjustKey(&festate->m_stateKey[start], pattern);
}

bool MOTAdaptor::IsScanEnd(MOTFdwStateSt* festate)
{
    bool res = false;
    EnsureSafeThreadAccessInline();

    // festate->cursor[1] (end iterator) might be NULL (in case it is not in use). If this is the case, return false
    // (which means we have not reached the end yet)
    if (festate->m_cursor[1] == nullptr) {
        return false;
    }

    if (!festate->m_cursor[1]->IsValid()) {
        return true;
    } else {
        const MOT::Key* startKey = nullptr;
        const MOT::Key* endKey = nullptr;
        MOT::Index* ix = (festate->m_bestIx != nullptr ? festate->m_bestIx->m_ix : festate->m_table->GetPrimaryIndex());

        startKey = reinterpret_cast<const MOT::Key*>(festate->m_cursor[0]->GetKey());
        endKey = reinterpret_cast<const MOT::Key*>(festate->m_cursor[1]->GetKey());
        if (startKey != nullptr && endKey != nullptr) {
            int cmpRes = memcmp(startKey->GetKeyBuf(), endKey->GetKeyBuf(), ix->GetKeySizeNoSuffix());

            if (festate->m_forwardDirectionScan) {
                if (cmpRes > 0)
                    res = true;
            } else {
                if (cmpRes < 0)
                    res = true;
            }
        }
    }

    return res;
}

void MOTAdaptor::PackRow(TupleTableSlot* slot, MOT::Table* table, uint8_t* attrs_used, uint8_t* destRow)
{
    errno_t erc;
    EnsureSafeThreadAccessInline();
    HeapTuple srcData = (HeapTuple)slot->tts_tuple;
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    bool hasnulls = HeapTupleHasNulls(srcData);
    uint64_t i = 0;
    uint64_t j = 1;
    uint64_t cols = table->GetFieldCount() - 1;  // column count includes null bits field

    // the null bytes are necessary and have to give them back
    if (!hasnulls) {
        erc = memset_s(destRow + table->GetFieldOffset(i), table->GetFieldSize(i), 0xff, table->GetFieldSize(i));
        securec_check(erc, "\0", "\0");
    } else {
        erc = memcpy_s(destRow + table->GetFieldOffset(i),
            table->GetFieldSize(i),
            &srcData->t_data->t_bits[0],
            table->GetFieldSize(i));
        securec_check(erc, "\0", "\0");
    }

    // we now copy the fields, for the time being the null ones will be copied as well
    for (; i < cols; i++, j++) {
        bool isnull = false;
        Datum value = heap_slot_getattr(slot, j, &isnull);

        if (!isnull) {
            DatumToMOT(table->GetField(j), value, tupdesc->attrs[i]->atttypid, destRow);
        }
    }
}

void MOTAdaptor::PackUpdateRow(TupleTableSlot* slot, MOT::Table* table, const uint8_t* attrs_used, uint8_t* destRow)
{
    EnsureSafeThreadAccessInline();
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    uint8_t* bits;
    uint64_t i = 0;
    uint64_t j = 1;

    // column count includes null bits field
    uint64_t cols = table->GetFieldCount() - 1;
    bits = destRow + table->GetFieldOffset(i);

    for (; i < cols; i++, j++) {
        if (BITMAP_GET(attrs_used, i)) {
            bool isnull = false;
            Datum value = heap_slot_getattr(slot, j, &isnull);

            if (!isnull) {
                DatumToMOT(table->GetField(j), value, tupdesc->attrs[i]->atttypid, destRow);
                BITMAP_SET(bits, i);
            } else {
                BITMAP_CLEAR(bits, i);
            }
        }
    }
}

void MOTAdaptor::UnpackRow(TupleTableSlot* slot, MOT::Table* table, const uint8_t* attrs_used, uint8_t* srcRow)
{
    EnsureSafeThreadAccessInline();
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    uint64_t i = 0;

    // column count includes null bits field
    uint64_t cols = table->GetFieldCount() - 1;

    for (; i < cols; i++) {
        if (BITMAP_GET(attrs_used, i))
            MOTToDatum(table, tupdesc->attrs[i], srcRow, &(slot->tts_values[i]), &(slot->tts_isnull[i]));
        else {
            slot->tts_isnull[i] = true;
            slot->tts_values[i] = PointerGetDatum(nullptr);
        }
    }
}

// useful functions for data conversion: utils/fmgr/gmgr.cpp
void MOTAdaptor::MOTToDatum(MOT::Table* table, const Form_pg_attribute attr, uint8_t* data, Datum* value, bool* is_null)
{
    EnsureSafeThreadAccessInline();
    if (!BITMAP_GET(data, (attr->attnum - 1))) {
        *is_null = true;
        *value = PointerGetDatum(nullptr);

        return;
    }

    size_t len = 0;
    MOT::Column* col = table->GetField(attr->attnum);

    *is_null = false;
    switch (attr->atttypid) {
        case VARCHAROID:
        case BPCHAROID:
        case TEXTOID:
        case CLOBOID:
        case BYTEAOID: {
            uintptr_t tmp;
            col->Unpack(data, &tmp, len);

            bytea* result = (bytea*)palloc(len + VARHDRSZ);
            errno_t erc = memcpy_s(VARDATA(result), len, (uint8_t*)tmp, len);
            securec_check(erc, "\0", "\0");
            SET_VARSIZE(result, len + VARHDRSZ);

            *value = PointerGetDatum(result);
            break;
        }
        case NUMERICOID: {
            MOT::DecimalSt* d;
            col->Unpack(data, (uintptr_t*)&d, len);

            *value = NumericGetDatum(MOTNumericToPG(d));
            break;
        }
        default:
            col->Unpack(data, value, len);
            break;
    }
}

void MOTAdaptor::DatumToMOT(MOT::Column* col, Datum datum, Oid type, uint8_t* data)
{
    EnsureSafeThreadAccessInline();
    switch (type) {
        case BYTEAOID:
        case TEXTOID:
        case VARCHAROID:
        case CLOBOID:
        case BPCHAROID: {
            bytea* txt = DatumGetByteaP(datum);
            size_t size = VARSIZE(txt);  // includes header len VARHDRSZ
            char* src = VARDATA(txt);
            col->Pack(data, (uintptr_t)src, size - VARHDRSZ);

            if ((char*)datum != (char*)txt) {
                pfree(txt);
            }

            break;
        }
        case NUMERICOID: {
            Numeric n = DatumGetNumeric(datum);
            char buf[DECIMAL_MAX_SIZE];
            MOT::DecimalSt* d = (MOT::DecimalSt*)buf;

            if (NUMERIC_NDIGITS(n) > DECIMAL_MAX_DIGITS) {
                ereport(ERROR,
                    (errmodule(MOD_MOT),
                        errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("Value exceeds maximum precision: %d", NUMERIC_MAX_PRECISION)));
                break;
            }
            PGNumericToMOT(n, *d);
            col->Pack(data, (uintptr_t)d, DECIMAL_SIZE(d));

            break;
        }
        default:
            col->Pack(data, datum, col->m_size);
            break;
    }
}

inline void MOTAdaptor::VarcharToMOTKey(
    MOT::Column* col, Oid datumType, Datum datum, Oid colType, uint8_t* data, size_t len, KEY_OPER oper, uint8_t fill)
{
    bool noValue = false;
    switch (datumType) {
        case BYTEAOID:
        case TEXTOID:
        case VARCHAROID:
        case CLOBOID:
        case BPCHAROID:
            break;
        default:
            noValue = true;
            errno_t erc = memset_s(data, len, 0x00, len);
            securec_check(erc, "\0", "\0");
            break;
    }

    if (noValue) {
        return;
    }

    bytea* txt = DatumGetByteaP(datum);
    size_t size = VARSIZE(txt);  // includes header len VARHDRSZ
    char* src = VARDATA(txt);

    if (size > len) {
        size = len;
    }

    size -= VARHDRSZ;
    if (oper == KEY_OPER::READ_KEY_LIKE) {
        if (src[size - 1] == '%') {
            size -= 1;
        } else {
            // switch to equal
            if (colType == BPCHAROID) {
                fill = 0x20;  // space ' ' == 0x20
            } else {
                fill = 0x00;
            }
        }
    } else if (colType == BPCHAROID) {  // handle padding for blank-padded type
        fill = 0x20;
    }
    col->PackKey(data, (uintptr_t)src, size, fill);

    if ((char*)datum != (char*)txt) {
        pfree(txt);
    }
}

inline void MOTAdaptor::FloatToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data)
{
    if (datumType == FLOAT8OID) {
        MOT::DoubleConvT dc;
        MOT::FloatConvT fc;
        dc.m_r = (uint64_t)datum;
        fc.m_v = (float)dc.m_v;
        uint64_t u = (uint64_t)fc.m_r;
        col->PackKey(data, u, col->m_size);
    } else {
        col->PackKey(data, datum, col->m_size);
    }
}

inline void MOTAdaptor::NumericToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data)
{
    Numeric n = DatumGetNumeric(datum);
    char buf[DECIMAL_MAX_SIZE];
    MOT::DecimalSt* d = (MOT::DecimalSt*)buf;
    PGNumericToMOT(n, *d);
    col->PackKey(data, (uintptr_t)d, DECIMAL_SIZE(d));
}

inline void MOTAdaptor::TimestampToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data)
{
    if (datumType == TIMESTAMPTZOID) {
        Timestamp result = DatumGetTimestamp(DirectFunctionCall1(timestamptz_timestamp, datum));
        col->PackKey(data, result, col->m_size);
    } else if (datumType == DATEOID) {
        Timestamp result = DatumGetTimestamp(DirectFunctionCall1(date_timestamp, datum));
        col->PackKey(data, result, col->m_size);
    } else {
        col->PackKey(data, datum, col->m_size);
    }
}

inline void MOTAdaptor::TimestampTzToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data)
{
    if (datumType == TIMESTAMPOID) {
        TimestampTz result = DatumGetTimestampTz(DirectFunctionCall1(timestamp_timestamptz, datum));
        col->PackKey(data, result, col->m_size);
    } else if (datumType == DATEOID) {
        TimestampTz result = DatumGetTimestampTz(DirectFunctionCall1(date_timestamptz, datum));
        col->PackKey(data, result, col->m_size);
    } else {
        col->PackKey(data, datum, col->m_size);
    }
}

inline void MOTAdaptor::DateToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data)
{
    if (datumType == TIMESTAMPOID) {
        DateADT result = DatumGetDateADT(DirectFunctionCall1(timestamp_date, datum));
        col->PackKey(data, result, col->m_size);
    } else if (datumType == TIMESTAMPTZOID) {
        DateADT result = DatumGetDateADT(DirectFunctionCall1(timestamptz_date, datum));
        col->PackKey(data, result, col->m_size);
    } else {
        col->PackKey(data, datum, col->m_size);
    }
}

void MOTAdaptor::DatumToMOTKey(
    MOT::Column* col, Oid datumType, Datum datum, Oid colType, uint8_t* data, size_t len, KEY_OPER oper, uint8_t fill)
{
    EnsureSafeThreadAccessInline();
    switch (colType) {
        case BYTEAOID:
        case TEXTOID:
        case VARCHAROID:
        case CLOBOID:
        case BPCHAROID:
            VarcharToMOTKey(col, datumType, datum, colType, data, len, oper, fill);
            break;
        case FLOAT4OID:
            FloatToMOTKey(col, datumType, datum, data);
            break;
        case NUMERICOID:
            NumericToMOTKey(col, datumType, datum, data);
            break;
        case TIMESTAMPOID:
            TimestampToMOTKey(col, datumType, datum, data);
            break;
        case TIMESTAMPTZOID:
            TimestampTzToMOTKey(col, datumType, datum, data);
            break;
        case DATEOID:
            DateToMOTKey(col, datumType, datum, data);
            break;
        default:
            col->PackKey(data, datum, col->m_size);
            break;
    }
}

MOTFdwStateSt* InitializeFdwState(void* fdwState, List** fdwExpr, uint64_t exTableID)
{
    MOTFdwStateSt* state = (MOTFdwStateSt*)palloc0(sizeof(MOTFdwStateSt));
    List* values = (List*)fdwState;

    state->m_allocInScan = true;
    state->m_foreignTableId = exTableID;
    if (list_length(values) > 0) {
        ListCell* cell = list_head(values);
        int type = ((Const*)lfirst(cell))->constvalue;
        if (type != FDW_LIST_STATE) {
            return state;
        }
        cell = lnext(cell);
        state->m_cmdOper = (CmdType)((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->m_order = (SORTDIR_ENUM)((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->m_hasForUpdate = (bool)((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->m_foreignTableId = ((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->m_numAttrs = ((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->m_ctidNum = ((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->m_numExpr = ((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);

        int len = BITMAP_GETLEN(state->m_numAttrs);
        state->m_attrsUsed = (uint8_t*)palloc0(len);
        state->m_attrsModified = (uint8_t*)palloc0(len);
        BitmapDeSerialize(state->m_attrsUsed, len, &cell);

        if (cell != NULL) {
            state->m_bestIx = &state->m_bestIxBuf;
            state->m_bestIx->Deserialize(cell, exTableID);
        }

        if (fdwExpr != NULL && *fdwExpr != NULL) {
            ListCell* c = NULL;
            int i = 0;

            // divide fdw expr to param list and original expr
            state->m_remoteCondsOrig = NULL;

            foreach (c, *fdwExpr) {
                if (i < state->m_numExpr) {
                    i++;
                    continue;
                } else {
                    state->m_remoteCondsOrig = lappend(state->m_remoteCondsOrig, lfirst(c));
                }
            }

            *fdwExpr = list_truncate(*fdwExpr, state->m_numExpr);
        }
    }
    return state;
}

void* SerializeFdwState(MOTFdwStateSt* state)
{
    List* result = NULL;

    // set list type to FDW_LIST_STATE
    result = lappend(result, makeConst(INT4OID, -1, InvalidOid, 4, FDW_LIST_STATE, false, true));
    result = lappend(result, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(state->m_cmdOper), false, true));
    result = lappend(result, makeConst(INT1OID, -1, InvalidOid, 4, Int8GetDatum(state->m_order), false, true));
    result = lappend(result, makeConst(BOOLOID, -1, InvalidOid, 1, BoolGetDatum(state->m_hasForUpdate), false, true));
    result =
        lappend(result, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(state->m_foreignTableId), false, true));
    result = lappend(result, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(state->m_numAttrs), false, true));
    result = lappend(result, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(state->m_ctidNum), false, true));
    result = lappend(result, makeConst(INT2OID, -1, InvalidOid, 2, Int16GetDatum(state->m_numExpr), false, true));
    int len = BITMAP_GETLEN(state->m_numAttrs);
    result = BitmapSerialize(result, state->m_attrsUsed, len);

    if (state->m_bestIx != nullptr) {
        state->m_bestIx->Serialize(&result);
    }
    ReleaseFdwState(state);
    return result;
}

void ReleaseFdwState(MOTFdwStateSt* state)
{
    CleanCursors(state);

    if (state->m_currTxn) {
        state->m_currTxn->m_queryState.erase((uint64_t)state);
    }

    if (state->m_bestIx && state->m_bestIx != &state->m_bestIxBuf)
        pfree(state->m_bestIx);

    if (state->m_remoteCondsOrig != nullptr)
        list_free(state->m_remoteCondsOrig);

    if (state->m_attrsUsed != NULL)
        pfree(state->m_attrsUsed);

    if (state->m_attrsModified != NULL)
        pfree(state->m_attrsModified);

    state->m_table = NULL;
    pfree(state);
}














static uint64_t max_length = 10000;//cache中容器预设大小

template<typename T>
using BlockingConcurrentQueue =  moodycamel::BlockingConcurrentQueue<T>;
// using BlockingConcurrentQueue = BlockingMPMCQueue<T>;
struct pack_params {
    std::string* str;
    std::unique_ptr<merge::MergeRequest_Transaction> txn;
    uint64_t epoch, index;
    pack_params(std::string* s, std::unique_ptr<merge::MergeRequest_Transaction> &&t, uint64_t e, uint64_t i):str(s), epoch(e), index(i){
        txn = std::move(t);
    }
    pack_params(){}
};

struct pack_thread_params {
    uint64_t index;
    uint64_t current_epoch;
    std::shared_ptr<std::vector<std::shared_ptr<BlockingConcurrentQueue<std::string*>>>> local_change_set_txn_ptr_temp;
    std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>> local_change_set_txn_num_ptr_temp;
    pack_thread_params(uint64_t index, uint64_t ce, 
        std::shared_ptr<std::vector<std::shared_ptr<BlockingConcurrentQueue<std::string*>>>> ptr1, 
        std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>> ptr2): 
        index(index), current_epoch(ce), local_change_set_txn_ptr_temp(ptr1), local_change_set_txn_num_ptr_temp(ptr2){}
    pack_thread_params(){}
};

struct send_thread_params {
    uint64_t current_epoch;
    uint64_t tot;
    std::string* merge_request_ptr;
    send_thread_params(uint64_t ce, uint64_t tot_temp, std::string* ptr1):
        current_epoch(ce), tot(tot_temp), merge_request_ptr(ptr1){}
    send_thread_params(){}
};

struct commit_thread_params {
    std::unique_ptr<merge::MergeRequest_Transaction> txn;
    std::unique_ptr<std::vector<MOT::Row*>> row_vector;
    commit_thread_params(std::unique_ptr<merge::MergeRequest_Transaction> &&ptr1, std::unique_ptr<std::vector<MOT::Row*>> &&ptr2): 
        txn(std::move(ptr1)), row_vector(std::move(ptr2)) {}
    commit_thread_params() {}
};

class LocalWriteSet {
public:
    uint64_t _max_length = 10000, _pack_num;
    std::vector<std::shared_ptr<BlockingConcurrentQueue<std::unique_ptr<pack_params>>>>
        txn_queue_ptrs;
    //当前epoch放入多少个txn
    std::vector<std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>> txn_num_ptrs;
    //pack_thread取出了多少个 两者比较来判断是否发送epoch_end message
    std::vector<std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>> packd_txn_num_ptrs;

    void Init(uint64_t pack_num, uint64_t length) {
        _pack_num = pack_num;
        _max_length = length;
        txn_queue_ptrs.resize(_pack_num + 2);
        txn_num_ptrs.resize(_max_length + 2);
        packd_txn_num_ptrs.resize(_max_length + 2);
        for(int i = 0; i <= (int)_pack_num; i++) {
            txn_queue_ptrs[i] = std::make_shared<BlockingConcurrentQueue<std::unique_ptr<pack_params>>>();
        }
        for(int i = 0; i <= (int)length; i++) {
            txn_num_ptrs[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
            packd_txn_num_ptrs[i] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
            txn_num_ptrs[i]->resize(_pack_num + 2);
            packd_txn_num_ptrs[i]->resize(_pack_num + 2);
            for(int j = 0; j <= (int)_pack_num; j++){
                (*txn_num_ptrs[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
                (*packd_txn_num_ptrs[i])[j] = std::make_shared<std::atomic<uint64_t>>(0);
            }
        }
    }
    
    void PushBack(uint64_t epoch, std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>> ptr2, 
        std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>> ptr3) {
        auto epoch_mod = epoch % _max_length;
        txn_num_ptrs[epoch_mod] = ptr2;
        packd_txn_num_ptrs[epoch_mod] = ptr3;
    }
    
    uint64_t LoadChangeSet(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*txn_num_ptrs[epoch_mod])[i]->load();
        return ans;
    }

    uint64_t LoadPackedTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*packd_txn_num_ptrs[epoch_mod])[i]->load();
        return ans;
    }

    bool IsCurrentEpochFinished(uint64_t epoch) {
        epoch %= _max_length;
        return (this->LoadPackedTxnNum(epoch) == this->LoadChangeSet(epoch));
    }

    bool AddSendTask(uint64_t epoch_num) {
        if((txn_queue_ptrs[0]->enqueue(std::move(std::make_unique<pack_params>(nullptr, nullptr, epoch_num, 0))))){
            if(txn_queue_ptrs[0]->enqueue(std::move(std::make_unique<pack_params>(nullptr, nullptr, 0, 0)))) Assert(false);//防止moodycamel取不出
            // MOT_LOG_INFO("添加一个epoch send task %llu", epoch_num);
            return true;
        }
        else {
            Assert(false);
            return false;
        }
    }
    
    bool Enqueue(std::string* ptr1, std::unique_ptr<merge::MergeRequest_Transaction> &&txn, uint64_t epoch_num, uint64_t index) {
        if((txn_queue_ptrs[index]->enqueue(std::move(std::make_unique<pack_params>(ptr1, std::move(txn), epoch_num, index))))){
            if(txn_queue_ptrs[index]->enqueue(std::move(std::make_unique<pack_params>(nullptr, nullptr, 0, 0)))) Assert(false);//防止moodycamel取不出
            return true;
        }
        else {
            Assert(false);
            return false;
        }
    }

    bool TryDequeue(std::unique_ptr<pack_params> &v, uint64_t index) {
        if(txn_queue_ptrs[index]->try_dequeue(v)){
            return true;
        }
        return false;
    }

    bool TryAddNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
        if((*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_add(value) % MOD_NUM == 0
            && epoch_num < MOTAdaptor::GetPhysicalEpoch() ){
                (*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_sub(value);
                return false;
            }
        else {
            return true;
        }
    }

    bool AddNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
        (*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_add(value);
        return true;
    }

    bool SubNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
        (*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_sub(value);
        return true;
    }

    bool AddPackedNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
        (*(packd_txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_add(value);
        return true;
    }

    bool SubPackedNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
        (*(packd_txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_sub(value);
        return true;
    }

};

class RemoteCache {
public:    
    uint64_t _max_length = 10000, _pack_num;
    std::unique_ptr<std::atomic<uint64_t>> should_receive_pack_num, online_server_num;
    std::vector<std::unique_ptr<std::atomic<uint64_t>>> is_server_online;
    std::vector<std::vector<std::unique_ptr<std::atomic<uint64_t>>>> 
        received_pack_num, received_txn_num, should_receive_txn_num;
    std::vector<std::unique_ptr<std::atomic<uint64_t>>> 
        received_total_pack_num, received_total_txn_num;

    void Init(uint64_t pack_num, uint64_t length) {
        _max_length = length;
        _pack_num = pack_num;
        received_total_pack_num.reserve(_max_length + 1); 
        received_total_txn_num.reserve(_max_length + 1);
        received_pack_num.reserve(_max_length + 1);
        received_txn_num.reserve(_max_length + 1);
        should_receive_txn_num.reserve(_max_length + 1);
        is_server_online.reserve(kServerNum + 2);
        online_server_num = std::make_unique<std::atomic<uint64_t>>(kServerNum);
        should_receive_pack_num= std::make_unique<std::atomic<uint64_t>>(kServerNum - 1);
        
        for(int i = 0; i <= static_cast<int>(_max_length); i ++){
            received_total_pack_num.emplace_back(std::make_unique<std::atomic<uint64_t>>(0));
            received_total_txn_num.emplace_back(std::make_unique<std::atomic<uint64_t>>(0));
            received_pack_num.emplace_back(std::vector<std::unique_ptr<std::atomic<uint64_t>>>(0));
            received_txn_num.emplace_back(std::vector<std::unique_ptr<std::atomic<uint64_t>>>(0));
            should_receive_txn_num.emplace_back(std::vector<std::unique_ptr<std::atomic<uint64_t>>>(0));
            for(int j = 0; j < (int)kServerIp.size() + 2; j++){
                received_pack_num[i].push_back(std::make_unique<std::atomic<uint64_t>>(0));
                received_txn_num[i].push_back(std::make_unique<std::atomic<uint64_t>>(0));
                should_receive_txn_num[i].push_back(std::make_unique<std::atomic<uint64_t>>(0));
            }
        }
        for(int j = 0; j < (int)kServerIp.size() + 2; j++) {
            is_server_online.push_back(std::make_unique<std::atomic<uint64_t>>(0));
        }
        for(int j = 0; j < (int)kServerNum; j++) {
            is_server_online[j]->store(1);
        }
    }
        // received_total_pack_num->fetch_add(value);

    
    uint64_t AddShouldReceiveTxnNum(uint64_t epoch, uint64_t index, uint64_t value) {
        return should_receive_txn_num[epoch % _max_length][index]->fetch_add(value);
    }
    void StoreShouldReceiveTxnNum(uint64_t epoch, uint64_t index, uint64_t value) {
        should_receive_txn_num[epoch % _max_length][index]->store(value);
    }
    uint64_t GetShouldReceiveTxnNum(uint64_t epoch, uint64_t index) {
        return should_receive_txn_num[epoch % _max_length][index]->load();
    }
    uint64_t GetShouldReceiveTxnNum(uint64_t epoch) {
        epoch %= _max_length;
        uint64_t ans = 0;
        for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
            if(received_pack_num[epoch][i]->load() == 1)
                ans += should_receive_txn_num[epoch][i]->load();
        }
        return ans;
    }

    uint64_t AddReceivedPackNum(uint64_t epoch, uint64_t index, uint64_t value) {
        return received_pack_num[epoch % _max_length][index]->fetch_add(value);
    }
    void StoreReceivedPackNum(uint64_t epoch, uint64_t index, uint64_t value) {
        received_pack_num[epoch % _max_length][index]->store(value);
    }
    uint64_t GetReceivedPackNum(uint64_t epoch, uint64_t index) {
        return received_pack_num[epoch % _max_length][index]->load();
    }
    uint64_t GetReceivedPackNum(uint64_t epoch) {
        epoch %= _max_length;
        uint64_t ans = 0;
        for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
            if(received_pack_num[epoch][i]->load() == 1)
                ans ++;
        }
        return ans;
    }

    uint64_t AddReceivedTxnNum(uint64_t epoch, uint64_t index, uint64_t value) {
        return received_txn_num[epoch % _max_length][index]->fetch_add(value);
    }
    void StoreReceivedTxnNum(uint64_t epoch, uint64_t index, uint64_t value) {
        received_txn_num[epoch % _max_length][index]->store(value);
    }
    uint64_t GetReceivedTxnNum(uint64_t epoch, uint64_t index) {
        return received_txn_num[epoch % _max_length][index]->load();
    }
    uint64_t GetReceivedTxnNum(uint64_t epoch) {
        epoch %= _max_length;
        uint64_t ans = 0;
        for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
            if(received_pack_num[epoch][i]->load() == 1)
                ans += received_txn_num[epoch][i]->load();
        }
        return ans;
    }

    uint64_t AddReceivedPackNumTotal(uint64_t epoch, uint64_t value) {
        return received_total_pack_num[epoch % _max_length]->fetch_add(value);
    }
    uint64_t GetReceivedPackNumTotal(uint64_t epoch) {
        return received_total_pack_num[epoch % _max_length]->load();
    }
    uint64_t AddReceivedTxnNumTotal(uint64_t epoch, uint64_t value) {
        return received_total_txn_num[epoch % _max_length]->fetch_add(value);
    }
    uint64_t GetReceivedTxnNumTotal(uint64_t epoch) {
        return received_total_txn_num[epoch % _max_length]->load();
    }

    uint64_t AddShouldReceivePackNum(uint64_t value) {
        return should_receive_pack_num->fetch_add(value);
    }
    uint64_t SubShouldReceivePackNum(uint64_t value) {
        return should_receive_pack_num->fetch_sub(value);
    }
    void StoreShouldReceivePackNum(uint64_t value) {
        should_receive_pack_num->store(value);
    }
    uint64_t GetShouldReceivePackNum() {
        return should_receive_pack_num->load();
    }

    uint64_t AddOnLineServerNum(uint64_t value) {
        return online_server_num->fetch_add(value);
    }
    uint64_t SubOnLineServerNum(uint64_t value) {
        return online_server_num->fetch_sub(value);
    }
    void StoreOnLineServerNum(uint64_t value) {
        online_server_num->store(value);
    }
    uint64_t GetOnLineServerNum() {
        return online_server_num->load();
    }

    
    void Clear(uint64_t epoch) {
        epoch %= _max_length;
        for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
            received_pack_num[epoch][i]->store(0);
            received_txn_num[epoch][i]->store(0);
            should_receive_txn_num[epoch][i]->store(0);
        }
        received_total_pack_num[epoch]->store(0);
        received_total_txn_num[epoch]->store(0);
    }

    void SetServerOnLine(std::string ip) {
        for(int i = 0; i < (int)kServerIp.size(); i++) {
            if(ip == kServerIp[i]) {
                is_server_online[i]->store(1);
            }
        }
    }

    void SetServerOffLine(std::string ip) {
        for(int i = 0; i < (int)kServerIp.size(); i++) {
            if(ip == kServerIp[i]) {
                is_server_online[i]->store(0);
            }
        }
    }

    void SetServerOnLine(uint64_t index) {
        is_server_online[index]->store(1);
    }

    void SetServerOffLine(uint64_t index) {
        is_server_online[index]->store(0);
    }

    bool IsServerOnLine(uint64_t index) {
        return (is_server_online[index]->load() == 1);
    }


    

    void ReceiveAMessage() {

    }

};

class RegionCacheServerState{
public:
    uint64_t _max_length = 10000;
    std::vector<std::unique_ptr<std::atomic<uint64_t>>>  received_epoch;

    void Init(uint64_t pack_num, uint64_t length) {
        _max_length = length;
        received_epoch.reserve(max_length + 1);
        uint64_t val = 1;
        if(is_cache_server_available) {
            val = 0;
        }
        for(int i = 0; i <= static_cast<int>(_max_length); i ++) {
            received_epoch.emplace_back(std::make_unique<std::atomic<uint64_t>>(val));
        }
    }
    
    bool IsCacheServerStored(uint64_t epoch) {
        if(received_epoch[epoch % _max_length]->load() == static_cast<uint64_t>(1)) {
            return true;
        }
        else {
            return false;
        }
    }

    void SetCacheServerStored(uint64_t epoch, uint64_t value) {
        received_epoch[epoch % _max_length]->store(value);
    }

};

bool MOTAdaptor::timerStop = false;
volatile bool 
    MOTAdaptor::remote_execed = false, MOTAdaptor::record_committed = true, MOTAdaptor::remote_record_committed = true, 
    MOTAdaptor::is_current_epoch_abort = false;
volatile uint64_t 
    MOTAdaptor::logical_epoch = 1, MOTAdaptor::physical_epoch = 0, epoch_commit_time = 0;
std::vector<std::unique_ptr<std::atomic<uint64_t>>> 
    MOTAdaptor::local_txn_counters, MOTAdaptor::local_txn_exc_counters, MOTAdaptor::local_txn_index, 
    MOTAdaptor::remote_merged_txn_counters, MOTAdaptor::remote_commit_txn_counters,
    MOTAdaptor::remote_committed_txn_counters, MOTAdaptor::record_commit_txn_counters, MOTAdaptor::record_committed_txn_counters;
std::map<uint64_t, std::unique_ptr<std::vector<MOT::Row*>>> 
    MOTAdaptor::remote_row_ptr_map;
aum::concurrent_unordered_map<std::string, std::string, std::string> 
    MOTAdaptor::insertSet, MOTAdaptor::insertSetForCommit, MOTAdaptor::abort_transcation_csn_set;

uint64_t 
    start_time_ll, start_physical_epoch = 1, new_start_physical_epoch = 1, new_sleep_time = 10000, start_merge_time = 0, commit_time = 0;
struct timeval 
    start_time;
std::atomic<bool> 
    init_ok(false), is_epoch_advance_started(false);
LocalWriteSet local_write_set = LocalWriteSet();
RemoteCache remote_cache = RemoteCache();
RegionCacheServerState cache_server = RegionCacheServerState();
BlockingConcurrentQueue<std::unique_ptr<zmq::message_t>> 
    message_send_pool, message_receive_pool, listen_message_queue;
BlockingConcurrentQueue<std::unique_ptr<send_thread_params>> send_pool;
BlockingConcurrentQueue<std::unique_ptr<merge::MergeRequest_Transaction>> merge_queue;
BlockingConcurrentQueue<std::unique_ptr<commit_thread_params>> commit_txn_queue_struct;


void SetCPU(); 
uint64_t GetSleeptime(uint64_t kSleepTime);
void GenerateSendTasks(uint64_t kPackageNum);
void InsertRowToLocalSet(MOT::TxnManager * txMan, uint64_t index, void* txn_void);

void EpochLogicalTimerManagerThreadMain(uint64_t id);
void EpochPhysicalTimerManagerThreadMain(uint64_t id);
void EpochPackThreadMain(uint64_t id);
void EpochSendThreadMain(uint64_t id);

void EpochListenThreadMain(uint64_t id);
void EpochUnseriThreadMain(uint64_t id);
void EpochMessageCacheManagerThreadMain(uint64_t id);
void EpochUnpackThreadMain(uint64_t id);
void EpochMergeThreadMain(uint64_t id);
void EpochCommitThreadMain(uint64_t id);
void EpochRecordCommitThreadMain(uint64_t id);
void EpochMessageSendThreadMain(uint64_t id);
void EpochMessageListenThreadMain(uint64_t id);

void GenerateServerSendMessage(uint32_t type, uint32_t server_id, std::string ip, uint64_t physical_epoch,
                               uint64_t logical_epoch,uint64_t epoch_size, uint64_t commit_time, uint64_t new_server_id,
                               std::string new_server_ip);

void SetCPU(){
    std::atomic<int> cpu_index(1);
    cpu_set_t logicalEpochSet;
    CPU_ZERO(&logicalEpochSet);
    CPU_SET(cpu_index.fetch_add(1), &logicalEpochSet); //2就是核心号
    int rc = sched_setaffinity(0, sizeof(cpu_set_t), &logicalEpochSet);
    if (rc == -1) {
        ereport(FATAL, (errmsg("绑核失败")));
    }
}
uint64_t now_to_us(){
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
uint64_t GetSleeptime(){
    uint64_t sleep_time;
    struct timeval current_time;
    uint64_t current_time_ll;
    gettimeofday(&current_time, NULL);
    current_time_ll = current_time.tv_sec * 1000000 + current_time.tv_usec;
    sleep_time = current_time_ll - (start_time_ll + (long)(MOTAdaptor::GetPhysicalEpoch() - start_physical_epoch) * kSleepTime);
    if(sleep_time >= kSleepTime){
        MOT_LOG_INFO("start time : %llu, current time : %llu, 差值 %llu ,sleep time : %d", start_time_ll, current_time_ll, sleep_time, 0);
        return 0;
    }
    else{
        // MOT_LOG_INFO("start time : %llu, current time : %llu, 差值 %llu, sleep time : %llu", start_time_ll, current_time_ll, sleep_time, ksleeptime - sleep_time);
        return kSleepTime - sleep_time;
    } 
}

void ChangeEpochInfo(){
}

void string_free(void *data, void *hint){
    delete static_cast<std::string*>(hint);
}

bool MOTAdaptor::InsertTxntoLocalChangeSet(MOT::TxnManager* txMan, const uint64_t& index_pack, const uint64_t& index_unique){
    auto txn = std::make_unique<merge::MergeRequest_Transaction>();
    if(kServerNum > 1) {
        merge::MergeRequest_Transaction_Row *row;
        merge::MergeRequest_Transaction_Row_Column* col;
        MOT::Row* local_row = nullptr;
        MOT::Key* key = nullptr;
        void* buf = nullptr; 
        uint64_t fieldCnt;
        const MOT::Access* access = nullptr;
        MOT::BitmapSet* bmp;
        MOT::TxnOrderedSet_t &orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
        int op_type = 0; //  0-update, 1-insert, 2-delete
        for (const auto &raPair : orderedSet){
            access = raPair.second;
            if (access->m_type == MOT::RD) {
                continue;
            }
            row = txn->add_row();
            if (access->m_type == MOT::WR){
                op_type = 0;
                local_row = access->m_localRow;
            }
            else if (access->m_type == MOT::INS){
                op_type = 1;
                local_row = access->m_localInsertRow;
            }
            else if (access->m_type == MOT::DEL){
                op_type = 2;
                local_row = access->m_localRow;
            }
            if(local_row == nullptr || local_row->GetTable() == nullptr){
                return false;
            }
            key = local_row->GetTable()->BuildKeyByRow(local_row, txMan, buf);
            if (key == nullptr) {
                return false;
            }
            row->set_key(std::move(std::string(key->GetKeyBuf(), key->GetKeyBuf() + key->GetKeyLength() )));
            row->set_tablename(local_row->GetTable()->GetLongTableName());
            if (op_type == 0) {
                // row->set_data(local_row->GetData(), local_row->GetTable()->GetTupleSize());

                fieldCnt = local_row->GetTable()->GetFieldCount();
                bmp = const_cast<MOT::BitmapSet*>(&(access->m_modifiedColumns));
                for (uint16_t field_i = 0; field_i < fieldCnt - 1 ; field_i ++) { 
                    if (bmp->GetBit(field_i)) { //真正的列有一个单位的偏移量，这是经过试验得出的结论 
                        int real_field = field_i + 1;
                        col = row->add_column(); 
                        col->set_id(real_field); 
                        col->set_value(local_row->GetValue(real_field),local_row->GetTable()->GetFieldSize(real_field));
                    }
                }
            }
            else if (op_type == 1) {
                row->set_data(local_row->GetData(), local_row->GetTable()->GetTupleSize());
            }
            row->set_type(op_type);
        }
        
        txn->set_startepoch(txMan->GetStartEpoch());
        txn->set_server_id(local_ip_index);
        txn->set_txnid(local_ip_index * 100000000000000 + index_pack * 10000000000 + index_unique * 1000000);
    }

    add_num_again:
    txMan->SetCommitEpoch(MOTAdaptor::GetPhysicalEpoch());
    if(!local_write_set.TryAddNum(txMan->GetCommitEpoch(), index_pack, ADD_NUM)){
        goto add_num_again;
    }
    txMan->SetCommitSequenceNumber(now_to_us());

    if(kServerNum > 1) {
        txn->set_commitepoch(txMan->GetCommitEpoch());
        txn->set_csn(txMan->GetCommitSequenceNumber());
        if(is_total_pack) {
            if(local_write_set.Enqueue(nullptr, std::move(txn), txMan->GetCommitEpoch(), index_pack)){
                txn = nullptr;
                return true;
            }
            else{
                local_write_set.SubNum(txMan->GetCommitEpoch(), index_pack, ADD_NUM);
                txn = nullptr;
                return false;
            }
        }
        else {
            std::string* serialized_txn_str_ptr = new std::string();
            if(is_protobuf_gzip == true) {
                google::protobuf::io::GzipOutputStream::Options options;
                options.format = google::protobuf::io::GzipOutputStream::GZIP;
                options.compression_level = 9;
                google::protobuf::io::StringOutputStream outputStream(&(*serialized_txn_str_ptr));
                google::protobuf::io::GzipOutputStream gzipStream(&outputStream, options);
                txn->SerializeToZeroCopyStream(&gzipStream);
                gzipStream.Close();
            }
            else {
                txn->SerializeToString(serialized_txn_str_ptr);
            }
            MOT_LOG_INFO("=**= protobuf size %lu", serialized_txn_str_ptr->size());
            if(local_write_set.Enqueue(serialized_txn_str_ptr, nullptr, txMan->GetCommitEpoch(), index_pack)){
                txn = nullptr;
                return true;
            }
            else{
                local_write_set.SubNum(txMan->GetCommitEpoch(), index_pack, ADD_NUM);
                txn = nullptr;
                return false;
            }
        }
        
    }
    else {
        txn = nullptr;
        return true;
    }
}

void InitEpochTimerManager(){
    //==========Cache===============
    max_length = kCacheMaxLength;
    local_write_set.Init(kPackageNum, max_length);
    remote_cache.Init(kPackageNum, max_length);
    cache_server.Init(kPackageNum, max_length);
    //==========Logical=============
    for(int i = 0; i <= (int)kPackageNum; i++){
        MOTAdaptor::local_txn_counters.push_back(std::move(std::make_unique<std::atomic<uint64_t>>(0)));
        MOTAdaptor::local_txn_exc_counters.push_back(std::move(std::make_unique<std::atomic<uint64_t>>(0)));
        MOTAdaptor::local_txn_index.push_back(std::move(std::make_unique<std::atomic<uint64_t>>(0)));
        MOTAdaptor::record_commit_txn_counters.push_back(std::move(std::make_unique<std::atomic<uint64_t>>(0)));
        MOTAdaptor::record_committed_txn_counters.push_back(std::move(std::make_unique<std::atomic<uint64_t>>(0)));
        MOTAdaptor::remote_merged_txn_counters.push_back(std::move(std::make_unique<std::atomic<uint64_t>>(0)));
        MOTAdaptor::remote_commit_txn_counters.push_back(std::move(std::make_unique<std::atomic<uint64_t>>(0)));
        MOTAdaptor::remote_committed_txn_counters.push_back(std::move(std::make_unique<std::atomic<uint64_t>>(0)));
    }

    GenerateSendTasks(kPackageNum);
    MOTAdaptor::AddPhysicalEpoch();
    init_ok.store(true);
}

void GenerateServerSendMessage(uint32_t type, uint32_t server_id, std::string ip, uint64_t physical_epoch,
                               uint64_t logical_epoch,uint64_t epoch_size, uint64_t commit_time, uint64_t new_server_id,
                               std::string new_server_ip){
}

void GenerateSendTasks(uint64_t kPackageNum){
    auto ptr2 = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
    ptr2->resize(kPackageNum + 2);
    auto ptr3 = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
    ptr3->resize(kPackageNum + 2);
    for(int i = 0; i <= (int)kPackageNum; i++){
        (*ptr2)[i] = std::make_shared<std::atomic<uint64_t>>(0);
        (*ptr3)[i] = std::make_shared<std::atomic<uint64_t>>(0);
    }
    auto num = MOTAdaptor::GetPhysicalEpoch() + 1;
    local_write_set.PushBack(num, ptr2, ptr3);
}

































void EpochLogicalTimerManagerThreadMain(uint64_t id){
    SetCPU();
    MOT_LOG_INFO("线程开始工作 EpochLogicalTimerManagerThreadMain ");
    uint64_t remote_merged_txn_num = 0, remote_commit_txn_num = 0, current_local_txn_num = 0,
        cnt = 0, epoch = 1, epoch_mod = 1, cache_sercer_available = 1;
    while( MOTAdaptor::GetPhysicalEpoch() == 0) {
        usleep(50);
    }
    if(is_cache_server_available)
        cache_sercer_available = 0;
    for(;;){
        //等所有上一个epoch 的事务写完 再开始下一个epoch
        cnt = 0;
        while(MOTAdaptor::GetRecordCommittedTxnCounters() != MOTAdaptor::GetRecordCommitTxnCounters()) usleep(20);
        MOTAdaptor::SetRecordCommitted(true);
        MOT_LOG_INFO("上一个epoch所有事务写入完成 %llu, %llu %llu", MOTAdaptor::GetRecordCommittedTxnCounters(), 
            MOTAdaptor::GetRecordCommitTxnCounters(), now_to_us());

        while(MOTAdaptor::GetPhysicalEpoch() <= MOTAdaptor::GetLogicalEpoch() + kDelayEpochNum) usleep(20);
        
        while(static_cast<uint64_t>(local_write_set.LoadChangeSet(epoch_mod) / MOD_NUM) 
            != MOTAdaptor::GetLocalTxnExcCounters()){
            cnt++;
            if(cnt % 100 == 0){
                MOT_LOG_INFO("等待本地事务进入commit完成 %llu %llu %llu, %llu %llu", MOTAdaptor::GetLogicalEpoch(), 
                    static_cast<uint64_t>(local_write_set.LoadChangeSet(epoch_mod) / MOD_NUM)  , MOTAdaptor::GetLocalTxnExcCounters(), 
                    0, now_to_us());
            }
            usleep(20);
        } 
        while(remote_cache.GetReceivedPackNum(epoch_mod) != remote_cache.GetShouldReceivePackNum() ) {
            cnt++;
            if(cnt % 100 == 0){
                MOT_LOG_INFO("等待远端事务接收完成 %llu, %llu, %llu, %llu %llu", MOTAdaptor::GetLogicalEpoch(), 
                    remote_cache.GetShouldReceivePackNum() , remote_cache.GetReceivedPackNum(epoch_mod), 
                    remote_cache.GetShouldReceiveTxnNum(epoch_mod), now_to_us());
            }
            usleep(20);
        }
        remote_merged_txn_num = remote_cache.GetShouldReceiveTxnNum(epoch_mod);
        while(MOTAdaptor::GetRemoteMergedTxnCounters() != remote_merged_txn_num) {
            cnt++;
            if(cnt % 100 == 0){
                MOT_LOG_INFO("等待远端事务执行完成 %llu, %llu, %llu, %llu %llu", MOTAdaptor::GetLogicalEpoch(), 
                    remote_cache.GetReceivedTxnNum(epoch_mod) , MOTAdaptor::GetRemoteMergedTxnCounters(), 
                    remote_merged_txn_num, now_to_us());
            }
            usleep(20);
        }
        while(!MOTAdaptor::IsLocalTxnCountersExcEqualZero()){
            cnt++;
            if(cnt % 100 == 0){
                MOT_LOG_INFO("等待本地事务完成 %llu, %llu, %llu, %llu %llu", MOTAdaptor::GetLogicalEpoch(), 
                    MOTAdaptor::GetLocalTxnCounters() , MOTAdaptor::GetRemoteMergedTxnCounters(), 
                    remote_merged_txn_num, now_to_us());
            }
            usleep(20);
        } 

        while(!cache_server.IsCacheServerStored(epoch_mod)) {
            cnt++;
            if(cnt % 100 == 0){
                MOT_LOG_INFO("等待cache接收完成 %llu %llu %llu", MOTAdaptor::GetLogicalEpoch(), 
                    cache_server.IsCacheServerStored(epoch_mod), now_to_us());
            }
            usleep(20);
        }

        // ======= Merge结束 开始Commit ============   
        MOTAdaptor::SetRecordCommitted(false);
        MOTAdaptor::SetRemoteRecordCommitted(false);
        MOTAdaptor::SetRemoteExeced(true);
        current_local_txn_num = static_cast<uint64_t>(MOTAdaptor::GetLocalTxnCounters());
        remote_commit_txn_num = MOTAdaptor::GetRemoteCommitTxnCounters();

        MOT_LOG_INFO("==进行一个Epoch的合并 当前logic epoch：%llu current_local_txn_num %llu, remote_merged_txn_num %llu remote_commit_txn_num %llu %llu", 
            MOTAdaptor::GetLogicalEpoch(), current_local_txn_num, remote_merged_txn_num, 
            remote_commit_txn_num, now_to_us());
        
        while(MOTAdaptor::GetRemoteCommittedTxnCounters() != remote_commit_txn_num) {
            cnt++;
            if(cnt % 100 == 0){
                MOT_LOG_INFO("==进行一个Epoch的合并 当前logic epoch：%llu current_local_txn_num %llu, remote_merged_txn_num %llu remote_commit_txn_num %llu remote_committed_txn_num %llu %llu", 
                    MOTAdaptor::GetLogicalEpoch(), current_local_txn_num, remote_merged_txn_num, 
                    remote_commit_txn_num, MOTAdaptor::GetRemoteCommittedTxnCounters(), now_to_us());
            }
            usleep(20);
        }
        MOTAdaptor::SetRemoteRecordCommitted(true);
        while(!MOTAdaptor::IsLocalTxnCountersComEqualZero()) {
            cnt++;
            if(cnt % 100 == 0){
                MOT_LOG_INFO("==进行一个Epoch的合并 commit 当前logic epoch：%llu current_local_txn_num %llu,  remote_commit_txn_num %llu remote_committed_txn_num %llu local_txn_counter %llu %llu", 
                    MOTAdaptor::GetLogicalEpoch(), current_local_txn_num, remote_commit_txn_num, 
                    MOTAdaptor::GetRemoteCommittedTxnCounters(), MOTAdaptor::GetLocalTxnCounters(), now_to_us());
            }
            usleep(20);
        }
        
        // ============= 结束处理 ==================
        //远端事务已经写完，不写完无法开始下一个logical epoch
        cache_server.SetCacheServerStored(epoch_mod, cache_sercer_available);
        remote_cache.Clear(epoch_mod);
        epoch ++;
        epoch_mod = epoch % max_length;
        MOTAdaptor::LogicalEndEpoch();
        epoch_commit_time = commit_time = now_to_us();
        MOT_LOG_INFO("==================完成一个Epoch的合并，physical epoch: %llu 到达新的logic epoch：%llu,\
        local commit %llu, remote merge %llu, remote commit %llu, time %llu", 
            MOTAdaptor::GetPhysicalEpoch(), MOTAdaptor::GetLogicalEpoch(), current_local_txn_num, remote_merged_txn_num, 
            remote_commit_txn_num, now_to_us());
    }
}

void EpochPhysicalTimerManagerThreadMain(uint64_t id){
    SetCPU();
    InitEpochTimerManager();
    //==========同步============
    zmq::message_t message;
    zmq::context_t context(1);
    zmq::socket_t request_puller(context, ZMQ_PULL);
    request_puller.bind("tcp://*:5546");
    MOT_LOG_INFO("EpochPhysicalTimer 等待接受同步消息");
    request_puller.recv(&message);
    gettimeofday(&start_time, NULL);
    start_time_ll = start_time.tv_sec * 1000000 + start_time.tv_usec;
    if(!is_epoch_advanced_by_message){
        uint64_t sleep_time = static_cast<uint64_t>((((start_time.tv_sec / 60) + 1) * 60) * 1000000);
        usleep(sleep_time - start_time_ll);
        gettimeofday(&start_time, NULL);
        start_time_ll = start_time.tv_sec * 1000000 + start_time.tv_usec;
    }
    MOT_LOG_INFO("EpochTimerManager 同步完成，数据库开始正常运行 %d %d", is_stable_epoch_send, is_epoch_advanced_by_message);
    epoch_commit_time = commit_time = now_to_us();
    is_epoch_advance_started.store(true);
    uint64_t epoch = MOTAdaptor::GetPhysicalEpoch(), epoch_mod = MOTAdaptor::GetPhysicalEpoch();
    if(is_epoch_advanced_by_message){
        while(!MOTAdaptor::IsTimerStop()){
            MOT_LOG_INFO("====开始一个Physiacal Epoch physical epoch：%llu logical epoch %llu time %lu",MOTAdaptor::GetPhysicalEpoch(), 
                MOTAdaptor::GetLogicalEpoch(), now_to_us());
            request_puller.recv(&message);
            GenerateSendTasks(kPackageNum);
            ChangeEpochInfo();
            MOTAdaptor::AddPhysicalEpoch();
            epoch ++;
            epoch_mod = epoch % max_length; 
            local_write_set.AddSendTask(epoch - 1);
        }
    }
    else{
        while(!MOTAdaptor::IsTimerStop()){
            MOT_LOG_INFO("====开始一个Physiacal Epoch physical epoch：%llu logical epoch %llu time %lu",MOTAdaptor::GetPhysicalEpoch(), 
                MOTAdaptor::GetLogicalEpoch(), now_to_us());
            // request_puller.recv(&message);
            usleep(GetSleeptime());
            GenerateSendTasks(kPackageNum);
            ChangeEpochInfo();
            MOTAdaptor::AddPhysicalEpoch();
            epoch ++;
            epoch_mod = epoch % max_length; 
            local_write_set.AddSendTask(epoch - 1);
        }
    }
}

BlockingConcurrentQueue<std::unique_ptr<pack_params>> q[32];
void PackMergeRequest(uint64_t id){
    uint64_t current_epoch = 0, send_index = 0, index = id, epoch = id, min_epoch = id, max_epoch = id;
    if(id == 0) max_epoch = min_epoch = epoch = kPackThreadNum;
    std::shared_ptr<zmq::message_t> merge_request_ptr;
    std::unique_ptr<pack_params> pack_param;
    std::unique_ptr<pack_thread_params> params;
    std::unique_ptr<zmq::message_t> msg;
    std::string *serialized_txn_str_ptr;
    std::unique_ptr<merge::MergeRequest_Transaction> txn_end = std::make_unique<merge::MergeRequest_Transaction>();
    txn_end->set_startepoch(0);
    txn_end->set_commitepoch(0);
    txn_end->set_csn(0);
    txn_end->set_txnid(0);
    txn_end->set_server_id(local_ip_index);
    bool sleep_flag = false;
    std::vector<merge::MergeRequest*> merge_request_vector;
    merge_request_vector.push_back(new merge::MergeRequest());
    merge_request_vector[0]->set_epoch(epoch);
    merge_request_vector[0]->set_server_id(local_ip_index);
    while(true) {
        sleep_flag = true;
        while(max_epoch <= min_epoch) {
            auto merge_request = new merge::MergeRequest();
            max_epoch += kPackThreadNum;
            merge_request->set_epoch(max_epoch);
            merge_request->set_server_id(local_ip_index);
            merge_request_vector.push_back(merge_request);
        }

        while(q[id].try_dequeue(pack_param)) {
            if(kServerNum == 1) continue;
            if(pack_param->epoch == 0) continue;
            current_epoch = pack_param->epoch;
            while(max_epoch <= current_epoch) {
                auto merge_request = new merge::MergeRequest();
                max_epoch += kPackThreadNum;
                merge_request->set_epoch(max_epoch);
                merge_request->set_server_id(local_ip_index);
                merge_request_vector.push_back(merge_request);
            }
            sleep_flag = false;
            if(pack_param->txn != nullptr) {
                auto i = (current_epoch - min_epoch) / kPackThreadNum;
                auto ptr = merge_request_vector[i]->add_txn();
                *(ptr) = std::move(*(pack_param->txn));
                local_write_set.AddPackedNum(current_epoch, pack_param->index, MOD_NUM);
                local_write_set.SubNum(current_epoch, pack_param->index, 1);
            }
        }

        while(kServerNum > 1 && MOTAdaptor::GetPhysicalEpoch() > min_epoch &&  local_write_set.IsCurrentEpochFinished(min_epoch) == true) {
            sleep_flag = false;
            serialized_txn_str_ptr = new std::string(); 
            if(is_protobuf_gzip == true) {
                google::protobuf::io::GzipOutputStream::Options options;
                options.format = google::protobuf::io::GzipOutputStream::GZIP;
                options.compression_level = 9;
                google::protobuf::io::StringOutputStream outputStream(&(*serialized_txn_str_ptr));
                google::protobuf::io::GzipOutputStream gzipStream(&outputStream, options);
                merge_request_vector[0]->SerializeToZeroCopyStream(&gzipStream);
                gzipStream.Close();
            }
            else {
                merge_request_vector[0]->SerializeToString(serialized_txn_str_ptr);
            }
            MOT_LOG_INFO("Merge_Request size %lu", serialized_txn_str_ptr->size());
            if(!send_pool.enqueue(std::move(std::make_unique<send_thread_params>(min_epoch, 1, serialized_txn_str_ptr))) ) Assert(false);
            if(!send_pool.enqueue(std::move(std::make_unique<send_thread_params>(0, 0, nullptr))) )  Assert(false); //防止moodycamel取不出
            MOT_LOG_INFO("MergeRequest 到达发送时间，开始发送 第%llu个Pack线程, 第%llu个package, epoch:%llu 共%llu : %llu", 
                id, send_index, min_epoch, local_write_set.LoadPackedTxnNum(min_epoch), local_write_set.LoadChangeSet(min_epoch));
            min_epoch += kPackThreadNum;
            while(max_epoch <= min_epoch) {
                auto merge_request = new merge::MergeRequest();
                max_epoch += kPackThreadNum;
                merge_request->set_epoch(max_epoch);
                merge_request->set_server_id(local_ip_index);
                merge_request_vector.push_back(merge_request);
            }
            merge_request_vector.erase(merge_request_vector.begin());
        }

        for(int i = 0; i < (int)kPackageNum; i++) {
            index = (index + 1) % kPackageNum;
            while(local_write_set.TryDequeue(pack_param, index)) {
                current_epoch = pack_param->epoch;
                q[current_epoch % kPackThreadNum].enqueue(std::move(pack_param));
                q[current_epoch % kPackThreadNum].enqueue(std::move(std::make_unique<pack_params>(nullptr, nullptr, 0, 0)));
            }
        }
        if(sleep_flag)
            usleep(100);
    }
}


void EpochPackThreadMain(uint64_t id){
    MOT_LOG_INFO("线程开始工作 Pack id: %llu %llu", id, kBatchNum);
    SetCPU();
    uint64_t current_epoch = 0, send_index = 0, index = id;
    std::shared_ptr<zmq::message_t> merge_request_ptr;
    std::unique_ptr<pack_params> pack_param;
    std::unique_ptr<pack_thread_params> params;
    std::unique_ptr<zmq::message_t> msg;
    std::string *serialized_txn_str_ptr;
    std::unique_ptr<merge::MergeRequest_Transaction> txn_end = std::make_unique<merge::MergeRequest_Transaction>();
    txn_end->set_startepoch(0);
    txn_end->set_commitepoch(0);
    txn_end->set_csn(0);
    txn_end->set_txnid(0);
    txn_end->set_server_id(local_ip_index);
    bool sleep_flag = false;
    while(!init_ok.load()) usleep(100);
    if(is_total_pack) PackMergeRequest(id);
    while(true){
        sleep_flag = true;
        for(int i = 0; i < (int)kPackageNum; i++) {
            index = (index + 1) % kPackageNum;
            if(local_write_set.TryDequeue(pack_param, index)) {
                if(kServerNum == 1) continue;
                current_epoch = pack_param->epoch;
                if((MOTAdaptor::GetLogicalEpoch() % max_length) ==  ((MOTAdaptor::GetPhysicalEpoch() + 2) % max_length) ) assert(false);
                if(pack_param->str == nullptr){
                    if(pack_param->epoch == 0) continue;
                    // MOT_LOG_INFO("取出一个epoch send task %llu", pack_param->epoch);
                    if(local_write_set.IsCurrentEpochFinished(current_epoch) == false) {
                        // MOT_LOG_INFO("重新放入epoch send task %llu %llu %llu", 
                        //     pack_param->epoch, local_write_set.LoadPackedTxnNum(pack_param->epoch), local_write_set.LoadChangeSet(pack_param->epoch));
                        local_write_set.Enqueue(nullptr, nullptr, current_epoch, 0);
                        sleep_flag = false;

                    }
                    else {
                        sleep_flag = false;
                        txn_end->set_commitepoch(current_epoch);
                        txn_end->set_csn(static_cast<uint64_t>(local_write_set.LoadChangeSet(current_epoch) / MOD_NUM));
                        serialized_txn_str_ptr = new std::string();
                        if(is_protobuf_gzip == true) {
                            google::protobuf::io::GzipOutputStream::Options options;
                            options.format = google::protobuf::io::GzipOutputStream::GZIP;
                            options.compression_level = 9;
                            google::protobuf::io::StringOutputStream outputStream(&(*serialized_txn_str_ptr));
                            google::protobuf::io::GzipOutputStream gzipStream(&outputStream, options);
                            txn_end->SerializeToZeroCopyStream(&gzipStream);
                            gzipStream.Close();
                        }
                        else {
                            txn_end->SerializeToString(serialized_txn_str_ptr);
                        }
                        if(!send_pool.enqueue(std::move(std::make_unique<send_thread_params>(current_epoch, 1, serialized_txn_str_ptr)))) Assert(false);
                        if(!send_pool.enqueue(std::move(std::make_unique<send_thread_params>(0, 0, nullptr)))) Assert(false); //防止moodycamel取不出
                        MOT_LOG_INFO("MergeRequest 到达发送时间，开始发送 第%llu个Pack线程, 第%llu个package, epoch:%llu 共%llu : %llu", 
                            id, send_index, current_epoch, local_write_set.LoadPackedTxnNum(current_epoch), local_write_set.LoadChangeSet(current_epoch));
                    }
                }
                else {
                    sleep_flag = false;
                    if(!send_pool.enqueue(std::move(std::make_unique<send_thread_params>(current_epoch, 0, pack_param->str))) ) Assert(false);
                    if(!send_pool.enqueue(std::move(std::make_unique<send_thread_params>(0, 0, nullptr))) )  Assert(false); //防止moodycamel取不出
                    local_write_set.AddPackedNum(current_epoch, pack_param->index, MOD_NUM);
                    local_write_set.SubNum(current_epoch, pack_param->index, 1);
                }
            }
        }
        if(sleep_flag)
            usleep(100);
    }
}


void EpochSendThreadMain(uint64_t id){
    SetCPU();
    zmq::context_t context(1);
    zmq::message_t reply(5);
    int queue_length = 0;
    zmq::socket_t socket_send(context, ZMQ_PUB);
    socket_send.setsockopt(ZMQ_SNDHWM, &queue_length, sizeof(queue_length));
    socket_send.bind("tcp://*:5557");//to server
    socket_send.bind("tcp://*:5558");//to az_cache_server
    // socket_send.bind("tcp://*:" + std::to_string(20000 + local_ip_index * 100));
    std::unique_ptr<send_thread_params> params;
    std::unique_ptr<zmq::message_t> msg;
    while(init_ok.load() == false) ;
    MOT_LOG_INFO("线程开始工作 break; Sned  %llu %s", id, 
        (kServerIp[local_ip_index] + ":5557/5558").c_str());
    while(true){
        send_pool.wait_dequeue(params);
        if(params->merge_request_ptr == nullptr) continue;
        msg = std::make_unique<zmq::message_t>(static_cast<void*>(const_cast<char*>(params->merge_request_ptr->data())),
                params->merge_request_ptr->size(), string_free, static_cast<void*>(params->merge_request_ptr));
        socket_send.send(*(msg));
    }
}




































void EpochListenThreadMain(uint64_t id){
    SetCPU();
    MOT_LOG_INFO("线程开始工作 ListenThread");
	zmq::context_t listen_context(1);
    zmq::socket_t socket_listen(listen_context, ZMQ_SUB);
    int queue_length = 0;
    socket_listen.setsockopt(ZMQ_SUBSCRIBE, "", 0);
    socket_listen.setsockopt(ZMQ_RCVHWM, &queue_length, sizeof(queue_length));
    for(int i = 0; i < (int)kServerIp.size(); i ++){
        if(i == (int)local_ip_index) continue;
        socket_listen.connect("tcp://" +kServerIp[i] + ":5557");
        MOT_LOG_INFO("线程开始工作 ListenThread %s", ("tcp://" +kServerIp[i] + ":5557").c_str());
    }

    for (;;) {
        std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
        socket_listen.recv(&(*message_ptr));//防止上次遗留消息造成message cache出现问题
        if(is_epoch_advance_started.load() == true){
            if(!listen_message_queue.enqueue(std::move(message_ptr))) assert(false);
            if(!listen_message_queue.enqueue(std::move(std::make_unique<zmq::message_t>()))) assert(false); //防止moodycamel取不出
            break;
        }
    }

    for(;;) {
        std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
        socket_listen.recv(&(*message_ptr));
        if(!listen_message_queue.enqueue(std::move(message_ptr))) assert(false);
        if(!listen_message_queue.enqueue(std::move(std::make_unique<zmq::message_t>()))) assert(false); //防止moodycamel取不出
    }
}



void EpochUnseriThreadMain(uint64_t id){//任务转移至merge线程工作
}

void EpochMessageCacheManagerThreadMain(uint64_t id){//任务转移至merge线程和Logical线程工作
}

void EpochUnpackThreadMain(uint64_t id){
}

void EpochMergeThreadMain(uint64_t id){
    bool result, sleep_flag = false;
    MOT::SessionContext* session_context = MOT::GetSessionManager()->
        CreateSessionContext(IS_PGXC_COORDINATOR, 0, nullptr, INVALID_CONNECTION_ID);
    MOT::Table* table = nullptr;
    MOT::Row* localRow = nullptr;
    MOT::Key* key;
    std::unique_ptr<zmq::message_t> message_ptr;
    std::unique_ptr<std::string> message_string_ptr;
    std::unique_ptr<merge::MergeRequest_Transaction> txn_ptr;
    std::unique_ptr<std::vector<MOT::Key*>> key_vector_ptr;
    std::unique_ptr<std::vector<MOT::Row*>> row_vector_ptr;
    std::string csn_temp, key_temp, key_str, table_name, csn_result;
    uint64_t message_epoch_id = 0, csn = 0, index_pack = id % kPackageNum, epoch_mod = 0, message_epoch_mod = 0, received_pack_num = 0, 
        epoch = 0, server_id = 0, clear_epoch = 0;
    uint32_t op_type = 0;
    int KeyLength;
    void* buf;
    std::vector<std::vector<std::unique_ptr<std::queue<std::unique_ptr<merge::MergeRequest_Transaction>>>>> message_cache;
    message_cache.reserve(max_length + 1);
    for(int i = 0; i < (int)max_length; i ++) {
        message_cache.emplace_back(std::vector<std::unique_ptr<std::queue<std::unique_ptr<merge::MergeRequest_Transaction>>>>());
        for(int j = 0; j < (int)kServerIp.size() + 2; j++){
            message_cache[i].push_back(std::make_unique<std::queue<std::unique_ptr<merge::MergeRequest_Transaction>>>());
        }
    }
    
    while(!init_ok.load()) usleep(100);
    for(;;){
        sleep_flag = true;
        if(listen_message_queue.try_dequeue(message_ptr) && message_ptr->size() > 1){
            if(is_total_pack == true) {
                auto merge_ptr = std::make_unique<merge::MergeRequest>();
                message_string_ptr = std::make_unique<std::string>(static_cast<const char*>(message_ptr->data()), message_ptr->size());
                if(is_protobuf_gzip == true) {
                    google::protobuf::io::ArrayInputStream inputStream(message_string_ptr->data(), message_string_ptr->size());
                    google::protobuf::io::GzipInputStream gzipStream(&inputStream);
                    merge_ptr->ParseFromZeroCopyStream(&gzipStream);
                }
                else {
                    merge_ptr->ParseFromString(*message_string_ptr);
                }
                message_epoch_mod = merge_ptr->epoch() % max_length;
                server_id = merge_ptr->server_id();
                remote_cache.StoreShouldReceiveTxnNum(message_epoch_mod, server_id, merge_ptr->txn_size());
                remote_cache.StoreReceivedPackNum(message_epoch_mod, server_id, 1);
                remote_cache.AddReceivedPackNumTotal(message_epoch_mod, 1);
                for(int i = 0; i < merge_ptr->txn_size(); i ++) {
                    txn_ptr = std::make_unique<merge::MergeRequest_Transaction>(merge_ptr->txn(i));
                    // *(txn_ptr) = std::move(merge_ptr->txn(i));
                    // message_epoch_id = txn_ptr->commitepoch();
                    // server_id = txn_ptr->server_id();
                    // message_epoch_mod = message_epoch_id % max_length;
                    message_cache[message_epoch_mod][server_id]->push(std::move(txn_ptr));
                    remote_cache.AddReceivedTxnNum(message_epoch_mod, server_id, 1);
                    remote_cache.AddReceivedTxnNumTotal(message_epoch_mod, 1);
                }
                MOT_LOG_INFO("收到一个pack server: %llu, epoch: %llu, pack num %llu, txn num: %llu,  %llu", 
                        merge_ptr->server_id(), merge_ptr->epoch(), remote_cache.GetReceivedPackNum(message_epoch_mod),
                        remote_cache.GetShouldReceiveTxnNum(message_epoch_mod), now_to_us());
            }

            else {
                txn_ptr = std::make_unique<merge::MergeRequest_Transaction>();
                message_string_ptr = std::make_unique<std::string>(static_cast<const char*>(message_ptr->data()), message_ptr->size());
                if(is_protobuf_gzip == true) {
                    google::protobuf::io::ArrayInputStream inputStream(message_string_ptr->data(), message_string_ptr->size());
                    google::protobuf::io::GzipInputStream gzipStream(&inputStream);
                    txn_ptr->ParseFromZeroCopyStream(&gzipStream);
                }
                else {
                    txn_ptr->ParseFromString(*message_string_ptr);
                }
                message_epoch_id = txn_ptr->commitepoch();
                server_id = txn_ptr->server_id();
                message_epoch_mod = message_epoch_id % max_length;
                if(txn_ptr->row_size() == 0 && txn_ptr->startepoch() == 0 && txn_ptr->txnid() == 0){
                    if((MOTAdaptor::GetLogicalEpoch() % max_length) ==  ((message_epoch_id + 2) % max_length) ) assert(false);
                    remote_cache.StoreShouldReceiveTxnNum(message_epoch_mod, server_id, txn_ptr->csn());
                    remote_cache.StoreReceivedPackNum(message_epoch_mod, server_id, 1);
                    remote_cache.AddReceivedPackNumTotal(message_epoch_mod, 1);
                    MOT_LOG_INFO("收到一个pack server: %llu, epoch: %llu, pack num %llu, txn num: %llu,  %llu", 
                        txn_ptr->server_id(), message_epoch_id, remote_cache.GetReceivedPackNum(message_epoch_mod),
                        remote_cache.GetShouldReceiveTxnNum(message_epoch_mod), now_to_us());
                }
                else{
                    message_cache[message_epoch_mod][server_id]->push(std::move(txn_ptr));
                    remote_cache.AddReceivedTxnNum(message_epoch_mod, server_id, 1);
                    remote_cache.AddReceivedTxnNumTotal(message_epoch_mod, 1);
                }
            }
            sleep_flag = false;
        } 

        epoch_mod = MOTAdaptor::GetLogicalEpoch() % max_length;
        if(epoch != epoch_mod) {
            epoch = epoch_mod;
            received_pack_num = 0;
        }
        if(received_pack_num != remote_cache.GetReceivedPackNum(epoch_mod)) {
            clear_epoch = (epoch_mod - 1 + max_length) % max_length;
            for(int i = 0; i < (int)kServerNum; i++){
                if(remote_cache.IsServerOnLine(i) && remote_cache.GetReceivedPackNum(epoch_mod, i) == 1 &&
                    remote_cache.GetReceivedTxnNum(epoch_mod, i) == remote_cache.GetShouldReceiveTxnNum(epoch_mod, i) &&
                    !message_cache[epoch_mod][i]->empty()) {
                    received_pack_num ++;
                    while(!message_cache[epoch_mod][i]->empty()){
                        auto txn_ptr_tmp = std::move(message_cache[epoch_mod][i]->front());
                        message_cache[epoch_mod][i]->pop();
                        if(!merge_queue.enqueue(std::move(txn_ptr_tmp))) Assert(false); //防止moodycamel取不出
                    }
                    if(!merge_queue.enqueue(nullptr)) Assert(false); //防止moodycamel取不出
                }
                while(!message_cache[clear_epoch][i]->empty()) message_cache[clear_epoch][i]->pop();
            }
        }


        if(merge_queue.try_dequeue(txn_ptr)) {
            result = true;
            sleep_flag = false;
            if(txn_ptr == nullptr) continue;
            csn = txn_ptr->csn();
            server_id = txn_ptr->server_id();
            csn_temp = std::to_string(csn) + ":" + std::to_string(server_id);
            row_vector_ptr = std::make_unique<std::vector<MOT::Row*>>();
            row_vector_ptr->reserve(txn_ptr->row_size());
            for(int j = 0; j < txn_ptr->row_size(); j++){
                table_name = txn_ptr->row(j).tablename();
                op_type = txn_ptr->row(j).type();
                table = MOTAdaptor::m_engine->GetTableManager()->GetTable(table_name);
                if(table == nullptr){
                    result = false;
                }
                KeyLength = txn_ptr->row(j).key().length();
                buf = MOT::MemSessionAlloc(KeyLength);
                if(buf == nullptr) Assert(false);
                key = new (buf) MOT::Key(KeyLength);
                key->CpKey((uint8_t*)txn_ptr->row(j).key().c_str(),KeyLength);
                key_str = key->GetKeyStr();
                if(op_type == 0 || op_type == 2){
                    if (table->FindRow(key,localRow, 0) != MOT::RC::RC_OK) {
                        result = false;
                        continue;
                    }
                    if (!localRow->ValidateAndSetWriteForRemote(csn, txn_ptr->startepoch(), txn_ptr->commitepoch(), server_id)){
                        result = false;
                    }
                    row_vector_ptr->emplace_back(localRow);
                } 
                else {//insert 
                    if (table->FindRow(key, localRow, 0) == MOT::RC::RC_OK) {
                        result = false;
                    }
                    key_temp = table_name + key_str;
                    if(!MOTAdaptor::insertSetForCommit.insert(key_temp, csn_temp, &csn_result)){
                        result = false;
                    }
                    MOTAdaptor::abort_transcation_csn_set.insert(csn_result, csn_result);
                    row_vector_ptr->emplace_back(nullptr);
                }
                MOT::MemSessionFree(buf);
            }
            if(!result) {
                MOTAdaptor::abort_transcation_csn_set.insert(csn_temp, csn_temp);
            }
            else{
                if(commit_txn_queue_struct.enqueue(std::make_unique<commit_thread_params>(std::move(txn_ptr), std::move(row_vector_ptr)))){
                    MOTAdaptor::IncRemoteCommitTxnCounters(index_pack);
                    if(!commit_txn_queue_struct.enqueue(std::make_unique<commit_thread_params>(nullptr, nullptr))) Assert(false); //防止moodycamel取不出
                }
                else{
                    MOT_LOG_INFO("Merge 放入队列失败 ");
                    Assert(false);
                }
            }
            MOTAdaptor::IncRemoteMergedTxnCounters(index_pack);
            continue;
        }
        if(sleep_flag) {
            usleep(100);
        }
    }
}

void EpochCommitThreadMain(uint64_t id){//validate
    MOT::SessionContext* session_context = MOT::GetSessionManager()->
        CreateSessionContext(IS_PGXC_COORDINATOR, 0, nullptr, INVALID_CONNECTION_ID);
    MOT::TxnManager* txn_manager = session_context->GetTxnManager();
    MOT::Table* table = nullptr;
    MOT::Row* localRow = nullptr, *new_row = nullptr;
    MOT::Key* key;
    MOT::RC res;
    std::unique_ptr<commit_thread_params> params;
    std::unique_ptr<std::vector<MOT::Row*>> row_vector_ptr;
    std::unique_ptr<merge::MergeRequest_Transaction> txn_ptr;
    std::string csn_temp, key_temp, key_str, table_name;
    uint64_t csn = 0, index_pack = id % kPackageNum;
    uint32_t op_type = 0;
    uint32_t server_id = 0;
    int KeyLength;
    void* buf;
    // bool sleep_flag = false;
    while(!init_ok.load()) usleep(100);
    for(;;){
        if(commit_txn_queue_struct.try_dequeue(params)) {
            txn_ptr = std::move(params->txn);
            row_vector_ptr = std::move(params->row_vector);
            if(txn_ptr == nullptr) {
                continue;
            }
            while(!MOTAdaptor::IsRemoteExeced()) {
                usleep(100);
            }

            csn = txn_ptr->csn();
            server_id = txn_ptr->server_id();
            csn_temp = std::to_string(csn) + ":" + std::to_string(server_id);
            if(!MOTAdaptor::abort_transcation_csn_set.contain(csn_temp, csn_temp)){
                MOTAdaptor::IncRecordCommitTxnCounters(index_pack);
                txn_manager->CleanTxn();
                for(int j = 0; j < txn_ptr->row_size(); j++){
                    const auto& row = txn_ptr->row(j);
                    table = MOTAdaptor::m_engine->GetTableManager()->GetTable(row.tablename());
                    MOT_ASSERT(table != nullptr);
                    op_type = row.type();
                    KeyLength = row.key().length();
                    buf = MOT::MemSessionAlloc(KeyLength);
                    key = new (buf) MOT::Key(KeyLength);
                    key->CpKey((uint8_t*)row.key().c_str(),KeyLength);
                    if(op_type == 0 || op_type == 2) {
                        localRow = (*row_vector_ptr)[j];
                        localRow->GetRowHeader()->KeepStable();
                        if(op_type == 2)
                            localRow->GetPrimarySentinel()->SetDirty();
                        else{
                            // localRow->CopyData((uint8_t*)row.data().c_str(),table->GetTupleSize());

                            for (int k=0; k < row.column_size(); k++){
                                const auto &col = row.column(k);
                                localRow->SetValueVariable_1(col.id(),col.value().c_str(),col.value().length());
                            }
                        }
                    }
                    else{
                        new_row = table->CreateNewRow();
                        new_row->CopyData((uint8_t*)row.data().c_str(),table->GetTupleSize());  //insert 
                        res = table->InsertRow(new_row, txn_manager);
                        if ((res != MOT::RC_OK) && (res != MOT::RC_UNIQUE_VIOLATION)) {
                            MOT_REPORT_ERROR(
                                MOT_ERROR_OOM, "Insert Row ", "Failed to insert new row for table %s", table->GetLongTableName().c_str());
                        }
                    }
                    MOT::MemSessionFree(buf);
                }
                txn_manager->CommitForRemote();
                MOTAdaptor::IncRecordCommittedTxnCounters(index_pack);
            }
            MOTAdaptor::IncRemoteCommittedTxnCounters(index_pack);
        }
        else {
            usleep(100);
        }
    }
}



void EpochRecordCommitThreadMain(uint64_t id) {//deleted
    
}

void MOTAdaptor::Merge(MOT::TxnManager* txMan, uint64_t& index_pack) {
    
}

void MOTAdaptor::Commit(MOT::TxnManager* txMan, uint64_t& index_pack) {
    
}

struct ListenParams {
    uint64_t remote_ip_index, current_epoch;
    std::shared_ptr<zmq::socket_t> socket_listen;
    std::shared_ptr<std::atomic<uint64_t>> total_pack_num, message_received;
    std::shared_ptr<std::vector<std::unique_ptr<std::atomic<uint64_t>>>> job_num;
    std::shared_ptr<BlockingConcurrentQueue<std::unique_ptr<merge::MergeRequest_Transaction>>> queue;
    void Init(uint64_t value1, uint64_t value, std::shared_ptr<zmq::socket_t> socket, std::shared_ptr<std::atomic<uint64_t>> value2, 
        std::shared_ptr<std::atomic<uint64_t>> value3, std::shared_ptr<std::vector<std::unique_ptr<std::atomic<uint64_t>>>> val,
        std::shared_ptr<BlockingConcurrentQueue<std::unique_ptr<merge::MergeRequest_Transaction>>> value4) {
            remote_ip_index = value1 , current_epoch = value, socket_listen = socket, total_pack_num = value2, message_received = value3, job_num = val,
            queue = value4;
        }
    
};

void* ListenThread(void* param) {
    ListenParams* params = (ListenParams*) param;
    auto port = 40000 + params->remote_ip_index * 100 + local_ip_index;
    auto index = params->remote_ip_index;
    auto txn_queue = params->queue;
    uint64_t received_total_pack = 0, received_total_txn_num = 0, should_received_total_txn_num = 0;
    zmq::context_t listen_context(1);
    zmq::socket_t socket_listen(listen_context, ZMQ_PULL);
    int queue_length = 0;
    socket_listen.setsockopt(ZMQ_RCVHWM, &queue_length, sizeof(queue_length));
    MOT_LOG_INFO("***Receive From CacheServer Start PULL %s", 
        ("tcp://" +kCacheServerIp[params->remote_ip_index] + ":" + std::to_string(port)).c_str());
    socket_listen.bind("tcp://*:" + std::to_string(port));
    while(true) {
        auto message_ptr = std::make_unique<zmq::message_t>();
        socket_listen.recv(&(*message_ptr));
        auto txn_ptr = std::make_unique<merge::MergeRequest_Transaction>();
        auto message_string_ptr = std::make_unique<std::string>(static_cast<const char*>(message_ptr->data()), message_ptr->size());
        txn_ptr->ParseFromString(*message_string_ptr);
        auto message_epoch_id = txn_ptr->commitepoch();
        auto server_id = txn_ptr->server_id();
        if(server_id != index) Assert(false);
        auto message_epoch_mod = message_epoch_id % max_length;
        if(txn_ptr->row_size() == 0 && txn_ptr->startepoch() == 0 && txn_ptr->txnid() == 0) {
            remote_cache.StoreShouldReceiveTxnNum(message_epoch_id, index, txn_ptr->csn());
            remote_cache.StoreReceivedPackNum(message_epoch_id, index, 1);
            should_received_total_txn_num += txn_ptr->csn();
            received_total_pack ++;
            MOT_LOG_INFO("Receive From Cache Server 收到一个pack server: %llu, epoch: %llu, index:%llu pack num %llu, txn num: %llu,  %llu", 
                    txn_ptr->server_id(), message_epoch_id, index, remote_cache.GetReceivedPackNum(message_epoch_mod),
                    remote_cache.GetShouldReceiveTxnNum(message_epoch_mod), now_to_us());
        }
        else{
            txn_queue->enqueue(std::move(txn_ptr));
            received_total_txn_num ++;
            txn_queue->enqueue(nullptr);//防止moodycamel取不出
        }
        //未接收到cache的消息之前和epoch txn接受完全为完成之前 不退出
        if(params->message_received->load() == 1 && received_total_pack == params->total_pack_num->load() &&  
            received_total_txn_num == should_received_total_txn_num) {
            break;
        }
    }
    MOT_LOG_INFO("***Receive From CacheServer End PULL %s", 
        ("tcp://" +kCacheServerIp[params->remote_ip_index] + ":" + std::to_string(port)).c_str());
}

void* MergeThread(void* param){
    ListenParams* params = (ListenParams*) param;
    auto index = params->remote_ip_index;
    auto txn_queue = params->queue;
    bool sleep_flag = false;
    std::unique_ptr<merge::MergeRequest_Transaction> txn_ptr;
    std::vector<std::vector<std::unique_ptr<std::queue<std::unique_ptr<merge::MergeRequest_Transaction>>>>> message_cache;
    message_cache.reserve(max_length + 1);
    for(int i = 0; i < (int)max_length; i ++) {
        message_cache.emplace_back(std::vector<std::unique_ptr<std::queue<std::unique_ptr<merge::MergeRequest_Transaction>>>>());
        for(int j = 0; j < (int)kServerIp.size() + 2; j++) {
            message_cache[i].push_back(std::make_unique<std::queue<std::unique_ptr<merge::MergeRequest_Transaction>>>());
        }
    }
    //未接收到cache的消息之前和epoch merge为完成之前 不退出
    while(true) {
        sleep_flag = true;
        if(params->message_received->load() == 1 && 
            (params->total_pack_num->load() + params->current_epoch) <= MOTAdaptor::GetLogicalEpoch()) {
                break;
        }
        if(txn_queue->try_dequeue(txn_ptr) && txn_ptr != nullptr) {
            auto message_epoch_mod = txn_ptr->commitepoch() % max_length;
            message_cache[message_epoch_mod][index]->push(std::move(txn_ptr));
            sleep_flag = false;
        }
        auto epoch_mod = MOTAdaptor::GetLogicalEpoch();
        if(remote_cache.GetReceivedPackNum(epoch_mod, index) == 1 && 
            remote_cache.GetReceivedTxnNum(epoch_mod, index) == remote_cache.GetShouldReceiveTxnNum(epoch_mod, index)) {
            while(!message_cache[epoch_mod][index]->empty()){
                auto txn_ptr_tmp = std::move(message_cache[epoch_mod][index]->front());
                message_cache[epoch_mod][index]->pop();
                if(!merge_queue.enqueue(std::move(txn_ptr_tmp))) Assert(false); //防止moodycamel取不出
            }
            if(!merge_queue.enqueue(nullptr)) Assert(false); //防止moodycamel取不出
        }
        if(sleep_flag) {
            usleep(100);
        }
    }
    remote_cache.SubShouldReceivePackNum(1);
    remote_cache.SetServerOffLine(index);
    (*(params->job_num))[index]->store(0);
    MOT_LOG_INFO("***服务器退出集群 server_ip_index : %llu ip:%s", index, kServerIp[index].c_str() );
}

uint64_t StartThreads(ListenParams* ptr) {
    pthread_t thread_listen, thread_merge;
    if(pthread_create(&thread_listen, NULL, ListenThread, (void*)ptr)) {      
        return 1;
    }
    if(pthread_create(&thread_merge, NULL, MergeThread, (void*)ptr)) {      
        return 1;
    } 
    return 0;
}

uint64_t GetRaftServerNum() {
    if(remote_cache.GetShouldReceivePackNum() == 0) return remote_cache.GetShouldReceivePackNum();
    return (remote_cache.GetShouldReceivePackNum() - 1);
}

void EpochMessageManagerThreadMain(uint64_t id) {
    SetCPU();
    std::unique_ptr<zmq::message_t> message_ptr;
    std::string local_ip = kServerIp[local_ip_index];
    std::map<std::string, std::string> _map;
    std::map<uint64_t, std::shared_ptr<std::atomic<uint64_t>>> _map_message_received, _map_total_pack_num;
    std::unique_ptr<merge::ServerMessage> _msg;
    uint64_t remote_ip_index;
    bool sleep_flag = true;
    std::shared_ptr<std::vector<std::unique_ptr<std::atomic<uint64_t>>>> job_num = std::make_shared<std::vector<std::unique_ptr<std::atomic<uint64_t>>>>(0);
    for(int i = 0; i < (int)kServerIp.size() + 2; i ++) {
        job_num->push_back(std::make_unique<std::atomic<uint64_t>>(0));
        _map_message_received[i] = std::make_shared<std::atomic<uint64_t>>(0);
        _map_total_pack_num[i] = std::make_shared<std::atomic<uint64_t>>(0);
    }
    while(!is_epoch_advance_started.load()) usleep(100);
    for(;;) {
        sleep_flag = true;
        while(message_receive_pool.try_dequeue(message_ptr)) {
            if(message_ptr == nullptr) continue;
            _map.clear();
            std::unique_ptr<std::string> message_string_ptr= 
                std::make_unique<std::string>(static_cast<const char*>(message_ptr->data()), message_ptr->size());
            _msg = std::make_unique<merge::ServerMessage>();
            _msg->ParseFromString(*message_string_ptr);
            // MOT_LOG_INFO("MessageManager接收一条消息 %s  %s\n", _msg->receive_ip().c_str(), local_ip.c_str());
            if(_msg->receive_ip().compare(local_ip) == 0 || _msg->receive_ip().compare(kPrivateIp) == 0) {
                for(int i = 0; i < _msg->msg_size(); i++) {
                        _map[_msg->msg(i).key()] = _msg->msg(i).value();
                }
                _map["send_ip"] = _msg->send_ip();
                for(int i = 0; i < (int)kServerIp.size(); i++) {
                    if(kServerIp[i] == _map["send_ip"] || kCacheServerIp[i] == _map["send_ip"]) {
                        remote_ip_index = i;
                        break;
                    }
                }
                _map["port"] = _msg->port();
                if(_map["message_type"] == "cache_reply") {
                    if(_map["receive_compelete"] == std::to_string(true)) {
                        cache_server.SetCacheServerStored(std::stoull(_map["epoch_num"]), 1);
                    }
                }

                if(_map["message_type"] == "cache_txn_reply" ) {
                    MOT_LOG_INFO("***CacheServer Txn Reply cache_index:%llu, cache_epoch_num:%llu", remote_ip_index, std::stoull(_map["cache_epoch_num"]))
                        _map_total_pack_num[remote_ip_index]->store(std::stoull(_map["cache_epoch_num"]));
                    _map_message_received[remote_ip_index]->store(1);
                }
                sleep_flag = false;

            }
            else {
                continue;
            }
        }
        auto now_time = now_to_us();
        if(is_fault_tolerance_enable &&
            now_time - epoch_commit_time >= kServerTimeOut_us && MOTAdaptor::GetRecordCommittedTxnCounters() >= kStartCheckStateNum) {
            //load数据是会出现短暂的线程调度问题，导致误判，需要度过这一段时间，默认load数量为100000
            uint64_t current_epoch = MOTAdaptor::GetLogicalEpoch();
            if(remote_cache.GetReceivedPackNum(current_epoch) != remote_cache.GetShouldReceivePackNum()){    
                if(remote_cache.GetShouldReceivePackNum() > GetRaftServerNum() ) {
                    remote_ip_index = local_ip_index;
                    for(int i = 0; i < (int)kServerIp.size(); i++) {
                        if(i != (int)local_ip_index &&  (*job_num)[i]->load() != 1 && 
                            remote_cache.IsServerOnLine(i) && remote_cache.GetReceivedPackNum(current_epoch, i) != 1) {
                                remote_ip_index = i;
                                _map_total_pack_num[remote_ip_index]->store(kCacheMaxLength);
                                _map_message_received[remote_ip_index]->store(0);
                                auto param = new ListenParams();
                                param->Init(remote_ip_index, current_epoch, nullptr/*socket_vector[remote_ip_index]*/, 
                                    _map_total_pack_num[remote_ip_index], _map_message_received[remote_ip_index], job_num, 
                                    std::make_shared<BlockingConcurrentQueue<std::unique_ptr<merge::MergeRequest_Transaction>>>());
                                if(StartThreads(param) == 1) {
                                        Assert(false);
                                    }
                                (*job_num)[i]->store(1);
                                usleep(1000);
                                break;
                        }
                    }
                    if(remote_ip_index != local_ip_index) {
                        MOT_LOG_INFO("***出现服务器超时现象 server_ip_index :%llu %llu ip:%s",MOTAdaptor::GetLogicalEpoch(),
                            remote_ip_index, kServerIp[remote_ip_index].c_str() );
                        std::unique_ptr<merge::ServerMessage> send_server_message_ptr = std::make_unique<merge::ServerMessage>();
                        merge::ServerMessage_Msg* msg;
                        send_server_message_ptr->set_send_ip(kServerIp[local_ip_index]);
                        send_server_message_ptr->set_receive_ip(kCacheServerIp[remote_ip_index]);
                        msg = send_server_message_ptr->add_msg();
                        msg->set_key("message_type");
                        msg->set_value("cache_txn_request");
                        msg = send_server_message_ptr->add_msg();
                        msg->set_key("epoch_num");
                        msg->set_value(std::to_string(current_epoch));

                        auto* serialized_txn_str_ptr = new std::string();
                        send_server_message_ptr->SerializeToString(serialized_txn_str_ptr);
                        std::unique_ptr<zmq::message_t> send_message_ptr = std::make_unique<zmq::message_t>(serialized_txn_str_ptr->size());
                        memcpy(send_message_ptr->data(), serialized_txn_str_ptr->c_str(), serialized_txn_str_ptr->size());
                        message_send_pool.enqueue(std::move(send_message_ptr));
                        sleep_flag = false;
                        if(is_cache_server_available == false) {
                            _map_total_pack_num[remote_ip_index]->store(0);
                            _map_message_received[remote_ip_index]->store(1);
                        }
                    }
                }
                else {

                }
            }

        }
        if(sleep_flag)
            usleep(100);
        // if(MOTAdaptor::GetPhysicalEpoch() == new_start_physical_epoch - 1){
        //     gettimeofday(&start_time, NULL);
        //     start_time_ll = start_time.tv_sec * 1000000 + start_time.tv_usec;
        //     start_physical_epoch = new_start_physical_epoch;
        //     kSleepTime = new_sleep_time;
        // }
    }
}


void EpochMessageSendThreadMain(uint64_t id) {
    SetCPU();
    zmq::context_t context(1);
    zmq::message_t reply(5);
    int queue_length = 0;
    zmq::socket_t socket_send(context, ZMQ_PUB);
    socket_send.setsockopt(ZMQ_SNDHWM, &queue_length, sizeof(queue_length)); 
    socket_send.bind("tcp://*:5547");//to server
    socket_send.bind("tcp://*:5548");//to cache
    MOT_LOG_INFO("线程开始工作 EpochMessageSendThreadMain");
    std::unique_ptr<send_thread_params> params;
    std::unique_ptr<zmq::message_t> msg;
    while(true) {
        message_send_pool.wait_dequeue(msg);
        socket_send.send(*(msg));
        MOT_LOG_INFO("发送一条消息");
    }
}

void EpochMessageListenThreadMain(uint64_t id) {
    SetCPU();
	zmq::context_t listen_context(1);
    zmq::socket_t socket_listen(listen_context, ZMQ_SUB);
    int queue_length = 0;
    socket_listen.setsockopt(ZMQ_SUBSCRIBE, "", 0);
    socket_listen.setsockopt(ZMQ_RCVHWM, &queue_length, sizeof(queue_length));
    for(int i = 0; i < (int)kServerIp.size(); i ++) {
        if(i == (int)local_ip_index) continue;
        socket_listen.connect("tcp://" +kServerIp[i] + ":5547");
        MOT_LOG_INFO("线程开始工作 EpochMessageListenThreadMain %s", ("tcp://" +kServerIp[i] + ":5547").c_str());
    }
    for(int i = 0; i < (int)kCacheServerIp.size(); i ++) {
        socket_listen.connect("tcp://" +kCacheServerIp[i] + ":5549");
        MOT_LOG_INFO("线程开始工作 EpochMessageListenThreadMain %s", ("tcp://" +kCacheServerIp[i] + ":5549").c_str());
    }
    for(;;) {
        std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
        socket_listen.recv(&(*message_ptr));

        message_receive_pool.enqueue(std::move(message_ptr));
        message_receive_pool.enqueue(nullptr);
    }
}

/*
server
39.101.128.98
39.99.152.57
cache
8.142.85.130
39.99.158.230
*/