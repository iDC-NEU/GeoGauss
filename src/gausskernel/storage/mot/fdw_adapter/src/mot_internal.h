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
 * mot_internal.h
 *    MOT Foreign Data Wrapper internal interfaces to the MOT engine.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/fdw_adapter/src/mot_internal.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_INTERNAL_H
#define MOT_INTERNAL_H

// #include "message.pb.h"
#include <map>
#include <string>
#include "catalog_column_types.h"
#include "foreign/fdwapi.h"
#include "nodes/nodes.h"
#include "nodes/makefuncs.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "pgstat.h"
#include "global.h"
#include "mot_fdw_error.h"
#include "mot_fdw_xlog.h"
#include "system/mot_engine.h"
#include "bitmapset.h"
#include "storage/mot/jit_exec.h"
#include "mot_match_index.h"
#include <atomic>
#include <unordered_set>
#include <unordered_map>
#include <functional>
#include <mutex>
#include <condition_variable>
#include "pthread.h"
#include "table.h"
#include "row.h"
#include "neu_concurrency_tools/blockingconcurrentqueue.h"
#include "neu_concurrency_tools/blocking_mpmc_queue.h"
#include "neu_concurrency_tools/ThreadPool.h"
#include <vector>
#include <typeinfo>
// #include <semaphore.h>

using std::map;
using std::string;

#define MAX_TXN_NUM 100000

extern void EpochLogicalTimerManagerThreadMain(uint64_t id, std::vector<std::string> kServerIp, uint64_t kServerNum, uint64_t kPackageNum, uint64_t kNotifyNum, uint64_t kPackThreadNum, uint64_t kNotifyThreadNum, uint64_t local_ip_index);
extern void EpochPhysicalTimerManagerThreadMain(uint64_t id, std::vector<std::string> kServerIp, uint64_t kServerNum, uint64_t kPackageNum, uint64_t kNotifyNum, uint64_t kPackThreadNum, uint64_t kNotifyThreadNum, uint64_t local_ip_index, uint64_t kSleepTime);
extern void EpochMessageManagerThreadMain(uint64_t id, std::vector<std::string> kServerIp, uint64_t kServerNum, uint64_t kPackageNum, uint64_t kNotifyNum, uint64_t kPackThreadNum, uint64_t kNotifyThreadNum, uint64_t local_ip_index);
extern void EpochMessageCacheManagerThreadMain(uint64_t id, std::vector<std::string> kServerIp, uint64_t kServerNum, uint64_t kPackageNum, uint64_t kNotifyNum, uint64_t kPackThreadNum, uint64_t kNotifyThreadNum, uint64_t local_ip_index);

extern void EpochNotifyThreadMain(uint64_t id);
extern void EpochPackThreadMain(uint64_t id, std::vector<std::string> kServerIp, uint64_t kServerNum, uint64_t kPackageNum, uint64_t kNotifyNum, uint64_t kPackThreadNum, uint64_t kNotifyThreadNum, uint64_t local_ip_index);
extern void EpochSendThreadMain(uint64_t id, std::string kServerIp, uint64_t port);

extern void EpochListenThreadMain(uint64_t id, uint64_t port);
extern void EpochUnseriThreadMain(uint64_t id);
extern void EpochUnpackThreadMain(uint64_t id, std::vector<std::string> kServerIp);
extern void EpochMergeThreadMain(uint64_t id);
extern void EpochCommitThreadMain(uint64_t id);

class LockCV{//只能提供给两个线程同时访问，多个线程同时访问同一个mutex需要多个创建多个unique对这个mutex进行上锁set_lock()，不能使用同一个unique_lock，否则会出现死锁(同一unique的多次调用)
private:
public:
    std::mutex _mutex;
    std::condition_variable _cv;
    bool _is_notified = false;

    template <class _Predicate>
    void wait(std::unique_lock<std::mutex>& _lock, _Predicate _Pred) {
        _lock.lock();
        while(!_Pred()){
            _cv.wait( _lock);
        }
        _lock.unlock();
    }

    void wait(std::unique_lock<std::mutex>& _lock) { 
        _lock.lock();
        _cv.wait( _lock);
        _lock.unlock();
    }

    void notify_all(std::unique_lock<std::mutex>& _lock){
        _lock.lock();
        _cv.notify_all();
        _lock.unlock();
    }

    void notify_one(std::unique_lock<std::mutex>& _lock){
        _lock.lock();
        _cv.notify_one();
        _lock.unlock();
    }

    void notify_all_without_lock(){
        _cv.notify_all();
    }

    void notify_one_without_lock(){
        _cv.notify_one();
    }
    
    void set_lock(std::unique_lock<std::mutex>& _lock){
        std::unique_lock<std::mutex> lock_tmp(_mutex);
        _lock = std::move(lock_tmp);
        _lock.unlock();
    }


    template <class _Predicate>
    void wait(_Predicate _Pred) {
        std::unique_lock<std::mutex> lock(_mutex);
        while(!_Pred()){
            _cv.wait( lock);
        }
    }
    template <class _Predicate>
    void wait_with_mark(_Predicate _Pred){
        std::unique_lock<std::mutex> lock(_mutex);
        while(!_Pred() && _is_notified == false){
            _cv.wait( lock);
        }
        _is_notified = false;
    }

    void wait() {
        std::unique_lock<std::mutex> lock(_mutex);
        _cv.wait( lock);
    }

    void notify_all(){
        std::unique_lock<std::mutex> lock(_mutex);
        _cv.notify_all();
    }

    void notify_one(){
        std::unique_lock<std::mutex> lock(_mutex);
        _cv.notify_one();
        _is_notified = true;
    }

};

namespace aum {
    class SpinLock {
        public:
        // constructors
        SpinLock() = default;

        SpinLock(const SpinLock &) = delete;            // non construction-copyable
        SpinLock &operator=(const SpinLock &) = delete; // non copyable

        // Modifiers
        void lock() {
            while (lock_.test_and_set(std::memory_order_acquire));
        }

        void unlock() { lock_.clear(std::memory_order_release); }

        private:
        std::atomic_flag lock_ = ATOMIC_FLAG_INIT;
    };

    template<typename key, typename value, typename pointer>
    class atomic_unordered_map {
    public:
        typedef typename std::unordered_map<key, value>::iterator map_iterator;
        typedef typename std::unordered_map<key, value>::size_type size_type;
        // typedef typename std::map<key, value>::iterator map_iterator;
        // typedef typename std::map<key, value>::size_type size_type;
    public:

        bool insert(key &k, value &v, pointer *ptr, std::function<bool(const value &, const value &)> cmp = [](const value& o, const value& n){return n < o;}) //为insert服务
        {
            bool result = true;
            lock.lock();
            map_iterator iter = map.find(k);
            if (iter == map.end()) {
                map[k] = v;
                *ptr = "0";
                result = true;
            } else {
                // cmp(oldCsn, newCsn) -> bool
                // if true: 新的事务覆盖旧的事务
                if (iter->second > v) {
                    *ptr = map[k];
                    map[k] = v;
                    result = true;
                } 
                else if(iter->second == v){//相同则为同一事务执行的插入，不能将map[k]返回abort掉
                    *ptr = "0";
                    result = true;
                }
                else{
                    *ptr = v;
                    result = false;
                }
            }
            lock.unlock();
            return result;
        }

        void insert(key &k, value &v){//为update服务
            lock.lock();
            map[k] = v;
            lock.unlock();
        }


        void remove(key &k, value &v) 
        {
            lock.lock();
            map_iterator iter = map.find(k);
            if (iter != map.end() && iter->second == v) {//if the abort txn has insert row and has not been modifid by ohters then remove it from map;or keep it 
                map.erase(iter);
            }
            lock.unlock();
        }

        void clear() {
            lock.lock();
            map.clear();
            lock.unlock();
        }

        void unsafe_clear() { map.clear(); }

        map_iterator unsafe_begin() {
            return map.begin();
        }

        map_iterator unsafe_end() {
            return map.end();
        }

        map_iterator unsafe_find(key &k) {
            return map.find(k);
        }

        bool contain(key &k, value &v){
            map_iterator iter = map.find(k);
            if(iter != map.end() && iter->second == v){
                return true;
            }
            return false;
        }

        size_type size() { return map.size(); }

    public:
        std::unordered_map<key, value> map;
        // std::map<key, value> map;
        aum::SpinLock lock;
    };

    template<typename key, typename value, typename pointer>
    class concurrent_unordered_map {
    public:
        typedef typename std::unordered_map<key, value>::iterator map_iterator;
        typedef typename std::unordered_map<key, value>::size_type size_type;

        bool insert(key &k, value &v, pointer *ptr, std::function<bool(const value &, const value &)> cmp = [](const value& o, const value& n){return n < o;}) //为insert服务
        {
            std::mutex& _mutex_temp = GetMutexRef(k);
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            bool result = true;
            std::unique_lock<std::mutex> lock(_mutex_temp);
            map_iterator iter = _map_temp.find(k);
            if (iter == _map_temp.end()) {
                _map_temp[k] = v;
                *ptr = "0";
                result = true;
            } else {
                // cmp(oldCsn, newCsn) -> bool
                // if true: 新的事务覆盖旧的事务
                if (iter->second > v) {
                    *ptr = _map_temp[k];
                    _map_temp[k] = v;
                    result = true;
                } 
                else if(iter->second == v){//相同则为同一事务执行的插入，不能将map[k]返回abort掉
                    *ptr = "0"; //暂且只支持string
                    result = true;
                }
                else{
                    *ptr = v;
                    result = false;
                }
            }
            lock.unlock();
            return result;
        }

        void insert(key &k, value &v){
            std::mutex& _mutex_temp = GetMutexRef(k);
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(_mutex_temp);
            _map_temp[k] = v;
        }


        void remove(key &k, value &v) 
        {
            std::mutex& _mutex_temp = GetMutexRef(k);
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(_mutex_temp);
            map_iterator iter = _map_temp.find(k);
            if (iter != _map_temp.end()) {
                if (iter->second == v) {//if the abort txn has insert row and has not been modifid by ohters then remove it from map;or keep it 
                    _map_temp.erase(iter);
                } 
            }
            lock.unlock();
        }

        void clear() {
            for(uint64_t i = 0; i < _N; i ++){
                std::unique_lock<std::mutex> lock(_mutex[i]);
                _map[i].clear();
            }
        }

        void unsafe_clear() { 
            for(uint64_t i = 0; i < _N; i ++){
                _map[i].clear();
            }
        }

        bool contain(key &k, value &v){
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            // const std::mutex& _mutex_temp = GetMutexRef(k);
            // std::unique_lock<std::mutex> lock(_mutex_temp);
            map_iterator iter = _map_temp.find(k);
            if(iter != _map_temp.end()){
                if(iter->second == v){
                    return true;
                }
                else return false;
            }
            return false;
        }

        bool unsafe_contain(key &k, value &v){
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            map_iterator iter = _map_temp.find(k);
            if(iter != _map_temp.end()){
                if(iter->second == v){
                    return true;
                }
                else return false;
            }
            return false;
        }

        size_type size() {
            size_type ans = 0;
            for(uint64_t i = 0; i < _N; i ++){
                std::unique_lock<std::mutex> lock(_mutex[i]);
                ans += _map[i].size();
            }
            return ans;
        }

        std::unordered_map<key, value>& GetMapRef(key k){ return _map[(_hash(k) % _N)]; }
        std::unordered_map<key, value>& GetMapRef(key k) const { return _map[(_hash(k) % _N)]; }
        std::mutex& GetMutexRef(key k) { return _mutex[(_hash(k) % _N)]; }
        std::mutex& GetMutexRef(key k) const {return _mutex[(_hash(k) % _N)]; }

    private:
        const static uint64_t _N = 12281;//1217 12281 122777 prime
        std::hash<key> _hash;
        std::unordered_map<key, value> _map[_N];
        std::mutex _mutex[_N];
    };
}; // NAMESPACE AUM


#define MIN_DYNAMIC_PROCESS_MEMORY 2 * 1024 * 1024

#define MOT_INSERT_FAILED_MSG "Insert failed"
#define MOT_UPDATE_FAILED_MSG "Update failed"
#define MOT_DELETE_FAILED_MSG "Delete failed"

#define MOT_UNIQUE_VIOLATION_MSG "duplicate key value violates unique constraint \"%s\""
#define MOT_UNIQUE_VIOLATION_DETAIL "Key %s already exists."
#define MOT_TABLE_NOTFOUND "Table \"%s\" doesn't exist"
#define MOT_UPDATE_INDEXED_FIELD_NOT_SUPPORTED "Update indexed field \"%s\" in table \"%s\" is not supported"

#define NULL_DETAIL ((char*)nullptr)
#define abortParentTransaction(msg, detail) \
    ereport(ERROR,                          \
        (errmodule(MOD_MOT), errcode(ERRCODE_FDW_ERROR), errmsg(msg), (detail != nullptr ? errdetail(detail) : 0)));

#define abortParentTransactionParams(error, msg, msg_p, detail, detail_p) \
    ereport(ERROR, (errmodule(MOD_MOT), errcode(error), errmsg(msg, msg_p), errdetail(detail, detail_p)));

#define abortParentTransactionParamsNoDetail(error, msg, ...) \
    ereport(ERROR, (errmodule(MOD_MOT), errcode(error), errmsg(msg, __VA_ARGS__)));

#define isMemoryLimitReached()                                                                                         \
    {                                                                                                                  \
        if (MOTAdaptor::m_engine->IsSoftMemoryLimitReached()) {                                                        \
            MOT_LOG_ERROR("Maximum logical memory capacity %lu bytes of allowed %lu bytes reached",                    \
                (uint64_t)MOTAdaptor::m_engine->GetCurrentMemoryConsumptionBytes(),                                    \
                (uint64_t)MOTAdaptor::m_engine->GetHardMemoryLimitBytes());                                            \
            ereport(ERROR,                                                                                             \
                (errmodule(MOD_MOT),                                                                                   \
                    errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),                                                            \
                    errmsg("You have reached a maximum logical capacity"),                                             \
                    errdetail("Only destructive operations are allowed, please perform database cleanup to free some " \
                              "memory.")));                                                                            \
        }                                                                                                              \
    }

namespace MOT {
class Table;
class Index;
class IndexIterator;
class Column;
class MOTEngine;
}  // namespace MOT

#ifndef MOTFdwStateSt
typedef struct MOTFdwState_St MOTFdwStateSt;
#endif

typedef enum : uint8_t { SORTDIR_NONE = 0, SORTDIR_ASC = 1, SORTDIR_DESC = 2 } SORTDIR_ENUM;
typedef enum : uint8_t { FDW_LIST_STATE = 1, FDW_LIST_BITMAP = 2 } FDW_LIST_TYPE;

typedef struct Order_St {
    SORTDIR_ENUM m_order;
    int m_lastMatch;
    int m_cols[MAX_KEY_COLUMNS];

    void init()
    {
        m_order = SORTDIR_NONE;
        m_lastMatch = -1;
        for (uint32_t i = 0; i < MAX_KEY_COLUMNS; i++)
            m_cols[i] = 0;
    }
} OrderSt;

#define MOT_REC_TID_NAME "ctid"

typedef struct MOTRecConvert {
    union {
        uint64_t m_ptr;
        ItemPointerData m_self; /* SelfItemPointer */
    } m_u;
} MOTRecConvertSt;

#define SORT_STRATEGY(x) ((x == BTGreaterStrategyNumber) ? SORTDIR_DESC : SORTDIR_ASC)

struct MOTFdwState_St {
    ::TransactionId m_txnId;
    bool m_allocInScan;
    CmdType m_cmdOper;
    SORTDIR_ENUM m_order;
    bool m_hasForUpdate;
    Oid m_foreignTableId;
    AttrNumber m_numAttrs;
    AttrNumber m_ctidNum;
    uint16_t m_numExpr;
    uint8_t* m_attrsUsed;
    uint8_t* m_attrsModified;  // this will be merged into attrs_used in BeginModify
    List* m_remoteConds;
    List* m_remoteCondsOrig;
    List* m_localConds;
    List* m_execExprs;
    ExprContext* m_econtext;
    double m_startupCost;
    double m_totalCost;

    MatchIndex* m_bestIx;
    MatchIndex* m_paramBestIx;
    MatchIndex m_bestIxBuf;

    // ENGINE
    MOT::Table* m_table;
    MOT::IndexIterator* m_cursor[2] = {nullptr, nullptr};
    MOT::TxnManager* m_currTxn;
    void* m_currItem = nullptr;
    uint32_t m_rowsFound = 0;
    bool m_cursorOpened = false;
    MOT::MaxKey m_stateKey[2];
    bool m_forwardDirectionScan;
    MOT::AccessType m_internalCmdOper;
};

class MOTAdaptor {
public:
    static void Init();
    static void Destroy();
    static void NotifyConfigChange();
    static void InitDataNodeId();

    static inline void GetCmdOper(MOTFdwStateSt* festate)
    {
        switch (festate->m_cmdOper) {
            case CMD_SELECT:
                if (festate->m_hasForUpdate) {
                    festate->m_internalCmdOper = MOT::AccessType::RD_FOR_UPDATE;
                } else {
                    festate->m_internalCmdOper = MOT::AccessType::RD;
                }
                break;
            case CMD_DELETE:
                festate->m_internalCmdOper = MOT::AccessType::DEL;
                break;
            case CMD_UPDATE:
                festate->m_internalCmdOper = MOT::AccessType::WR;
                break;
            case CMD_INSERT:
                festate->m_internalCmdOper = MOT::AccessType::INS;
                break;
            case CMD_UNKNOWN:
            case CMD_MERGE:
            case CMD_UTILITY:
            case CMD_NOTHING:
            default:
                festate->m_internalCmdOper = MOT::AccessType::INV;
                break;
        }
    }

    static MOT::TxnManager* InitTxnManager(
        const char* callerSrc, MOT::ConnectionId connection_id = INVALID_CONNECTION_ID);
    static void DestroyTxn(int status, Datum ptr);
    static void DeleteTablePtr(MOT::Table* t);

    static MOT::RC CreateTable(CreateForeignTableStmt* table, TransactionId tid);
    static MOT::RC CreateIndex(IndexStmt* index, TransactionId tid);
    static MOT::RC DropIndex(DropForeignStmt* stmt, TransactionId tid);
    static MOT::RC DropTable(DropForeignStmt* stmt, TransactionId tid);
    static MOT::RC TruncateTable(Relation rel, TransactionId tid);
    static MOT::RC VacuumTable(Relation rel, TransactionId tid);
    static uint64_t GetTableIndexSize(uint64_t tabId, uint64_t ixId);
    static MotMemoryDetail* GetMemSize(uint32_t* nodeCount, bool isGlobal);
    static MotSessionMemoryDetail* GetSessionMemSize(uint32_t* sessionCount);
    static MOT::RC ValidateCommit();
    static void RecordCommit(uint64_t csn);
    static MOT::RC Commit(uint64_t csn);  // Does both ValidateCommit and RecordCommit
    static void EndTransaction();
    static void Rollback();
    static MOT::RC Prepare();
    static void CommitPrepared(uint64_t csn);
    static void RollbackPrepared();
    static MOT::RC InsertRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot);
    static MOT::RC UpdateRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot, MOT::Row* currRow);
    static MOT::RC DeleteRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot);

    /* Convertors */
    inline static void PGNumericToMOT(const Numeric n, MOT::DecimalSt& d)
    {
        int sign = NUMERIC_SIGN(n);

        d.m_hdr.m_flags = 0;
        d.m_hdr.m_flags |= (sign == NUMERIC_POS
                                ? DECIMAL_POSITIVE
                                : (sign == NUMERIC_NEG ? DECIMAL_NEGATIVE : ((sign == NUMERIC_NAN) ? DECIMAL_NAN : 0)));
        d.m_hdr.m_ndigits = NUMERIC_NDIGITS(n);
        d.m_hdr.m_scale = NUMERIC_DSCALE(n);
        d.m_hdr.m_weight = NUMERIC_WEIGHT(n);
        d.m_round = 0;
        if (d.m_hdr.m_ndigits > 0) {
            errno_t erc = memcpy_s(d.m_digits,
                DECIMAL_MAX_SIZE - sizeof(MOT::DecimalSt),
                (void*)NUMERIC_DIGITS(n),
                d.m_hdr.m_ndigits * sizeof(NumericDigit));
            securec_check(erc, "\0", "\0");
        }
    }

    inline static Numeric MOTNumericToPG(MOT::DecimalSt* d)
    {
        NumericVar v;

        v.ndigits = d->m_hdr.m_ndigits;
        v.dscale = d->m_hdr.m_scale;
        v.weight = (int)(int16_t)(d->m_hdr.m_weight);
        v.sign = (d->m_hdr.m_flags & DECIMAL_POSITIVE
                      ? NUMERIC_POS
                      : (d->m_hdr.m_flags & DECIMAL_NEGATIVE ? NUMERIC_NEG
                                                             : ((d->m_hdr.m_flags & DECIMAL_NAN) ? DECIMAL_NAN : 0)));
        v.buf = (NumericDigit*)&d->m_round;
        v.digits = (NumericDigit*)d->m_digits;

        return makeNumeric(&v);
    }

    // data conversion
    static void DatumToMOT(MOT::Column* col, Datum datum, Oid type, uint8_t* data);
    static void DatumToMOTKey(MOT::Column* col, Oid datumType, Datum datum, Oid colType, uint8_t* data, size_t len,
        KEY_OPER oper, uint8_t fill = 0x00);
    static void MOTToDatum(MOT::Table* table, const Form_pg_attribute attr, uint8_t* data, Datum* value, bool* is_null);

    static void PackRow(TupleTableSlot* slot, MOT::Table* table, uint8_t* attrs_used, uint8_t* destRow);
    static void PackUpdateRow(TupleTableSlot* slot, MOT::Table* table, const uint8_t* attrs_used, uint8_t* destRow);
    static void UnpackRow(TupleTableSlot* slot, MOT::Table* table, const uint8_t* attrs_used, uint8_t* srcRow);

    // scan helpers
    static void OpenCursor(Relation rel, MOTFdwStateSt* festate);
    static bool IsScanEnd(MOTFdwStateSt* festate);
    static void CreateKeyBuffer(Relation rel, MOTFdwStateSt* festate, int start);

    // planning helpers
    static bool SetMatchingExpr(MOTFdwStateSt* state, MatchIndexArr* marr, int16_t colId, KEY_OPER op, Expr* expr,
        Expr* parent, bool set_local);
    static MatchIndex* GetBestMatchIndex(
        MOTFdwStateSt* festate, MatchIndexArr* marr, int numClauses, bool setLocal = true);
    inline static int32_t AddParam(List** params, Expr* expr)
    {
        int32_t index = 0;
        ListCell* cell = nullptr;

        foreach (cell, *params) {
            ++index;
            if (equal(expr, (Node*)lfirst(cell))) {
                break;
            }
        }
        if (cell == nullptr) {
            /* add the parameter to the list */
            ++index;
            *params = lappend(*params, expr);
        }

        return index;
    }

    static MOT::MOTEngine* m_engine;
    static bool m_initialized;
    static bool m_callbacks_initialized;

private:
    /**
     * @brief Adds all the columns.
     * @param table Table object being created.
     * @param tableElts Column definitions list.
     * @param[out] hasBlob Whether any column is a blob.
     * NOTE: On failure, table object will be deleted and ereport will be done.
     */
    static void AddTableColumns(MOT::Table* table, List *tableElts, bool& hasBlob);

    static void ValidateCreateIndex(IndexStmt* index, MOT::Table* table, MOT::TxnManager* txn);

    static void VarcharToMOTKey(MOT::Column* col, Oid datumType, Datum datum, Oid colType, uint8_t* data, size_t len,
        KEY_OPER oper, uint8_t fill);
    static void FloatToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data);
    static void NumericToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data);
    static void TimestampToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data);
    static void TimestampTzToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data);
    static void DateToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data);






































private:
    typedef typename std::unordered_set<MOT::TxnManager *> TxnBuffer;
    typedef typename TxnBuffer::iterator TxnBufferIter;
    static TxnBuffer txnBuffer;
    
    static bool timerStop;
    //ADDBY NUE Concurrency
    static volatile bool remoteExeced;
    static volatile uint64_t logical_epoch;
    static volatile uint64_t physical_epoch;

    static std::atomic<uint64_t> unpackaged_message_num;
    static std::atomic<uint64_t> unpackaged_txn_num;
    static std::atomic<uint64_t> merged_txn_num;
    static std::atomic<uint64_t> commit_txn_num;
    static std::atomic<uint64_t> committed_txn_num;
    

public:
    static LockCV lock_cv_epoch_cache;
    static LockCV lock_cv_epoch_manager;
    static std::vector<LockCV*> lock_cv_pack_threads;
    static std::vector<LockCV*> lock_cv_notify_threads;

    static std::vector<LockCV*> lock_cv_txn_v;
    static std::vector<LockCV*> lock_cv_commit_v;

    static std::vector<LockCV*> lock_cv_commit_l;//本地阻塞线程条件变量数组   
    static std::atomic<uint64_t> lock_cv_commit_wait_counter;//本地阻塞线程数量  
    static std::atomic<uint64_t> lock_cv_commit_notify_counter;//本地阻塞线程唤醒数量  

    // static sem_t sem_commit;

    static std::vector<std::atomic<uint64_t>*> local_txn_counters;//本地事务执行阶段计数器 多个
    static std::vector<std::atomic<uint64_t>*> local_txn_execed_counters;//本地事务执行阶段完成计数器 多个 为了防止频繁访问，将加和减才分成两个(如果一个影响性能的话)
    static std::vector<std::atomic<uint64_t>*> local_txn_index;//本地事务pack_index

    static std::atomic<uint64_t> local_txn_counter;//本地事务执行阶段计数器 单个
    static std::atomic<uint64_t> local_txn_execed_counter;//本地事务执行阶段完成计数器 单个 为了防止频繁访问，将加和减才分成两个(如果一个影响性能的话) 目前还是将加和减合并为对一个变量的操作


    static std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>> local_change_set_txn_num_ptr1;
    static std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>> local_change_set_txn_num_ptr2;
    static std::shared_ptr<std::atomic<uint64_t>> local_change_set_num_ptr1;
    static std::shared_ptr<std::atomic<uint64_t>> local_change_set_num_ptr2;


    // static aum::atomic_unordered_map<std::string, std::string, std::string> insertSet;
    // static aum::atomic_unordered_map<std::string, std::string, std::string> insertSetForCommit;
    // static aum::atomic_unordered_map<std::string, std::string, std::string> abort_transcation_csn_set;

    static aum::concurrent_unordered_map<std::string, std::string, std::string> insertSet;
    static aum::concurrent_unordered_map<std::string, std::string, std::string> insertSetForCommit;
    static aum::concurrent_unordered_map<std::string, std::string, std::string> abort_transcation_csn_set;

    static uint64_t kPackageNum, kNotifyNum, kPackThreadNum, kNotifyThreadNum, local_ip_index;
    static uint32_t kServerId;

    static std::atomic<uint64_t> local_commit_count1, local_commit_count2;

    static void setTimerStop(bool value) {timerStop = value;}

    static bool isTimerStop() {return timerStop;}

    static bool isRemoteExeced() {return remoteExeced;}

    static void setRemoteExeced(bool value) {remoteExeced = value;}

    static bool add(MOT::TxnManager *txnManager){return txnBuffer.insert(txnManager).second;}

    static void clear(){txnBuffer.clear();}

    static TxnBufferIter begin(){return txnBuffer.begin();}

    static TxnBufferIter end(){return txnBuffer.end();}
    

    static void IncLocalTxnCounter(uint64_t index){ local_txn_counters[index]->fetch_add(10000000001);}// 10 000 000 000 + 1
    
    static void DecLocalTxnCounter(uint64_t index){ local_txn_counters[index]->fetch_sub(10000000001);}

    static void IncLocalTxnEcxCounter(uint64_t index){ local_txn_execed_counters[index]->fetch_add(10000000001);}

    static void DecExeCounter(uint64_t index){ local_txn_counters[index]->fetch_sub(1);}
    
    static void IncExeCounter(uint64_t index){ local_txn_execed_counters[index]->fetch_add(1);}

    static uint64_t GetExeCounter(uint64_t index) { return local_txn_counters[index]->load() % static_cast<uint64_t>(1e10);}

    static void DecComCounter(uint64_t index){ local_txn_counters[index]->fetch_sub(10000000000);}
    
    static void IncComCounter(uint64_t index){ local_txn_execed_counters[index]->fetch_add(10000000000);}

    static uint64_t GetComCounter(uint64_t index) {return local_txn_counters[index]->load();}

    static uint64_t GetAllComCounter(){
        uint64_t res = 0;
        for(auto &i : local_txn_counters){
            res += (i->load() / static_cast<uint64_t>(1e10) );
        }
        return res;
    }

    static bool IsExeced(){
        for(int i = 0; i < (int)local_txn_counters.size(); i++){
            if(local_txn_counters[i]->load() % static_cast<uint64_t>(1e10) != local_txn_execed_counters[i]->load() % static_cast<uint64_t>(1e10)){
                return false;
            }
        }
        return true;
    }

    static bool IsCommited(){
        for(int i = 0; i < (int)local_txn_counters.size(); i++){
            if(local_txn_counters[i]->load() != local_txn_execed_counters[i]->load()){
                return false;
            }
        }
        return true;
    }

    static bool IsExcCounterEqualZero(){
        for(auto &i : local_txn_counters){
            if( i->load() % static_cast<uint64_t>(1e10) != 0) return false;
        }
        return true;
    }

    static bool IsComCounterEqualZero(){
        for(auto &i : local_txn_counters){
            if(i->load() != 0) return false;
        }
        return true;
    }

    static uint64_t IncLocalTxnIndex(uint64_t index){ return local_txn_index[index]->fetch_add(1);}
    
    static void IncLocalTxnCounter(){ local_txn_counter.fetch_add(10000000001);}// 10 000 000 000 + 1
    
    static void DecLocalTxnCounter(){ local_txn_counter.fetch_sub(10000000001);}
    static void IncLocalTxnExcCounter(){ local_txn_execed_counter.fetch_add(10000000001);}

    static void DecExeCounter(){ local_txn_counter.fetch_sub(1);}

    static void IncExeCounter(){ local_txn_execed_counter.fetch_add(1);}

    static uint64_t GetExeCounter() { return  local_txn_counter.load() % static_cast<uint64_t>(1e10);}
    static bool IsExeCounterEqual(){ return ((local_txn_counter.load() == local_txn_execed_counter.load()) && (local_txn_counter.load() % static_cast<uint64_t>(1e10) == 0)); }

    static void DecComCounter(){ local_txn_counter.fetch_sub(10000000000);}
    static void IncComCounter(){ local_txn_execed_counter.fetch_add(10000000000);}

    static uint64_t GetComCounter() { return local_txn_counter.load();}

    static bool IsComCounterEqual(){ return local_txn_counter.load() == local_txn_execed_counter.load(); }




    static void AddUnpackagedMessageNum(){unpackaged_message_num.fetch_add(1);}

    static uint64_t GetUnpackagedMessageNum(){return unpackaged_message_num.load();}

    static void AddUnpackagedTxnNum(){unpackaged_txn_num.fetch_add(1);}

    static uint64_t GetUnpackagedTxnNum(){return unpackaged_txn_num.load();}

    static void AddMergedTxnNum(){merged_txn_num.fetch_add(1);}

    static void SubMergedTxnNum(){merged_txn_num.fetch_sub(1);}

    static uint64_t GetMergedTxnNum(){return merged_txn_num.load();}

    static void AddCommitTxnNum(){commit_txn_num.fetch_add(1);}

    static uint64_t GetCommitTxnNum(){return commit_txn_num.load();}

    static void AddCommittedTxnNum(){committed_txn_num.fetch_add(1);}

    static uint64_t GetCommittedTxnNum(){return committed_txn_num.load();}


    static void SetPhysicalEpoch(int value){physical_epoch = value;}

    static void AddPhysicalEpoch(){physical_epoch++;}

    static uint64_t GetPhysicalEpoch(){return physical_epoch;}

    static void SetLogicalEpoch(int value){logical_epoch = value;}

    static void AddLogicalEpoch(){logical_epoch++;}

    static uint64_t GetLogicalEpoch(){return logical_epoch;}


    static uint64_t GetLocalChangeSetNum(uint64_t index){ return (*local_change_set_txn_num_ptr1)[index]->load(); }

    static void IncLocalChangeSetNum(uint64_t index){ 
        (*((*local_change_set_txn_num_ptr1)[index])) ++; 
    }

    static void DecLocalChangeSetNum(uint64_t index){
        (*((*local_change_set_txn_num_ptr1)[index])) --; 
    }


    static void LogicalEndEpochClear(){
        unpackaged_message_num = 0;
        unpackaged_txn_num = 0;
        merged_txn_num = 0;
        commit_txn_num = 0;
        committed_txn_num = 0;
        lock_cv_commit_wait_counter = 0;
        lock_cv_commit_notify_counter = 0;
        for(int i = 0; i < (int)local_txn_counters.size(); i++){
            local_txn_counters[i]->store(0);
            local_txn_execed_counters[i]->store(0);
            local_txn_index[i]->store(0);
        }
        // for(auto &i : local_txn_execed_counters) i = 0;
        local_txn_counter = local_txn_execed_counter = 0;
        insertSet.clear();
        insertSetForCommit.clear();
        abort_transcation_csn_set.clear();
        remoteExeced = false;
    }

    static bool InsertRowToSet(MOT::TxnManager* txMan, void* txn_void, const uint64_t& index_pack, const uint64_t& index_unique);
    
    static void LocalTxnSafeExit(const uint64_t& index_pack, void* txn_void);

    static void* InsertRowToMergeRequestTxn(MOT::TxnManager* txMan);

    // static void Notify_Commit_Txn(){
    //     uint64_t index = lock_cv_commit_notify_counter.fetch_add(1);
    //     if(index >= lock_cv_commit_wait_counter){
    //         lock_cv_commit_notify_counter.fetch_sub(1);
    //         return ;
    //     }
    //     else{
    //         lock_cv_commit_l[index]->notify_one();
    //     }
    // }


};

inline MOT::TxnManager* GetSafeTxn(const char* callerSrc, ::TransactionId txn_id = 0)
{
    if (!u_sess->mot_cxt.txn_manager) {
        MOTAdaptor::InitTxnManager(callerSrc);
        if (u_sess->mot_cxt.txn_manager != nullptr) {
            if (txn_id != 0) {
                u_sess->mot_cxt.txn_manager->SetTransactionId(txn_id);
            }
        } else {
            report_pg_error(MOT_GET_ROOT_ERROR_RC());
        }
    }
    return u_sess->mot_cxt.txn_manager;
}

extern void EnsureSafeThreadAccess();

inline List* BitmapSerialize(List* result, uint8_t* bitmap, int16_t len)
{
    // set list type to FDW_LIST_BITMAP
    result = lappend(result, makeConst(INT4OID, -1, InvalidOid, 4, FDW_LIST_BITMAP, false, true));
    for (int i = 0; i < len; i++)
        result = lappend(result, makeConst(INT1OID, -1, InvalidOid, 1, Int8GetDatum(bitmap[i]), false, true));

    return result;
}

inline void BitmapDeSerialize(uint8_t* bitmap, int16_t len, ListCell** cell)
{
    if (cell != nullptr && *cell != nullptr) {
        int type = ((Const*)lfirst(*cell))->constvalue;
        if (type == FDW_LIST_BITMAP) {
            *cell = lnext(*cell);
            for (int i = 0; i < len; i++) {
                bitmap[i] = (uint8_t)((Const*)lfirst(*cell))->constvalue;
                *cell = lnext(*cell);
            }
        }
    }
}

inline void CleanCursors(MOTFdwStateSt* state)
{
    for (int i = 0; i < 2; i++) {
        if (state->m_cursor[i]) {
            state->m_cursor[i]->Invalidate();
            state->m_cursor[i]->Destroy();
            delete state->m_cursor[i];
            state->m_cursor[i] = NULL;
        }
    }
}

inline void CleanQueryStatesOnError(MOT::TxnManager* txn)
{
    if (txn != nullptr) {
        for (auto& itr : txn->m_queryState) {
            MOTFdwStateSt* state = (MOTFdwStateSt*)itr.second;
            if (state != nullptr) {
                CleanCursors(state);
            }
        }
        txn->m_queryState.clear();
    }
}

MOTFdwStateSt* InitializeFdwState(void* fdwState, List** fdwExpr, uint64_t exTableID);
void* SerializeFdwState(MOTFdwStateSt* state);
void ReleaseFdwState(MOTFdwStateSt* state);

#endif  // MOT_INTERNAL_H
