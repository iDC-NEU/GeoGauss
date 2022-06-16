// /*
//  * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
//  *
//  * openGauss is licensed under Mulan PSL v2.
//  * You can use this software according to the terms and conditions of the Mulan PSL v2.
//  * You may obtain a copy of Mulan PSL v2 at:
//  *
//  *          http://license.coscl.org.cn/MulanPSL2
//  *
//  * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
//  * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
//  * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
//  * See the Mulan PSL v2 for more details.
//  * -------------------------------------------------------------------------
//  *
//  * mot_internal.h
//  *    MOT Foreign Data Wrapper internal interfaces to the MOT engine.
//  *
//  * IDENTIFICATION
//  *    src/gausskernel/storage/mot/fdw_adapter/src/mot_internal.h
//  *
//  * -------------------------------------------------------------------------
//  */

// #ifndef MOT_INTERNAL_H
// #define MOT_INTERNAL_H

// #include <map>
// #include <string>
// #include "catalog_column_types.h"
// #include "foreign/fdwapi.h"
// #include "nodes/nodes.h"
// #include "nodes/makefuncs.h"
// #include "utils/numeric.h"
// #include "utils/numeric_gs.h"
// #include "pgstat.h"
// #include "global.h"
// #include "mot_fdw_error.h"
// #include "mot_fdw_xlog.h"
// #include "system/mot_engine.h"
// #include "bitmapset.h"
// #include "storage/mot/jit_exec.h"
// #include "mot_match_index.h"
// #include <atomic>
// #include <unordered_set>
// #include <unordered_map>
// #include <functional>
// #include <mutex>
// #include <condition_variable>
// #include "pthread.h"
// #include "table.h"
// #include "row.h"
// #include "neu_concurrency_tools/blockingconcurrentqueue.h"
// #include "neu_concurrency_tools/blocking_mpmc_queue.h"
// #include "neu_concurrency_tools/readerwriterqueue.h"
// #include <vector>
// #include <typeinfo>
// // #include <semaphore.h>

// using std::map;
// using std::string;

// #define MAX_TXN_NUM 100000
// #define ADD_NUM 10000000001
// #define MOD_NUM 10000000000

// extern void EpochLogicalTimerManagerThreadMain(uint64_t id);
// extern void EpochPhysicalTimerManagerThreadMain(uint64_t id);
// extern void EpochMessageManagerThreadMain(uint64_t id);
// extern void EpochMessageCacheManagerThreadMain(uint64_t id);

// extern void EpochNotifyThreadMain(uint64_t id);
// extern void EpochPackThreadMain(uint64_t id);
// extern void EpochSendThreadMain(uint64_t id);

// extern void EpochListenThreadMain(uint64_t id);
// extern void EpochUnseriThreadMain(uint64_t id);
// extern void EpochUnpackThreadMain(uint64_t id);
// extern void EpochMergeThreadMain(uint64_t id);
// extern void EpochCommitThreadMain(uint64_t id);
// extern void EpochRecordCommitThreadMain(uint64_t id);

// extern void EpochMessageSendThreadMain(uint64_t id);
// extern void EpochMessageListenThreadMain(uint64_t id);

// namespace aum {
//     class SpinLock {
//         public:
//         // constructors
//         SpinLock() = default;

//         SpinLock(const SpinLock &) = delete;            // non construction-copyable
//         SpinLock &operator=(const SpinLock &) = delete; // non copyable

//         // Modifiers
//         void lock() {
//             while (lock_.test_and_set());
//         }

//         void unlock() { lock_.clear(std::memory_order_release); }

//         private:
//         std::atomic_flag lock_ = ATOMIC_FLAG_INIT;
//     };

//     template<typename key, typename value, typename pointer>
//     class atomic_unordered_map {
//     public:
//         typedef typename std::unordered_map<key, value>::iterator map_iterator;
//         typedef typename std::unordered_map<key, value>::size_type size_type;
//         // typedef typename std::map<key, value>::iterator map_iterator;
//         // typedef typename std::map<key, value>::size_type size_type;
//     public:

//         bool insert(key &k, value &v, pointer *ptr, std::function<bool(const value &, const value &)> 
//             cmp = [](const value& o, const value& n){return n < o;}) //为insert服务
//         {
//             bool result = true;
//             lock.lock();
//             map_iterator iter = map.find(k);
//             if (iter == map.end()) {
//                 map[k] = v;
//                 *ptr = "0";
//                 result = true;
//             } else {
//                 // cmp(oldCsn, newCsn) -> bool
//                 // if true: 新的事务覆盖旧的事务
//                 if (iter->second > v) {
//                     *ptr = map[k];
//                     map[k] = v;
//                     result = true;
//                 } 
//                 else if(iter->second == v){//相同则为同一事务执行的插入，不能将map[k]返回abort掉
//                     *ptr = "0";
//                     result = true;
//                 }
//                 else{
//                     *ptr = v;
//                     result = false;
//                 }
//             }
//             lock.unlock();
//             return result;
//         }

//         void insert(key &k, value &v){//为update服务
//             lock.lock();
//             map[k] = v;
//             lock.unlock();
//         }


//         void remove(key &k, value &v) 
//         {
//             lock.lock();
//             map_iterator iter = map.find(k);
//             if (iter != map.end() && iter->second == v) {
//                 //if the abort txn has insert row and has not been modifid by ohters 
//                 //then remove it from map;or keep it 
//                 map.erase(iter);
//             }
//             lock.unlock();
//         }

//         void clear() {
//             lock.lock();
//             map.clear();
//             lock.unlock();
//         }

//         void unsafe_clear() { map.clear(); }

//         map_iterator unsafe_begin() {
//             return map.begin();
//         }

//         map_iterator unsafe_end() {
//             return map.end();
//         }

//         map_iterator unsafe_find(key &k) {
//             return map.find(k);
//         }

//         bool contain(key &k, value &v){
//             map_iterator iter = map.find(k);
//             if(iter != map.end() && iter->second == v){
//                 return true;
//             }
//             return false;
//         }

//         size_type size() { return map.size(); }

//     public:
//         std::unordered_map<key, value> map;
//         // std::map<key, value> map;
//         aum::SpinLock lock;
//     };

//     template<typename key, typename value, typename pointer>
//     class concurrent_unordered_map {
//     public:
//         typedef typename std::unordered_map<key, value>::iterator map_iterator;
//         typedef typename std::unordered_map<key, value>::size_type size_type;

//         bool insert(key &k, value &v, pointer *ptr, std::function<bool(const value &, const value &)> 
//             cmp = [](const value& o, const value& n){return n < o;}) //为insert服务
//         {
//             std::mutex& _mutex_temp = GetMutexRef(k);
//             std::unordered_map<key, value>& _map_temp = GetMapRef(k);
//             bool result = true;
//             std::unique_lock<std::mutex> lock(_mutex_temp);
//             map_iterator iter = _map_temp.find(k);
//             if (iter == _map_temp.end()) {
//                 _map_temp[k] = v;
//                 *ptr = "0";
//                 result = true;
//             } else {
//                 // cmp(oldCsn, newCsn) -> bool
//                 // if true: 新的事务覆盖旧的事务
//                 if (iter->second > v) {
//                     *ptr = _map_temp[k];
//                     _map_temp[k] = v;
//                     result = true;
//                 } 
//                 else if(iter->second == v){//相同则为同一事务执行的插入，不能将map[k]返回abort掉
//                     *ptr = "0"; //暂且只支持string
//                     result = true;
//                 }
//                 else{
//                     *ptr = v;
//                     result = false;
//                 }
//             }
//             lock.unlock();
//             return result;
//         }

//         void insert(key &k, value &v){
//             std::mutex& _mutex_temp = GetMutexRef(k);
//             std::unordered_map<key, value>& _map_temp = GetMapRef(k);
//             std::unique_lock<std::mutex> lock(_mutex_temp);
//             _map_temp[k] = v;
//         }


//         void remove(key &k, value &v) 
//         {
//             std::mutex& _mutex_temp = GetMutexRef(k);
//             std::unordered_map<key, value>& _map_temp = GetMapRef(k);
//             std::unique_lock<std::mutex> lock(_mutex_temp);
//             map_iterator iter = _map_temp.find(k);
//             if (iter != _map_temp.end()) {
//                 if (iter->second == v) {
//                     //if the abort txn has insert row and has not been modifid by ohters 
//                     //then remove it from map;or keep it 
//                     _map_temp.erase(iter);
//                 } 
//             }
//             lock.unlock();
//         }

//         void remove(key &k) 
//         {
//             std::mutex& _mutex_temp = GetMutexRef(k);
//             std::unordered_map<key, value>& _map_temp = GetMapRef(k);
//             std::unique_lock<std::mutex> lock(_mutex_temp);
//             map_iterator iter = _map_temp.find(k);
//             if (iter != _map_temp.end()) {
//                 _map_temp.erase(iter);
//             }
//             lock.unlock();
//         }

//         void clear() {
//             for(uint64_t i = 0; i < _N; i ++){
//                 std::unique_lock<std::mutex> lock(_mutex[i]);
//                 _map[i].clear();
//             }
//         }

//         void unsafe_clear() { 
//             for(uint64_t i = 0; i < _N; i ++){
//                 _map[i].clear();
//             }
//         }

//         bool contain(key &k, value &v){
//             std::unordered_map<key, value>& _map_temp = GetMapRef(k);
//             map_iterator iter = _map_temp.find(k);
//             if(iter != _map_temp.end()){
//                 if(iter->second == v){
//                     return true;
//                 }
//             }
//             return false;
//         }

//         bool contain(key &k){
//             std::unordered_map<key, value>& _map_temp = GetMapRef(k);
//             map_iterator iter = _map_temp.find(k);
//             if(iter != _map_temp.end()){
//                 return true;
//             }
//             return false;
//         }

//         bool unsafe_contain(key &k, value &v){
//             std::unordered_map<key, value>& _map_temp = GetMapRef(k);
//             map_iterator iter = _map_temp.find(k);
//             if(iter != _map_temp.end()){
//                 if(iter->second == v){
//                     return true;
//                 }
//             }
//             return false;
//         }

//         size_type size() {
//             size_type ans = 0;
//             for(uint64_t i = 0; i < _N; i ++){
//                 std::unique_lock<std::mutex> lock(_mutex[i]);
//                 ans += _map[i].size();
//             }
//             return ans;
//         }

// protected:
//         inline std::unordered_map<key, value>& GetMapRef(const key k){ return _map[(_hash(k) % _N)]; }
//         inline std::unordered_map<key, value>& GetMapRef(const key k) const { return _map[(_hash(k) % _N)]; }
//         inline std::mutex& GetMutexRef(const key k) { return _mutex[(_hash(k) % _N)]; }
//         inline std::mutex& GetMutexRef(const key k) const {return _mutex[(_hash(k) % _N)]; }

//     private:
//         const static uint64_t _N = 997;//1217 12281 122777 prime
//         std::hash<key> _hash;
//         std::unordered_map<key, value> _map[_N];
//         std::mutex _mutex[_N];
//     };
// }; // NAMESPACE AUM


// #define MIN_DYNAMIC_PROCESS_MEMORY 2 * 1024 * 1024

// #define MOT_INSERT_FAILED_MSG "Insert failed"
// #define MOT_UPDATE_FAILED_MSG "Update failed"
// #define MOT_DELETE_FAILED_MSG "Delete failed"

// #define MOT_UNIQUE_VIOLATION_MSG "duplicate key value violates unique constraint \"%s\""
// #define MOT_UNIQUE_VIOLATION_DETAIL "Key %s already exists."
// #define MOT_TABLE_NOTFOUND "Table \"%s\" doesn't exist"
// #define MOT_UPDATE_INDEXED_FIELD_NOT_SUPPORTED "Update indexed field \"%s\" in table \"%s\" is not supported"

// #define NULL_DETAIL ((char*)nullptr)
// #define abortParentTransaction(msg, detail) \
//     ereport(ERROR,                          \
//         (errmodule(MOD_MOT), errcode(ERRCODE_FDW_ERROR), errmsg(msg), (detail != nullptr ? errdetail(detail) : 0)));

// #define abortParentTransactionParams(error, msg, msg_p, detail, detail_p) \
//     ereport(ERROR, (errmodule(MOD_MOT), errcode(error), errmsg(msg, msg_p), errdetail(detail, detail_p)));

// #define abortParentTransactionParamsNoDetail(error, msg, ...) \
//     ereport(ERROR, (errmodule(MOD_MOT), errcode(error), errmsg(msg, __VA_ARGS__)));

// #define isMemoryLimitReached()                                                                                         \
//     {                                                                                                                  \
//         if (MOTAdaptor::m_engine->IsSoftMemoryLimitReached()) {                                                        \
//             MOT_LOG_ERROR("Maximum logical memory capacity %lu bytes of allowed %lu bytes reached",                    \
//                 (uint64_t)MOTAdaptor::m_engine->GetCurrentMemoryConsumptionBytes(),                                    \
//                 (uint64_t)MOTAdaptor::m_engine->GetHardMemoryLimitBytes());                                            \
//             ereport(ERROR,                                                                                             \
//                 (errmodule(MOD_MOT),                                                                                   \
//                     errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),                                                            \
//                     errmsg("You have reached a maximum logical capacity"),                                             \
//                     errdetail("Only destructive operations are allowed, please perform database cleanup to free some " \
//                               "memory.")));                                                                            \
//         }                                                                                                              \
//     }

// namespace MOT {
// class Table;
// class Index;
// class IndexIterator;
// class Column;
// class MOTEngine;
// }  // namespace MOT

// #ifndef MOTFdwStateSt
// typedef struct MOTFdwState_St MOTFdwStateSt;
// #endif

// typedef enum : uint8_t { SORTDIR_NONE = 0, SORTDIR_ASC = 1, SORTDIR_DESC = 2 } SORTDIR_ENUM;
// typedef enum : uint8_t { FDW_LIST_STATE = 1, FDW_LIST_BITMAP = 2 } FDW_LIST_TYPE;

// typedef struct Order_St {
//     SORTDIR_ENUM m_order;
//     int m_lastMatch;
//     int m_cols[MAX_KEY_COLUMNS];

//     void init()
//     {
//         m_order = SORTDIR_NONE;
//         m_lastMatch = -1;
//         for (uint32_t i = 0; i < MAX_KEY_COLUMNS; i++)
//             m_cols[i] = 0;
//     }
// } OrderSt;

// #define MOT_REC_TID_NAME "ctid"

// typedef struct MOTRecConvert {
//     union {
//         uint64_t m_ptr;
//         ItemPointerData m_self; /* SelfItemPointer */
//     } m_u;
// } MOTRecConvertSt;

// #define SORT_STRATEGY(x) ((x == BTGreaterStrategyNumber) ? SORTDIR_DESC : SORTDIR_ASC)

// struct MOTFdwState_St {
//     ::TransactionId m_txnId;
//     bool m_allocInScan;
//     CmdType m_cmdOper;
//     SORTDIR_ENUM m_order;
//     bool m_hasForUpdate;
//     Oid m_foreignTableId;
//     AttrNumber m_numAttrs;
//     AttrNumber m_ctidNum;
//     uint16_t m_numExpr;
//     uint8_t* m_attrsUsed;
//     uint8_t* m_attrsModified;  // this will be merged into attrs_used in BeginModify
//     List* m_remoteConds;
//     List* m_remoteCondsOrig;
//     List* m_localConds;
//     List* m_execExprs;
//     ExprContext* m_econtext;
//     double m_startupCost;
//     double m_totalCost;

//     MatchIndex* m_bestIx;
//     MatchIndex* m_paramBestIx;
//     MatchIndex m_bestIxBuf;

//     // ENGINE
//     MOT::Table* m_table;
//     MOT::IndexIterator* m_cursor[2] = {nullptr, nullptr};
//     MOT::TxnManager* m_currTxn;
//     void* m_currItem = nullptr;
//     uint32_t m_rowsFound = 0;
//     bool m_cursorOpened = false;
//     MOT::MaxKey m_stateKey[2];
//     bool m_forwardDirectionScan;
//     MOT::AccessType m_internalCmdOper;
// };

// class MOTAdaptor {
// public:
//     static void Init();
//     static void Destroy();
//     static void NotifyConfigChange();
//     static void InitDataNodeId();

//     static inline void GetCmdOper(MOTFdwStateSt* festate)
//     {
//         switch (festate->m_cmdOper) {
//             case CMD_SELECT:
//                 if (festate->m_hasForUpdate) {
//                     festate->m_internalCmdOper = MOT::AccessType::RD_FOR_UPDATE;
//                 } else {
//                     festate->m_internalCmdOper = MOT::AccessType::RD;
//                 }
//                 break;
//             case CMD_DELETE:
//                 festate->m_internalCmdOper = MOT::AccessType::DEL;
//                 break;
//             case CMD_UPDATE:
//                 festate->m_internalCmdOper = MOT::AccessType::WR;
//                 break;
//             case CMD_INSERT:
//                 festate->m_internalCmdOper = MOT::AccessType::INS;
//                 break;
//             case CMD_UNKNOWN:
//             case CMD_MERGE:
//             case CMD_UTILITY:
//             case CMD_NOTHING:
//             default:
//                 festate->m_internalCmdOper = MOT::AccessType::INV;
//                 break;
//         }
//     }

//     static MOT::TxnManager* InitTxnManager(
//         const char* callerSrc, MOT::ConnectionId connection_id = INVALID_CONNECTION_ID);
//     static void DestroyTxn(int status, Datum ptr);
//     static void DeleteTablePtr(MOT::Table* t);

//     static MOT::RC CreateTable(CreateForeignTableStmt* table, TransactionId tid);
//     static MOT::RC CreateIndex(IndexStmt* index, TransactionId tid);
//     static MOT::RC DropIndex(DropForeignStmt* stmt, TransactionId tid);
//     static MOT::RC DropTable(DropForeignStmt* stmt, TransactionId tid);
//     static MOT::RC TruncateTable(Relation rel, TransactionId tid);
//     static MOT::RC VacuumTable(Relation rel, TransactionId tid);
//     static uint64_t GetTableIndexSize(uint64_t tabId, uint64_t ixId);
//     static MotMemoryDetail* GetMemSize(uint32_t* nodeCount, bool isGlobal);
//     static MotSessionMemoryDetail* GetSessionMemSize(uint32_t* sessionCount);
//     static MOT::RC ValidateCommit();
//     static void RecordCommit(uint64_t csn);
//     static MOT::RC Commit(uint64_t csn);  // Does both ValidateCommit and RecordCommit
//     static void EndTransaction();
//     static void Rollback();
//     static MOT::RC Prepare();
//     static void CommitPrepared(uint64_t csn);
//     static void RollbackPrepared();
//     static MOT::RC InsertRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot);
//     static MOT::RC UpdateRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot, MOT::Row* currRow);
//     static MOT::RC DeleteRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot);

//     /* Convertors */
//     inline static void PGNumericToMOT(const Numeric n, MOT::DecimalSt& d)
//     {
//         int sign = NUMERIC_SIGN(n);

//         d.m_hdr.m_flags = 0;
//         d.m_hdr.m_flags |= (sign == NUMERIC_POS
//                                 ? DECIMAL_POSITIVE
//                                 : (sign == NUMERIC_NEG ? DECIMAL_NEGATIVE : ((sign == NUMERIC_NAN) ? DECIMAL_NAN : 0)));
//         d.m_hdr.m_ndigits = NUMERIC_NDIGITS(n);
//         d.m_hdr.m_scale = NUMERIC_DSCALE(n);
//         d.m_hdr.m_weight = NUMERIC_WEIGHT(n);
//         d.m_round = 0;
//         if (d.m_hdr.m_ndigits > 0) {
//             errno_t erc = memcpy_s(d.m_digits,
//                 DECIMAL_MAX_SIZE - sizeof(MOT::DecimalSt),
//                 (void*)NUMERIC_DIGITS(n),
//                 d.m_hdr.m_ndigits * sizeof(NumericDigit));
//             securec_check(erc, "\0", "\0");
//         }
//     }

//     inline static Numeric MOTNumericToPG(MOT::DecimalSt* d)
//     {
//         NumericVar v;

//         v.ndigits = d->m_hdr.m_ndigits;
//         v.dscale = d->m_hdr.m_scale;
//         v.weight = (int)(int16_t)(d->m_hdr.m_weight);
//         v.sign = (d->m_hdr.m_flags & DECIMAL_POSITIVE
//                       ? NUMERIC_POS
//                       : (d->m_hdr.m_flags & DECIMAL_NEGATIVE ? NUMERIC_NEG
//                                                              : ((d->m_hdr.m_flags & DECIMAL_NAN) ? DECIMAL_NAN : 0)));
//         v.buf = (NumericDigit*)&d->m_round;
//         v.digits = (NumericDigit*)d->m_digits;

//         return makeNumeric(&v);
//     }

//     // data conversion
//     static void DatumToMOT(MOT::Column* col, Datum datum, Oid type, uint8_t* data);
//     static void DatumToMOTKey(MOT::Column* col, Oid datumType, Datum datum, Oid colType, uint8_t* data, size_t len,
//         KEY_OPER oper, uint8_t fill = 0x00);
//     static void MOTToDatum(MOT::Table* table, const Form_pg_attribute attr, uint8_t* data, Datum* value, bool* is_null);

//     static void PackRow(TupleTableSlot* slot, MOT::Table* table, uint8_t* attrs_used, uint8_t* destRow);
//     static void PackUpdateRow(TupleTableSlot* slot, MOT::Table* table, const uint8_t* attrs_used, uint8_t* destRow);
//     static void UnpackRow(TupleTableSlot* slot, MOT::Table* table, const uint8_t* attrs_used, uint8_t* srcRow);

//     // scan helpers
//     static void OpenCursor(Relation rel, MOTFdwStateSt* festate);
//     static bool IsScanEnd(MOTFdwStateSt* festate);
//     static void CreateKeyBuffer(Relation rel, MOTFdwStateSt* festate, int start);

//     // planning helpers
//     static bool SetMatchingExpr(MOTFdwStateSt* state, MatchIndexArr* marr, int16_t colId, KEY_OPER op, Expr* expr,
//         Expr* parent, bool set_local);
//     static MatchIndex* GetBestMatchIndex(
//         MOTFdwStateSt* festate, MatchIndexArr* marr, int numClauses, bool setLocal = true);
//     inline static int32_t AddParam(List** params, Expr* expr)
//     {
//         int32_t index = 0;
//         ListCell* cell = nullptr;

//         foreach (cell, *params) {
//             ++index;
//             if (equal(expr, (Node*)lfirst(cell))) {
//                 break;
//             }
//         }
//         if (cell == nullptr) {
//             /* add the parameter to the list */
//             ++index;
//             *params = lappend(*params, expr);
//         }

//         return index;
//     }

//     static MOT::MOTEngine* m_engine;
//     static bool m_initialized;
//     static bool m_callbacks_initialized;

// private:
//     /**
//      * @brief Adds all the columns.
//      * @param table Table object being created.
//      * @param tableElts Column definitions list.
//      * @param[out] hasBlob Whether any column is a blob.
//      * NOTE: On failure, table object will be deleted and ereport will be done.
//      */
//     static void AddTableColumns(MOT::Table* table, List *tableElts, bool& hasBlob);

//     static void ValidateCreateIndex(IndexStmt* index, MOT::Table* table, MOT::TxnManager* txn);

//     static void VarcharToMOTKey(MOT::Column* col, Oid datumType, Datum datum, Oid colType, uint8_t* data, size_t len,
//         KEY_OPER oper, uint8_t fill);
//     static void FloatToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data);
//     static void NumericToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data);
//     static void TimestampToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data);
//     static void TimestampTzToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data);
//     static void DateToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data);






































// private:
//     typedef typename std::unordered_set<MOT::TxnManager *> TxnBuffer;
//     typedef typename TxnBuffer::iterator TxnBufferIter;
//     static TxnBuffer txnBuffer;
    
//     static bool timerStop;
//     //ADDBY NUE Concurrency
//     static volatile bool remote_execed, record_committed, remote_record_committed, is_current_epoch_abort;
//     static volatile uint64_t logical_epoch;
//     static volatile uint64_t physical_epoch;

// public:
//     static uint64_t max_length, pack_num;
//     static std::vector<std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>> local_txn_counters, local_txn_exc_counters,
//         local_txn_index,
//         record_commit_txn_counters, record_committed_txn_counters, remote_merged_txn_counters, remote_commit_txn_counters, 
//         remote_committed_txn_counters;
//     static std::map<uint64_t, std::unique_ptr<std::vector<MOT::Row*>>> remote_row_ptr_map;
//     static aum::concurrent_unordered_map<std::string, std::string, std::string> insertSet;
//     static aum::concurrent_unordered_map<std::string, std::string, std::string> insertSetForCommit;
//     static aum::concurrent_unordered_map<std::string, std::string, std::string> abort_transcation_csn_set;


//     static void SetTimerStop(bool value) {timerStop = value;}
//     static bool IsTimerStop() {return timerStop;}
    
//     static bool IsRemoteExeced() {return remote_execed;}
//     static void SetRemoteExeced(bool value) {remote_execed = value;}
    
//     static bool IsRecordCommitted(){ return record_committed;}
//     static void SetRecordCommitted(bool value){ record_committed = value;}
    
//     static bool IsRemoteRecordCommitted(){ return remote_record_committed;}
//     static void SetRemoteRecordCommitted(bool value){ remote_record_committed = value;}

//     static bool IsCurrentEpochAbort(){ return is_current_epoch_abort;}
//     static void SetCurrentEpochAbort(bool value){ is_current_epoch_abort = value;}
    
//     static void SetPhysicalEpoch(int value){ physical_epoch = value;}
//     static uint64_t AddPhysicalEpoch(){ return ++ physical_epoch;}
//     static uint64_t GetPhysicalEpoch(){ return physical_epoch;}

//     static void SetLogicalEpoch(int value){ logical_epoch = value;}
//     static uint64_t AddLogicalEpoch(){ return ++ logical_epoch;}
//     static uint64_t GetLogicalEpoch(){ return logical_epoch;}

//     static uint64_t IncLocalTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*local_txn_counters[epoch_mod % max_length])[index]->fetch_add(ADD_NUM);}
//     static uint64_t DecLocalTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*local_txn_counters[epoch_mod % max_length])[index]->fetch_sub(ADD_NUM);}
//     static uint64_t DecExeCounters(uint64_t epoch_mod, uint64_t index){ return (*local_txn_counters[epoch_mod % max_length])[index]->fetch_sub(1);}
//     static uint64_t DecComCounters(uint64_t epoch_mod, uint64_t index){ return (*local_txn_counters[epoch_mod % max_length])[index]->fetch_sub(MOD_NUM);}
//     static bool IsLocalTxnCountersExcEqualZero(uint64_t epoch_mod){
//         epoch_mod %= max_length;
//         for(int i = 0; i < (int)pack_num; i ++){
//             if( (*local_txn_counters[epoch_mod])[i]->load() % static_cast<uint64_t>(1e10) != 0) return false;
//         }
//         return true;
//     }
//     static bool IsLocalTxnCountersComEqualZero(uint64_t epoch_mod){
//         epoch_mod %= max_length;
//         for(int i = 0; i < (int)pack_num; i ++){
//             if( (*local_txn_counters[epoch_mod])[i]->load() != 0) return false;
//         }
//         return true;
//     }
//     static uint64_t GetLocalTxnCounters(uint64_t epoch_mod){
//         uint64_t ans = 0;
//         epoch_mod %= max_length;
//         for(int i = 0; i < (int)pack_num; i ++){
//             ans += (*local_txn_counters[epoch_mod])[i]->load();
//         }
//         return ans;
//     }

//     static void SetLocalTxnExcCounters(uint64_t epoch_mod, uint64_t index, uint64_t value){ (*local_txn_exc_counters[epoch_mod % max_length])[index]->store(value);}
//     static uint64_t IncLocalTxnExcCounters(uint64_t epoch_mod, uint64_t index){ return (*local_txn_exc_counters[epoch_mod % max_length])[index]->fetch_add(1);}
//     static uint64_t DecLocalTxnExcCounters(uint64_t epoch_mod, uint64_t index){ return (*local_txn_exc_counters[epoch_mod % max_length])[index]->fetch_sub(1);}
//     static uint64_t GetLocalTxnExcCounters(uint64_t epoch_mod){
//         uint64_t ans = 0;
//         epoch_mod %= max_length;
//         for(int i = 0; i < (int)pack_num; i ++) ans += (*local_txn_exc_counters[epoch_mod])[i]->load();
//         return ans;
//     }

//     static void SetRecordCommitTxnCounters(uint64_t epoch_mod, uint64_t index, uint64_t value){ (*record_commit_txn_counters[epoch_mod % max_length])[index]->store(value);}
//     static uint64_t IncRecordCommitTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*record_commit_txn_counters[epoch_mod % max_length])[index]->fetch_add(1);}
//     static uint64_t GetRecordCommitTxnCounters(uint64_t epoch_mod){ 
//         uint64_t ans = 0;
//         epoch_mod %= max_length;
//         for(int i = 0; i < (int)pack_num; i ++) ans += (*record_commit_txn_counters[epoch_mod])[i]->load();
//         return ans;
//     }

//     static void SetRecordCommittedTxnCounters(uint64_t epoch_mod, uint64_t index, uint64_t value){ (*record_committed_txn_counters[epoch_mod % max_length])[index]->store(value);}
//     static uint64_t IncRecordCommittedTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*record_committed_txn_counters[epoch_mod % max_length])[index]->fetch_add(1);}
//     static uint64_t GetRecordCommittedTxnCounters(uint64_t epoch_mod){ 
//         uint64_t ans = 0;
//         epoch_mod %= max_length;
//         for(int i = 0; i < (int)pack_num; i ++) ans += (*record_committed_txn_counters[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t IncLocalTxnIndex(uint64_t epoch_mod, uint64_t index){ return (*local_txn_index[epoch_mod % max_length])[index]->fetch_add(1);}

//     static void SetRemoteMergedTxnCounters(uint64_t epoch_mod, uint64_t index, uint64_t value){ (*remote_merged_txn_counters[epoch_mod % max_length])[index]->store(value);}
//     static uint64_t IncRemoteMergedTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*remote_merged_txn_counters[epoch_mod % max_length])[index]->fetch_add(1);}
//     static uint64_t GetRemoteMergedTxnCounters(uint64_t epoch_mod){ 
//         uint64_t ans = 0;
//         epoch_mod %= max_length;
//         for(int i = 0; i < (int)pack_num; i ++) ans += (*remote_merged_txn_counters[epoch_mod])[i]->load();
//         return ans;
//     }

//     static void SetRemoteCommitTxnCounters(uint64_t epoch_mod, uint64_t index, uint64_t value){ (*remote_commit_txn_counters[epoch_mod % max_length])[index]->store(value);}
//     static uint64_t IncRemoteCommitTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*remote_commit_txn_counters[epoch_mod % max_length])[index]->fetch_add(1);}
//     static uint64_t GetRemoteCommitTxnCounters(uint64_t epoch_mod){ 
//         uint64_t ans = 0;
//         epoch_mod %= max_length;
//         for(int i = 0; i < (int)pack_num; i ++) ans += (*remote_commit_txn_counters[epoch_mod])[i]->load();
//         return ans;
//     }

//     static void SetRemoteCommittedTxnCounters(uint64_t epoch_mod, uint64_t index, uint64_t value){ (*remote_committed_txn_counters[epoch_mod % max_length])[index]->store(value);}
//     static uint64_t IncRemoteCommittedTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*remote_committed_txn_counters[epoch_mod % max_length])[index]->fetch_add(1);}
//     static uint64_t GetRemoteCommittedTxnCounters(uint64_t epoch_mod){ 
//         uint64_t ans = 0;
//         epoch_mod %= max_length;
//         for(int i = 0; i < (int)pack_num; i ++) ans += (*remote_committed_txn_counters[epoch_mod])[i]->load();
//         return ans;
//     }

//     static void LogicalEndEpoch(uint64_t epoch_mod){
//         epoch_mod = (logical_epoch + 1) %  max_length;
//         // local_txn_counters[epoch_mod] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
//         // local_txn_exc_counters[epoch_mod] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
//         // local_txn_index[epoch_mod] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
//         // record_commit_txn_counters[epoch_mod] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
//         // record_committed_txn_counters[epoch_mod] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
//         // remote_merged_txn_counters[epoch_mod] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
//         // remote_commit_txn_counters[epoch_mod] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
//         // remote_committed_txn_counters[epoch_mod] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        
//         // local_txn_counters[epoch_mod]->resize(pack_num + 2);
//         // local_txn_exc_counters[epoch_mod]->resize(pack_num + 2);
//         // local_txn_index[epoch_mod]->resize(pack_num + 2);
//         // record_commit_txn_counters[epoch_mod]->resize(pack_num + 2);
//         // record_committed_txn_counters[epoch_mod]->resize(pack_num + 2);
//         // remote_merged_txn_counters[epoch_mod]->resize(pack_num + 2);
//         // remote_commit_txn_counters[epoch_mod]->resize(pack_num + 2);
//         // remote_committed_txn_counters[epoch_mod]->resize(pack_num + 2);
//         // for(int j = 0; j <= (int)pack_num; j++){
//         //     (*local_txn_counters[epoch_mod])[j] = std::make_shared<std::atomic<uint64_t>>(0);
//         //     (*local_txn_exc_counters[epoch_mod])[j] = std::make_shared<std::atomic<uint64_t>>(0);
//         //     (*local_txn_index[epoch_mod])[j] = std::make_shared<std::atomic<uint64_t>>(0);
//         //     (*record_commit_txn_counters[epoch_mod])[j] = std::make_shared<std::atomic<uint64_t>>(0);
//         //     (*record_committed_txn_counters[epoch_mod])[j] = std::make_shared<std::atomic<uint64_t>>(0);
//         //     (*remote_merged_txn_counters[epoch_mod])[j] = std::make_shared<std::atomic<uint64_t>>(0);
//         //     (*remote_commit_txn_counters[epoch_mod])[j] = std::make_shared<std::atomic<uint64_t>>(0);
//         //     (*remote_committed_txn_counters[epoch_mod])[j] = std::make_shared<std::atomic<uint64_t>>(0);
//         // }
//         for(int i = 0; i <= (int)pack_num; i++){
//             (*local_txn_counters[epoch_mod])[i]->store(0);
//             (*local_txn_exc_counters[epoch_mod])[i]->store(0);
//             (*local_txn_index[epoch_mod])[i]->store(1);
//             (*remote_merged_txn_counters[epoch_mod])[i]->store(0);
//             (*remote_commit_txn_counters[epoch_mod])[i]->store(0);
//             (*remote_committed_txn_counters[epoch_mod])[i]->store(0);
//         }
//         remote_row_ptr_map.clear();
//         insertSet.clear();
//         insertSetForCommit.clear();
//         abort_transcation_csn_set.clear();
//         remote_execed = false;
//         ++ logical_epoch;
//     }
    
//     static bool InsertTxntoLocalChangeSet(MOT::TxnManager* txMan, const uint64_t& index_pack, const uint64_t& index_unique);
//     static bool TryIncLocalChangeSetNum(uint64_t epoch, uint64_t index_pack, uint64_t value);
//     static bool IncLocalChangeSetNum(uint64_t epoch, uint64_t index_pack, uint64_t value);
//     static bool DecLocalChangeSetNum(uint64_t epoch, uint64_t index_pack, uint64_t value);
//     static bool InsertRowToSet(MOT::TxnManager* txMan, void* txn_void, const uint64_t& index_pack, const uint64_t& index_unique);
//     static bool InsertTxnIntoRecordCommitQueue(MOT::TxnManager* txMan, void* txn_void, MOT::RC &rc);
//     static void LocalTxnSafeExit(const uint64_t& index_pack, void* txn_void);
//     static void Output(std::string v);
//     static void Merge(MOT::TxnManager* txMan, uint64_t& index_pack);
//     static void Commit(MOT::TxnManager* txMan, uint64_t& index_pack);

// };

// inline MOT::TxnManager* GetSafeTxn(const char* callerSrc, ::TransactionId txn_id = 0)
// {
//     if (!u_sess->mot_cxt.txn_manager) {
//         MOTAdaptor::InitTxnManager(callerSrc);
//         if (u_sess->mot_cxt.txn_manager != nullptr) {
//             if (txn_id != 0) {
//                 u_sess->mot_cxt.txn_manager->SetTransactionId(txn_id);
//             }
//         } else {
//             report_pg_error(MOT_GET_ROOT_ERROR_RC());
//         }
//     }
//     return u_sess->mot_cxt.txn_manager;
// }

// extern void EnsureSafeThreadAccess();

// inline List* BitmapSerialize(List* result, uint8_t* bitmap, int16_t len)
// {
//     // set list type to FDW_LIST_BITMAP
//     result = lappend(result, makeConst(INT4OID, -1, InvalidOid, 4, FDW_LIST_BITMAP, false, true));
//     for (int i = 0; i < len; i++)
//         result = lappend(result, makeConst(INT1OID, -1, InvalidOid, 1, Int8GetDatum(bitmap[i]), false, true));

//     return result;
// }

// inline void BitmapDeSerialize(uint8_t* bitmap, int16_t len, ListCell** cell)
// {
//     if (cell != nullptr && *cell != nullptr) {
//         int type = ((Const*)lfirst(*cell))->constvalue;
//         if (type == FDW_LIST_BITMAP) {
//             *cell = lnext(*cell);
//             for (int i = 0; i < len; i++) {
//                 bitmap[i] = (uint8_t)((Const*)lfirst(*cell))->constvalue;
//                 *cell = lnext(*cell);
//             }
//         }
//     }
// }

// inline void CleanCursors(MOTFdwStateSt* state)
// {
//     for (int i = 0; i < 2; i++) {
//         if (state->m_cursor[i]) {
//             state->m_cursor[i]->Invalidate();
//             state->m_cursor[i]->Destroy();
//             delete state->m_cursor[i];
//             state->m_cursor[i] = NULL;
//         }
//     }
// }

// inline void CleanQueryStatesOnError(MOT::TxnManager* txn)
// {
//     if (txn != nullptr) {
//         for (auto& itr : txn->m_queryState) {
//             MOTFdwStateSt* state = (MOTFdwStateSt*)itr.second;
//             if (state != nullptr) {
//                 CleanCursors(state);
//             }
//         }
//         txn->m_queryState.clear();
//     }
// }

// MOTFdwStateSt* InitializeFdwState(void* fdwState, List** fdwExpr, uint64_t exTableID);
// void* SerializeFdwState(MOTFdwStateSt* state);
// void ReleaseFdwState(MOTFdwStateSt* state);









// template<typename T>
// using BlockingConcurrentQueue =  moodycamel::BlockingConcurrentQueue<T>;
// // using BlockingConcurrentQueue = BlockingMPMCQueue<T>;
// // struct pack_params;
// // struct commit_thread_params;

// struct pack_thread_params {
//     uint64_t index;
//     uint64_t current_epoch;
//     std::shared_ptr<std::vector<std::shared_ptr<BlockingConcurrentQueue<std::string*>>>> local_change_set_txn_ptr_temp;
//     std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>> local_change_set_txn_num_ptr_temp;
//     pack_thread_params(uint64_t index, uint64_t ce, 
//         std::shared_ptr<std::vector<std::shared_ptr<BlockingConcurrentQueue<std::string*>>>> ptr1, 
//         std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>> ptr2): 
//         index(index), current_epoch(ce), local_change_set_txn_ptr_temp(ptr1), local_change_set_txn_num_ptr_temp(ptr2){}
//     pack_thread_params(){}
// };

// struct send_thread_params {
//     uint64_t current_epoch;
//     uint64_t tot;
//     std::string* merge_request_ptr;
//     send_thread_params(uint64_t ce, uint64_t tot_temp, std::string* ptr1):
//         current_epoch(ce), tot(tot_temp), merge_request_ptr(ptr1){}
//     send_thread_params(){}
// };


// class LocalWriteSet {
// public:
//     static uint64_t _max_length, _pack_num;
//     //当前epoch放入多少个txn
//     static std::vector<std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>> txn_num_ptrs;
//     //pack_thread取出了多少个 两者比较来判断是否发送epoch_end message
//     static std::vector<std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>> packd_txn_num_ptrs, write_abort_before_send_txn_num, 
//         write_abort_after_send_txn_num, total_abort_txn_num, read_abort_txn_num, read_committed_txn_num, write_committed_txn_num, 
//         read_total_txn_num, write_total_txn_num, pack_txn_num;

//     static void Init(uint64_t pack_num, uint64_t length);
    
    
//     static void PushBack(uint64_t epoch, std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>> ptr2, 
//         std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>> ptr3) {
//         auto epoch_mod = epoch % _max_length;
//         txn_num_ptrs[epoch_mod] = ptr2;
//         packd_txn_num_ptrs[epoch_mod] = ptr3;
//     }
    
//     static uint64_t LoadChangeSet(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*txn_num_ptrs[epoch_mod])[i]->load();
//         return ans;
//     }


//     static uint64_t LoadTotalAbortTxnNum(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*total_abort_txn_num[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t LoadWriteAbortBeforeSendTxnNum(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*write_abort_before_send_txn_num[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t LoadWriteAbortAfterSendTxnNum(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*write_abort_after_send_txn_num[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t LoadWriteCommittedTxnNum(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*write_committed_txn_num[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t LoadReadAbortTxnNum(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*read_abort_txn_num[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t LoadReadCommittedTxnNum(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*read_committed_txn_num[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t LoadReadTotalTxnNum(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*read_total_txn_num[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t LoadPackTxnNum(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*pack_txn_num[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t LoadWriteTotalTxnNum(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*write_total_txn_num[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t LoadPackedTxnNum(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*packd_txn_num_ptrs[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t LoadShouldSendTxnNum(uint64_t epoch) {
//         if(is_sync_exec) {
//             // if(LoadChangeSet(epoch) < LoadReadCommittedTxnNum(epoch) - (LoadTotalAbortTxnNum(epoch) - LoadWriteAbortAfterSendTxnNum(epoch))) {
//             //     return LoadPackedTxnNum(epoch);
//             // }
//             return LoadChangeSet(epoch) - LoadReadCommittedTxnNum(epoch) - 
//                 (LoadTotalAbortTxnNum(epoch) - LoadWriteAbortAfterSendTxnNum(epoch)) ;
//         }
//         else 
//             return LoadChangeSet(epoch);
//     }
//     static uint64_t LoadShouldExecTxnNum(uint64_t epoch) {// 只在 is_sync_exec中使用
//         // if(LoadChangeSet(epoch) < LoadReadCommittedTxnNum(epoch) - (LoadTotalAbortTxnNum(epoch) - LoadWriteAbortAfterSendTxnNum(epoch))) {
//         //     return MOTAdaptor::GetLocalTxnExcCounters();
//         // }
//         return LoadChangeSet(epoch) - LoadReadCommittedTxnNum(epoch) - 
//             (LoadTotalAbortTxnNum(epoch) - LoadWriteAbortAfterSendTxnNum(epoch));
//     }

//     static bool IsCurrentEpochFinished(uint64_t epoch) {
//         return ((LoadPackedTxnNum(epoch) >= LoadShouldSendTxnNum(epoch)) &&  epoch < MOTAdaptor::GetPhysicalEpoch());
//     }

//     static bool TryAddNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
//         (*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_add(value);
//         return true;
//         // if((*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_add(value) == 0
//         //     && epoch_num < MOTAdaptor::GetPhysicalEpoch() ){
//         //         (*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_sub(value);
//         //         return false;
//         //     }
//         // else {
//         //     // MOT_LOG_INFO("txn add num %llu %llu %llu", epoch_num, LoadChangeSet(epoch_num));
//         //     return true;
//         // }
//     }

//     static bool AddNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
//         (*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_add(value);
//         return true;
//     }

//     static bool SubNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
//         // MOT_LOG_INFO("txn sub num %llu %llu %llu", epoch_num, LoadChangeSet(epoch_num), value);
//         (*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_sub(value);
//         return true;
//     }

//     static bool AddPackedNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
//         (*(packd_txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_add(value);
//         return true;
//     }

//     static bool SubPackedNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
//         (*(packd_txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_sub(value);
//         return true;
//     }

// };

// class RemoteCache {
// public:    
//     static uint64_t _max_length, _pack_num;
//     static std::unique_ptr<std::atomic<uint64_t>> should_receive_pack_num, online_server_num;
//     static std::vector<std::unique_ptr<std::atomic<uint64_t>>> is_server_online;
//     static std::vector<std::vector<std::unique_ptr<std::atomic<uint64_t>>>> 
//         received_pack_num, received_txn_num, should_receive_txn_num;
//     static std::vector<std::unique_ptr<std::atomic<uint64_t>>> 
//         received_total_pack_num, received_total_txn_num;

//     static void Init(uint64_t pack_num, uint64_t length);
//         // received_total_pack_num->fetch_add(value);

    
//     static uint64_t AddShouldReceiveTxnNum(uint64_t epoch, uint64_t index, uint64_t value) {
//         return should_receive_txn_num[epoch % _max_length][index]->fetch_add(value);
//     }
//     static void StoreShouldReceiveTxnNum(uint64_t epoch, uint64_t index, uint64_t value) {
//         should_receive_txn_num[epoch % _max_length][index]->store(value);
//     }
//     static uint64_t GetShouldReceiveTxnNum(uint64_t epoch, uint64_t index) {
//         return should_receive_txn_num[epoch % _max_length][index]->load();
//     }
//     static uint64_t GetShouldReceiveTxnNum(uint64_t epoch) {
//         epoch %= _max_length;
//         uint64_t ans = 0;
//         for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
//             if(received_pack_num[epoch][i]->load() == 1)
//                 ans += should_receive_txn_num[epoch][i]->load();
//         }
//         return ans;
//     }

//     static uint64_t AddReceivedPackNum(uint64_t epoch, uint64_t index, uint64_t value) {
//         return received_pack_num[epoch % _max_length][index]->fetch_add(value);
//     }
//     static void StoreReceivedPackNum(uint64_t epoch, uint64_t index, uint64_t value) {
//         received_pack_num[epoch % _max_length][index]->store(value);
//     }
//     static uint64_t GetReceivedPackNum(uint64_t epoch, uint64_t index) {
//         return received_pack_num[epoch % _max_length][index]->load();
//     }
//     static uint64_t GetReceivedPackNum(uint64_t epoch) {
//         epoch %= _max_length;
//         uint64_t ans = 0;
//         for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
//             if(received_pack_num[epoch][i]->load() == 1)
//                 ans ++;
//         }
//         return ans;
//     }

//     static uint64_t AddReceivedTxnNum(uint64_t epoch, uint64_t index, uint64_t value) {
//         return received_txn_num[epoch % _max_length][index]->fetch_add(value);
//     }
//     static void StoreReceivedTxnNum(uint64_t epoch, uint64_t index, uint64_t value) {
//         received_txn_num[epoch % _max_length][index]->store(value);
//     }
//     static uint64_t GetReceivedTxnNum(uint64_t epoch, uint64_t index) {
//         return received_txn_num[epoch % _max_length][index]->load();
//     }
//     static uint64_t GetReceivedTxnNum(uint64_t epoch) {
//         epoch %= _max_length;
//         uint64_t ans = 0;
//         for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
//             if(received_pack_num[epoch][i]->load() == 1)
//                 ans += received_txn_num[epoch][i]->load();
//         }
//         return ans;
//     }

//     static uint64_t AddReceivedPackNumTotal(uint64_t epoch, uint64_t value) {
//         return received_total_pack_num[epoch % _max_length]->fetch_add(value);
//     }
//     static uint64_t GetReceivedPackNumTotal(uint64_t epoch) {
//         return received_total_pack_num[epoch % _max_length]->load();
//     }
//     static uint64_t AddReceivedTxnNumTotal(uint64_t epoch, uint64_t value) {
//         return received_total_txn_num[epoch % _max_length]->fetch_add(value);
//     }
//     static uint64_t GetReceivedTxnNumTotal(uint64_t epoch) {
//         return received_total_txn_num[epoch % _max_length]->load();
//     }

//     static uint64_t AddShouldReceivePackNum(uint64_t value) {
//         return should_receive_pack_num->fetch_add(value);
//     }
//     static uint64_t SubShouldReceivePackNum(uint64_t value) {
//         return should_receive_pack_num->fetch_sub(value);
//     }
//     static void StoreShouldReceivePackNum(uint64_t value) {
//         should_receive_pack_num->store(value);
//     }
//     static uint64_t GetShouldReceivePackNum() {
//         return should_receive_pack_num->load();
//     }

//     static uint64_t AddOnLineServerNum(uint64_t value) {
//         return online_server_num->fetch_add(value);
//     }
//     static uint64_t SubOnLineServerNum(uint64_t value) {
//         return online_server_num->fetch_sub(value);
//     }
//     static void StoreOnLineServerNum(uint64_t value) {
//         online_server_num->store(value);
//     }
//     static uint64_t GetOnLineServerNum() {
//         return online_server_num->load();
//     }

    
//     static void Clear(uint64_t epoch) {
//         epoch %= _max_length;
//         for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
//             received_pack_num[epoch][i]->store(0);
//             received_txn_num[epoch][i]->store(0);
//             should_receive_txn_num[epoch][i]->store(0);
//         }
//         received_total_pack_num[epoch]->store(0);
//         received_total_txn_num[epoch]->store(0);
//     }

//     static void SetServerOnLine(std::string ip) {
//         for(int i = 0; i < (int)kServerIp.size(); i++) {
//             if(ip == kServerIp[i]) {
//                 is_server_online[i]->store(1);
//             }
//         }
//     }

//     static void SetServerOffLine(std::string ip) {
//         for(int i = 0; i < (int)kServerIp.size(); i++) {
//             if(ip == kServerIp[i]) {
//                 is_server_online[i]->store(0);
//             }
//         }
//     }

//     static void SetServerOnLine(uint64_t index) {
//         is_server_online[index]->store(1);
//     }

//     static void SetServerOffLine(uint64_t index) {
//         is_server_online[index]->store(0);
//     }

//     static bool IsServerOnLine(uint64_t index) {
//         return (is_server_online[index]->load() == 1);
//     }


    

//     static void ReceiveAMessage() {

//     }

// };

// class RegionCacheServerState{
// public:
//     static uint64_t _max_length;
//     static std::vector<std::unique_ptr<std::atomic<uint64_t>>>  received_epoch;

//     static void Init(uint64_t pack_num, uint64_t length);
    
//     static bool IsCacheServerStored(uint64_t epoch) {
//         if(received_epoch[epoch % _max_length]->load() == static_cast<uint64_t>(1)) {
//             return true;
//         }
//         else {
//             return false;
//         }
//     }

//     static void SetCacheServerStored(uint64_t epoch, uint64_t value) {
//         received_epoch[epoch % _max_length]->store(value);
//     }

// };


// #endif  // MOT_INTERNAL_H















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
#include "neu_concurrency_tools/readerwriterqueue.h"
#include <vector>
#include <typeinfo>
// #include <semaphore.h>

using std::map;
using std::string;

#define MAX_TXN_NUM 100000
#define ADD_NUM 10000000001
#define MOD_NUM 10000000000

extern void EpochLogicalTimerManagerThreadMain(uint64_t id);
extern void EpochPhysicalTimerManagerThreadMain(uint64_t id);
extern void EpochMessageManagerThreadMain(uint64_t id);
extern void EpochMessageCacheManagerThreadMain(uint64_t id);

extern void EpochNotifyThreadMain(uint64_t id);
extern void EpochPackThreadMain(uint64_t id);
extern void EpochSendThreadMain(uint64_t id);

extern void EpochListenThreadMain(uint64_t id);
extern void EpochUnseriThreadMain(uint64_t id);
extern void EpochUnpackThreadMain(uint64_t id);
extern void EpochMergeThreadMain(uint64_t id);
extern void EpochCommitThreadMain(uint64_t id);
extern void EpochRecordCommitThreadMain(uint64_t id);

extern void EpochMessageSendThreadMain(uint64_t id);
extern void EpochMessageListenThreadMain(uint64_t id);

namespace aum {
    class SpinLock {
        public:
        // constructors
        SpinLock() = default;

        SpinLock(const SpinLock &) = delete;            // non construction-copyable
        SpinLock &operator=(const SpinLock &) = delete; // non copyable

        // Modifiers
        void lock() {
            while (lock_.test_and_set());
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

        bool insert(key &k, value &v, pointer *ptr, std::function<bool(const value &, const value &)> 
            cmp = [](const value& o, const value& n){return n < o;}) //为insert服务
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
            if (iter != map.end() && iter->second == v) {
                //if the abort txn has insert row and has not been modifid by ohters 
                //then remove it from map;or keep it 
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

        bool insert(key &k, value &v, pointer *ptr, std::function<bool(const value &, const value &)> 
            cmp = [](const value& o, const value& n){return n < o;}) //为insert服务
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
                if (iter->second == v) {
                    //if the abort txn has insert row and has not been modifid by ohters 
                    //then remove it from map;or keep it 
                    _map_temp.erase(iter);
                } 
            }
            lock.unlock();
        }

        void remove(key &k) 
        {
            std::mutex& _mutex_temp = GetMutexRef(k);
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(_mutex_temp);
            map_iterator iter = _map_temp.find(k);
            if (iter != _map_temp.end()) {
                _map_temp.erase(iter);
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
            map_iterator iter = _map_temp.find(k);
            if(iter != _map_temp.end()){
                if(iter->second == v){
                    return true;
                }
            }
            return false;
        }

        bool contain(key &k){
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            map_iterator iter = _map_temp.find(k);
            if(iter != _map_temp.end()){
                return true;
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

protected:
        inline std::unordered_map<key, value>& GetMapRef(const key k){ return _map[(_hash(k) % _N)]; }
        inline std::unordered_map<key, value>& GetMapRef(const key k) const { return _map[(_hash(k) % _N)]; }
        inline std::mutex& GetMutexRef(const key k) { return _mutex[(_hash(k) % _N)]; }
        inline std::mutex& GetMutexRef(const key k) const {return _mutex[(_hash(k) % _N)]; }

    private:
        const static uint64_t _N = 997;//1217 12281 122777 prime
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
    static volatile bool remote_execed, record_committed, remote_record_committed, is_current_epoch_abort;
    static volatile uint64_t logical_epoch;
    static volatile uint64_t physical_epoch;

public:
    static uint64_t max_length, pack_num;
    static std::vector<std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>> local_txn_counters, local_txn_exc_counters,
        local_txn_index,
        record_commit_txn_counters, record_committed_txn_counters, remote_merged_txn_counters, remote_commit_txn_counters, 
        remote_committed_txn_counters;
    static std::map<uint64_t, std::unique_ptr<std::vector<MOT::Row*>>> remote_row_ptr_map;
    static aum::concurrent_unordered_map<std::string, std::string, std::string> insertSet;
    static aum::concurrent_unordered_map<std::string, std::string, std::string> insertSetForCommit;
    static aum::concurrent_unordered_map<std::string, std::string, std::string> abort_transcation_csn_set;


    static void SetTimerStop(bool value) {timerStop = value;}
    static bool IsTimerStop() {return timerStop;}
    
    static bool IsRemoteExeced() {return remote_execed;}
    static void SetRemoteExeced(bool value) {remote_execed = value;}
    
    static bool IsRecordCommitted(){ return record_committed;}
    static void SetRecordCommitted(bool value){ record_committed = value;}
    
    static bool IsRemoteRecordCommitted(){ return remote_record_committed;}
    static void SetRemoteRecordCommitted(bool value){ remote_record_committed = value;}

    static bool IsCurrentEpochAbort(){ return is_current_epoch_abort;}
    static void SetCurrentEpochAbort(bool value){ is_current_epoch_abort = value;}
    
    static void SetPhysicalEpoch(int value){ physical_epoch = value;}
    static uint64_t AddPhysicalEpoch(){ return ++ physical_epoch;}
    static uint64_t GetPhysicalEpoch(){ return physical_epoch;}

    static void SetLogicalEpoch(int value){ logical_epoch = value;}
    static uint64_t AddLogicalEpoch(){ return ++ logical_epoch;}
    static uint64_t GetLogicalEpoch(){ return logical_epoch;}

    static uint64_t IncLocalTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*local_txn_counters[epoch_mod % max_length])[index]->fetch_add(ADD_NUM);}
    static uint64_t DecLocalTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*local_txn_counters[epoch_mod % max_length])[index]->fetch_sub(ADD_NUM);}
    static uint64_t DecExeCounters(uint64_t epoch_mod, uint64_t index){ return (*local_txn_counters[epoch_mod % max_length])[index]->fetch_sub(1);}
    static uint64_t DecComCounters(uint64_t epoch_mod, uint64_t index){ return (*local_txn_counters[epoch_mod % max_length])[index]->fetch_sub(MOD_NUM);}
    static bool IsLocalTxnCountersExcEqualZero(uint64_t epoch_mod){
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++){
            if( (*local_txn_counters[epoch_mod])[i]->load() % static_cast<uint64_t>(1e10) != 0) return false;
        }
        return true;
    }
    static bool IsLocalTxnCountersComEqualZero(uint64_t epoch_mod){
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++){
            if( (*local_txn_counters[epoch_mod])[i]->load() != 0) return false;
        }
        return true;
    }
    static uint64_t GetLocalTxnCounters(uint64_t epoch_mod){
        uint64_t ans = 0;
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++){
            ans += (*local_txn_counters[epoch_mod])[i]->load();
        }
        return ans;
    }

    static void SetLocalTxnExcCounters(uint64_t epoch_mod, uint64_t index, uint64_t value){ (*local_txn_exc_counters[epoch_mod % max_length])[index]->store(value);}
    static uint64_t IncLocalTxnExcCounters(uint64_t epoch_mod, uint64_t index){ return (*local_txn_exc_counters[epoch_mod % max_length])[index]->fetch_add(1);}
    static uint64_t DecLocalTxnExcCounters(uint64_t epoch_mod, uint64_t index){ return (*local_txn_exc_counters[epoch_mod % max_length])[index]->fetch_sub(1);}
    static uint64_t GetLocalTxnExcCounters(uint64_t epoch_mod){
        uint64_t ans = 0;
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++) ans += (*local_txn_exc_counters[epoch_mod])[i]->load();
        return ans;
    }

    static void SetRecordCommitTxnCounters(uint64_t epoch_mod, uint64_t index, uint64_t value){ (*record_commit_txn_counters[epoch_mod % max_length])[index]->store(value);}
    static uint64_t IncRecordCommitTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*record_commit_txn_counters[epoch_mod % max_length])[index]->fetch_add(1);}
    static uint64_t GetRecordCommitTxnCounters(uint64_t epoch_mod){ 
        uint64_t ans = 0;
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++) ans += (*record_commit_txn_counters[epoch_mod])[i]->load();
        return ans;
    }

    static void SetRecordCommittedTxnCounters(uint64_t epoch_mod, uint64_t index, uint64_t value){ (*record_committed_txn_counters[epoch_mod % max_length])[index]->store(value);}
    static uint64_t IncRecordCommittedTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*record_committed_txn_counters[epoch_mod % max_length])[index]->fetch_add(1);}
    static uint64_t GetRecordCommittedTxnCounters(uint64_t epoch_mod){ 
        uint64_t ans = 0;
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++) ans += (*record_committed_txn_counters[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t IncLocalTxnIndex(uint64_t epoch_mod, uint64_t index){ return (*local_txn_index[epoch_mod % max_length])[index]->fetch_add(1);}

    static void SetRemoteMergedTxnCounters(uint64_t epoch_mod, uint64_t index, uint64_t value){ (*remote_merged_txn_counters[epoch_mod % max_length])[index]->store(value);}
    static uint64_t IncRemoteMergedTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*remote_merged_txn_counters[epoch_mod % max_length])[index]->fetch_add(1);}
    static uint64_t GetRemoteMergedTxnCounters(uint64_t epoch_mod){ 
        uint64_t ans = 0;
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++) ans += (*remote_merged_txn_counters[epoch_mod])[i]->load();
        return ans;
    }

    static void SetRemoteCommitTxnCounters(uint64_t epoch_mod, uint64_t index, uint64_t value){ (*remote_commit_txn_counters[epoch_mod % max_length])[index]->store(value);}
    static uint64_t IncRemoteCommitTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*remote_commit_txn_counters[epoch_mod % max_length])[index]->fetch_add(1);}
    static uint64_t GetRemoteCommitTxnCounters(uint64_t epoch_mod){ 
        uint64_t ans = 0;
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++) ans += (*remote_commit_txn_counters[epoch_mod])[i]->load();
        return ans;
    }

    static void SetRemoteCommittedTxnCounters(uint64_t epoch_mod, uint64_t index, uint64_t value){ (*remote_committed_txn_counters[epoch_mod % max_length])[index]->store(value);}
    static uint64_t IncRemoteCommittedTxnCounters(uint64_t epoch_mod, uint64_t index){ return (*remote_committed_txn_counters[epoch_mod % max_length])[index]->fetch_add(1);}
    static uint64_t GetRemoteCommittedTxnCounters(uint64_t epoch_mod){ 
        uint64_t ans = 0;
        epoch_mod %= max_length;
        for(int i = 0; i < (int)pack_num; i ++) ans += (*remote_committed_txn_counters[epoch_mod])[i]->load();
        return ans;
    }



    static uint64_t _max_length, _pack_num;
    static std::vector<std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>> txn_num_ptrs,
        packd_txn_num_ptrs, write_abort_before_send_txn_num, 
        write_abort_after_send_txn_num, total_abort_txn_num, read_abort_txn_num, read_committed_txn_num, write_committed_txn_num, 
        read_total_txn_num, write_total_txn_num, pack_txn_num;

    static void Init(uint64_t pack_num, uint64_t length);
    
    
    static void PushBack(uint64_t epoch, std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>> ptr2, 
        std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>> ptr3) {
        auto epoch_mod = epoch % _max_length;
        txn_num_ptrs[epoch_mod] = ptr2;
        packd_txn_num_ptrs[epoch_mod] = ptr3;
    }
    
    static uint64_t LoadChangeSet(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*txn_num_ptrs[epoch_mod])[i]->load();
        return ans;
    }


    static uint64_t LoadTotalAbortTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*total_abort_txn_num[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadWriteAbortBeforeSendTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*write_abort_before_send_txn_num[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadWriteAbortAfterSendTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*write_abort_after_send_txn_num[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadWriteCommittedTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*write_committed_txn_num[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadReadAbortTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*read_abort_txn_num[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadReadCommittedTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*read_committed_txn_num[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadReadTotalTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*read_total_txn_num[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadPackTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*pack_txn_num[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadWriteTotalTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*write_total_txn_num[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadPackedTxnNum(uint64_t epoch) {
        uint64_t ans = 0, epoch_mod = epoch % _max_length;
        for(int i = 0; i < (int)_pack_num; i ++)
            ans += (*packd_txn_num_ptrs[epoch_mod])[i]->load();
        return ans;
    }

    static uint64_t LoadShouldSendTxnNum(uint64_t epoch) {
        if(is_sync_exec) {
            // if(LoadChangeSet(epoch) < LoadReadCommittedTxnNum(epoch) - (LoadTotalAbortTxnNum(epoch) - LoadWriteAbortAfterSendTxnNum(epoch))) {
            //     return LoadPackedTxnNum(epoch);
            // }
            return LoadChangeSet(epoch) - LoadReadCommittedTxnNum(epoch) - 
                (LoadTotalAbortTxnNum(epoch) - LoadWriteAbortAfterSendTxnNum(epoch)) ;
        }
        else 
            return LoadChangeSet(epoch);
    }
    static uint64_t LoadShouldExecTxnNum(uint64_t epoch) {// 只在 is_sync_exec中使用
        // if(LoadChangeSet(epoch) < LoadReadCommittedTxnNum(epoch) - (LoadTotalAbortTxnNum(epoch) - LoadWriteAbortAfterSendTxnNum(epoch))) {
        //     return MOTAdaptor::GetLocalTxnExcCounters();
        // }
        return LoadChangeSet(epoch) - LoadReadCommittedTxnNum(epoch) - 
            (LoadTotalAbortTxnNum(epoch) - LoadWriteAbortAfterSendTxnNum(epoch));
    }

    static bool IsCurrentEpochFinished(uint64_t epoch) {
        return ((LoadPackedTxnNum(epoch) >= LoadShouldSendTxnNum(epoch)) &&  epoch < MOTAdaptor::GetPhysicalEpoch());
    }

    static bool TryAddNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
        (*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_add(value);
        return true;
        // if((*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_add(value) == 0
        //     && epoch_num < MOTAdaptor::GetPhysicalEpoch() ){
        //         (*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_sub(value);
        //         return false;
        //     }
        // else {
        //     // MOT_LOG_INFO("txn add num %llu %llu %llu", epoch_num, LoadChangeSet(epoch_num));
        //     return true;
        // }
    }

    static bool AddNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
        (*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_add(value);
        return true;
    }

    static bool SubNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
        // MOT_LOG_INFO("txn sub num %llu %llu %llu", epoch_num, LoadChangeSet(epoch_num), value);
        (*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_sub(value);
        return true;
    }

    static bool AddPackedNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
        (*(packd_txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_add(value);
        return true;
    }

    static bool SubPackedNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
        (*(packd_txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_sub(value);
        return true;
    }

    static void ClearMergeEpochState() {
        remote_row_ptr_map.clear();
        insertSet.clear();
        insertSetForCommit.clear();
        abort_transcation_csn_set.clear();
        remote_execed = false;
    }

    static void ClearEpochState(uint64_t epoch_mod){
        epoch_mod %= max_length;
        // local_txn_counters[epoch_mod] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        // local_txn_exc_counters[epoch_mod] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        // local_txn_index[epoch_mod] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        // record_commit_txn_counters[epoch_mod] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        // record_committed_txn_counters[epoch_mod] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        // remote_merged_txn_counters[epoch_mod] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        // remote_commit_txn_counters[epoch_mod] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        // remote_committed_txn_counters[epoch_mod] = std::make_shared<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>();
        
        // local_txn_counters[epoch_mod]->resize(pack_num + 2);
        // local_txn_exc_counters[epoch_mod]->resize(pack_num + 2);
        // local_txn_index[epoch_mod]->resize(pack_num + 2);
        // record_commit_txn_counters[epoch_mod]->resize(pack_num + 2);
        // record_committed_txn_counters[epoch_mod]->resize(pack_num + 2);
        // remote_merged_txn_counters[epoch_mod]->resize(pack_num + 2);
        // remote_commit_txn_counters[epoch_mod]->resize(pack_num + 2);
        // remote_committed_txn_counters[epoch_mod]->resize(pack_num + 2);
        // for(int j = 0; j <= (int)pack_num; j++){
        //     (*local_txn_counters[epoch_mod])[j] = std::make_shared<std::atomic<uint64_t>>(0);
        //     (*local_txn_exc_counters[epoch_mod])[j] = std::make_shared<std::atomic<uint64_t>>(0);
        //     (*local_txn_index[epoch_mod])[j] = std::make_shared<std::atomic<uint64_t>>(0);
        //     (*record_commit_txn_counters[epoch_mod])[j] = std::make_shared<std::atomic<uint64_t>>(0);
        //     (*record_committed_txn_counters[epoch_mod])[j] = std::make_shared<std::atomic<uint64_t>>(0);
        //     (*remote_merged_txn_counters[epoch_mod])[j] = std::make_shared<std::atomic<uint64_t>>(0);
        //     (*remote_commit_txn_counters[epoch_mod])[j] = std::make_shared<std::atomic<uint64_t>>(0);
        //     (*remote_committed_txn_counters[epoch_mod])[j] = std::make_shared<std::atomic<uint64_t>>(0);
        // }
        for(int i = 0; i <= (int)pack_num; i++){
            (*local_txn_counters[epoch_mod])[i]->store(0);
            (*local_txn_exc_counters[epoch_mod])[i]->store(0);
            (*local_txn_index[epoch_mod])[i]->store(1);
            (*remote_merged_txn_counters[epoch_mod])[i]->store(0);
            (*remote_commit_txn_counters[epoch_mod])[i]->store(0);
            (*remote_committed_txn_counters[epoch_mod])[i]->store(0);

            (*txn_num_ptrs[epoch_mod])[i]->store(0);
            (*packd_txn_num_ptrs[epoch_mod])[i]->store(0);
            (*write_abort_before_send_txn_num[epoch_mod])[i]->store(0);
            (*write_abort_after_send_txn_num[epoch_mod])[i]->store(0);
            (*total_abort_txn_num[epoch_mod])[i]->store(0);
            (*read_abort_txn_num[epoch_mod])[i]->store(0);
            (*read_committed_txn_num[epoch_mod])[i]->store(0);
            (*write_committed_txn_num[epoch_mod])[i]->store(0);
            (*read_total_txn_num[epoch_mod])[i]->store(0);
            (*write_total_txn_num[epoch_mod])[i]->store(0);
            (*pack_txn_num[epoch_mod])[i]->store(0);

        }
    }
    
    static bool InsertTxntoLocalChangeSet(MOT::TxnManager* txMan, const uint64_t& index_pack, const uint64_t& index_unique);
    static bool TryIncLocalChangeSetNum(uint64_t epoch, uint64_t index_pack, uint64_t value);
    static bool IncLocalChangeSetNum(uint64_t epoch, uint64_t index_pack, uint64_t value);
    static bool DecLocalChangeSetNum(uint64_t epoch, uint64_t index_pack, uint64_t value);
    static bool InsertRowToSet(MOT::TxnManager* txMan, void* txn_void, const uint64_t& index_pack, const uint64_t& index_unique);
    static bool InsertTxnIntoRecordCommitQueue(MOT::TxnManager* txMan, void* txn_void, MOT::RC &rc);
    static void LocalTxnSafeExit(const uint64_t& index_pack, void* txn_void);
    static void Output(std::string v);
    static void Merge(MOT::TxnManager* txMan, uint64_t& index_pack);
    static void Commit(MOT::TxnManager* txMan, uint64_t& index_pack);
    static void EnqueueEmpty(uint64_t index);

    
    
    static std::unique_ptr<std::atomic<uint64_t>> should_receive_pack_num, online_server_num;
    static std::vector<std::unique_ptr<std::atomic<uint64_t>>> is_server_online;
    static std::vector<std::vector<std::unique_ptr<std::atomic<uint64_t>>>> 
        received_pack_num, received_txn_num, should_receive_txn_num;
    static std::vector<std::unique_ptr<std::atomic<uint64_t>>> 
        received_total_pack_num, received_total_txn_num;

    static uint64_t AddShouldReceiveTxnNum(uint64_t epoch, uint64_t index, uint64_t value) {
        return should_receive_txn_num[epoch % _max_length][index]->fetch_add(value);
    }
    static void StoreShouldReceiveTxnNum(uint64_t epoch, uint64_t index, uint64_t value) {
        should_receive_txn_num[epoch % _max_length][index]->store(value);
    }
    static uint64_t GetShouldReceiveTxnNum(uint64_t epoch, uint64_t index) {
        return should_receive_txn_num[epoch % _max_length][index]->load();
    }
    static uint64_t GetShouldReceiveTxnNum(uint64_t epoch) {
        epoch %= _max_length;
        uint64_t ans = 0;
        for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
            if(received_pack_num[epoch][i]->load() == 1)
                ans += should_receive_txn_num[epoch][i]->load();
        }
        return ans;
    }

    static uint64_t AddReceivedPackNum(uint64_t epoch, uint64_t index, uint64_t value) {
        return received_pack_num[epoch % _max_length][index]->fetch_add(value);
    }
    static void StoreReceivedPackNum(uint64_t epoch, uint64_t index, uint64_t value) {
        received_pack_num[epoch % _max_length][index]->store(value);
    }
    static uint64_t GetReceivedPackNum(uint64_t epoch, uint64_t index) {
        return received_pack_num[epoch % _max_length][index]->load();
    }
    static uint64_t GetReceivedPackNum(uint64_t epoch) {
        epoch %= _max_length;
        uint64_t ans = 0;
        for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
            if(received_pack_num[epoch][i]->load() == 1)
                ans ++;
        }
        return ans;
    }

    static uint64_t AddReceivedTxnNum(uint64_t epoch, uint64_t index, uint64_t value) {
        return received_txn_num[epoch % _max_length][index]->fetch_add(value);
    }
    static void StoreReceivedTxnNum(uint64_t epoch, uint64_t index, uint64_t value) {
        received_txn_num[epoch % _max_length][index]->store(value);
    }
    static uint64_t GetReceivedTxnNum(uint64_t epoch, uint64_t index) {
        return received_txn_num[epoch % _max_length][index]->load();
    }
    static uint64_t GetReceivedTxnNum(uint64_t epoch) {
        epoch %= _max_length;
        uint64_t ans = 0;
        for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
            if(received_pack_num[epoch][i]->load() == 1)
                ans += received_txn_num[epoch][i]->load();
        }
        return ans;
    }

    static uint64_t AddReceivedPackNumTotal(uint64_t epoch, uint64_t value) {
        return received_total_pack_num[epoch % _max_length]->fetch_add(value);
    }
    static uint64_t GetReceivedPackNumTotal(uint64_t epoch) {
        return received_total_pack_num[epoch % _max_length]->load();
    }
    static uint64_t AddReceivedTxnNumTotal(uint64_t epoch, uint64_t value) {
        return received_total_txn_num[epoch % _max_length]->fetch_add(value);
    }
    static uint64_t GetReceivedTxnNumTotal(uint64_t epoch) {
        return received_total_txn_num[epoch % _max_length]->load();
    }

    static uint64_t AddShouldReceivePackNum(uint64_t value) {
        return should_receive_pack_num->fetch_add(value);
    }
    static uint64_t SubShouldReceivePackNum(uint64_t value) {
        return should_receive_pack_num->fetch_sub(value);
    }
    static void StoreShouldReceivePackNum(uint64_t value) {
        should_receive_pack_num->store(value);
    }
    static uint64_t GetShouldReceivePackNum() {
        return should_receive_pack_num->load();
    }

    static uint64_t AddOnLineServerNum(uint64_t value) {
        return online_server_num->fetch_add(value);
    }
    static uint64_t SubOnLineServerNum(uint64_t value) {
        return online_server_num->fetch_sub(value);
    }
    static void StoreOnLineServerNum(uint64_t value) {
        online_server_num->store(value);
    }
    static uint64_t GetOnLineServerNum() {
        return online_server_num->load();
    }

    
    static void RemoteCacheClear(uint64_t epoch) {
        epoch %= _max_length;
        for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
            received_pack_num[epoch][i]->store(0);
            received_txn_num[epoch][i]->store(0);
            should_receive_txn_num[epoch][i]->store(0);
        }
        received_total_pack_num[epoch]->store(0);
        received_total_txn_num[epoch]->store(0);
    }

    static void SetServerOnLine(std::string ip) {
        for(int i = 0; i < (int)kServerIp.size(); i++) {
            if(ip == kServerIp[i]) {
                is_server_online[i]->store(1);
            }
        }
    }

    static void SetServerOffLine(std::string ip) {
        for(int i = 0; i < (int)kServerIp.size(); i++) {
            if(ip == kServerIp[i]) {
                is_server_online[i]->store(0);
            }
        }
    }

    static void SetServerOnLine(uint64_t index) {
        is_server_online[index]->store(1);
    }

    static void SetServerOffLine(uint64_t index) {
        is_server_online[index]->store(0);
    }

    static bool IsServerOnLine(uint64_t index) {
        return (is_server_online[index]->load() == 1);
    }

    static void ReceiveAMessage() {}

    static std::vector<std::unique_ptr<std::atomic<uint64_t>>>  received_epoch;
    
    static bool IsCacheServerStored(uint64_t epoch) {
        if(received_epoch[epoch % _max_length]->load() == static_cast<uint64_t>(1)) {
            return true;
        }
        else {
            return false;
        }
    }

    static void SetCacheServerStored(uint64_t epoch, uint64_t value) {
        received_epoch[epoch % _max_length]->store(value);
    }
    

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









template<typename T>
using BlockingConcurrentQueue =  moodycamel::BlockingConcurrentQueue<T>;
// using BlockingConcurrentQueue = BlockingMPMCQueue<T>;
// struct pack_params;
// struct commit_thread_params;

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


// class LocalWriteSet {
// public:
//     static uint64_t _max_length, _pack_num;
//     //当前epoch放入多少个txn
//     static std::vector<std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>> txn_num_ptrs;
//     //pack_thread取出了多少个 两者比较来判断是否发送epoch_end message
//     static std::vector<std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>>> packd_txn_num_ptrs, write_abort_before_send_txn_num, 
//         write_abort_after_send_txn_num, total_abort_txn_num, read_abort_txn_num, read_committed_txn_num, write_committed_txn_num, 
//         read_total_txn_num, write_total_txn_num, pack_txn_num;

//     static void Init(uint64_t pack_num, uint64_t length);
    
    
//     static void PushBack(uint64_t epoch, std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>> ptr2, 
//         std::shared_ptr<std::vector<std::shared_ptr<std::atomic<uint64_t>>>> ptr3) {
//         auto epoch_mod = epoch % _max_length;
//         txn_num_ptrs[epoch_mod] = ptr2;
//         packd_txn_num_ptrs[epoch_mod] = ptr3;
//     }
    
//     static uint64_t LoadChangeSet(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*txn_num_ptrs[epoch_mod])[i]->load();
//         return ans;
//     }


//     static uint64_t LoadTotalAbortTxnNum(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*total_abort_txn_num[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t LoadWriteAbortBeforeSendTxnNum(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*write_abort_before_send_txn_num[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t LoadWriteAbortAfterSendTxnNum(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*write_abort_after_send_txn_num[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t LoadWriteCommittedTxnNum(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*write_committed_txn_num[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t LoadReadAbortTxnNum(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*read_abort_txn_num[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t LoadReadCommittedTxnNum(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*read_committed_txn_num[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t LoadReadTotalTxnNum(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*read_total_txn_num[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t LoadPackTxnNum(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*pack_txn_num[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t LoadWriteTotalTxnNum(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*write_total_txn_num[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t LoadPackedTxnNum(uint64_t epoch) {
//         uint64_t ans = 0, epoch_mod = epoch % _max_length;
//         for(int i = 0; i < (int)_pack_num; i ++)
//             ans += (*packd_txn_num_ptrs[epoch_mod])[i]->load();
//         return ans;
//     }

//     static uint64_t LoadShouldSendTxnNum(uint64_t epoch) {
//         if(is_sync_exec) {
//             // if(LoadChangeSet(epoch) < LoadReadCommittedTxnNum(epoch) - (LoadTotalAbortTxnNum(epoch) - LoadWriteAbortAfterSendTxnNum(epoch))) {
//             //     return LoadPackedTxnNum(epoch);
//             // }
//             return LoadChangeSet(epoch) - LoadReadCommittedTxnNum(epoch) - 
//                 (LoadTotalAbortTxnNum(epoch) - LoadWriteAbortAfterSendTxnNum(epoch)) ;
//         }
//         else 
//             return LoadChangeSet(epoch);
//     }
//     static uint64_t LoadShouldExecTxnNum(uint64_t epoch) {// 只在 is_sync_exec中使用
//         // if(LoadChangeSet(epoch) < LoadReadCommittedTxnNum(epoch) - (LoadTotalAbortTxnNum(epoch) - LoadWriteAbortAfterSendTxnNum(epoch))) {
//         //     return MOTAdaptor::GetLocalTxnExcCounters();
//         // }
//         return LoadChangeSet(epoch) - LoadReadCommittedTxnNum(epoch) - 
//             (LoadTotalAbortTxnNum(epoch) - LoadWriteAbortAfterSendTxnNum(epoch));
//     }

//     static bool IsCurrentEpochFinished(uint64_t epoch) {
//         return ((LoadPackedTxnNum(epoch) >= LoadShouldSendTxnNum(epoch)) &&  epoch < MOTAdaptor::GetPhysicalEpoch());
//     }

//     static bool TryAddNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
//         (*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_add(value);
//         return true;
//         // if((*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_add(value) == 0
//         //     && epoch_num < MOTAdaptor::GetPhysicalEpoch() ){
//         //         (*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_sub(value);
//         //         return false;
//         //     }
//         // else {
//         //     // MOT_LOG_INFO("txn add num %llu %llu %llu", epoch_num, LoadChangeSet(epoch_num));
//         //     return true;
//         // }
//     }

//     static bool AddNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
//         (*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_add(value);
//         return true;
//     }

//     static bool SubNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
//         // MOT_LOG_INFO("txn sub num %llu %llu %llu", epoch_num, LoadChangeSet(epoch_num), value);
//         (*(txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_sub(value);
//         return true;
//     }

//     static bool AddPackedNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
//         (*(packd_txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_add(value);
//         return true;
//     }

//     static bool SubPackedNum(uint64_t epoch_num, uint64_t index, uint64_t value) {
//         (*(packd_txn_num_ptrs[epoch_num % _max_length]))[index]->fetch_sub(value);
//         return true;
//     }

//     static void Clear(uint64_t epoch_num) {
//         auto epoch_mod = epoch_num % _max_length;
//         for(int i = 0; i <= (int)_pack_num; i++){
//             (*txn_num_ptrs[epoch_mod])[i]->store(0);
//             (*packd_txn_num_ptrs[epoch_mod])[i]->store(0);
//             (*write_abort_before_send_txn_num[epoch_mod])[i]->store(0);
//             (*write_abort_after_send_txn_num[epoch_mod])[i]->store(0);
//             (*total_abort_txn_num[epoch_mod])[i]->store(0);
//             (*read_abort_txn_num[epoch_mod])[i]->store(0);
//             (*read_committed_txn_num[epoch_mod])[i]->store(0);
//             (*write_committed_txn_num[epoch_mod])[i]->store(0);
//             (*read_total_txn_num[epoch_mod])[i]->store(0);
//             (*write_total_txn_num[epoch_mod])[i]->store(0);
//             (*pack_txn_num[epoch_mod])[i]->store(0);
//         }
//     }
// };

// class RemoteCache {
// public:    
//     static uint64_t _max_length, _pack_num;
//     static std::unique_ptr<std::atomic<uint64_t>> should_receive_pack_num, online_server_num;
//     static std::vector<std::unique_ptr<std::atomic<uint64_t>>> is_server_online;
//     static std::vector<std::vector<std::unique_ptr<std::atomic<uint64_t>>>> 
//         received_pack_num, received_txn_num, should_receive_txn_num;
//     static std::vector<std::unique_ptr<std::atomic<uint64_t>>> 
//         received_total_pack_num, received_total_txn_num;

//     static void Init(uint64_t pack_num, uint64_t length);
//         // received_total_pack_num->fetch_add(value);

    
//     static uint64_t AddShouldReceiveTxnNum(uint64_t epoch, uint64_t index, uint64_t value) {
//         return should_receive_txn_num[epoch % _max_length][index]->fetch_add(value);
//     }
//     static void StoreShouldReceiveTxnNum(uint64_t epoch, uint64_t index, uint64_t value) {
//         should_receive_txn_num[epoch % _max_length][index]->store(value);
//     }
//     static uint64_t GetShouldReceiveTxnNum(uint64_t epoch, uint64_t index) {
//         return should_receive_txn_num[epoch % _max_length][index]->load();
//     }
//     static uint64_t GetShouldReceiveTxnNum(uint64_t epoch) {
//         epoch %= _max_length;
//         uint64_t ans = 0;
//         for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
//             if(received_pack_num[epoch][i]->load() == 1)
//                 ans += should_receive_txn_num[epoch][i]->load();
//         }
//         return ans;
//     }

//     static uint64_t AddReceivedPackNum(uint64_t epoch, uint64_t index, uint64_t value) {
//         return received_pack_num[epoch % _max_length][index]->fetch_add(value);
//     }
//     static void StoreReceivedPackNum(uint64_t epoch, uint64_t index, uint64_t value) {
//         received_pack_num[epoch % _max_length][index]->store(value);
//     }
//     static uint64_t GetReceivedPackNum(uint64_t epoch, uint64_t index) {
//         return received_pack_num[epoch % _max_length][index]->load();
//     }
//     static uint64_t GetReceivedPackNum(uint64_t epoch) {
//         epoch %= _max_length;
//         uint64_t ans = 0;
//         for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
//             if(received_pack_num[epoch][i]->load() == 1)
//                 ans ++;
//         }
//         return ans;
//     }

//     static uint64_t AddReceivedTxnNum(uint64_t epoch, uint64_t index, uint64_t value) {
//         return received_txn_num[epoch % _max_length][index]->fetch_add(value);
//     }
//     static void StoreReceivedTxnNum(uint64_t epoch, uint64_t index, uint64_t value) {
//         received_txn_num[epoch % _max_length][index]->store(value);
//     }
//     static uint64_t GetReceivedTxnNum(uint64_t epoch, uint64_t index) {
//         return received_txn_num[epoch % _max_length][index]->load();
//     }
//     static uint64_t GetReceivedTxnNum(uint64_t epoch) {
//         epoch %= _max_length;
//         uint64_t ans = 0;
//         for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
//             if(received_pack_num[epoch][i]->load() == 1)
//                 ans += received_txn_num[epoch][i]->load();
//         }
//         return ans;
//     }

//     static uint64_t AddReceivedPackNumTotal(uint64_t epoch, uint64_t value) {
//         return received_total_pack_num[epoch % _max_length]->fetch_add(value);
//     }
//     static uint64_t GetReceivedPackNumTotal(uint64_t epoch) {
//         return received_total_pack_num[epoch % _max_length]->load();
//     }
//     static uint64_t AddReceivedTxnNumTotal(uint64_t epoch, uint64_t value) {
//         return received_total_txn_num[epoch % _max_length]->fetch_add(value);
//     }
//     static uint64_t GetReceivedTxnNumTotal(uint64_t epoch) {
//         return received_total_txn_num[epoch % _max_length]->load();
//     }

//     static uint64_t AddShouldReceivePackNum(uint64_t value) {
//         return should_receive_pack_num->fetch_add(value);
//     }
//     static uint64_t SubShouldReceivePackNum(uint64_t value) {
//         return should_receive_pack_num->fetch_sub(value);
//     }
//     static void StoreShouldReceivePackNum(uint64_t value) {
//         should_receive_pack_num->store(value);
//     }
//     static uint64_t GetShouldReceivePackNum() {
//         return should_receive_pack_num->load();
//     }

//     static uint64_t AddOnLineServerNum(uint64_t value) {
//         return online_server_num->fetch_add(value);
//     }
//     static uint64_t SubOnLineServerNum(uint64_t value) {
//         return online_server_num->fetch_sub(value);
//     }
//     static void StoreOnLineServerNum(uint64_t value) {
//         online_server_num->store(value);
//     }
//     static uint64_t GetOnLineServerNum() {
//         return online_server_num->load();
//     }

    
//     static void Clear(uint64_t epoch) {
//         epoch %= _max_length;
//         for(int i = 0; i < (int)received_pack_num[epoch].size(); i++) {
//             received_pack_num[epoch][i]->store(0);
//             received_txn_num[epoch][i]->store(0);
//             should_receive_txn_num[epoch][i]->store(0);
//         }
//         received_total_pack_num[epoch]->store(0);
//         received_total_txn_num[epoch]->store(0);
//     }

//     static void SetServerOnLine(std::string ip) {
//         for(int i = 0; i < (int)kServerIp.size(); i++) {
//             if(ip == kServerIp[i]) {
//                 is_server_online[i]->store(1);
//             }
//         }
//     }

//     static void SetServerOffLine(std::string ip) {
//         for(int i = 0; i < (int)kServerIp.size(); i++) {
//             if(ip == kServerIp[i]) {
//                 is_server_online[i]->store(0);
//             }
//         }
//     }

//     static void SetServerOnLine(uint64_t index) {
//         is_server_online[index]->store(1);
//     }

//     static void SetServerOffLine(uint64_t index) {
//         is_server_online[index]->store(0);
//     }

//     static bool IsServerOnLine(uint64_t index) {
//         return (is_server_online[index]->load() == 1);
//     }


    

//     static void ReceiveAMessage() {

//     }

// };

// class RegionCacheServerState{
// public:
//     static uint64_t _max_length;
//     static std::vector<std::unique_ptr<std::atomic<uint64_t>>>  received_epoch;

//     static void Init(uint64_t pack_num, uint64_t length);
    
//     static bool IsCacheServerStored(uint64_t epoch) {
//         if(received_epoch[epoch % _max_length]->load() == static_cast<uint64_t>(1)) {
//             return true;
//         }
//         else {
//             return false;
//         }
//     }

//     static void SetCacheServerStored(uint64_t epoch, uint64_t value) {
//         received_epoch[epoch % _max_length]->store(value);
//     }

// };


#endif  // MOT_INTERNAL_H
