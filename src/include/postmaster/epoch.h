#ifndef _EPOCH_H
#define _EPOCH_H
//ADDBY NEU
#include <thread>
#include <vector>
extern std::vector<uint64_t> epoch_logical_thread_ids;
extern std::vector<uint64_t> epoch_physical_thread_ids;
extern std::vector<uint64_t> epoch_cache_thread_ids;
extern std::vector<uint64_t> epoch_manager_thread_ids;

extern std::vector<uint64_t> epoch_notify_thread_ids;
extern std::vector<uint64_t> epoch_pack_thread_ids;
extern std::vector<uint64_t> epoch_send_thread_ids;

extern std::vector<uint64_t> epoch_listen_thread_ids;
extern std::vector<uint64_t> epoch_unseri_thread_ids;
extern std::vector<uint64_t> epoch_unpack_thread_ids;
extern std::vector<uint64_t> epoch_merge_thread_ids;
extern std::vector<uint64_t> epoch_commit_thread_ids;

extern void EpochLogicalTimerManagerMain(void);
extern void EpochPhysicalTimerManagerMain(void);
extern void EpochMessageCacheManagerMain(void);
extern void EpochMessageManagerMain(void);

extern void EpochNotifyMain(void);
extern void EpochPackMain(void);
extern void EpochSendMain(void);

extern void EpochListenMain(void);
extern void EpochUnseriMain(void);
extern void EpochUnpackMain(void);
extern void EpochMergeMain(void);
extern void EpochCommitMain(void);

#endif