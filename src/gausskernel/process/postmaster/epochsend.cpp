#include "postgres.h"
#include "knl/knl_variable.h"

#include <signal.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "access/xlog.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
// #include "postmaster/walwriter.h"
#include "postmaster/epoch.h"
#include "storage/buf/bufmgr.h"
#include "storage/ipc.h"
#include "storage/lock/lwlock.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include "gssignal/gs_signal.h"

#include "storage/mot/mot_fdw.h"
#include <thread>
#include <sstream>
#include <iostream>
#include "postmaster/postmaster.h"
std::vector<uint64_t> epoch_send_thread_ids;
#define NANOSECONDS_PER_MILLISECOND 1000000L
#define NANOSECONDS_PER_SECOND 1000000000L

/* Signal handlers */
static void _quickdie(SIGNAL_ARGS);
static void _SigHupHandler(SIGNAL_ARGS);
static void _ShutdownHandler(SIGNAL_ARGS);
static void _sigusr1_handler(SIGNAL_ARGS);

THR_LOCAL const int g_sleep_timeout_ms = 300; /* WAL writer sleep timeout in millisecond. */

/*
 * Main entry point for walwriter process
 *
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */


void EpochSendMain(void)
{
    sigjmp_buf local_sigjmp_buf;
    MemoryContext epochsend_context;
    sigset_t old_sig_mask;
    // bool wrote_something = true;
    // long times_wrote_nothing = 0;
    // struct timespec time_to_wait;
    // int sleep_times_counter = 0;
    // int time_out_counter = 0;

    // load_server_mode();
    // if (t_thrd.xlog_cxt.server_mode == PRIMARY_MODE ||
    //     t_thrd.xlog_cxt.server_mode == NORMAL_MODE) {
    //     /*
    //      * Different from WalWriterPID, isWalWriterUp is used to signal that
    //      * the WAL writer thread is not only created, it is also created to
    //      * write WAL from the WAL buffer to disks. 
    //      *
    //      * We need isWalWriterUp instead of WalWriterPID in places where threads
    //      * to see whether there is a WAL writer running and flushing WAL buffer.
    //      * In standbys where we don't write WAL from the WAL buffer during redo, 
    //      * the WAL writer can be potentially re-purposed, in which case
    //      * WalWriterPID check is insufficient
    //      */
    //     g_instance.wal_cxt.isWalWriterUp = true;
    // }

    // ereport(LOG, (errmsg("epochsend started")));

    // if (g_instance.attr.attr_storage.wal_writer_cpu >= 0) {
    //     cpu_set_t walWriterSet;
    //     CPU_ZERO(&walWriterSet);
    //     CPU_SET(g_instance.attr.attr_storage.wal_writer_cpu, &walWriterSet);

    //     int rc = sched_setaffinity(0, sizeof(cpu_set_t), &walWriterSet);
    //     if (rc == -1) {
    //         ereport(WARNING,
    //             (errmsg("Failed to schedule WalWriter on wal_writer_cpu, sched_setaffinity() set errno as %d.",
    //                 errno)));
    //     }
    // }

    /*
     * Properly accept or ignore signals the postmaster might send us.
     * We have no particular use for SIGINT at the moment, but seems
     * reasonable to treat like SIGTERM.
     *
     * Reset some signals that are accepted by postmaster but not here.
     */
    (void)gspqsignal(SIGHUP, _SigHupHandler);    /* set flag to read config file */
    (void)gspqsignal(SIGINT, _ShutdownHandler);  /* request shutdown */
    (void)gspqsignal(SIGTERM, _ShutdownHandler); /* request shutdown */
    (void)gspqsignal(SIGQUIT, _quickdie);       /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, _sigusr1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN); /* not used */

    /*
     * Reset some signals that are accepted by postmaster but not here.
     */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /* We allow SIGQUIT (quickdie) at all times */
    sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    /*
     * Create a resource owner to keep track of our resources (not clear that
     * we need this, but may as well have one).
     */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "epoch send", MEMORY_CONTEXT_STORAGE);

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.  Formerly this code just ran in
     * t_thrd.top_mem_cxt, but resetting that would be a really bad idea.
     */
    epochsend_context = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "epoch send",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(epochsend_context);

    /*
     * If an exception is encountered, processing resumes here.
     *
     * This code is heavily based on bgwriter.c, q.v.
     */
    int curTryCounter;
    int* oldTryCounter = NULL;
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        /* We need restore the signal mask of current thread. */
        pthread_sigmask(SIG_SETMASK, &old_sig_mask, NULL);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        /* abort async io, must before LWlock release */
        AbortAsyncListIO();

        /*
         * These operations are really just a minimal subset of
         * AbortTransaction().	We don't have very many resources to worry
         * about in epochsend, but we do have LWLocks, and perhaps buffers?
         */
        LWLockReleaseAll();
        pgstat_report_waitevent(WAIT_EVENT_END);
        AbortBufferIO();
        UnlockBuffers();
        /* buffer pins are released here: */
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
        /* we needn't bother with the other ResourceOwnerRelease phases */
        AtEOXact_Buffers(false);
        AtEOXact_SMgr();
        AtEOXact_Files();
        AtEOXact_HashTables(false);

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        (void)MemoryContextSwitchTo(epochsend_context);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(epochsend_context);

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /*
         * Sleep at least 1 second after any error.  A write error is likely
         * to be repeated, and we don't want to be filling the error logs as
         * fast as we can.
         */
        pg_usleep(1000000L);

        /*
         * Close all open files after any error.  This is helpful on Windows,
         * where holding deleted files open causes various strange errors.
         * It's not clear we need it elsewhere, but shouldn't hurt.
         */
        smgrcloseall();
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /*
     * Reset hibernation state after any error.
     */
    SetWalWriterSleeping(false);

    /*
     * Advertise our latch that backends can use to wake us up while we're
     * sleeping.
     */
    // g_instance.proc_base->epochsendLatch = &t_thrd.proc->procLatch;

    pgstat_report_appname("epoch send");
    pgstat_report_activity(STATE_IDLE, NULL);

    /*
     * Loop forever
     */
    uint64_t id = 0;
    std::stringstream thread_id_stream;
    thread_id_stream << std::this_thread::get_id();
    uint64_t thread_id = std::stoull(thread_id_stream.str());
    for(auto &i : epoch_send_thread_ids ){
        if(i == thread_id){
            break;
        }
        id++;
    }
    if(id < (uint64_t)epoch_send_thread_ids.size())
        ereport(LOG, (errmsg("epoch send pid %llu %llu %lu", id, epoch_send_thread_ids[id], thread_id)));
    for (;;) {
        pgstat_report_activity(STATE_RUNNING, NULL);

        /*
         * Process any requests or signals received recently.
         */
        if (t_thrd.epochsend_cxt.got_SIGHUP) {
            t_thrd.epochsend_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        if (t_thrd.epochsend_cxt.shutdown_requested) {
            /* Normal exit from the epochsend is here */
            proc_exit(0); /* done */
        }
        FDWEpochSendThreadMain(id, send_ips[id], send_ports[id]);
        // LWLockAcquire(WALWriteLock, LW_EXCLUSIVE);
        // wrote_something = XLogBackgroundFlush();
        // LWLockRelease(WALWriteLock);

        // if (!wrote_something && ++times_wrote_nothing > g_instance.attr.attr_storage.xlog_idle_flushes_before_sleep) {
        //     /*
        //      * Wait for the first entry after last flushed entry to be updated
        //      */
        //     int lastFlushedEntry = g_instance.wal_cxt.lastWalStatusEntryFlushed;
        //     volatile WalInsertStatusEntry *pCriticalEntry =
        //         &g_instance.wal_cxt.walInsertStatusTable[GET_NEXT_STATUS_ENTRY(lastFlushedEntry)];
        //     if (g_instance.wal_cxt.isWalWriterUp && pCriticalEntry->status == WAL_NOT_COPIED) {
        //         sleep_times_counter++;
        //         (void)pthread_mutex_lock(&g_instance.wal_cxt.criticalEntryMutex);
        //         g_instance.wal_cxt.isWalWriterSleeping = true;
        //         while (pCriticalEntry->status == WAL_NOT_COPIED && !t_thrd.epochsend_cxt.shutdown_requested) {
        //             (void)clock_gettime(CLOCK_MONOTONIC, &time_to_wait);
        //             time_to_wait.tv_nsec += g_sleep_timeout_ms * NANOSECONDS_PER_MILLISECOND;
        //             if (time_to_wait.tv_nsec >= NANOSECONDS_PER_SECOND) {
        //                 time_to_wait.tv_nsec -= NANOSECONDS_PER_SECOND;
        //                 time_to_wait.tv_sec += 1;
        //             }
        //             int res = pthread_cond_timedwait(&g_instance.wal_cxt.criticalEntryCV,
        //                 &g_instance.wal_cxt.criticalEntryMutex, &time_to_wait);
        //             if (res == 0) {
        //                 /*
        //                  * We should not break out here because we may be notified by an
        //                  * entry after the critiacal entry. We must check again if critical
        //                  * entry status is WAL_NOT_COPIED.
        //                  */
        //                 continue;
        //             } else if (res == ETIMEDOUT) {
        //                 time_out_counter++;
        //             } else {
        //                 ereport(WARNING, (errmsg("WAL writer pthread_cond_timedwait returned error code = %d.",
        //                     errno)));
        //             }
        //             /* wakeup other producer if possible to avoid hang */
        //             WakeupWalSemaphore(&g_instance.wal_cxt.walFlushWaitLock->l.sem);
        //             WakeupWalSemaphore(&g_instance.wal_cxt.walBufferInitWaitLock->l.sem);
        //             CHECK_FOR_INTERRUPTS();
        //         }
        //         g_instance.wal_cxt.isWalWriterSleeping = false;
        //         (void)pthread_mutex_unlock(&g_instance.wal_cxt.criticalEntryMutex);
        //         time_out_counter = 0;
        //     }
        //     times_wrote_nothing = 0;
        // }

        pgstat_report_activity(STATE_IDLE, NULL);
    }
}

/* --------------------------------
 *		signal handler routines
 * --------------------------------
 */
/*
 * _quickdie() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm,
 * so we need to stop what we're doing and exit.
 */
static void _quickdie(SIGNAL_ARGS)
{
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

    // g_instance.wal_cxt.isWalWriterUp = false;
    // pg_memory_barrier();
    /* Stop WalWriterAuxiliary from waiting. */
    // WakeupWalSemaphore(&g_instance.wal_cxt.walInitSegLock->l.sem);

    /*
     * We DO NOT want to run proc_exit() callbacks -- we're here because
     * shared memory may be corrupted, so we don't want to try to clean up our
     * transaction.  Just nail the windows shut and get out of town.  Now that
     * there's an atexit callback to prevent third-party code from breaking
     * things by calling exit() directly, we have to reset the callbacks
     * explicitly to make this work as intended.
     */
    on_exit_reset();

    /*
     * Note we do exit(2) not exit(0).	This is to force the postmaster into a
     * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
     * backend.  This is necessary precisely because we don't clean up our
     * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
     * should ensure the postmaster sees this as a crash, too, but no harm in
     * being doubly sure.)
     */
    exit(2);
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void _SigHupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.epochsend_cxt.got_SIGHUP = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

/* SIGTERM: set flag to exit normally */
static void _ShutdownHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.epochsend_cxt.shutdown_requested = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;

    // g_instance.wal_cxt.isWalWriterUp = false;
    // pg_memory_barrier();
    /* Stop WalWriterAuxiliary from waiting. */
    // WakeupWalSemaphore(&g_instance.wal_cxt.walInitSegLock->l.sem);
    
}

/* SIGUSR1: used for latch wakeups */
static void _sigusr1_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    latch_sigusr1_handler();

    errno = save_errno;
}
