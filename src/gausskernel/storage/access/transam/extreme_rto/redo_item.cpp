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
 * redo_item.cpp
 *      Each RedoItem represents a log record ready to be replayed by one of
 *      the redo threads.  To decouple the lifetime of a RedoItem from its
 *      log record's original XLogReaderState, contents necessary for the
 *      actual replay are duplicated into RedoItem's internal XLogReaderState.

 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/transam/extreme_rto/redo_item.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <assert.h>
#include <string.h>

#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/xlogrecord.h"
#include "access/xlog_internal.h"
#include "utils/palloc.h"
#include "utils/guc.h"

#include "access/extreme_rto/dispatcher.h"
#include "access/extreme_rto/redo_item.h"
#include "postmaster/postmaster.h"
#include "access/xlog.h"

namespace extreme_rto {

/* Run from the dispatcher thread. */
RedoItem *CreateRedoItem(XLogReaderState *record, uint32 shareCount, uint32 designatedWorker, List *expectedTLIs,
                         TimestampTz recordXTime, bool buseoriginal, bool isForceAll)
{
    RedoItem *item = GetRedoItemPtr(record);
    if (t_thrd.xlog_cxt.redoItemIdx == 0) {
        /*
         * Some blocks are optional and redo functions rely on the correct
         * value of in_use to determine if optional blocks are present.
         * Explicitly set all unused blocks' in_use to false.
         */
        for (int i = record->max_block_id + 1; i <= XLR_MAX_BLOCK_ID; i++)
            item->record.blocks[i].in_use = false;
    }
    if (buseoriginal && (t_thrd.xlog_cxt.redoItemIdx == 0)) {
        t_thrd.xlog_cxt.redoItemIdx++;
    } else {
        /* if shareCount is 1, we should make a copy of record in NewReaderState function */
        Assert(shareCount == 1);
        /* not only need copy state, but also need copy data */
        item = GetRedoItemPtr(NewReaderState(record, true));
    }

    item->oldVersion = t_thrd.xlog_cxt.redo_oldversion_xlog;
    item->needImmediateCheckpoint = false;
    item->needFullSyncCheckpoint = false;
    item->shareCount = shareCount;
    item->designatedWorker = designatedWorker;
    item->expectedTLIs = expectedTLIs;
    item->recordXTime = recordXTime;
    item->freeNext = NULL;
    item->syncXLogReceiptTime = t_thrd.xlog_cxt.XLogReceiptTime;
    item->syncXLogReceiptSource = t_thrd.xlog_cxt.XLogReceiptSource;
    item->RecentXmin = u_sess->utils_cxt.RecentXmin;
    item->syncServerMode = GetServerMode();
    item->isForceAll = isForceAll;
    pg_atomic_init_u32(&item->refCount, shareCount);
    pg_atomic_init_u32(&item->replayed, 0);
    pg_atomic_init_u32(&item->blkShareCount, 0);
    pg_atomic_init_u32(&item->distributeCount, shareCount);
    return item;
}

void ApplyRedoRecord(XLogReaderState *record, bool bOld)
{
    t_thrd.xlog_cxt.redo_oldversion_xlog = bOld;
    ErrorContextCallback errContext;
    errContext.callback = rm_redo_error_callback;
    errContext.arg = (void *)record;
    errContext.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &errContext;
    if (module_logging_is_on(MOD_REDO)) {
        DiagLogRedoRecord(record, "ApplyRedoRecord");
    }
    RmgrTable[XLogRecGetRmid(record)].rm_redo(record);

    t_thrd.log_cxt.error_context_stack = errContext.previous;
    t_thrd.xlog_cxt.redo_oldversion_xlog = false;
}
}  // namespace extreme_rto
