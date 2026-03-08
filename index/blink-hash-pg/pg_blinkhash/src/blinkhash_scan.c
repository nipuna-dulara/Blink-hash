

#include "blinkhash_am.h"
#include "blinkhash_scan.h"
#include "blinkhash_core.h"
#include "blinkhash_utils.h"

#include "postgres.h"
#include "access/genam.h"
#include "access/relscan.h"
#include "access/skey.h"
#include "catalog/pg_type.h"
#include "nodes/tidbitmap.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#define BH_RANGE_BUF_SIZE 256  



IndexScanDesc
blinkhash_ambeginscan(Relation indexRelation,
                      int nkeys, int norderbys)
{
    IndexScanDesc scan = RelationGetIndexScan(indexRelation,
                                              nkeys, norderbys);

    BHScanOpaque so = (BHScanOpaque)
        palloc0(sizeof(BHScanOpaqueData));

    BHIndexState *state = bh_get_index_state(indexRelation);
    if (state)
    {
        so->tree        = state->tree;
        so->thread_info = state->thread_info;
        so->key_class   = state->key_class;
        so->key_typid   = state->key_typid;
    }

    so->range_buf_capacity = BH_RANGE_BUF_SIZE;
    so->range_buf = (uint64 *) palloc(sizeof(uint64) * BH_RANGE_BUF_SIZE);
    so->scan_started  = false;
    so->scan_finished = false;

    scan->opaque = so;
    return scan;
}



void
blinkhash_amrescan(IndexScanDesc scan,
                   ScanKey scankey, int nscankeys,
                   ScanKey orderbys, int norderbys)
{
    BHScanOpaque so = (BHScanOpaque) scan->opaque;

    so->scan_started  = false;
    so->scan_finished = false;
    so->range_buf_count = 0;
    so->range_buf_pos   = 0;

    if (scankey && scan->numberOfKeys > 0)
        memmove(scan->keyData, scankey,
                scan->numberOfKeys * sizeof(ScanKeyData));
}



bool
blinkhash_amgettuple(IndexScanDesc scan, ScanDirection direction)
{
    BHScanOpaque so = (BHScanOpaque) scan->opaque;

    if (!so->tree || so->scan_finished)
        return false;

    /* If we have buffered results, return the next one */
    if (so->range_buf_pos < so->range_buf_count)
    {
        ItemPointerData tid;
        bh_value_to_tid(so->range_buf[so->range_buf_pos], &tid);
        scan->xs_heaptid = tid;
        scan->xs_recheck = false;
        so->range_buf_pos++;
        return true;
    }

    /* First call or buffer exhausted — perform the lookup */
    if (scan->numberOfKeys < 1)
        return false;

    ScanKey key = &scan->keyData[0];

    if (key->sk_strategy == BTEqualStrategyNumber)
    {
        /* Point lookup */
        int found = 0;
        uint64 val;

        if (so->key_class == 'i')
        {
            bool ok;
            uint64 k = bh_datum_to_key64(key->sk_argument,
                                         so->key_typid, &ok);
            if (!ok)
                return false;
            val = bh_lookup(so->tree, 'i', &k, sizeof(k),
                            so->thread_info, &found);
        }
        else
        {
            char key_buf[32];
            bh_datum_to_string_key(key->sk_argument,
                                   so->key_typid, key_buf, 32);
            val = bh_lookup(so->tree, 's', key_buf, 32,
                            so->thread_info, &found);
        }

        if (found)
        {
            ItemPointerData tid;
            bh_value_to_tid(val, &tid);
            scan->xs_heaptid = tid;
            scan->xs_recheck = false;
            so->scan_finished = true;
            return true;
        }
        return false;
    }
    else
    {
        /*
         * Range scan: use range_lookup() to fill the buffer.
         *
         * For GE/GT: scan from the given key forward.
         * For LE/LT: not supported (amcanbackward = false).
         *
         * We fill the buffer and return results one at a time.
         */
        if (so->scan_started && so->range_buf_count == 0)
        {
            so->scan_finished = true;
            return false;
        }

        int count;
        if (so->key_class == 'i')
        {
            bool ok;
            uint64 k = bh_datum_to_key64(key->sk_argument,
                                         so->key_typid, &ok);
            if (!ok) return false;
            count = bh_range_lookup(so->tree, 'i',
                                    &k, sizeof(k),
                                    so->range_buf_capacity,
                                    so->range_buf,
                                    so->thread_info);
        }
        else
        {
            char key_buf[32];
            bh_datum_to_string_key(key->sk_argument,
                                   so->key_typid, key_buf, 32);
            count = bh_range_lookup(so->tree, 's',
                                    key_buf, 32,
                                    so->range_buf_capacity,
                                    so->range_buf,
                                    so->thread_info);
        }

        so->scan_started    = true;
        so->range_buf_count = count;
        so->range_buf_pos   = 0;

        if (count <= 0)
        {
            so->scan_finished = true;
            return false;
        }

        ItemPointerData tid;
        bh_value_to_tid(so->range_buf[0], &tid);
        scan->xs_heaptid = tid;
        scan->xs_recheck = false;
        so->range_buf_pos = 1;
        return true;
    }
}



int64
blinkhash_amgetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
    BHScanOpaque so = (BHScanOpaque) scan->opaque;
    int64 ntids = 0;

    if (!so->tree || scan->numberOfKeys < 1)
        return 0;

    ScanKey key = &scan->keyData[0];

    if (key->sk_strategy == BTEqualStrategyNumber)
    {
        /* Point lookup — add single TID to bitmap */
        int found = 0;
        uint64 val;

        if (so->key_class == 'i')
        {
            bool ok;
            uint64 k = bh_datum_to_key64(key->sk_argument,
                                         so->key_typid, &ok);
            if (!ok) return 0;
            val = bh_lookup(so->tree, 'i', &k, sizeof(k),
                            so->thread_info, &found);
        }
        else
        {
            char key_buf[32];
            bh_datum_to_string_key(key->sk_argument,
                                   so->key_typid, key_buf, 32);
            val = bh_lookup(so->tree, 's', key_buf, 32,
                            so->thread_info, &found);
        }

        if (found)
        {
            ItemPointerData tid;
            bh_value_to_tid(val, &tid);
            tbm_add_tuples(tbm, &tid, 1, false);
            ntids = 1;
        }
    }
    else
    {
        /* Range scan — add all TIDs to bitmap */
        uint64 buf[1024];
        int count;

        if (so->key_class == 'i')
        {
            bool ok;
            uint64 k = bh_datum_to_key64(key->sk_argument,
                                         so->key_typid, &ok);
            if (!ok) return 0;
            count = bh_range_lookup(so->tree, 'i',
                                    &k, sizeof(k), 1024,
                                    buf, so->thread_info);
        }
        else
        {
            char key_buf[32];
            bh_datum_to_string_key(key->sk_argument,
                                   so->key_typid, key_buf, 32);
            count = bh_range_lookup(so->tree, 's',
                                    key_buf, 32, 1024,
                                    buf, so->thread_info);
        }

        for (int i = 0; i < count; i++)
        {
            ItemPointerData tid;
            bh_value_to_tid(buf[i], &tid);
            tbm_add_tuples(tbm, &tid, 1, false);
        }
        ntids = count;
    }

    return ntids;
}



void
blinkhash_amendscan(IndexScanDesc scan)
{
    BHScanOpaque so = (BHScanOpaque) scan->opaque;
    if (so->range_buf)
        pfree(so->range_buf);
    pfree(so);
    scan->opaque = NULL;
}



void
blinkhash_ammarkpos(IndexScanDesc scan)
{

    elog(ERROR, "blinkhash does not support mark/restore");
}

void
blinkhash_amrestrpos(IndexScanDesc scan)
{
    elog(ERROR, "blinkhash does not support mark/restore");
}