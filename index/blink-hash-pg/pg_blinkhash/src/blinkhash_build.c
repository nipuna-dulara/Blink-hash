

#include "blinkhash_am.h"
#include "blinkhash_build.h"
#include "blinkhash_core.h"
#include "blinkhash_utils.h"

#include "postgres.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"


void
blinkhash_ambuildempty(Relation indexRelation)
{
    /* Nothing to do — the tree is created on first insert. */
}

/*
 * Callback for table_index_build_scan: called once per heap tuple.
 */
void
blinkhash_build_callback(Relation indexRelation,
                         ItemPointer tid,
                         Datum *values,
                         bool *isnull,
                         bool tupleIsAlive,
                         void *state)
{
    BHBuildState *bs = (BHBuildState *) state;

    /* Skip NULL keys */
    if (isnull[0])
        return;

    uint64 packed_tid = bh_tid_to_value(tid);

    if (bs->key_class == 'i')
    {
        bool ok;
        uint64 key = bh_datum_to_key64(values[0], bs->key_typid, &ok);
        if (ok)
        {
            bh_insert(bs->tree, 'i', &key, sizeof(key),
                      packed_tid, bs->thread_info);
            bs->tuples_inserted++;
        }
    }
    else
    {
        char key_buf[32];
        bh_datum_to_string_key(values[0], bs->key_typid, key_buf, 32);
        bh_insert(bs->tree, 's', key_buf, 32,
                  packed_tid, bs->thread_info);
        bs->tuples_inserted++;
    }
}

IndexBuildResult *
blinkhash_ambuild(Relation heapRelation,
                  Relation indexRelation,
                  IndexInfo *indexInfo)
{
    IndexBuildResult *result;

    /* Determine key type from the first indexed column */
    Oid key_typid = TupleDescAttr(
        RelationGetDescr(indexRelation), 0)->atttypid;
    char key_class = bh_classify_type(key_typid);

    /* Create the tree */
    void *tree = bh_tree_create(key_class);
    void *ti   = bh_get_thread_info(tree, key_class);

    BHBuildState bs;
    bs.tree           = tree;
    bs.thread_info    = ti;
    bs.tuples_inserted = 0;
    bs.key_class      = key_class;
    bs.key_typid      = key_typid;

    /* Scan the heap and build the index */
    double reltuples = table_index_build_scan(heapRelation,
                                              indexRelation,
                                              indexInfo,
                                              true,     /* allow_sync */
                                              false,    /* progress */
                                              blinkhash_build_callback,
                                              &bs,
                                              NULL);    /* scan */
    #ifdef BH_USE_PG_WAL

    if (RelationNeedsWAL(indexRelation))
    {
        /* Mark all index pages as needing full page write on next modify */
        // This is automatic with REGBUF_STANDARD
    }
    #endif

    /*
     * Store the tree handle in rd_amcache for later use by
     * insert/scan/vacuum.
     */
    BHIndexState *state = (BHIndexState *)
        MemoryContextAllocZero(indexRelation->rd_indexcxt,
                               sizeof(BHIndexState));
    state->tree         = tree;
    state->thread_info  = ti;
    state->key_class    = key_class;
    state->key_typid    = key_typid;
    indexRelation->rd_amcache = state;

    /* Return statistics */
    result = (IndexBuildResult *) palloc0(sizeof(IndexBuildResult));
    result->heap_tuples  = reltuples;
    result->index_tuples = bs.tuples_inserted;

    return result;
}