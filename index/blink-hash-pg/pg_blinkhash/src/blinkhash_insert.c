

#include "blinkhash_am.h"
#include "blinkhash_core.h"
#include "blinkhash_utils.h"
#ifdef BH_USE_PG_WAL
#include "blinkhash_wal.h"
#endif

#include "postgres.h"
#include "access/genam.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/*
 * Ensure the index has a BHIndexState in rd_amcache.
 * If this is the first use after a relcache reset, create it.
 */
static BHIndexState *
ensure_index_state(Relation indexRelation)
{
    BHIndexState *state = bh_get_index_state(indexRelation);
    if (state != NULL)
        return state;

    /* First-time init: need to create the tree */
    Oid key_typid = TupleDescAttr(
        RelationGetDescr(indexRelation), 0)->atttypid;
    char key_class = bh_classify_type(key_typid);

    void *tree = bh_tree_create(key_class);
    void *ti   = bh_get_thread_info(tree, key_class);

    state = (BHIndexState *)
        MemoryContextAllocZero(indexRelation->rd_indexcxt,
                               sizeof(BHIndexState));
    state->tree        = tree;
    state->thread_info = ti;
    state->key_class   = key_class;
    state->key_typid   = key_typid;
    indexRelation->rd_amcache = state;

    return state;
}

bool
blinkhash_aminsert(Relation indexRelation,
                   Datum *values,
                   bool *isnull,
                   ItemPointer heap_tid,
                   Relation heapRelation,
                   IndexUniqueCheck checkUnique,
                   bool indexUnchanged,
                   IndexInfo *indexInfo)
{
    BHIndexState *state = ensure_index_state(indexRelation);

    /* Skip NULL keys */
    if (isnull[0])
        return false;

    uint64 packed_tid = bh_tid_to_value(heap_tid);

    if (state->key_class == 'i')
    {
        bool ok;
        uint64 key = bh_datum_to_key64(values[0], state->key_typid, &ok);
        if (!ok)
            return false;
        bh_insert(state->tree, 'i', &key, sizeof(key),
                  packed_tid, state->thread_info);

        #ifdef BH_USE_PG_WAL
        if (RelationNeedsWAL(indexRelation))
        {
            blinkhash_xlog_insert(/* node_id */ 0,
                                  /* bucket_idx */ 0,
                                  &key, sizeof(key),
                                  packed_tid);
        }
        #endif
    }
    else
    {
        char key_buf[32];
        bh_datum_to_string_key(values[0], state->key_typid, key_buf, 32);
        bh_insert(state->tree, 's', key_buf, 32,
                  packed_tid, state->thread_info);

        #ifdef BH_USE_PG_WAL
        if (RelationNeedsWAL(indexRelation))
        {
            blinkhash_xlog_insert(/* node_id */ 0,
                                  /* bucket_idx */ 0,
                                  key_buf, 32,
                                  packed_tid);
        }
        #endif
    }

    return false;  
}