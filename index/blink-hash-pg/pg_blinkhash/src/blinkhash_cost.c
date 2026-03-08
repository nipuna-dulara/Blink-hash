

#include "blinkhash_am.h"
#include "blinkhash_utils.h"

#include "postgres.h"
#include "access/amapi.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "utils/selfuncs.h"

/*

 * B^link-hash cost model:
 *   Point lookup:  O(1) hash probe + O(1) bucket scan
 *   Range scan:    O(log N) descent + O(range) leaf scan
 *
 */
void
blinkhash_amcostestimate(PlannerInfo *root,
                         IndexPath *path,
                         double loop_count,
                         Cost *indexStartupCost,
                         Cost *indexTotalCost,
                         Selectivity *indexSelectivity,
                         double *indexCorrelation,
                         double *indexPages)
{
    GenericCosts costs;
    MemSet(&costs, 0, sizeof(costs));

    /* Let PG's generic cost estimator do the base work */
    genericcostestimate(root, path, loop_count, &costs);


    bool is_point_lookup = false;
    if (list_length(path->indexclauses) == 1)
    {

        IndexClause *ic = (IndexClause *) linitial(path->indexclauses);
        RestrictInfo *ri = ic->rinfo;
        if (ri && IsA(ri->clause, OpExpr))
        {
            Oid opno = ((OpExpr *)ri->clause)->opno;
            
            if (ic->indexcol == 0)
                is_point_lookup = true;
        }
    }

    if (is_point_lookup)
    {
     
        costs.indexTotalCost *= 0.3;   /* 70% cheaper than generic btree */
        *indexCorrelation = 0.0;       /* hash order ≠ heap order */
    }
    else
    {
      
        *indexCorrelation = 1.0;       /* sorted leaves */
    }

    *indexStartupCost  = costs.indexStartupCost;
    *indexTotalCost     = costs.indexTotalCost;
    *indexSelectivity   = costs.indexSelectivity;
    *indexPages         = costs.numIndexPages;
}