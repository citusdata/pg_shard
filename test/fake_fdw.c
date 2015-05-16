#include "postgres.h"

#include "access/reloptions.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"

PG_FUNCTION_INFO_V1(fake_fdw_handler);

static void FakeGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
								  Oid foreigntableid);
static void FakeGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
								Oid foreigntableid);
static ForeignScan * FakeGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
										Oid foreigntableid, ForeignPath *best_path,
										List *tlist, List *scan_clauses);
static void FakeBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot * FakeIterateForeignScan(ForeignScanState *node);
static void FakeReScanForeignScan(ForeignScanState *node);
static void FakeEndForeignScan(ForeignScanState *node);


Datum
fake_fdw_handler(PG_FUNCTION_ARGS __attribute__((unused)))
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	/* Required by notations: S=SELECT I=INSERT U=UPDATE D=DELETE */

	fdwroutine->GetForeignRelSize = FakeGetForeignRelSize;
	fdwroutine->GetForeignPaths = FakeGetForeignPaths;
	fdwroutine->GetForeignPlan = FakeGetForeignPlan;
	fdwroutine->BeginForeignScan = FakeBeginForeignScan;
	fdwroutine->IterateForeignScan = FakeIterateForeignScan;
	fdwroutine->ReScanForeignScan = FakeReScanForeignScan;
	fdwroutine->EndForeignScan = FakeEndForeignScan;

	PG_RETURN_POINTER(fdwroutine);
}


static void
FakeGetForeignRelSize(PlannerInfo *root __attribute__((unused)), RelOptInfo *baserel,
					  Oid foreigntableid __attribute__((unused)))
{
	baserel->rows = 0;
	baserel->fdw_private = (void *) palloc0(1);
}


static void
FakeGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
					Oid foreigntableid __attribute__((unused)))
{
	Cost startup_cost = 0;
	Cost total_cost = startup_cost + baserel->rows;

	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL,
									 NULL,
									 NIL));
}


static ForeignScan *
FakeGetForeignPlan(PlannerInfo *root __attribute__((unused)),
				   RelOptInfo *baserel __attribute__((unused)),
				   Oid foreigntableid __attribute__((unused)),
				   ForeignPath *best_path __attribute__((unused)),
				   List *tlist __attribute__((unused)),
				   List *scan_clauses __attribute__((unused)))
{
	Index scan_relid = baserel->relid;
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	return make_foreignscan(tlist, scan_clauses, scan_relid, NIL, NIL);
}


static void
FakeBeginForeignScan(ForeignScanState *node __attribute__((unused)),
					 int eflags __attribute__((unused))) { }


static TupleTableSlot *
FakeIterateForeignScan(ForeignScanState *node)
{
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	ExecClearTuple(slot);

	return slot;
}


static void
FakeReScanForeignScan(ForeignScanState *node __attribute__((unused))) { }


static void
FakeEndForeignScan(ForeignScanState *node __attribute__((unused))) { }
