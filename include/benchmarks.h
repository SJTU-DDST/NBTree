#ifndef _BENCHMARKS_H_
#define _BENCHMARKS_H_

#include "microbench.h"
#include "config.h"

static Benchmark *getBenchmark(Config conf, int workerid = 0)
{
	switch (conf.benchmark)
	{
	case READ_ONLY:
		// printf("Benchmark: Search\n");
		return new ReadOnlyBench(conf);
	case INSERT_ONLY:
		// printf("Benchmark: Insert\n");
		return new InsertOnlyBench(conf, workerid);
	case UPDATE_ONLY:
		// printf("Benchmark: Update\n");
		return new UpdateOnlyBench(conf);
	case DELETE_ONLY:
		// printf("Benchmark: Delete\n");
		return new DeleteOnlyBench(conf);
	case YCSB_A:
		// printf("Benchmark: YCSB (Update)\n");
		return new YSCBA(conf);
	case UPSERT:
		// printf("Benchmark: YCSB (Upsert)\n");
		return new Upsert(conf);
	case MIXED_BENCH:
		return new MixedBench(conf);
	case MIX_RI:
		return new RIBench(conf);
	case MIX_RD:
		printf("workload: mixed read and delete\n");
		return new RDBench(conf);
	case MIX_UI:
		return new UIBench(conf);
	case MIX_ID:
		// printf("Mixture workload of delete and insert!\n");
		return new IDBench(conf);
	case SCAN_BENCH:
		return new ScanBench(conf);
	case RECOVERY_BENCH:
		return new UpdateOnlyBench(conf);
	default:
		printf("none support benchmark %d\n", conf.benchmark);
		exit(0);
	}
	return NULL;
}
#endif
