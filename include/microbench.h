#ifndef _MICRO_BEHCN_H
#define _MICRO_BEHCN_H

#include "util.h"
#include "config.h"
#include <utility>
#include <assert.h>

#define INTERVAL (1llu << 10)
enum OperationType
{
	INSERT,
	REMOVE,
	UPDATE,
	GET,
	_OpreationTypeNumber
};

inline void swap(long long &a, long long &b)
{
	long long tmp = a;
	a = b;
	b = tmp;
}

long long *random_shuffle(int s)
{
	long long *x = new long long[s];
	for (int i = 0; i < s; i++)
	{
		x[i] = i;
	}
	for (int i = 0; i < s; i++)
	{
		swap(x[i], x[random() % s]);
	}
	return x;
}

class Benchmark
{
public:
	WorkloadGenerator *workload;
	long long key_range;
	long long init_key;
	long long *x;
	Config _conf;

	Benchmark(Config &conf) : init_key(0), _conf(conf)
	{
		if (conf.workload == RANDOM)
		{
			workload = new RandomGenerator();
		}
		else if (conf.workload == ZIPFIAN)
		{
			workload = new ZipfWrapper(conf.skewness, conf.init_keys);
		}

		x = NULL;
	}
	virtual ~Benchmark()
	{
		if (x != NULL)
			delete[] x;
	}

	virtual void test()
	{
		printf("not implementation\n");
	}

	virtual std::pair<OperationType, long long> nextOperation()
	{
		return std::make_pair(INSERT, workload->Next());
	}

	virtual long long nextInitKey()
	{
		if (x == NULL)
			x = random_shuffle(_conf.init_keys);
		return x[init_key++ % _conf.init_keys] + 1;
	}
};

class ReadOnlyBench : public Benchmark
{
public:
	ReadOnlyBench(Config &conf) : Benchmark(conf)
	{
	}
	std::pair<OperationType, long long> nextOperation()
	{
		long long d = workload->Next();
		return std::make_pair(GET, d % _conf.init_keys + 1);
	}
};

class InsertOnlyBench : public Benchmark
{
	RandomGenerator *salt;
	int *ct;
	int count;
	int id;

public:
	InsertOnlyBench(Config &conf, int workerid = 0) : Benchmark(conf)
	{
		salt = new RandomGenerator();
		ct = new int[10];
		for (int i = 0; i < 10; ++i)
			ct[i] = 0;
		count = 10;
		id = workerid;
	}
	std::pair<OperationType, long long> nextOperation()
	{
		long long d = workload->Next() % _conf.init_keys;
		long long x = salt->Next() % (INTERVAL - 1) + 1;
		return std::make_pair(INSERT, (d + 1) * INTERVAL + x);	
	}
	long long nextInitKey()
	{
		return INTERVAL * Benchmark::nextInitKey();
	}
};

class UpdateOnlyBench : public Benchmark
{
public:
	UpdateOnlyBench(Config &conf) : Benchmark(conf)
	{
	}
	OperationType nextOp()
	{
		return UPDATE;
	}
	std::pair<OperationType, long long> nextOperation()
	{
		long long d = workload->Next() % _conf.init_keys;
		return std::make_pair(UPDATE, d + 1);
	}
};

class DeleteOnlyBench : public Benchmark
{
public:
	DeleteOnlyBench(Config &conf) : Benchmark(conf)
	{
	}
	std::pair<OperationType, long long> nextOperation()
	{
		long long d = workload->Next() % _conf.init_keys;
		return std::make_pair(REMOVE, d + 1);
	}
};

class YSCBA : public Benchmark
{
public:
	int read_ratio;

	RandomGenerator rdm;
	YSCBA(Config &conf) : Benchmark(conf)
	{
		read_ratio = conf.read_ratio;
	}
	virtual std::pair<OperationType, long long> nextOperation()
	{
		int k = rdm.randomInt() % 100;

		if (k > read_ratio)
		{
			return std::make_pair(UPDATE, workload->Next() % _conf.init_keys + 1);
		}
		else
		{
			return std::make_pair(GET, workload->Next() % _conf.init_keys + 1);
		}
	}
};

class Upsert : public Benchmark
{
public:
	int read_ratio = 50;
	int interval;
	RandomGenerator rdm;
	RandomGenerator *salt;
	Upsert(Config &conf) : Benchmark(conf)
	{
		read_ratio = conf.read_ratio;
		interval = conf.interval;
		salt = new RandomGenerator();
	}
	virtual std::pair<OperationType, long long> nextOperation()
	{
		int k = rdm.randomInt() % 100;
		long long d = workload->Next() % _conf.init_keys;
		if (k >= read_ratio)
		{
			// generate (1, INTEVEL-1)
			long long x = salt->Next() % interval;
			return std::make_pair(INSERT, (d + 1) * interval + x);
		}
		else
		{
			return std::make_pair(GET, (d + 1) * interval);
		}
	}
	long long nextInitKey()
	{
		return interval * Benchmark::nextInitKey();
	}
};

#endif
