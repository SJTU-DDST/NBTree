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
	SCAN,
	PRINT,
	MIXED,
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
		else
		{
			workload = new MonotonicGenerator();
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
		if (_conf.workload == MONOTONIC)
		{
			return std::make_pair(INSERT, workload->Next() * (id + 1) + _conf.init_keys);
		}
		else
		{
			long long d = workload->Next() % _conf.init_keys;
			long long x = salt->Next() % (INTERVAL - 1) + 1;
			return std::make_pair(INSERT, (d + 1) * INTERVAL + x);
		}
	}
	long long nextInitKey()
	{
		if (_conf.workload == MONOTONIC)
		{
			return Benchmark::nextInitKey();
		}
		else
			return INTERVAL * Benchmark::nextInitKey();
	}
	void test()
	{
		for (int i = 0; i < 128; ++i)
		{
			printf("%d\n", ct[i]);
		}
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

class MixedBench : public Benchmark
{

	RandomGenerator rdm;
	int round;
	long long key;

public:
	MixedBench(Config &conf) : Benchmark(conf)
	{
	}
	std::pair<OperationType, long long> nextOperation()
	{
		std::pair<OperationType, long long> result;
		long long _key = workload->Next() % _conf.init_keys + 1;
		switch (round)
		{
		case 0:
			key = workload->Next() % _conf.init_keys + 1;
			result = std::make_pair(REMOVE, key);
			break;
		case 1:
			result = std::make_pair(INSERT, key);
			break;
		case 2:
			result = std::make_pair(UPDATE, _key);
			break;
		case 3:
			result = std::make_pair(GET, _key);
			break;
		default:
			assert(0);
		}
		round++;
		round %= 4;
		return result;
	}
};

class ScanBench : public Benchmark
{
public:
	ScanBench(Config &conf) : Benchmark(conf) {}

	std::pair<OperationType, long long> nextOperation()
	{
		long long d = workload->Next() % _conf.init_keys + 1;
		return std::make_pair(SCAN, d);
	}
};

class YSCBA : public Benchmark
{
public:
	//	readRate = 0.5;
	//	writeRate = 0.5;
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
	//	readRate = 0.5;
	//	writeRate = 0.5;
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

class RIBench : public Benchmark
{
public:
	RandomGenerator rdm;
	RandomGenerator *salt;
	int read_ratio = 90;
	RIBench(Config &conf) : Benchmark(conf)
	{
		read_ratio = conf.read_ratio;
		salt = new RandomGenerator();
	}
	virtual std::pair<OperationType, long long> nextOperation()
	{
		int k = rdm.randomInt() % 100;
		if (k < read_ratio)
		{

			long long d = workload->Next() % _conf.init_keys + 1;
			return std::make_pair(GET, d * INTERVAL);
		}
		else
		{
			long long d = workload->Next() % _conf.init_keys + 1;
			long long x = salt->Next() % (INTERVAL - 1) + 1;
			return std::make_pair(INSERT, d * INTERVAL + x);
		}
	}
	long long nextInitKey()
	{
		return INTERVAL * Benchmark::nextInitKey();
	}
};

class RDBench : public Benchmark
{
public:
	int read_ratio = 90;

	RandomGenerator rdm;
	RDBench(Config &conf) : Benchmark(conf)
	{
		read_ratio = conf.read_ratio;
	}
	virtual std::pair<OperationType, long long> nextOperation()
	{
		int k = rdm.randomInt() % 100;

		if (k > read_ratio)
		{
			return std::make_pair(REMOVE, workload->Next() % _conf.init_keys + 1);
		}
		else
		{
			return std::make_pair(GET, workload->Next() % _conf.init_keys + 1);
		}
	}
};

class UIBench : public Benchmark
{
public:
	RandomGenerator rdm;
	RandomGenerator *salt;
	int update_ratio = 90;
	UIBench(Config &conf) : Benchmark(conf)
	{
		update_ratio = conf.read_ratio;
		salt = new RandomGenerator();
	}
	virtual std::pair<OperationType, long long> nextOperation()
	{
		int k = rdm.randomInt() % 100;
		if (k < update_ratio)
		{
			long long d = workload->Next() % _conf.init_keys + 1;
			return std::make_pair(UPDATE, d * INTERVAL);
		}
		else
		{
			long long d = workload->Next() % _conf.init_keys + 1;
			long long x = salt->Next() % (INTERVAL - 1) + 1;
			return std::make_pair(INSERT, d * INTERVAL + x);
		}
	}
	long long nextInitKey()
	{
		return INTERVAL * Benchmark::nextInitKey();
	}
};

class IDBench : public Benchmark
{
public:
	RandomGenerator rdm;
	RandomGenerator *salt;
	int insert_ratio = 50;
	IDBench(Config &conf) : Benchmark(conf)
	{
		insert_ratio = conf.read_ratio;
		salt = new RandomGenerator();
	}
	virtual std::pair<OperationType, long long> nextOperation()
	{
		int k = rdm.randomInt() % 100;
		if (k > insert_ratio)
		{
			long long d = workload->Next() % _conf.init_keys + 1;
			return std::make_pair(REMOVE, d * INTERVAL);
		}
		else
		{
			long long d = workload->Next() % _conf.init_keys + 1;
			long long x = salt->Next() % (INTERVAL - 1) + 1;
			return std::make_pair(INSERT, d * INTERVAL + x);
		}
	}
	long long nextInitKey()
	{
		return INTERVAL * Benchmark::nextInitKey();
	}
};
class YSCBC : public Benchmark
{
public:
	YSCBC(Config &conf) : Benchmark(conf)
	{
	}
	OperationType nextOp()
	{
		return GET;
	}
};

class YSCBD : public Benchmark
{
public:
	YSCBD(Config &conf) : Benchmark(conf)
	{
	}
	OperationType nextOp()
	{
		return GET;
	}
};

class YSCBE : public Benchmark
{
public:
	YSCBE(Config &conf) : Benchmark(conf)
	{
	}
	OperationType nextOp()
	{
		return GET;
	}
};

#endif
