#include "util.h"
#include <getopt.h>
#include <string>
#include <iostream>
#include <fstream>

using namespace std;

static struct option dataopts[] = {
	{"help", no_argument, NULL, 'h'},
	{"type", required_argument, NULL, 't'},
	{"filename", required_argument, NULL, 'f'},
	{"skew", required_argument, NULL, 's'},
	{"datasize", required_argument, NULL, 'd'},
};

enum DataDistrubute
{
	RANDOM,
	ZIPFIAN,
	_DataDistrbuteNumber
};

DataDistrubute distribute = RANDOM;
std::string filename;
float zipfp;
unsigned long long datasize = 1llu << 28; // 64MB input;

static const uint64_t kFNVPrime64 = 1099511628211;
static inline unsigned int hashfunc(uint32_t val)
{
	uint32_t hash = 123;
	int i;
	for (i = 0; i < sizeof(uint32_t); i++)
	{
		uint64_t octet = val & 0x00ff;
		val = val >> 8;

		hash = hash ^ octet;
		hash = hash * kFNVPrime64;
	}
	return hash;
}

int main(int argc, char **argv)
{
	int number = 16000000;
	ZipfGenerator zipf(0.99, number);
	int *count = new int[number]; 
	for (unsigned long long i = 0; i < number; i++)
    {
		int d = zipf.randomInt();
		// printf("key:%lld\n", d);
		count[d]++;
    }
	for (int i=0;i<10;++i)
	{
		printf("%d:%lld\n",i,count[i]);
	}

}
