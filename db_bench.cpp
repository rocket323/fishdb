#include "btree.h"
#include "pager.h"

/*
 * benchmarks:
 *	   fillseq			-- write N values in sequential order in async mode
 *	   fillrandom		-- write N values in random key order in async mode
 *	   readseq			-- read N times in sequential order
 *	   readrandom		-- read N times in random order
static const char *FLAGS_benchmarks = 
	"fillseq,"
	"fillrando,"
	"readseq,"
	"readrandom,"
	;

static int FLAGS_num = 10000;
static int FLAGS_value_size = 100;
static bool FLAGS_use_existing_db = false;
static const char *FLAGS_db = NULL;

namespace fishdb
{

// helper for quickly generating random data.
class RandomGenerator
{
public:
	RandomGenerator()
	{
	}
private:
	std::string m_data;
};

}

*/

struct A
{
	int a;
	int b;
	int c;
	A() {}
	A(int _a, int _b, int _c): a(_a), b(_b), c(_c) {}
	void Print()
	{
		printf("a: %d, b: %d, c: %d\n", a, b, c);
	}
};

int main(int argc, char **argv)
{
	return 0;
}

