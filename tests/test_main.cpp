#include <string>
#include <stdlib.h>
#include <iostream>
#include <sstream>
#include <time.h>
#include "minunit.h"
#include "btree.h"

using namespace fishdb;

BTree *bt = NULL;

MU_TEST(test_btree_simple)
{
    srand(time(NULL));
    int ret = 0;
    std::string value1 = "hello", v1;
    bt->Put("key1", "hello");
    ret = bt->Get("key1", v1);
    mu_check(ret == 0);
    mu_check(value1 == v1);

    std::string value2 = "world", v2;
    bt->Put("key2", "world");
    ret = bt->Get("key2", v2);
    mu_check(ret == 0);
    mu_check(value2 == v2);

    int N = 20;
    for (int i = 0; i < N; ++i)
    {
        int keyNum = rand() % 10000;
        int valNum = rand() % 10000;
        std::ostringstream key_oss;
        std::ostringstream val_oss;
        key_oss << "key" << keyNum;
        val_oss << "val" << valNum;
        std::string key = key_oss.str();
        std::string val = val_oss.str();
        std::cout << key << ' ' << val << std::endl;
        std::string v;
        bt->Put(key, val.c_str());
        int ret = bt->Get(key, v);
        mu_check(ret == 0);
        mu_check(val == v);
    }

    auto iter = bt->NewIterator();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next())
    {
        std::cout << iter->Key() << ": " << iter->Value() << std::endl;
    }
    delete iter;
    bt->Close();
    delete bt;
}

MU_TEST_SUITE(test_suite)
{
    MU_RUN_TEST(test_btree_simple);
}

int main(int argc, char **argv)
{
    bt = BTree::Open("test.fdb");
    if (bt == NULL)
    {
        printf("open test.fdb failed");
        return -1;
    }

    MU_RUN_SUITE(test_suite);
    MU_REPORT();
    return 0;
}

