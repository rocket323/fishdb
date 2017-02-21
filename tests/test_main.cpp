#include <string>
#include <stdlib.h>
#include <iostream>
#include <sstream>
#include <time.h>
#include "minunit.h"
#include "btree.h"

using namespace fishdb;

BTree *bt = NULL;

int ks = 200;

void FillPage(std::shared_ptr<MemPage> mp)
{
    for (int i = 0; i < ks; ++i)
        mp->children.push_back(100);
    for (int i = 0; i < ks; ++i)
        mp->kvs.push_back(KV("hello", "world"));
}

MU_TEST(test_encode)
{
    Pager pager;
    int ret = pager.Init("test3.fdb");
    if (ret != 0)
    {
        printf("open test3.fdb failed\n");
        return;
    }
    srand(time(NULL));

    int N = 1;
    std::vector<int64_t> pgno;
    for (int i = 0; i < N; ++i)
    {
        auto mp = pager.NewPage();
        pgno.push_back(mp->header.page_no);
        FillPage(mp);
    }

    for (int i = 0; i < (int)pgno.size(); ++i)
    {
        auto mp = pager.GetPage(pgno[i]);
        printf("children_size[%zu], kvs_size[%zu]\n", mp->children.size(), mp->kvs.size());
        assert((int)mp->children.size() == ks);
        assert((int)mp->kvs.size() == ks);
    }

    pager.Close();
}

MU_TEST(test_pager)
{
    Pager pager;
    int ret = pager.Init("test1.fdb");
    if (ret != 0)
    {
        printf("open test1.fdb failed\n");
        return;
    }
    srand(time(NULL));

    int N = 2;
    std::vector<int64_t> pgno;
    for (int i = 0; i < N; ++i)
    {
        auto mp = pager.NewPage();
        pgno.push_back(mp->header.page_no);
        FillPage(mp);
    }

    pager.Prune(0, true);

    for (int i = 0; i < (int)pgno.size(); ++i)
    {
        auto mp = pager.GetPage(pgno[i]);
        printf("children_size[%zu], kvs_size[%zu]\n", mp->children.size(), mp->kvs.size());
        assert((int)mp->children.size() == ks);
        assert((int)mp->kvs.size() == ks);
    }

    pager.Close();
}

MU_TEST(test_btree_simple)
{
    bt = BTree::Open("test2.fdb");
    if (bt == NULL)
    {
        printf("open test2.fdb failed\n");
        return;
    }
    srand(time(NULL));

    int N = 50;
    char big_data[6 * 1025];
    for (int i = 0; i < N; ++i)
    {
        int keyNum = rand() % 10000;
        int valNum = rand() % 10000;
        std::ostringstream key_oss;
        std::ostringstream val_oss;
        key_oss << "key" << keyNum;
        val_oss << "val" << valNum;
        std::string key = key_oss.str();
        std::string val(big_data, sizeof(big_data));
        if (i < N / 2)
            val = val_oss.str();

        std::string v;
        bt->Put(key, val);
        int ret = bt->Get(key, v);
        mu_check(ret == 0);
        mu_check(val == v);
    }

    auto iter = bt->NewIterator();
    printf("iterating...\n");
    for (iter->SeekToFirst(); iter->Valid(); iter->Next())
    {
        std::cout << iter->Key() << std::endl;
    }
    delete iter;
    bt->Close();
    delete bt;
}

MU_TEST_SUITE(test_suite)
{
    MU_RUN_TEST(test_btree_simple);
    MU_RUN_SUITE(test_encode);
    MU_RUN_SUITE(test_pager);
}

int main(int argc, char **argv)
{

    MU_RUN_SUITE(test_suite);
    MU_REPORT();
    return 0;
}

