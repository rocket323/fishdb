#include <iostream>
#include "btree.h"

using namespace fishdb;

int main()
{
    const char *fname = "test.fdb";

    BTree bt;
    int ret = bt.Open(fname);
    if (ret != 0)
    {
        printf("open db[%s] failed, ret=%d\n", fname, ret);
        return ret;
    }

    bt.Put("name1", "tom");
    bt.Put("name2", "jack");
    bt.Put("name3", "michael");
    bt.Put("greet", "hello world");

    auto iter = bt.NewIterator();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next())
    {
        std::cout << iter->Key() << ": " << iter->Value() << std::endl;
    }
    delete iter;

    bt.Close();
    return 0;
}

