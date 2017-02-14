#include <iostream>
#include "btree.h"

using namespace fishdb;

int main(int argc, char **argv)
{
    const char *fname = argv[1];

    auto bt = BTree::Open(fname);
    if (bt == NULL)
    {
        printf("open db[%s] failed", fname);
        return -1;
    }

    /*
    bt->Put("name1", "tom");
    bt->Put("name2", "jack");
    bt->Put("name3", "michael");
    bt->Put("greet", "hello world");
    */

    auto iter = bt->NewIterator();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next())
    {
        // std::cout << iter->Key() << ": " << iter->Value() << std::endl;
        std::cout << iter->Key() << std::endl;
    }
    delete iter;
    bt->Close();
    delete bt;
    return 0;
}

