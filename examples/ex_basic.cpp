#include <iostream>
#include "btree.h"

using namespace fishdb;

int main()
{
    const char *fname = "test.fdb";

    auto bt = BTree::Open(fname);
    if (bt == NULL)
    {
        printf("open db[%s] failed", fname);
        return -1;
    }

    std::string val = "hello world";
    bt->Put("greet", val);

    std::string value;
    bt->Get("greet", value);
    std::cout << "greet: " << value << std::endl;

    bt->Close();
    delete bt;
    return 0;
}

