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

    bt.Put("greet", "hello world");

    std::string value;
    bt.Get("greet", value);
    std::cout << "greet: " << value << std::endl;

    bt.Close();
    return 0;
}

