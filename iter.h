#ifndef CURSOR_H_
#define CURSOR_H_

#include <vector>
#include <memory>

namespace fishdb
{

class BTree;
class BtNode;

class Iterator
{
public:
    Iterator();

    int SeekToFirst();
    int SeekToLast();
    int Seek();
    int Next();
    bool Valid();

private:
    std::vector<std::shared_ptr<BtNode>> m_stack;
    int m_kv_idx;
    bool m_valid;
};

}

#endif

