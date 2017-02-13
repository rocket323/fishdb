#ifndef CURSOR_H_
#define CURSOR_H_

#include <vector>
#include <memory>

namespace fishdb
{

class BTree;
struct MemPage;

class Iterator
{
public:
    Iterator(BTree *btree);

    void SeekToFirst();
    void SeekToLast();
    void Seek(const char *k);
    void Next();
    bool Valid();

    std::string Key();
    std::string Value();

private:
    BTree *m_btree;
    std::vector<std::shared_ptr<MemPage>> m_stack;
    int m_kv_idx;
    bool m_valid;
};

}

#endif


