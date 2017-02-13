#include "btree.h"

BTree::BTree(CmpFunc cmp_func, int min_key_num):
    m_min_key_num(min_key_num), m_cmp_func(cmp_func)
{
}

int BTree::Open(std::string dbfile)
{
    int ret = m_pager.Init(dbfile);
    if (ret)
        return ret;
    m_root = m_pager.GetRootPage();
    return BT_OK;
}

void BTree::Close()
{
    m_pager.Close();
}

Iterator * BTree::NewIterator()
{
    Iterator *iter = new Iterator(this);
    return iter;
}

bool BTree::Less(std::string &a, std::string &b)
{
    return m_cmp_func(a, b);
}

bool BTree::Equal(const std::string &a, const std::string &b)
{
    return !m_cmp_func(a, b) && !m_cmp_func(b, a);
}

KVIter BTree::LowerBound(std::shared_ptr<MemPage> mp, std::string &key)
{
    for (auto iter = mp->kvs.begin(); iter != mp->kvs.end(); ++iter)
    {
        if (!m_cmp_func(iter->key, key))
            return iter;
    }
    return mp->kvs.end();
}

KVIter BTree::UpperBound(std::shared_ptr<MemPage> mp, std::string &key)
{
    for (auto iter = mp->kvs.begin(); iter != mp->kvs.end(); ++iter)
    {
        if (m_cmp_func(key, iter->key))
            return iter;
    }
    return mp->kvs.end();
}

int BTree::Get(const char *key, std::string &data)
{
}

int BTree::Put(const char *key, const char *data)
{
}

int BTree::Del(const char *key)
{
}

void BTree::Insert(std::shared_ptr<MemPage> now, std::shared_ptr<MemPage> parent,
        int upper_idx, std::string &key, std::string &data)
{
}

int BTree::Delete(std::shared_ptr<MemPage> now, std::shared_ptr<MemPage> parent,
        int child_idx, std::string &key)
{
}

void BTree::Maintain(std::shared_ptr<MemPage> now, std::shared_ptr<MemPage> parent, int child_idx)
{
}

std::string BTree::Keys(MemPage *mp)
{
}

std::string BTree::Childen(MemPage *mp)
{
}

void BTree::Print(std::shared_ptr<MemPage> now)
{
}

