#include "btree.h"
#include "iter.h"

namespace fishdb
{

Iterator::Iterator(BTree *btree)
{
    m_btree = btree;
    m_kv_idx = -1;
    m_valid = false;
}

void Iterator::SeekToFirst()
{
    auto now = m_btree->m_root;
    m_stack.push_back(now);

    while (!now->is_leaf)
    {
        now = m_btree->ReadPage(now->children[0]);
        m_stack.push_back(now);
    }
    if (now->kvs.size() > 0)
    {
        m_valid = true;
        m_kv_idx = 0;
    }
    else
    {
        m_valid = false;
        m_stack.clear();
    }
}

void Iterator::SeekToLast()
{
    auto now = m_btree->m_root;
    m_stack.push_back(now);

    while (!now->is_leaf)
    {
        now = m_btree->ReadPage(now->children.back());
        m_stack.push_back(now);
    }
    if (now->kvs.size() > 0)
    {
        m_valid = true;
        m_kv_idx = now->kvs.size() - 1;
    }
    else
    {
        m_valid = false;
        m_stack.clear();
    }
}

void Iterator::Seek(const char *k)
{
    std::string key = k;
    auto now = m_btree->m_root;

    bool found_upper = false;
    while (now != NULL)
    {
        m_stack.push_back(now);
        auto iter = m_btree->LowerBound(now, key);
        size_t p = iter - now->kvs.begin();
        if (iter != now->kvs.end())
        {
            found_upper = true;
            m_kv_idx = p;
            now = m_btree->ReadPage(now->children[p]);
        }
        else if (!now->is_leaf)
        {
            now = m_btree->ReadPage(now->children[p]);
        }
        else
        {
            break;
        }
    }

    m_valid = found_upper;
    if (!m_valid)
        m_stack.clear();
}

void Iterator::Next()
{
    assert(Valid());

    auto now = m_stack.back();
    std::string cur_key = now->kvs[m_kv_idx].key;

    // 1. we are leaf
    if (now->is_leaf)
    {
        // 1.a leaf have more kv
        if (m_kv_idx < (int)now->kvs.size())
        {
            m_kv_idx++;
            return;
        }

        // 1.b no more kv, find first parent which has more kv
        while (m_stack.size() > 0)
        {
            auto nd = m_stack.back();
            auto iter = m_btree->UpperBound(nd, cur_key);
            if (iter != nd->kvs.end())
            {
                m_kv_idx = iter - nd->kvs.begin();
                return;
            }
            else
                m_stack.pop_back();
        }
    }
    // 2. we are inner node
    else
    {
        int p = m_kv_idx + 1;
        now = m_btree->ReadPage(now->children[p]);
        m_stack.push_back(now);
        while (!now->is_leaf)
        {
            now = m_btree->ReadPage(now->children[0]);
            m_stack.push_back(now);
        }
        m_kv_idx = 0;
        return;
    }

    // invalid if came here
    m_valid = false;
    m_kv_idx = -1;
    return;
}

bool Iterator::Valid()
{
    return m_valid;
}

std::string Iterator::Key()
{
    assert(Valid());
    auto &node = m_stack.back();
    return node->kvs[m_kv_idx].key;
}

std::string Iterator::Value()
{
    assert(Valid());
    auto &node = m_stack.back();
    return node->kvs[m_kv_idx].value;
}

}

