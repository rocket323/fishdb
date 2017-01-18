#include "btree.h"
#include "iterator.h"

Iterator::Iterator(std::shared_ptr<BtNode> root)
{
    m_root = root;
    m_kv_idx = -1;
    m_valid = false;
}

void Iterator::SeekToFirst()
{
    auto now = m_root;
    m_stack.push_back(m_root);

    while (!now->is_leaf)
    {
        now = m_btree->DiskRead(now->children[0]);
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
    auto now = m_root;
    m_stack.push_back(m_root);

    while (!now->is_leaf)
    {
        now = m_btree->DiskRead(now->children.back());
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
    auto now = m_root;

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
            now = m_btree->DiskRead(now->children[p]);
        }
        else if (!now->is_leaf)
        {
            now = m_btree->DiskRead(now->children[p]);
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
    auto &node = m_stack.back();

    // 1. can still take one step in current node
    if (m_kv_idx < node.kvs.size())
    {
        m_kv_idx++;
        return;
    }

    // find first parent that has larger key
    while (true)
    {
        auto &now = m_stack.back();
        if (m_kv_idx < now.kvs.size())
        {
            m_kv_idx++;
            return;
        }


    }
}

bool Iterator::Valid()
{
    return m_valid;
}

std::string Iterator::Key()
{
    assert(Valid());
    auto &node = m_stack.back();
    return node.kvs[m_kv_idx].key;
}

std::string Iterator::Value()
{
    assert(Valid());
    auto &node = m_stack.back();
    return node.kvs[m_kv_idx].value;
}

