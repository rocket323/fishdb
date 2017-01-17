#include "btree.h"
#include <stdlib.h>
#include <queue>
#include <sstream>

namespace fishdb
{
std::shared_ptr<BtNode> nil;

BTree::BTree(CmpFunc cmp_func, int min_key_num):
     m_min_key_num(min_key_num), m_cmp_func(cmp_func)
{
}

int BTree::Open(std::string dbfile)
{
    m_root = AllocNode();
    m_root->is_leaf = true;
	return BT_OK;
}

void BTree::Close()
{
}

std::shared_ptr<BtNode> BTree::AllocNode()
{
    return m_cache.AllocNode();
}

KVIter BTree::GetUpperIter(std::shared_ptr<BtNode> node, std::string &key)
{
    for (auto iter = node->kvs.begin(); iter != node->kvs.end(); ++iter)
    {
        if (!m_cmp_func(iter->key, key))
            return iter;
    }
    return node->kvs.end();
}

int BTree::Get(const char *k, std::string &data)
{
    std::string key = k;
    auto now = m_root;
    while (now != NULL)
    {
        auto iter = GetUpperIter(now, key);
        size_t p = iter - now->kvs.begin();
        if (iter != now->kvs.end() && Equal(iter->key, key))
        {
            data = iter->value;
            return BT_OK;
        }
        else if (!now->is_leaf)
			now = DiskRead(now->children[p]);
		else
			return BT_NOT_FOUND;
    }
    return BT_ERROR;
}

int BTree::Put(const char *k, const char *v)
{
    std::string key = k, data = v;
    Insert(m_root, nil, 0, key, data);
    return BT_OK;
}

void BTree::Insert(std::shared_ptr<BtNode> now, std::shared_ptr<BtNode> parent, int upper_idx,
        std::string &key, std::string &data)
{
    auto iter = GetUpperIter(now, key);
    size_t p = iter - now->kvs.begin();
    if (iter != now->kvs.end() && Equal(iter->key, key))
        iter->value = data;
    else if (!now->is_leaf)
		Insert(DiskRead(now->children[p]), now, p, key, data);
	else
		now->kvs.insert(iter, KV(key, data));

    if ((int)now->kvs.size() <= 2 * m_min_key_num) return;
    // split full
    size_t mid = now->kvs.size() / 2;
    auto left = AllocNode();
    auto right = AllocNode();

	left->is_leaf = right->is_leaf = now->is_leaf;
	left->kvs.assign(now->kvs.begin(), now->kvs.begin() + mid);
	right->kvs.assign(now->kvs.begin() + mid + 1, now->kvs.end());
	if (!now->is_leaf)
	{
		left->children.assign(now->children.begin(), now->children.begin() + mid + 1);
		right->children.assign(now->children.begin() + mid + 1, now->children.end());
	}

    if (!parent)
    {
        m_root = AllocNode();
        m_root->is_leaf = false;
        parent = m_root;
		assert(upper_idx == 0);
		parent->children.resize(upper_idx + 1);
    }
	assert(upper_idx < (int)parent->children.size());
    parent->kvs.insert(parent->kvs.begin() + upper_idx, now->kvs[mid]);
    parent->children[upper_idx] = left->offset;
    parent->children.insert(parent->children.begin() + upper_idx + 1, right->offset);
}

int BTree::Del(const char *k)
{
    std::string key = k;
    return Delete(m_root, nil, -1, key);
}

int BTree::Delete(std::shared_ptr<BtNode> now, std::shared_ptr<BtNode> parent, int child_idx, std::string &key)
{
    auto iter = GetUpperIter(now, key);
    size_t p = iter - now->kvs.begin();

	int del_ret = BT_NOT_FOUND;
    if (iter != now->kvs.end() && Equal(iter->key, key))
    {
        if (!now->is_leaf)
        {
			auto left = DiskRead(now->children[p]);
            auto nd = left;
            while (!nd->is_leaf)
                nd = DiskRead(nd->children.back());

            now->kvs[p] = nd->kvs.back();
			Delete(left, now, p, nd->kvs.back().key);
        }
        else
            now->kvs.erase(iter);
		del_ret = BT_OK;
    }
    else if (!now->is_leaf)
		del_ret = Delete(DiskRead(now->children[p]), now, p, key);

	// maintain now
	if (now == m_root)
	{
		if (now->is_leaf || now->kvs.size() > 0) return del_ret;
		assert(now->children.size() == 1);
		m_root = DiskRead(now->children[0]);
	}
	else if ((int)now->kvs.size() < m_min_key_num)
		Maintain(now, parent, child_idx);

	return del_ret;
}

void BTree::Maintain(std::shared_ptr<BtNode> now, std::shared_ptr<BtNode> parent, int child_idx)
{
	size_t left_sep = (child_idx > 0) ? child_idx - 1 : -1;
	size_t right_sep = (child_idx < (int)parent->children.size() - 1) ? child_idx : -1;
	auto left = (child_idx > 0) ? DiskRead(parent->children[child_idx - 1]) : nil;
	auto right = (child_idx < (int)parent->children.size() - 1) ? DiskRead(parent->children[child_idx + 1]) : nil;

	// 1.
    if (left && (int)left->kvs.size() > m_min_key_num)
    {
        now->kvs.insert(now->kvs.begin(), parent->kvs[left_sep]);
        if (!left->is_leaf)
        {
            now->children.insert(now->children.begin(), left->children.back());
            left->children.pop_back();
        }
        parent->kvs[left_sep] = left->kvs.back();
        left->kvs.pop_back();
        return;
    }
	// 2.
    if (right && (int)right->kvs.size() > m_min_key_num)
    {
        now->kvs.push_back(parent->kvs[right_sep]);
        if (!right->is_leaf)
        {
            now->children.push_back(right->children.front());
            right->children.erase(right->children.begin());
        }
        parent->kvs[right_sep] = right->kvs.front();
        right->kvs.erase(right->kvs.begin());
        return;
    }
	// 3a.
    if (left)
    {
        left->kvs.push_back(parent->kvs[left_sep]);
		left->kvs.insert(left->kvs.end(), now->kvs.begin(), now->kvs.end());
		left->children.insert(left->children.end(), now->children.begin(), now->children.end());

        parent->kvs.erase(parent->kvs.begin() + left_sep);
        parent->children.erase(parent->children.begin() + left_sep + 1);
    }
	// 3b.
    else if (right)
    {
        now->kvs.push_back(parent->kvs[right_sep]);
		now->kvs.insert(now->kvs.end(), right->kvs.begin(), right->kvs.end());
		now->children.insert(now->children.end(), right->children.begin(), right->children.end());

        parent->kvs.erase(parent->kvs.begin() + right_sep);
        parent->children.erase(parent->children.begin() + right_sep + 1);
    }
    else
        assert(false);
}

bool BTree::Equal(const std::string &a, const std::string &b)
{
    return !m_cmp_func(a, b) && !m_cmp_func(b, a);
}

std::string BTree::Keys(BtNode *node)
{
    std::ostringstream out;
    out << "[";
    for (size_t i = 0; i < node->kvs.size(); ++i)
    {
        out << node->kvs[i].key << ((i == node->kvs.size() - 1) ? "" : " ");
    }
    out << "]";

    return out.str();
}

std::string BTree::Childen(BtNode *node)
{
    std::ostringstream out;
    out << "[";
    for (size_t i = 0; i < node->children.size(); ++i)
    {
        out << node->children[i] << ((i == node->children.size() - 1) ? "" : " ");
    }
    out << "]";

    return out.str();
}

void BTree::Print(std::shared_ptr<BtNode> now)
{
    printf("%"PRId64", %s -> %s\n", now->offset, Keys(now.get()).c_str(), Childen(now.get()).c_str());
    for (size_t i = 0; i < now->children.size(); ++i)
    {
        auto child = DiskRead(now->children[i]);
        Print(DiskRead(now->children[i]));
    }
}

void BTNode::Serialize(char buf[], int &len)
{
    void *p = buf;
    *((int64_t *)p) = page_no;
    p += sizeof(int64_t);
    *((bool *)p) = is_leaf;
    p += sizeof(bool);

}

void BTNode::Parse(char buf[])
{
}

}

