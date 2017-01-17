#ifndef BTREE_H_
#define BTREE_H_

#include <string>
#include <vector>
#include <cstring>
#include <algorithm>
#include <functional>
#include <memory>
#include <map>
#include <assert.h>
#include <sstream>

namespace fishdb 
{

static const int BT_OK = 0;
static const int BT_ERROR = -1;
static const int BT_NOT_FOUND = -2;

static const int BT_BLOCK_SIZE = 4096;
static const int BT_DEFAULT_KEY_NUM = 2;

class BTreeIter;

struct BtNode
{
    bool is_leaf;
	int64_t offset;
    std::vector<std::pair<std::string, std::string>> kvs;
    std::vector<int64_t> children;
};

typedef std::vector<std::pair<std::string, std::string>>::iterator KvIter;

struct DefaultCmp
{
    bool operator()(const std::string &a, const std::string &b)
    {
        int min_len = std::min(a.length(), b.length());
        if (min_len == 0) return false;
        int result = memcmp(a.data(), b.data(), min_len);
        if (result != 0) return result < 0;
        return a.length() < b.length();
    }
};

class NodeCache
{
public:
    NodeCache(): m_next_node_id(0) {}

    std::shared_ptr<BtNode> AllocNode()
    {
        auto &node = m_pages[m_next_node_id++];
        node = std::make_shared<BtNode>();
        node->offset = m_next_node_id - 1;
        return node;
    }

    std::shared_ptr<BtNode> GetNode(size_t offset)
    {
        auto iter = m_pages.find(offset);
        assert(iter != m_pages.end());
        return iter->second;
    }

    void WriteNode(size_t offset, const char * data)
    {
    }
private:
    uint64_t m_next_node_id;
    std::map<size_t, std::shared_ptr<BtNode>> m_pages;
};

class BTree
{
public:
    typedef std::function<bool(const std::string &, const std::string &)> CmpFunc;

    BTree(CmpFunc cmp_func, int max_key_num = BT_DEFAULT_KEY_NUM);
    int Open(std::string dbfile);
    void Close();

    int Get(const char *key, std::string &data);
    int Put(const char *key, const char *data);
    int Del(const char *key);

protected:
    bool Equal(const std::string &a, const std::string &b);
    std::shared_ptr<BtNode> DiskRead(size_t offset);
    void DiskWrite(size_t offset, const BtNode &node);

    KvIter GetUpperIter(std::shared_ptr<BtNode> node, std::string &key);
    std::shared_ptr<BtNode> AllocNode();
    void Insert(std::shared_ptr<BtNode> now, std::shared_ptr<BtNode> parent,
            int upper_idx, std::string &key, std::string &data);
    int Delete(std::shared_ptr<BtNode> now, std::shared_ptr<BtNode> parent, int child_idx, std::string &key);
    void Maintain(std::shared_ptr<BtNode> now, std::shared_ptr<BtNode> parent, int child_idx);

	std::string Keys(BtNode *node)
	{
		std::ostringstream out;
		out << "[";
		for (size_t i = 0; i < node->kvs.size(); ++i)
		{
			out << node->kvs[i].first << ((i == node->kvs.size() - 1) ? "" : " ");
		}
		out << "]";

		return out.str();
	}

	std::string Childen(BtNode *node)
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

	void Print(std::shared_ptr<BtNode> now)
	{
		printf("%lld, %s -> %s\n", now->offset, Keys(now.get()).c_str(), Childen(now.get()).c_str());
		for (size_t i = 0; i < now->children.size(); ++i)
		{
			auto child = DiskRead(now->children[i]);
			Print(DiskRead(now->children[i]));
		}
	}

private:
    NodeCache m_cache;
    int m_min_key_num;
    CmpFunc m_cmp_func;

public:
    std::shared_ptr<BtNode> m_root;
};

class BTreeIter
{
public:
};

}

#endif

