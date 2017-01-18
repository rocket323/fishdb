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
#include <inttypes.h>
#include "pager.h"

namespace fishdb 
{

static const int BT_OK = 0;
static const int BT_ERROR = -1;
static const int BT_NOT_FOUND = -2;

static const int BT_BLOCK_SIZE = 4096;
static const int BT_DEFAULT_KEY_NUM = 2;

class BTreeIter;

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

struct KV
{
    std::string key;
    std::string value;
    KV(const std::string &_key, const std::string &_value):
        key(_key), value(_value) {}
};
typedef std::vector<KV>::iterator KVIter;

struct BtNode
{
    int64_t page_no;
    bool is_leaf;
    std::vector<int64_t> children;
    std::vector<KV> kvs;

    void Serialize(char *buf, int &len);
    void Parse(char *buf, int len);
};

class Iterator;

class BTree
{
public:
    friend class Iterator;
    typedef std::function<bool(const std::string &, const std::string &)> CmpFunc;

    BTree(CmpFunc cmp_func, int max_key_num = BT_DEFAULT_KEY_NUM);
    int Open(std::string dbfile);
    void Close();

    int Get(const char *key, std::string &data);
    int Put(const char *key, const char *data);
    int Del(const char *key);

    Iterator * NewIterator();
    bool Less(std::string &a, std::string &b);
    bool Equal(const std::string &a, const std::string &b);
    KVIter LowerBound(std::shared_ptr<BtNode> node, std::string &key);
    KVIter UpperBound(std::shared_ptr<BtNode> node, std::string &key);

protected:
    std::shared_ptr<BtNode> AllocNode();
    std::shared_ptr<BtNode> DiskRead(size_t page_no);
    void DiskWrite(size_t page_no, const BtNode &node);

    void Insert(std::shared_ptr<BtNode> now, std::shared_ptr<BtNode> parent,
            int upper_idx, std::string &key, std::string &data);
    int Delete(std::shared_ptr<BtNode> now, std::shared_ptr<BtNode> parent,
            int child_idx, std::string &key);
    void Maintain(std::shared_ptr<BtNode> now, std::shared_ptr<BtNode> parent, int child_idx);

	std::string Keys(BtNode *node);
	std::string Childen(BtNode *node);
	void Print(std::shared_ptr<BtNode> now);

private:
    Pager m_pager;
    int m_min_key_num;
    CmpFunc m_cmp_func;

public:
    std::shared_ptr<BtNode> m_root;
};

}

#endif

