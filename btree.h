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
#include "iter.h"

namespace fishdb
{

static const int BT_OK = 0;
static const int BT_ERROR = -1;
static const int BT_NOT_FOUND = -2;

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

class Iterator;

class BTree
{
public:
    friend class Iterator;
    typedef std::function<bool(const std::string &, const std::string &)> CmpFunc;

    BTree(CmpFunc cmp_func = DefaultCmp(), int min_key_num = BT_DEFAULT_KEY_NUM);
    int Open(std::string dbfile);
    void Close();

    int Get(const char *key, std::string &data);
    int Put(const char *key, const char *data);
    int Del(const char *key);
    int Get(const std::string &key, std::string &data);
    int Put(const std::string &key, const char *data);
    int Del(const std::string &key);
    Iterator *NewIterator();

protected:
    bool Less(std::string &a, std::string &b);
    bool Equal(const std::string &a, const std::string &b);
    KVIter LowerBound(std::shared_ptr<MemPage> mp, std::string &key);
    KVIter UpperBound(std::shared_ptr<MemPage> mp, std::string &key);

    void Insert(std::shared_ptr<MemPage> now, std::shared_ptr<MemPage> parent,
            int upper_idx, std::string &key, std::string &data);
    int Delete(std::shared_ptr<MemPage> now, std::shared_ptr<MemPage> parent,
            int child_idx, std::string &key);
    void Maintain(std::shared_ptr<MemPage> now, std::shared_ptr<MemPage> parent, int child_idx);

	std::string Keys(MemPage *mp);
	std::string Childen(MemPage *mp);
	void Print(std::shared_ptr<MemPage> now);

private:
    Pager m_pager;
    int m_min_key_num;
    CmpFunc m_cmp_func;
    std::shared_ptr<MemPage> m_root;
};

}

#endif

