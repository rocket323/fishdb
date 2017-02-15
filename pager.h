#ifndef PAGER_H_
#define PAGER_H_

#include <map>
#include <string>
#include <vector>
#include <memory>
#include <assert.h>
#include "util.h"

namespace fishdb
{

static const int PAGE_SIZE = 512;
static const int MAX_PAGE_CACHE = 1000;

enum PageType
{
    META_PAGE = 1,
    TREE_PAGE = 2,
    OF_PAGE = 3,
    FREE_PAGE = 4,
};

struct DBHeader
{
    int64_t free_list;
    int32_t total_pages;
    int32_t of_pages;
    int32_t page_size;
    int64_t root_page_no;
};

struct PageHeader
{
    int64_t page_no;
    int8_t type;
    int64_t next_free;
    int64_t of_page_no;
    int32_t data_size;
    int16_t page_cnt;
    int8_t is_leaf;
};

struct Page
{
    PageHeader header;
    char data[PAGE_SIZE];
};

struct KV
{
    std::string key;
    std::string value;
    KV(const std::string &_key, const std::string &_value):
        key(_key), value(_value) {}
};
typedef std::vector<KV>::iterator KVIter;
static const int PH_SIZE = sizeof(PageHeader);
static const int PAGE_CAPA = PAGE_SIZE - sizeof(PageHeader);

struct MemPage
{
    PageHeader header;
    std::vector<int64_t> children;
    std::vector<KV> kvs;
    bool stick;
    bool is_leaf;
    std::string data;

    MemPage *lru_next;
    MemPage *lru_prev;

    void SetData(char *buf, int size)
    {
        data.assign(buf, size);
    }

    void Serialize(char *buf, int32_t &size)
    {
        char *sp = buf;
        header.is_leaf = is_leaf;
        memcpy(buf, (void *)&header, sizeof(PageHeader));
        buf += sizeof(PageHeader);

        if (header.type == TREE_PAGE)
        {
            buf += EncodeInt32(buf, children.size());
            for (size_t i = 0; i < children.size(); ++i)
                buf += EncodeInt64(buf, children[i]);
            buf += EncodeInt32(buf, kvs.size());
            for (size_t i = 0; i < kvs.size(); ++i)
            {
                buf += EncodeString(buf, kvs[i].key);
                buf += EncodeString(buf, kvs[i].value);
            }
        }
        size = buf - sp;
        assert(size <= PAGE_SIZE * 100);
    }
    void Parse(char *buf, int32_t &size)
    {
        char *sp = buf;
        memcpy((void *)&header, buf, sizeof(PageHeader));
        buf += sizeof(PageHeader);
        is_leaf = header.is_leaf;

        if (header.type == TREE_PAGE)
        {
            int32_t num;
            buf += DecodeInt32(buf, num);
            for (int i = 0; i < num; ++i)
            {
                int64_t c;
                buf += DecodeInt64(buf, c);
                children.push_back(c);
            }
            buf += DecodeInt32(buf, num);
            for (int i = 0; i < num; ++i)
            {
                std::string k, v;
                buf += DecodeString(buf, k);
                buf += DecodeString(buf, v);
                kvs.push_back(KV(k, v));
            }
        }
        size = buf - sp;
        assert(size <= PAGE_SIZE * 100);
    }
};

class Pager
{
public:
    int Init(std::string file);
    void Close();

    std::shared_ptr<MemPage> GetRoot();
    void SetRoot(int64_t root_page_no);
    std::shared_ptr<MemPage> NewPage(PageType type = TREE_PAGE);
    std::shared_ptr<MemPage> GetPage(int64_t page_no, PageType type = TREE_PAGE, bool stick = false);
    void FlushPage(std::shared_ptr<MemPage> mp);
    int FreePage(std::shared_ptr<MemPage> mp);

    void Prune(int size_limit = MAX_PAGE_CACHE, bool force = false);

protected:
    void Attach(MemPage *mp);
    void Detach(MemPage *mp);
    void TouchPage(std::shared_ptr<MemPage> mp);
    void CachePage(std::shared_ptr<MemPage> mp);
    void WriteFile(char *buf, const int len, int64_t offset);

public:
    std::map<int64_t, std::shared_ptr<MemPage>> m_pages;
    DBHeader *m_db_header;
    MemPage *m_head;
    MemPage *m_tail;
    FILE *m_file;
    int m_file_size;
};

static const std::shared_ptr<MemPage> nil;

}

#endif

