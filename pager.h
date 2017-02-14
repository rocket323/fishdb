#ifndef PAGER_H_
#define PAGER_H_

#include <map>
#include <string>
#include <vector>
#include <memory>
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

    void Serialize(char *buf, int32_t &size)
    {
        char *sp = buf;
        buf += EncodeInt64(buf, page_no);
        buf += EncodeInt8(buf, type);
        buf += EncodeInt64(buf, next_free);
        buf += EncodeInt64(buf, of_page_no);
        buf += EncodeInt32(buf, data_size);
        buf += EncodeInt16(buf, page_cnt);
        buf += EncodeInt8(buf, is_leaf);
        size = buf - sp;
    }
    void Parse(char *buf, int32_t &size)
    {
        char *sp = buf;
        buf += DecodeInt64(buf, page_no);
        buf += DecodeInt8(buf, type);
        buf += DecodeInt64(buf, next_free);
        buf += DecodeInt64(buf, of_page_no);
        buf += DecodeInt32(buf, data_size);
        buf += DecodeInt16(buf, page_cnt);
        buf += DecodeInt8(buf, is_leaf);
        size = buf - sp;
    }
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
    std::string raw;
    bool stick;
    bool is_leaf;

    MemPage *lru_next;
    MemPage *lru_prev;

    void Serialize(char *buf, int32_t &size)
    {
        char *sp = buf;
        int32_t header_size = 0;
        header.is_leaf = is_leaf;
        header.Serialize(buf, header_size);
        buf += header_size;

        buf += EncodeInt32(buf, children.size());
        for (size_t i = 0; i < children.size(); ++i)
            buf += EncodeInt64(buf, children[i]);
        buf += EncodeInt32(buf, kvs.size());
        for (size_t i = 0; i < kvs.size(); ++i)
        {
            buf += EncodeString(buf, kvs[i].key);
            buf += EncodeString(buf, kvs[i].value);
        }
        size = buf - sp;
    }
    void Parse(char *buf, int32_t &size)
    {
        char *sp = buf;
        int32_t header_size = 0;
        header.Parse(buf, header_size);
        buf += header_size;
        is_leaf = header.is_leaf;

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
        size = buf - sp;
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
    std::shared_ptr<MemPage> GetPage(int64_t page_no, bool stick = false);
    void FlushPage(std::shared_ptr<MemPage> mp);
    int FreePage(std::shared_ptr<MemPage> mp);

protected:
    void Prune(int size_limit = MAX_PAGE_CACHE);
    void Attach(MemPage *mp);
    void Detach(MemPage *mp);
    void TouchPage(std::shared_ptr<MemPage> mp);
    void CachePage(std::shared_ptr<MemPage> mp);
    void WriteFile(char *buf, const int len, int64_t offset);

private:
    std::map<int64_t, std::shared_ptr<MemPage>> m_pages;
    DBHeader *m_db_header;
    MemPage *m_head;
    MemPage *m_tail;
    FILE *m_file;
    int m_file_size;
};

}

#endif

