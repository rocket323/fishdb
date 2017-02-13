#ifndef PAGER_H_
#define PAGER_H_

#include <string>
#include <vector>
#include <memory>

namespace fishdb
{

static const int PAGE_SIZE = 512;
static const int MAX_PAGE_CACHE = 1000;

enum PageType
{
    HEADER_PAGE = 0,
    BRANCH_PAGE = 1,
    LEAF_PAGE = 2,
    OF_PAGE = 3,
    FREE_PAGE = 4,
};

struct DBHeader
{
    int64_t free_list;
    int32_t total_pages;
    int32_t of_pages;
    int page_size;
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

    void Serialize(char *buf, int32_t &size)
    {
        char *sp = buf;
        buf += EncodeInt64(page_no);
        buf += EncodeInt8(type);
        buf += EncodeInt64(next_free);
        buf += EncodeInt64(of_page_no);
        buf += EncodeInt32(data_size);
        buf += EncodeInt16(page_cnt);
        size = buf - sp;
    }
    void Parse(const char *buf, int32_t &size)
    {
        buf += DecodeInt64(buf, page_no);
        buf += DecodeInt8(buf, type);
        buf += DecodeInt64(buf, next_free);
        buf += DecodeInt64(buf, of_page_no);
        buf += DecodeInt32(buf, data_size);
        buf += DecodeInt16(buf, page_cnt);
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

struct MemPage
{
    PageHeader header;
    std::vector<int64_t> children;
    std::vector<KV> kvs;
    std::string data;
    bool stick;
    bool is_leaf;

    MemPage *lru_next;
    MemPage *lru_prev;

    void Serialize(char *buf, int32_t &size)
    {
        char *sp = buf;
        int32_t header_size = 0;
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
    void Parse(const char *buf, int32_t &size)
    {
        char *sp = buf;
        int32_t header_size = 0;
        header.Parse(buf, header_size);
        buf += header_size;

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
    int Close();

    std::shared_ptr<MemPage> NewPage(PageType type);
    std::shared_ptr<MemPage> GetRootPage();
    void SetRoot(int64_t root_page_no);
    std::shared_ptr<MemPage> GetPage(int64_t page_no, bool stick = false);
    void FlushPage(int64_t page_no, std::shared_ptr<MemPage> mp);
    int FreePage(std::shared_ptr<MemPage> mp);

protected:
    void Prune(int size_limit = MAX_PAGE_CACHE);
    void Attach(std::shared_ptr<MemPage> mp);
    void Detach(std::shared_ptr<MemPage> mp);
    void TouchPage(std::shared_ptr<MemPage> mp);
    void CachePage(std::shared_ptr<MemPage> mp);

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

