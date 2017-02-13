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

    void Serialize(char *buf, int32_t &size);
    void Parse(const char *buf, int32_t size);
};

class Pager
{
public:
    int Init(std::string file);
    int Close();

    std::shared_ptr<MemPage> NewPage(PageType type);
    std::shared_ptr<MemPage> GetRootPage();
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

