#ifndef PAGE_H_
#define PAGE_H_

#include <string>
#include <vector>
#include <memory>
#include <assert.h>

namespace fishdb
{
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
    int64_t root_page;
    int64_t total_pages;
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
static const int PG_SIZE = 512;
static const int PH_SIZE = sizeof(PageHeader);
static const int PAGE_CAPA = PG_SIZE - sizeof(PageHeader);

struct KV
{
    std::string key;
    std::string value;
    KV(const std::string &_key, const std::string &_value):
        key(_key), value(_value) {}
};
typedef std::vector<KV>::iterator KVIter;

struct Page
{
    PageHeader header;
    char data[PAGE_CAPA];
};

struct MemPage
{
    PageHeader header;
    std::string data;

    std::vector<int64_t> children;
    std::vector<KV> kvs;
    bool stick;
    bool is_leaf;
    MemPage *lru_next;
    MemPage *lru_prev;

public:
    void Clear();
    void Feed(const char *buf, int size);
    void Serialize(char *buf, int &size);
    void Parse();
};

}

#endif

