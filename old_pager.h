#ifndef PAGER_H_
#define PAGER_H_
#include <stdint.h>
#include <memory>
#include <map>

namespace fishdb
{

static const int PAGE_SIZE = 512;
static const int MAX_PAGE_CACHE = 100;

class BtNode;

struct DBHeader
{
    int64_t free_list;
    int32_t total_pages;
    int32_t page_size;
};

enum PageType
{
    SINGLE_PAGE = 0,
    MULTI_PGAE = 1,
    OF_PAGE = 2,
};

struct PageHeader
{
    int8_t type;
    int16_t page_cnt;
    int32_t data_size;
    int64_t next_free;
    int64_t of_page_no;
};

struct Page
{
    PageHeader header;
    std::shared_ptr<BtNode> node;
    Page *lru_prev;
    Page *lru_next;
};

class Pager
{
public:
    int Init(std::string fname);
    void Close();

    std::shared_ptr<BtNode> AllocNode();
    int FreeNode(std::shared_ptr<BtNode> node);
    int WriteNode(int64_t page_no, std::shared_ptr<BtNode> node);
    int ReadPage(int64_t page_no, std::shared_ptr<Page> &page);

protected:

    std::shared_ptr<Page> NewSinglePage(int64_t page_no);

    void Attach(std::shared_ptr<Page> page);
    void Detach(std::shared_ptr<Page> page);
    void EvitPage();
    void CachePage(int64_t page_no, std::shared_ptr<Page> page);
    void TouchPage(std::shared_ptr<Page> page);

    void WriteFilePage(int64_t page_no, std::shared_ptr<Page> page);
    void ReadFilePage(int64_t page_no, std::shared_ptr<Page> &page);

    int64_t PageOffset(int64_t page_no);
    int PageOccupied(int64_t node_size);

private:
    std::map<int64_t, std::shared_ptr<Page>> m_pages;
    DBHeader *m_db_header;
    Page *m_head;
    Page *m_tail;
    FILE *m_file;
    int m_file_size;
};

}

#endif

