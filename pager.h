#ifndef PAGER_H_
#define PAGER_H_
#include <stdint.h>
#include <memory>
#include <map>

namespace fishdb
{

static const int PAGE_SIZE = 512;

class BtNode;

struct DBHeader
{
    int64_t free_list;
    int32_t total_pages;
    int32_t page_size;
};

enum PageType
{
    SINGLE = 0,
    MULTI = 1,
    OVERFLOW = 2
};

struct PageHeader
{
    int8_t type;
    int16_t page_cnt;
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

    int64_t AllocNode();
    int FreeNode(int64_t page_no);
    int WriteNode(int64_t page_no, std::shared_ptr<BtNode> node);
    int ReadNode(int64_t page_no, std::shared_ptr<BtNode> &node);

protected:
    void Attach(Page *page);
    void Detach(Page *page);
    void EvitPage();
    void CachePage(int64_t page_no, std::shared_ptr<Page> page);

    void WritePage(int64_t page_no, std::shared_ptr<Page> page);
    void ReadPage(int64_t page_no, std::shared_ptr<Page> &page);

    int64_t PageOffset(int64_t page_no)
    {
        return page_no * PAGE_SIZE;
    }

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

