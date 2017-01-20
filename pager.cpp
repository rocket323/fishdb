#include <assert.h>
#include <unistd.h>
#include <cstring>
#include "pager.h"
#include "btree.h"

namespace fishdb
{

std::shared_ptr<Page> page_nil;

int Pager::Init(std::string fname)
{
    m_db_header = new DBHeader();
    m_head = new Page();
    m_tail = new Page();
    m_head->lru_next = m_tail;
    m_tail->lru_prev = m_head;

    m_file = fopen(fname.c_str(), "rb+");
    if (m_file == NULL)
        m_file = fopen(fname.c_str(), "wb+");
    assert(m_file);

    fseek(m_file, 0L, SEEK_END);
    m_file_size = ftell(m_file);
    fseek(m_file, 0, SEEK_SET);

    if (m_file_size < PAGE_SIZE)
    {
        ftruncate(fileno(m_file), PAGE_SIZE);
        m_file_size = PAGE_SIZE;
        fread((void *)m_db_header, sizeof(DBHeader), 1, m_file);
        memset(m_db_header, 0, sizeof(DBHeader));
        m_db_header->free_list = -1;
        m_db_header->total_pages = 1;
    }
    else
    {
        fread((void *)m_db_header, sizeof(DBHeader), 1, m_file);
    }
    return 0;
}

void Pager::Close()
{
    fseek(m_file, 0, SEEK_SET);
    fwrite((void *)m_db_header, sizeof(DBHeader), 1, m_file);
    while (m_pages.size() > 0)
        EvitPage();
    fclose(m_file);
    delete m_db_header;
    delete m_head;
    delete m_tail;
}

std::shared_ptr<Page> Pager::NewSinglePage(int64_t page_no)
{
    auto page = std::make_shared<Page>();
    page->node = std::make_shared<BtNode>();
    page->node->page_no = page_no;
    page->lru_next = NULL;
    page->lru_prev = NULL;
    auto &header = page->header;
    header.type = SINGLE_PAGE;
    header.page_cnt = 1;
    header.data_size = 0;
    header.next_free = -1;
    header.of_page_no = -1;
    return page;
}

std::shared_ptr<BtNode> Pager::AllocNode()
{
    if (m_db_header->free_list != -1)
    {
        int64_t page_no = m_db_header->free_list;
        std::shared_ptr<Page> page;
        ReadPage(page_no, page);
        m_db_header->free_list = page->header.next_free;

        CachePage(page_no, page);
        return page;
    }
    else
    {
        int64_t page_no = m_db_header->total_pages;
        m_db_header->total_pages++;
        auto page = NewSinglePage();
        page->node->page_no = page_no;

        CachePage(page_no, page);
        return page;
    }
    return page_nil;
}

int Pager::FreeNode(std::shared_ptr<BtNode> node)
{
    std::shared_ptr<Page> page;
    ReadPage(node->page_no, page);
    m_db_header->free_list = page->header.next_free;
    page->header.next_free = -1;

    // free overflow pages
    int page_cnt = page->header.page_cnt;
    if (page_cnt > 1)
    {
        int64_t of_page_no = page->header.of_page_no;
        for (int i = 0; i < page_cnt - 1; ++i)
        {
            auto of_page = NewSinglePage(of_page_no + i);
            auto &ph = of_page->header;
            ph.next_free = m_db_header->free_list;
            m_db_header->free_list = of_page_no + i;
            WriteFilePage(of_page_no + i, of_page);
        }
    }
    return 0;
}

int Pager::WriteNode(int64_t page_no, std::shared_ptr<BtNode> node)
{
    auto iter = m_pages.find(page_no);
    if (iter != m_pages.end())
    {
        auto &page = iter->second;
        TouchPage(page);
        page->node = node;
    }
    else
    {
        auto page = NewSinglePage(page_no);
        page->node = node;
        CachePage(page_no, page);
    }
    return 0;
}

int Pager::ReadPage(int64_t page_no, std::shared_ptr<Page> &page)
{
    auto iter = m_pages.find(page_no);
    if (iter == m_pages.end())
    {
        ReadFilePage(page_no, page);
        CachePage(page_no, page);
    }
    else
    {
        TouchPage(iter->second);
        page = iter->second;
    }
    return 0;
}

int64_t Pager::PageOffset(int64_t page_no)
{
    return page_no * PAGE_SIZE;
}

int Pager::PageOccupied(int64_t node_size)
{
    if (node_size <= (int)(PAGE_SIZE - sizeof(PageHeader)))
    {
        return 1;
    }
    else
    {
        int64_t overflow_size = node_size - (PAGE_SIZE - sizeof(PageHeader));
        int64_t overflow_pages = (overflow_size + PAGE_SIZE - 1) / PAGE_SIZE;
        return 1 + overflow_pages;
    }
}

void Pager::Attach(std::shared_ptr<Page> page)
{
    page->lru_next = m_head->lru_next;
    page->lru_prev = m_head;
    page->lru_next->lru_prev = page.get();
    page->lru_prev->lru_next = page.get();
}

void Pager::Detach(std::shared_ptr<Page> page)
{
    page->lru_prev->lru_next = page->lru_next;
    page->lru_next->lru_prev = page->lru_prev;
}

void Pager::TouchPage(std::shared_ptr<Page> page)
{
    Detach(page);
    Attach(page);
}

void Pager::CachePage(int64_t page_no, std::shared_ptr<Page> page)
{
    Attach(page);

    m_pages.insert(std::make_pair(page_no, page));
    if (m_pages.size() > MAX_PAGE_CACHE)
        EvitPage();
}

void Pager::EvitPage()
{
    auto page = m_tail->lru_prev;
    assert(page != m_head);

    // write back page to file
    std::shared_ptr<Page> p(page);
    WriteFilePage(page->node->page_no, p);
    // delete from cache
    Detach(p);
    m_pages.erase(page->node->page_no);
}

char buf[PAGE_SIZE * 100];

void Pager::WriteFilePage(int64_t page_no, std::shared_ptr<Page> page)
{
    printf("writing page: %ld\n", page_no);
    assert(page_no > 0);
    int64_t offset = PageOffset(page_no);
    if (offset + PAGE_SIZE > m_file_size)
    {
        ftruncate(fileno(m_file), offset + PAGE_SIZE);
        m_file_size = offset + PAGE_SIZE;
    }

    auto &header = page->header;
    int node_size = 0;
    page->node->Serialize(buf, node_size);
    int page_cnt = PageOccupied(node_size);

    // need more pages now
    if (page_cnt > header.pager_cnt)
    {
        // realloc overflow pages
        if (header.of_page_no != -1)
        {
        }
    }

    int node_size = 0;
    page->node->Serialize(buf, node_size);
    PageHeader *page_header = &page->header;
    fseek(m_file, offset, SEEK_SET);
    if (node_size <= (int)(PAGE_SIZE - sizeof(PageHeader)))
    {
        page_header->type = SINGLE_PAGE;
        page_header->page_cnt = 1;
        page_header->of_page_no = -1;
        fwrite((void *)page_header, sizeof(PageHeader), 1, m_file);
        fwrite((void *)buf, node_size, 1, m_file);
    }
    else
    {
        int cur_page_cnt = PageOccupied(node_size);

        // TODO some problem
        // int remain_size = node_size - (PAGE_SIZE - sizeof(PageHeader));
        page_header->type = MULTI_PAGE;
        page_header->page_cnt = PageOccupied(node_size);
        int64_t of_page_no = 0;
        page_header->of_page_no = of_page_no;
        fwrite((void *)page_header, sizeof(PageHeader), 1, m_file);
        fwrite((void *)buf, PAGE_SIZE - sizeof(PageHeader), 1, m_file);

        // write overflow pages
        int64_t of_offset = PageOffset(of_page_no);
        int64_t of_size = PAGE_SIZE * (page_header->page_cnt - 1);
        if (of_offset + of_size > m_file_size)
        {
            ftruncate(fileno(m_file), of_offset + of_size);
            m_file_size = of_offset + of_size;
        }
        fseek(m_file, of_offset, SEEK_SET);
        for (int i = 0; i < page_header->page_cnt - 1; ++i)
        {
            int64_t base = PAGE_SIZE - sizeof(PageHeader) + i * PAGE_SIZE;
            int64_t len = (base + PAGE_SIZE <= node_size) ? PAGE_SIZE : node_size - base;
            fwrite((void *)(buf + base), len, 1, m_file);
        }
    }
}

void Pager::ReadFilePage(int64_t page_no, std::shared_ptr<Page> &page)
{
    int64_t offset = PageOffset(page_no);
    assert(offset + PAGE_SIZE <= m_file_size);

    std::shared_ptr<Page> tp;
    fseek(m_file, offset, SEEK_SET);
    fread((void *)buf, PAGE_SIZE, 1, m_file);
    PageHeader *page_header = (PageHeader *)buf;
    tp->header = *page_header;

    if (page_header->page_cnt > 1)
    {
        int64_t of_offset = PageOffset(page_header->of_page_no);
        fseek(m_file, of_offset, SEEK_SET);
        fread((void *)(buf + PAGE_SIZE), PAGE_SIZE, page_header->page_cnt - 1, m_file);
    }

    // decode node
    uint16_t node_size = PAGE_SIZE - sizeof(PageHeader);
    tp->node = std::make_shared<BtNode>();
    tp->node->Parse(buf, node_size);
    page = tp;
}

}

