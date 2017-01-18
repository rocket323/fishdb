#include "pager.h"

std::shared_ptr<Page> page_nil;

int Pager::Init(std::string fname)
{
    m_db_header = new DBHeader();
    m_head = new PageHeader();
    m_tail = new PageHeader();
    m_head->next = m_tail;
    m_tail->prev = m_head;

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
        fread((void *)m_db_header, sizeof(DBHeader), 1, m_file);
        memset(m_db_header, 0, sizeof(m_db_header));
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
    while (m_nodes.size() > 0)
        EvitNode();
    fclose(m_file);
    delete m_db_header;
    delete m_head;
    delete m_tail;
}

std::shared_ptr<Page> Pager::AllocPage(int n)
{
    if (m_db_header->free_list != -1 && n == 1)
    {
        int64_t page_no = m_db_header->free_list;
        std::shared_ptr<Page> page;
        ReadPage(page_no, page);
        m_db_header->free_list = page->header.next_free;

        CachePage(page_no, page);
        Attach(page);
        
        return page;
    }
    else
    {
        int64_t page_no = m_db_header->total_pages;
        m_db_header->total_pages += n;
        auto page = std::make_shared<Page>();
        page->node = std::make_shared<BtNode>();
        page->node->page_no = page_no;

        CachePage(page_no, page);
        Attach(page);

        return page;
    }
    return page_nil;
}

int Pager::FreeNode(int64_t page_no)
{
}

int Pager::WriteNode(int64_t page_no, std::shared_ptr<BTNode> node)
{
    auto iter = m_pages.find(page_no);
    if (iter != m_pages.end())
    {
        auto &page = iter->second;
        Detach(page);
        Attach(page);
        page->node = node;
    }
    else
    {
        auto page = std::make_shared<Page>();
        page->header.next_free = -1;
        page->node = node;
        CachePage(page);
        Attach(page);
    }
    return 0;
}

int Pager::ReadNode(int64_t page_no, std::shared_ptr<BTNode> &node)
{
    auto iter = m_pages.find(page_no);
    if (iter == m_pages.end())
    {
        auto page = std::make_shared<Page>();
        ReadPage(page_no, page);
        CachePage(page_no, page);
        Attach(page);
    }
    else
    {
        node = iter->second->node;
    }
    return 0;
}

void Pager::CachePage(int64_t page_no, std::shared_ptr<Page> page)
{
    m_pages.insert(std::make_pair(page_no, page));
    if (m_pages.size() > MAX_PAGE_CACHE)
        EvitPage();
}

void Pager::EvitPage()
{
    auto page = m_tail->prev;
    assert(page != m_head);

    // write back page to file
    WritePage(page->node->page_no, page);
    // delete from cache
    Detach(page);
    m_pages.erase(page->node->page_no);
}

char buf[PAGE_SIZE * 100];

void Pager::WritePage(int64_t page_no, std::shared_ptr<Page> page)
{
    printf("writing page: %ld\n", page_no);
    assert(page_no > 0);
    int64_t offset = PageOffset(page_no);
    if (offset + PAGE_SIZE > m_file_size)
    {
        ftruncate((fileno(m_file), offset + PAGE_SIZE));
        m_file_size = offset + PAGE_SIZE;
    }

    int node_size = 0;
    page->node->Serialize(buf, node_size);
    PageHeader *page_header = &page->header;
    fseek(m_file, offset, SEEK_SET);
    if (node_size <= PAGE_SIZE - sizeof(PageHeader))
    {
        page_header->type = SINGLE;
        page_header->page_cnt = 1;
        page_header->of_page_no = -1;
        fwrite((void *)page_header, sizeof(PageHeader), 1, m_file);
        fwrite((void *)buf, node_size, 1, m_file);
    }
    else
    {
        int remain_size = node_size - (PAGE_SIZE - sizeof(PageHeader));
        page_header->type = MULTI;
        page_header->page_cnt = PageOccupied(node_size);
        int64_t of_page_no = AllocPage(page_header->page_cnt - 1);
        page_header->of_page_no = of_page_no;
        fwrite((void *)page_header, sizeof(PageHeader), 1, m_file);
        fwrite((void *)buf, PAGE_SIZE - sizeof(PageHeader), 1, m_file);

        // write overflow pages
        int64_t of_offset = PageOffset(of_page_no);
        int64_t of_size = AGE_SIZE * (pager_header->page_cnt - 1);
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
            fwrite((void *)buf + base, len, 1, m_file);
        }
    }
}

void Pager::ReadPage(int64_t page_no, std::shared_ptr<Page> &page)
{
    int64_t offset = PageOffset(page_no);
    assert(offset + PAGE_SIZE <= m_file_size);

    std::shared_ptr<Page> tp;
    fseek(m_file, offset, SEEK_SET);
    fread((void *)buf, PAGE_SIZE, 1, m_file);
    PageHeader *page_header = (PageHeader *)buf;

    if (page_header->page_cnt > 1)
    {
        int64_t of_offset = PageOffset(page_header->of_page_no);
        fseek(m_file, of_offset, SEEK_SET);
        fread((void *)buf + PAGE_SIZE, PAGE_SIZE, buf->page_cnt - 1, m_file);
    }

    // decode node
    uint16_t node_size = PAGE_SIZE - sizeof(PageHeader);
    page->node = std::make_shared<BtNode>();
    page->node->Parse(buf, node_size);
}

