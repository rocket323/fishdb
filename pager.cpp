#include "pager.h"

namespace fishdb
{

std::shared_ptr<MemPage> pageNil; 

int Pager::Init(std::string file)
{
    m_db_header = new DBHeader();
    m_head = new MemPage();
    m_tail = new MemPage();
    m_head->lru_next = m_tail;
    m_tail->lru_prev = m_head;

    m_fiile = fopen(file.c_str(), "rb+");
    if (m_file == NULL)
        m_file = fopen(file.c_str(), "wb+");
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

int Pager::Close()
{
    fseek(m_file, 0, SEEK_SET);
    fwrite((void *)m_db_header, sizeof(DBHeader), 1, m_file);
    Prune(0);
    fclose(m_file);
    delete m_db_header;
    delete m_head;
    delete m_tail;
}

std::shared_ptr<MemPage> Pager::NewPage(PageType type)
{
    if (m_db_header->free_list != -1)
    {
        auto mp = GetPage(m_db_header->free_list);
        mp->header.type = type;
        assert(mp);
        m_db_header->free_list = mp->header.next_free;
        if (type == BRANCH_PAGE || type == LEAF_PAGE)
            CachePage(mp);
        return mp;
    }
    else
    {
        int64_t page_no = m_db_header->total_pages;
        m_db_header->total_pages++;
        auto mp = std::make_shared<MemPage>();
        auto &header = mp->header;
        header.page_no = page_no;
        header.type = type;
        if (type == BRANCH_PAGE || type == LEAF_PAGE)
            CachePage(mp);
        return mp;
    }
    return pageNil;
}

void Pager::SetRoot(int64_t root_page_no)
{
    m_db_header->root_page_no = root_page_no;
}

std::shared_ptr<MemPage> Pager::GetRootPage()
{
    if (m_db_header->root_page_no == -1)
        return NewPage(LEAF_PAGE);
    else
        return GetPage(m_db_header->root_page_no);
}

char buf[PAGE_SIZE * 100];
std::shared_ptr<MemPage> Pager::GetPage(int64_t page_no, bool stick)
{
    auto iter = m_pages.find(page_no);
    if (iter != m_pages.end())
    {
        TouchPage(iter->second);
        iter->second->stick = stick;
        return iter->second;
    }
    else
    {
        // read page from file
        int64_t offset = page_no * PAGE_SIZE;
        assert(offset + PAGE_SIZE <= m_file_size);

        memset(buf, 0, sizeof buf);
        auto mp = std::make_shared<MemPage>();
        fseek(m_file, offset, SEEK_SET);
        fread((void *)buf, PAGE_SIZE, 1, m_file);
        PageHeader *header = (PageHeader *)buf;
        mp->header = *header;

        if (header->type == BRANCH_PAGE || header->type == LEAF_PAGE)
        {
            int cur_size = PAGE_SIZE;
            while (header->of_page_no > 0)
            {
                int offset = header->of_page_no;
                fseek(m_file, offset, SEEK_SET);
                fread((void *)(buf + cur_size), PAGE, 1, m_file);
                header = (PageHeader *)(buf + base);
                cur_size += PAGE_SIZE;
            }

            mp->Parse(buf, cur_size);
            CachePage(mp);
        }
        else
        {
            mp->data.assign(buf + sizeof(PageHeader), PAGE_SIZE - sizeof(PageHeader));
        }

        mp->stick = stick;
        return mp;
    }
    return pageNil;
}

void Pager::FlushPage(int64_t page_no, std::shared_ptr<MemPage> mp)
{
    auto p = GetPage(page_no);
    memset(buf, 0, sizeof buf);
    int32_t size = 0;
    mp->Serialize(buf, size);
    PageHeader *header = (PageHeader *)buf;
    int32_t page_capa = PAGE_SIZE - sizeof(PageHeader);
    int page_cnt = size / page_capa + (size % page_capa > 0);

    if (page_cnt <= p->header.page_cnt)
    {
        int64_t base = PAGE_SIZE;
        int64_t of_page_no = p->header.of_page_no;
        for (int i = 0; i < page_cnt - 1 && of_page_no > 0; ++i)
        {
            fwrite((void *)(buf + base + sizeof(PageHeader)), page_capa, 1, m_file);
            base += PAGE_SIZE;
            auto of_mp = GetPage(of_page_no);
            of_page_no = of_mp->header.of_page_no;
        }
    }
    else
    {
        // free previous of_pages
        int64_t of_page_no = p->header.of_page_no;
        while (of_page_no > 0)
        {
            auto of_mp = GetPage(of_page_no);
            FreePage(of_mp);
            of_page_no = of_mp->header.of_page_no;
        }

        // reallocate of_pages
        int64_t last_of_page = -1;
        for (int i = page_cnt - 1; i > 0; --i)
        {
            int64_t base = i * PAGE_SIZE;
            auto of_mp = NewPage(OF_PAGE);
            of_mp->header.of_page_no = last_of_page;
            fwrite((void *)&of_mp->header, sizeof(PageHeader), 1, m_file);
            fwrite((void *)(buf + base + sizeof(PageHeader)), page_capa, 1, m_file);
            last_of_page = of_mp->header.page_no;
        }

        header->of_page_no = last_of_page;
    }

    int64_t offset = page_no * PAGE_SIZE;
    fseek(m_file, offset, SEEK_SET);
    fwrite((void *)buf, PAGE_SIZE, 1, m_file);
}

int Pager::FreePage(std::shared_ptr<MemPage> mp)
{
    int type = mp->header.type;
    auto header = mp->header;
    int64_t of_page_no = header->of_page_no;
    header->type = FREE_PAGE;
    header->of_page_no = -1;
    header->data_size = 0;
    header->page_cnt = 1;
    header->next_free = m_db_header->free_list;
    m_db_header->free_list = header->page_no;

    if (of_page_no > 0)
    {
        auto p = GetPage(of_page_no);
        FreePage(p);
    }
}

void Pager::Prune(int size_limit)
{
    auto now = m_tail;
    while (true)
    {
        auto mp = now->lru_prev;
        if (mp == now) break;
        if (m_pages.size() <= size_limit) break;
        now = mp;
        if (mp->stick) continue;

        FlushPage(mp->header.page_no, std::shared_ptr<MemPage>(mp));
        FreePage(std::shared_ptr<MemPage>(mp));

        mp->lru_prev->lru_next = mp->lru_next;
        mp->lru_next->lru_prev = mp->lru_prev;
        mp_pages.erase(mp->header.page_no);
    }
}

void Pager::Attach(std::shared_ptr<MemPage> mp)
{
    mp->lru_next = m_head->lru_next;
    mp->lru_prev = m_head;
    mp->lru_next->lru_prev = mp.get();
    mp->lru_prev->lru_next = mp.get();
}

void Pager::Detach(std::shared_ptr<MemPage> mp)
{
    mp->lru_prev->lru_next = mp->lru_next;
    mp->lru_next->lru_prev = mp->lru_prev;
}

void Pager::TouchPage(std::shared_ptr<MemPage> mp)
{
    Detach(mp);
    Attach(mp);
}

void Pager::CachePage(std::shared_ptr<MemPage> mp)
{
    Attach(mp);
    m_pages.insert(std::make_pair(mp->header.page_no, mp));
    if (m_pages.size() > MAX_PAGE_CACHE)
        Prune();
}

}

