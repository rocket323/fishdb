#include <inttypes.h>
#include <cstring>
#include <unistd.h>
#include <assert.h>
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
    m_head->lru_prev = m_tail;
    m_tail->lru_next = m_head;
    m_tail->lru_prev = m_head;

    m_file = fopen(file.c_str(), "rb+");
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
        memset(m_db_header, 0, sizeof(DBHeader));
        m_db_header->free_list = -1;
        m_db_header->total_pages = 1;
        m_db_header->page_size = PAGE_SIZE;
        m_db_header->root_page_no = -1;
    }
    else
    {
        // TODO do some check
        fread((void *)m_db_header, sizeof(DBHeader), 1, m_file);
    }
    return 0;
}

void Pager::Close()
{
    WriteFile((char *)m_db_header, sizeof(DBHeader), 0);
    Prune(0, true);
    fclose(m_file);
    delete m_db_header;
    delete m_head;
    delete m_tail;
}

std::shared_ptr<MemPage> Pager::NewPage(PageType type)
{
    std::shared_ptr<MemPage> mp;
    if (m_db_header->free_list != -1)
    {
        mp = GetPage(m_db_header->free_list);
        assert(mp);
        m_db_header->free_list = mp->header.next_free;
    }
    else
    {
        mp = std::make_shared<MemPage>();
        mp->header.page_no = m_db_header->total_pages++;
    }
    if (type == TREE_PAGE)
        CachePage(mp);

    // init page
    auto &header = mp->header;
    header.type = type;
    header.next_free = -1;
    header.of_page_no = -1;
    header.data_size = 0;
    header.page_cnt = 1;
    header.is_leaf = true;

    return mp;
}

void Pager::SetRoot(int64_t root_page_no)
{
    printf("set root_page_no[%" PRId64 "]\n", root_page_no);
    m_db_header->root_page_no = root_page_no;
}

std::shared_ptr<MemPage> Pager::GetRoot()
{
    if (m_db_header->root_page_no == -1)
    {
        auto mp = NewPage(TREE_PAGE);
        SetRoot(mp->header.page_no);
        mp->is_leaf = true;
        return mp;
    }
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
        iter->second->stick |= stick;
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

        if (header->type == TREE_PAGE)
        {
            int cur_size = PAGE_SIZE;
            int64_t of_page_no = header->of_page_no;
            while (of_page_no > 0)
            {
                int offset = header->of_page_no;
                assert(offset + PAGE_SIZE <= m_file_size);
                fseek(m_file, offset, SEEK_SET);
                char tmp_page[PAGE_SIZE];
                fread((void *)tmp_page, PAGE_SIZE, 1, m_file);
                header = (PageHeader *)tmp_page;
                memcpy(buf + cur_size, tmp_page + PH_SIZE, PAGE_CAPA);
                cur_size += PAGE_CAPA;
                of_page_no = header->of_page_no;
            }

            mp->Parse(buf, cur_size);
            CachePage(mp);
        }
        else
        {
            mp->raw.assign(buf + PH_SIZE, PAGE_SIZE - PH_SIZE);
        }
        printf("read page[%" PRId64 "], of_page_no[%" PRId64 "] is_leaf[%d]\n", page_no, header->of_page_no, mp->is_leaf);

        mp->stick |= stick;
        return mp;
    }
    return pageNil;
}

void Pager::WriteFile(char *buf, const int len, int64_t offset)
{
    if (offset + len > m_file_size)
    {
        ftruncate(fileno(m_file), offset + len + PAGE_SIZE);
        m_file_size = offset + len + PAGE_SIZE;
    }
    fseek(m_file, offset, SEEK_SET);
    fwrite((void *)buf, len, 1, m_file);
}

void Pager::FlushPage(std::shared_ptr<MemPage> mp)
{
    int64_t page_no = mp->header.page_no;
    auto p = GetPage(page_no);
    memset(buf, 0, sizeof buf);
    int32_t size = 0;
    mp->Serialize(buf, size);
    PageHeader *header = (PageHeader *)buf;
    int data_size = size - PH_SIZE;
    int page_cnt = data_size / PAGE_CAPA + (data_size % PAGE_CAPA > 0);

    if (page_cnt <= p->header.page_cnt)
    {
        int64_t base = PAGE_SIZE;
        int64_t of_page_no = p->header.of_page_no;
        for (int i = 0; i < page_cnt - 1 && of_page_no > 0; ++i)
        {
            int64_t offset = of_page_no * PAGE_SIZE + PH_SIZE;
            WriteFile(buf + base, PAGE_CAPA, offset);
            base += PAGE_CAPA;
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
            of_page_no = of_mp->header.of_page_no;
            FreePage(of_mp);
            FlushPage(of_mp);
        }

        // reallocate of_pages
        int64_t last_of_page = -1;
        for (int i = page_cnt; i >= 2; --i)
        {
            int64_t base = PAGE_SIZE + (i - 2) * PAGE_CAPA;
            auto of_mp = NewPage(OF_PAGE);
            of_mp->header.of_page_no = last_of_page;

            int64_t offset = of_mp->header.page_no * PAGE_SIZE;
            WriteFile((char *)&of_mp->header, PH_SIZE, offset);
            WriteFile(buf + base, PAGE_CAPA, offset + PH_SIZE);
            last_of_page = of_mp->header.page_no;
        }

        header->of_page_no = last_of_page;
    }

    int64_t offset = page_no * PAGE_SIZE;
    WriteFile(buf, PAGE_SIZE, offset);
    printf("flush page[%" PRId64 "], of_page_no[%" PRId64 "], file_size[%d]\n", page_no, header->of_page_no, m_file_size);
}

int Pager::FreePage(std::shared_ptr<MemPage> mp)
{
    auto header = mp->header;
    int64_t of_page_no = header.of_page_no;
    header.type = FREE_PAGE;
    header.of_page_no = -1;
    header.data_size = 0;
    header.page_cnt = 1;
    header.next_free = m_db_header->free_list;
    m_db_header->free_list = header.page_no;

    if (of_page_no > 0)
    {
        auto p = GetPage(of_page_no);
        FreePage(p);
    }
    return 0;
}

void Pager::Prune(int size_limit, bool force)
{
    auto now = m_tail;
    while (true)
    {
        auto mp = now->lru_prev;
        if (mp == m_tail) break;
        if ((int)m_pages.size() <= size_limit) break;
        now = mp;
        if (mp->stick && !force) continue;

        now = m_tail;
        Detach(mp);
        int64_t page_no = mp->header.page_no;
        auto iter = m_pages.find(page_no);
        if (iter != m_pages.end())
        {
            auto &_mp = iter->second;
            FlushPage(_mp);
            FreePage(_mp);
            m_pages.erase(iter);
        }
    }
}

void Pager::Attach(MemPage *mp)
{
    mp->lru_next = m_head->lru_next;
    mp->lru_prev = m_head;
    mp->lru_next->lru_prev = mp;
    mp->lru_prev->lru_next = mp;
}

void Pager::Detach(MemPage *mp)
{
    mp->lru_prev->lru_next = mp->lru_next;
    mp->lru_next->lru_prev = mp->lru_prev;
}

void Pager::TouchPage(std::shared_ptr<MemPage> mp)
{
    Detach(mp.get());
    Attach(mp.get());
}

void Pager::CachePage(std::shared_ptr<MemPage> mp)
{
    if (m_pages.size() > MAX_PAGE_CACHE - 1)
        Prune(MAX_PAGE_CACHE - 1);
    Attach(mp.get());
    m_pages.insert(std::make_pair(mp->header.page_no, mp));
}

}

