#include <unistd.h>
#include "pager.h"

namespace fishdb
{

int Pager::Init(std::string file)
{
    m_db_header = new DBHeader();

    m_file = fopen(file.c_str(), "rb+");
    if (m_file == NULL)
        m_file = fopen(file.c_str(), "wb+");
    assert(m_file);
    fseek(m_file, 0L, SEEK_END);
    m_file_size = ftell(m_file);
    fseek(m_file, 0, SEEK_SET);

    if (m_file_size < PG_SIZE)
    {
        memset(m_db_header, 0, sizeof(DBHeader));
        m_db_header->free_list = -1;
        m_db_header->root_page = -1;
        ftruncate(fileno(m_file), PG_SIZE);
        fseek(m_file, 0, SEEK_SET);
        fwrite((char *)m_db_header, sizeof(DBHeader), 1, m_file);
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
    fseek(m_file, 0, SEEK_SET);
    fwrite((char *)m_db_header, sizeof(DBHeader), 1, m_file);
    Prune(0, true);
    fclose(m_file);
    delete m_db_header;
}

std::shared_ptr<MemPage> Pager::GetRoot()
{
    if (m_db_header->root_page == -1)
    {
        auto mp = NewPage(TREE_PAGE);
        SetRoot(mp->header.page_no);
        mp->is_leaf = true;
        return mp;
    }
    else
        return GetPage(m_db_header->root_page);
}

void Pager::SetRoot(int64_t root_page)
{
    m_db_header->root_page = root_page;
}

std::shared_ptr<MemPage> Pager::NewPage(PageType type)
{
}

std::shared_ptr<MemPage> Pager::GetPage(int64_t page_no, bool stick)
{
    assert(page_no > 0);
    auto iter = m_pages.find(page_no);
    std::shared_ptr<MemPage> mp;
    if (iter != m_pages.end())
    {
        mp = iter->second;
    }
    else
    {
        // read page(s) from file
        mp = ReadPage(page_no);
        int64_t of_page_no = mp->header.of_page_no;
        while (of_page_no > 0)
        {
            auto of_mp = ReadPage(of_page_no);
            mp->Feed(of_mp->data.c_str(), of_mp->data.size());
            of_page_no = of_mp->header.of_page_no;
        }
        if (mp->header.type == TREE_PAGE)
        {
            mp->Parse();
            CachePage(mp);
        }
    }
    mp->stick |= stick;
    return mp;
}

char buf[PG_SIZE * 100];
void Pager::FlushPage(std::shared_ptr<MemPage> mp)
{
    assert(mp->header.type == TREE_PAGE);
    int64_t page_no = mp->header.page_no;
    int size = 0;
    mp->Serialize(buf, size);
    PageHeader *header = (PageHeader *)buf;
    int data_size = size - PH_SIZE;
    int page_cnt = data_size / PAGE_CAPA + (data_size % PAGE_CAPA > 0);
    header->page_cnt = page_cnt;

    auto p = ReadPage(page_no);
    std::vector<std::shared_ptr<MemPage>> pages;
    while (p != nil)
    {
        pages.push_back(p);
        if (p->header.of_page_no <= 0) break;
        p = ReadPage(p->header.of_page_no);
    }
    for (int i = 0; i < page_cnt - pages.size(); ++i)
        pages.push_back(NewPage());

    for (int i = 0; i < page_cnt; ++i)
    {
        auto &p = pages[i];
        int64_t base = PH_SIZE + i * PAGE_CAPA;
        p->Feed(buf + base, PAGE_CAPA);
        if (i < page_cnt - 1)
            p->header.of_page_no = pages[i + 1]->header.page_no;
        else
            p->header.of_page_no = -1;
        WritePage(p);
    }
}

void Pager::FreePage(std::shared_ptr<MemPage> mp)
{
    while (mp)
    {
        auto header = mp->header;
        int64_t of_page_no = header.of_page_no;
        header.type = FREE_PAGE;
        header.of_page_no = -1;
        header.data_size = 0;
        header.page_cnt = 1;
        header.next_free = m_db_header->free_list;
        m_db_header->free_list = header.page_no;
        WritePage(mp);
        mp = ReadPage(of_page_no);
    }
} 

void Pager::Prune(int size_limit, bool force)
{
    for (auto iter = m_pages.begin(); iter != m_pages.end(); )
    {
        if ((int)m_pages.size() <= size_limit) break;
        auto mp = iter->second;
        if (mp->stick && !force) continue;
        FreePage(mp);
        FlushPage(mp);
        iter = m_pages.erase(iter);
    }
}

void Pager::CachePage(std::shared_ptr<MemPage> mp)
{
    m_pages.insert(std::make_pair(mp->header.page_no, mp));
}

void Pager::WritePage(std::shared_ptr<MemPage> mp)
{
    int64_t page_no = mp->header.page_no;
    auto header = &mp->header;
    int64_t offset = page_no * PG_SIZE;
    if (offset + PG_SIZE > m_file_size)
    {
        ftruncate(fileno(m_file), offset + PG_SIZE);
        m_file_size = offset + PG_SIZE;
    }
    assert(mp->data.size() && mp->data.size() <= PAGE_CAPA);
    fseek(m_file, offset, SEEK_SET);
    fwrite((void *)header, sizeof(PageHeader), 1, m_file);
    fwrite((void *)mp->data.c_str(), mp->data.size(), 1, m_file);
}

std::shared_ptr<MemPage> Pager::ReadPage(int64_t page_no)
{
    // if invalid, return nil
    if (page_no <= 0) return nil;
    int64_t offset = page_no * PG_SIZE;
    assert(offset + PG_SIZE <= m_file_size);
    auto mp = std::make_shared<MemPage>();
    auto header = &mp->header;
    fread((void *)header, sizeof(PageHeader), 1, m_file);
    fread((void *)buf, PAGE_CAPA, 1, m_file);
    mp->Feed(buf, PAGE_CAPA);
    return mp;
}

}

