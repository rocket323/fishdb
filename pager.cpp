#include <unistd.h>
#include "pager.h"
#include <cmath>
#include <inttypes.h>

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
        m_db_header->total_pages = 1;
        ftruncate(fileno(m_file), PG_SIZE);
        m_file_size = PG_SIZE;
        fseek(m_file, 0, SEEK_SET);
        fwrite((char *)m_db_header, sizeof(DBHeader), 1, m_file);
    }
    else
    {
        // TODO do some check
        fread((void *)m_db_header, sizeof(DBHeader), 1, m_file);
    }
    printf("pager init, root_page[%" PRId64 "]\n", m_db_header->root_page);
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
    {
        auto mp = GetPage(m_db_header->root_page);
        assert(mp->header.page_no == m_db_header->root_page);
        return mp;
    }
}

void Pager::SetRoot(int64_t root_page)
{
    m_db_header->root_page = root_page;
}

std::shared_ptr<MemPage> Pager::NewPage(PageType type)
{
    std::shared_ptr<MemPage> mp;
    if (m_db_header->free_list != -1)
    {
        mp = ReadPage(m_db_header->free_list);
        m_db_header->free_list = mp->header.next_free;
    }
    else
    {
        mp = std::make_shared<MemPage>();
        mp->header.page_no = m_db_header->total_pages++;
    }
    // init page
    auto &header = mp->header;
    header.type = type;
    header.next_free = -1;
    header.of_page_no = -1;
    header.data_size = 0;
    header.page_cnt = 1;
    header.is_leaf = true;

    if (type == TREE_PAGE &&
            m_pages.find(mp->header.page_no) == m_pages.end())
        CachePage(mp);

    return mp;
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
        assert(mp != nil);
        int64_t of_page_no = mp->header.of_page_no;
        while (of_page_no > 0)
        {
            auto of_mp = ReadPage(of_page_no);
            mp->Feed(of_mp->data.c_str(), of_mp->data.size());
            of_page_no = of_mp->header.of_page_no;
        }
        printf("page[%" PRId64 "] of_page_no[%" PRId64 "], data_size[%zu]\n", page_no, mp->header.of_page_no, mp->data.size());
        if (mp->header.type == TREE_PAGE)
        {
            printf("parse\n");
            mp->Parse();
            CachePage(mp);
        }
    }
    mp->stick |= stick;
    return mp;
}

char buf[PG_SIZE * 1000];
void Pager::FlushPage(std::shared_ptr<MemPage> mp)
{
    assert(mp->header.type == TREE_PAGE);
    int64_t page_no = mp->header.page_no;
    int size = 0;
    mp->Serialize(buf, size);
    PageHeader *header = (PageHeader *)buf;
    int data_size = size - PH_SIZE;
    int page_cnt = data_size / PAGE_CAPA + (data_size % PAGE_CAPA > 0);
    printf("ph_size: %d\n", PH_SIZE);
    printf("flush page_no[%" PRId64 "], data_size[%d], page_cnt[%d]\n", page_no, data_size, page_cnt);

    for (int i = 0; i < std::min(50, (int)size); ++i)
    {
        printf("0x%x ", buf[PH_SIZE + i]);
    }
    puts("");

    std::vector<std::shared_ptr<MemPage>> pages;
    mp->header.page_cnt = page_cnt;
    pages.push_back(mp);
    auto p = ReadPage(page_no);
    while (p && p->header.of_page_no > 0)
    {
        p = ReadPage(p->header.of_page_no);
        pages.push_back(p);
    }
    int diff = page_cnt - (int)pages.size();
    printf("diff %d\n", diff);
    for (int i = 0; i < diff; ++i)
        pages.push_back(NewPage(OF_PAGE));

    // FIXME page leak
    assert(pages.size() >= page_cnt);
    for (int i = 0; i < page_cnt; ++i)
    {
        auto &p = pages[i];
        printf("feed page_no[%" PRId64 "]\n", p->header.page_no);
        int64_t base = PH_SIZE + i * PAGE_CAPA;
        if (i == 0)
        {
            for (int j = 0; j < PAGE_CAPA; ++j)
            {
                printf("0x%x ", buf[base + j]);
            }
            puts("");
        }
        p->Clear();
        p->Feed(buf + base, PAGE_CAPA);
        if (i == 0)
        {
            printf("after feed\n");
            for (int j = 0; j < 50; ++j)
            {
                printf("0x%x ", p->data[j]);
            }
            puts("");
        }
        if (i < page_cnt - 1)
        {
            p->header.of_page_no = pages[i + 1]->header.page_no;
        }
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
    std::vector<int64_t> ps;
    for (auto iter = m_pages.begin(); iter != m_pages.end(); ++iter)
    {
        if ((int)m_pages.size() <= size_limit) break;
        auto mp = iter->second;
        if (mp->stick && !force) continue;
        FlushPage(mp);
        ps.push_back(iter->first);
    }
    for (size_t i = 0; i < ps.size(); ++i)
        m_pages.erase(ps[i]);
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
    printf("write page[%" PRId64 "], of_page_no[%" PRId64 "] page_cnt[%d] data_size[%zu]\n",
            page_no, mp->header.of_page_no, mp->header.page_cnt, mp->data.size());
    printf("write num %zu\n", mp->children.size());

    if (mp->header.type == TREE_PAGE)
    {
        for (int i = 0; i < std::min(50, (int)mp->data.size()); ++i)
        {
            printf("0x%x ", mp->data[i]);
        }
        puts("");
    }

    assert(mp->data.size() <= PAGE_CAPA);
    fseek(m_file, offset, SEEK_SET);
    fwrite((void *)header, sizeof(PageHeader), 1, m_file);
    int len = std::min((int)PAGE_CAPA, (int)mp->data.size());
    fwrite((void *)mp->data.c_str(), len, 1, m_file);
}

std::shared_ptr<MemPage> Pager::ReadPage(int64_t page_no)
{
    // if page_no invalid, return nil
    if (page_no <= 0) return nil;
    int64_t offset = page_no * PG_SIZE;
    // printf("page[%" PRId64 "] offset[%" PRId64 "] file_size[%" PRId64 "]\n", page_no, offset, m_file_size);
    if (offset + PG_SIZE > m_file_size) return nil;

    auto mp = std::make_shared<MemPage>();
    auto header = &mp->header;
    fseek(m_file, offset, SEEK_SET);
    fread((void *)header, sizeof(PageHeader), 1, m_file);
    fread((void *)buf, PAGE_CAPA, 1, m_file);
    mp->Feed(buf, PAGE_CAPA);
    // printf("read page[%" PRId64 "], read_page_no[%" PRId64 "], data_size[%zu]\n", page_no, mp->header.page_no, mp->data.size());
    if (mp->header.page_no != page_no)
    {
        printf("read page[%" PRId64 "] uninited\n", page_no);
        auto p = std::make_shared<MemPage>();
        p->header.page_no = page_no;
        p->header.page_cnt = 1;
        p->header.of_page_no = -1;
        p->header.next_free = -1;
        return p;
    }
    return mp;
}

}

