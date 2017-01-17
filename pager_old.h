#ifndef PAGE_CACHE_H_
#define PAGE_CACHE_H_

#include <memory>
#include <stdint.h>
#include <map>
#include <string>
#include <assert.h>
#include <unistd.h>

namespace fishdb
{

static const int PAGE_SIZE = 512;
static const int MAX_PAGE_CACHE = 100;

template<typename NodeType>
class PageCache
{
public:
    struct FilePage
    {
        std::shared_ptr<NodeType> node;
        int64_t next;
        int64_t page_id;
    };

    struct Header
    {
        int64_t free_list;
        int32_t total_pages;
        int32_t page_size;
    };

    struct MemPage
    {
        MemPage *prev;
        MemPage *next;
        FilePage fpage;
    };

public:
    int Init(std::string filename);
    void Close();
    int64_t AllocPage();
    void FreeNode(int64_t page_id);
    int WriteNode(int64_t page_id, std::shared_ptr<NodeType> node);
    int ReadNode(int64_t page_id, std::shared_ptr<NodeType> &node);

	int TotalPages()
	{
		return m_header->total_pages;
	}

protected:
    void Attach(MemPage *page);
    void Detach(MemPage *page);

    void EvictPage();
    void WriteFilePage(int64_t page_id, FilePage &fpage);
    void ReadFilePage(int64_t page_id, FilePage &fpage);

    int64_t PageOffset(int64_t page_id)
    {
        return page_id * PAGE_SIZE;
    }

private:
    std::map<int64_t, std::shared_ptr<MemPage>> m_pages;
    Header *m_header;
    MemPage *m_head;
    MemPage *m_tail;
    FILE *m_file;
    int m_file_size;
};

template<typename NodeType>
int PageCache<NodeType>::Init(std::string filename)
{
	m_header = new Header();
	m_head = new MemPage();
	m_tail = new MemPage();
	m_head->next = m_tail;
	m_tail->prev = m_head;

    m_file = fopen(filename.c_str(), "rb+");
	if (m_file == NULL)
		m_file = fopen(filename.c_str(), "wb+");

    assert(m_file);
    fseek(m_file, 0L, SEEK_END);
    m_file_size = ftell(m_file);
    fseek(m_file, 0, SEEK_SET);

    if (m_file_size < PAGE_SIZE)
    {
        ftruncate(fileno(m_file), PAGE_SIZE);
        fread((void *)m_header, sizeof(Header), 1, m_file);
        memset(m_header, 0, sizeof(Header));
		m_header->free_list = -1;
		m_header->total_pages = 1;
    }
    else
    {
        fread((void *)m_header, sizeof(Header), 1, m_file);
    }
	return 0;
}

template<typename NodeType>
void PageCache<NodeType>::Close()
{
	fseek(m_file, 0, SEEK_SET);
	fwrite((void *)m_header, sizeof(Header), 1, m_file);

	while (m_pages.size() > 0)
		EvictPage();

    fclose(m_file);
    delete m_header;
    delete m_head;
    delete m_tail;
}

template<typename NodeType>
int64_t PageCache<NodeType>::AllocPage()
{
    if (m_header->free_list == -1)
        return m_header->total_pages++;
    else
    {
        int64_t page_id = m_header->free_list;
        FilePage fpage;
        ReadFilePage(page_id, fpage);
        m_header->free_list = fpage.next;
		return page_id;
    }
    return -1;
}

template<typename NodeType>
void PageCache<NodeType>::FreeNode(int64_t page_id)
{
    FilePage fpage;
    ReadFilePage(page_id, fpage);
    fpage.next = m_header->free_list;
    m_header->free_list = page_id;
}

template<typename NodeType>
void PageCache<NodeType>::EvictPage()
{
    auto mpage = m_tail->prev;
	assert(mpage != m_head);
	printf("evit page %ld\n", mpage->fpage.page_id);

    // write back to file
    WriteFilePage(mpage->fpage.page_id, mpage->fpage);

    // delete from cache
    Detach(mpage);
    m_pages.erase(mpage->fpage.page_id);
}

template<typename NodeType>
int PageCache<NodeType>::WriteNode(int64_t page_id, std::shared_ptr<NodeType> node)
{
    auto iter = m_pages.find(page_id);
    if (iter != m_pages.end())
    {
        auto &page = iter->second;
        Detach(page.get());
        Attach(page.get());
        page->fpage.node = node;
    }
    else
    {
        if (m_pages.size() >= MAX_PAGE_CACHE)
            EvictPage();

		auto page = new MemPage();
		page->fpage.page_id = page_id;
		page->fpage.next = -1;
        page->fpage.node = node;
        std::shared_ptr<MemPage> p_page(page);
        m_pages.insert(std::make_pair(page_id, p_page));
        Attach(page);

		printf("add page %ld, cur_pages: %zu\n", page->fpage.page_id, m_pages.size());
    }
	return 0;
}

template<typename NodeType>
int PageCache<NodeType>::ReadNode(int64_t page_id, std::shared_ptr<NodeType> &node)
{
    auto iter = m_pages.find(page_id);
    if (iter == m_pages.end())
    {
        auto mpage = std::make_shared<MemPage>();
        ReadFilePage(page_id, mpage->fpage);
        m_pages.insert(std::make_pair(page_id, mpage));
        node = mpage->fpage.node;
		Attach(mpage.get());

        if (m_pages.size() >= MAX_PAGE_CACHE)
            EvictPage();
    }
    else
    {
        node = iter->second->fpage.node;
    }
    return 0;
}

template<typename NodeType>
void PageCache<NodeType>::Attach(MemPage *mpage)
{
    mpage->next = m_head->next;
    mpage->prev = m_head;
    m_head->next->prev = mpage;
    m_head->next = mpage;
}

template<typename NodeType>
void PageCache<NodeType>::Detach(MemPage *mpage)
{
    mpage->next->prev = mpage->prev;
    mpage->prev->next = mpage->next;
}

template<typename NodeType>
void PageCache<NodeType>::WriteFilePage(int64_t page_id, FilePage &fpage)
{
	assert(page_id > 0);
	printf("writing fpage :%ld\n", page_id);
    int64_t offset = PageOffset(page_id);
    if (offset + PAGE_SIZE > m_file_size)
    {
        ftruncate(fileno(m_file), offset + PAGE_SIZE);
		m_file_size = offset + PAGE_SIZE;
    }

    fseek(m_file, offset, SEEK_SET);
    fwrite((void *)fpage.node.get(), sizeof(NodeType), 1, m_file);
    fwrite((void *)&fpage.next, sizeof(fpage.next), 1, m_file);
    fwrite((void *)&fpage.page_id, sizeof(fpage.page_id), 1, m_file);
}

template<typename NodeType>
void PageCache<NodeType>::ReadFilePage(int64_t page_id, FilePage &fpage)
{
    int64_t offset = PageOffset(page_id);
    assert(offset + PAGE_SIZE <= m_file_size);

    fseek(m_file, offset, SEEK_SET);
    fpage.node = std::make_shared<NodeType>();

    fread((void *)fpage.node.get(), sizeof(NodeType), 1, m_file);
    fread((void *)&fpage.next, sizeof(fpage.next), 1, m_file);
    fread((void *)&fpage.page_id, sizeof(fpage.page_id), 1, m_file);
}
}
#endif

