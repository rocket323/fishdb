#ifndef NP_H_
#define NP_H_

#include <map>
#include <string>
#include <vector>
#include <memory>
#include <assert.h>
#include "util.h"
#include "page.h"

namespace fishdb
{

static const int MAX_PAGE_CACHE = 1000;

class Pager
{
public:
    int Init(std::string file);
    void Close();

    std::shared_ptr<MemPage> GetRoot();
    void SetRoot(int64_t root_page);

    std::shared_ptr<MemPage> NewPage(PageType type = TREE_PAGE);
    std::shared_ptr<MemPage> GetPage(int64_t page_no, bool stick = false);
    void FlushPage(std::shared_ptr<MemPage> mp);
    void FreePage(std::shared_ptr<MemPage> mp);

    void Prune(int size_limit = MAX_PAGE_CACHE, bool force = false);

protected:
    void CachePage(std::shared_ptr<MemPage> mp);
    void WritePage(std::shared_ptr<MemPage> mp);
    std::shared_ptr<MemPage> ReadPage(int64_t page_no);

public:
    std::map<int64_t, std::shared_ptr<MemPage>> m_pages;
    DBHeader *m_db_header;
    FILE *m_file;
    int m_file_size;
};
static const std::shared_ptr<MemPage> nil;

}

#endif

