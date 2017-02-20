#include "page.h"
#include "util.h"
#include <inttypes.h>
#include <cmath>

namespace fishdb
{

void MemPage::Clear()
{
    data.clear();
}

void MemPage::Feed(const char *buf, int size)
{
    data.append(buf, size);
}

void MemPage::Serialize(char *buf, int &size)
{
    char *sp = buf;
    header.is_leaf = is_leaf;
    memcpy(buf, (char *)&header, sizeof(PageHeader));
    buf += sizeof(PageHeader);

    buf += EncodeInt32(buf, children.size());
    for (size_t i = 0; i < children.size(); ++i)
        buf += EncodeInt64(buf, children[i]);

    buf += EncodeInt32(buf, kvs.size());
    for (size_t i = 0; i < kvs.size(); ++i)
    {
        buf += EncodeString(buf, kvs[i].key);
        buf += EncodeString(buf, kvs[i].value);
    }
    size = buf - sp;
    printf("yy page_no %" PRId64 " size: %d\n", header.page_no, size);
}

char sbuf[PG_SIZE * 1000];
void MemPage::Parse()
{
    char *buf = sbuf;
    memcpy(sbuf, data.c_str(), data.size());
    printf("page data_size[%zu]\n", data.size());
    is_leaf = header.is_leaf;
    printf("xx page_no: %" PRId64 " page_cnt %d\n", header.page_no, header.page_cnt);
    int32_t num;

    for (int i = 0; i < std::min(50, (int)data.size()); ++i)
    {
        printf("0x%x ", buf[i]);
    }
    puts("");

    buf += DecodeInt32(buf, num);
    printf("num: %d\n", num);
    for (int i = 0; i < num; ++i)
    {
        int64_t c;
        buf += DecodeInt64(buf, c);
        children.push_back(c);
    }

    buf += DecodeInt32(buf, num);
    for (int i = 0; i < num; ++i)
    {
        std::string k, v;
        buf += DecodeString(buf, k);
        buf += DecodeString(buf, v);
        kvs.push_back(KV(k, v));
    }
}

}

