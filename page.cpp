#include "page.h"
#include "util.h"

namespace fishdb
{

void MemPage::Feed(const char *buf, int size)
{
    data.assign(buf, size);
}

void MemPage::Serialize(char *buf, int &size)
{
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
}

char buf[PG_SIZE * 100];
void MemPage::Parse()
{
    char *buf = buf;
    memcpy(buf, data.c_str(), data.size());
    memcpy((char *)&header, buf, sizeof(PageHeader));
    buf += sizeof(PageHeader);
    int32_t num;
    buf += DecodeInt32(buf, num);
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
    }
}

}

