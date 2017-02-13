#ifndef FORMAT_H_
#define FORMAT_H_
#include <stdint.h>
#include <string>

namespace fishdb
{

static int EncodeBool(char *buf, bool num)
{
    *((bool *)buf) = num;
    return 1;
}

static int EncodeInt8(char *buf, int8_t num)
{
    *((int8_t *)buf) = num;
    return 1;
}

static int EncodeInt16(char *buf, int16_t num)
{
    *((int16_t *)buf) = num;
    return 2;
}

static int EncodeInt32(char *buf, int32_t num)
{
    *((int32_t *)buf) = num;
    return 4;
}

static int EncodeInt64(char *buf, int64_t num)
{
    *((int64_t *)buf) = num;
    return 8;
}

static int EncodeString(char *buf, std::string &str)
{
    EncodeInt32(buf, str.length());
    memcpy(buf + 4, str.c_str(), str.length());
    return 4 + str.length();
}

static int DecodeBool(char *buf, bool &num)
{
    num = *((bool *)buf);
    return 1;
}

static int DecodeInt8(char *buf, int8_t &num)
{
    num = *((int8_t *)buf);
    return 1;
}

static int DecodeInt16(char *buf, int16_t &num)
{
    num = *((int16_t *)buf);
    return 2;
}

static int DecodeInt32(char *buf, int32_t &num)
{
    num = *((int32_t *)buf);
    return 4;
}

static int DecodeInt64(char *buf, int64_t &num)
{
    num = *((int64_t *)buf);
    return 8;
}

static int DecodeString(char *buf, std::string &str)
{
    int32_t len = 0;
    DecodeInt32(buf, len);
    str = std::string(buf + 4, len);
    return 4 + len;
}

}

#endif

