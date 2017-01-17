#ifndef FORMAT_H_
#define FORMAT_H_

namespace fishdb
{

int EncodeInt8(char *buf, int8_t num)
{
    *((int8_t *)buf) = num;
    return 1;
}

int EncodeInt16(char *buf, int16_t num)
{
    *((int16_t *)buf) = num;
    return 2;
}

int EncodeInt32(char *buf, int32_t num)
{
    *((int32_t *)buf) = num;
    return 4;
}

int EncodeInt64(char *buf, int64_t num)
{
    *((int64_t *)buf) = num;
    return 8;
}

int EncodeString(char *buf, std::string &str)
{
    buf += EncodeInt32(str.length());
    memcpy(buf, str.c_str(), str.length());
    return 4 + str.length();
}

int DecodeInt8();
int DecodeInt16();
int DecodeInt32();
int DecodeInt64();
int DecodeString();

}

#endif

