#ifndef FORMAT_H_
#define FORMAT_H_
#include <stdint.h>
#include <string>
#include <cstring>

namespace fishdb
{

int EncodeBool(char *buf, bool num);
int EncodeInt8(char *buf, int8_t num);
int EncodeInt16(char *buf, int16_t num);
int EncodeInt32(char *buf, int32_t num);
int EncodeInt64(char *buf, int64_t num);
int EncodeString(char *buf, std::string &str);

int DecodeBool(char *buf, bool &num);
int DecodeInt8(char *buf, int8_t &num);
int DecodeInt16(char *buf, int16_t &num);
int DecodeInt32(char *buf, int32_t &num);
int DecodeInt64(char *buf, int64_t &num);
int DecodeString(char *buf, std::string &str);

}

#endif

