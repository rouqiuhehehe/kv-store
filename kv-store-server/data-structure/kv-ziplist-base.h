//
// Created by 115282 on 2023/10/11.
//

#ifndef LINUX_SERVER_KV_STORE_SERVER_DATA_STRUCTURE_KV_ZIPLIST_BASE_H_
#define LINUX_SERVER_KV_STORE_SERVER_DATA_STRUCTURE_KV_ZIPLIST_BASE_H_

#include "util/endian.h"
#include "util/global.h"
#define ZIP_END 0xff
#define ZIP_END_SIZE sizeof(uint8_t)
#define ZIP_BIG_PREV_LEN 0xfe

#define ZIP_ENCODING_SIZE_INVALID 0xff

/** encoding 如果开头为 00 01 10 则占用2个bit 编码长度分别为1 2 5
    如果开头为11（即 >= 0xc0），则表示储存的是整数

    11000000	1 byte	int16_t 类型的整数
    11010000	1 byte	int32_t 类型的整数
    11100000	1 byte	int64_t 类型的整数
    11110000	1 byte	24 bit 有符号整数
    11111110	1 byte	8 bit 有符号整数
    1111xxxx	1 byte	4 bit 无符号整数，介于 0 至 12 之间
*/
#define ZIP_STR_MASK 0xc0 // 11000000
#define ZIP_CONTENT_MASK 0x3f // 00111111
#define ZIP_STR_06B 0
#define ZIP_STR_14B (1 << 6)
#define ZIP_STR_32B (1 << 7)

#define ZIP_INT_16B (0xc0 | 0<<4)  // 11000000
#define ZIP_INT_32B (0xc0 | 1<<4)  // 11010000
#define ZIP_INT_64B (0xc0 | 2<<4)  // 11100000
#define ZIP_INT_24B (0xc0 | 3<<4)  // 11110000
#define ZIP_INT_8B 0xfe  // 11111110
#define ZIP_INT_IMM_MIN 0xf1    /* 11110001 */
#define ZIP_INT_IMM_MAX 0xfd    /* 11111101 */

#define ZIP_PREV_LEN_SIZE(prevLen) prevLen < ZIP_BIG_PREV_LEN ? 1 : sizeof(int32_t) + 1
using namespace Utils::Endian;
class ZipListHelper
{
public:
    struct ZipListHead
    {
        uint32_t size; // 压缩列表总字节大小
        uint32_t tailOffset; // 压缩列表最后一个结点对应头的偏移量
        uint16_t count; // 压缩列表总节点数
    } __attribute__((__packed__));
    struct Entry
    {
        uint32_t prevLenSize;       //记录prevLen所占用字节数，如果开头1字节为ZIP_BIG_PREV_LEN，长度为5，否则为1
        uint32_t prevLen;           //记录保存在prevLen字段中前序节点的长度
        uint32_t lenSize;           //记录当前entry中data数据所占用的字节长度
        uint32_t len;               //记录当前entry中data数据所占用的长度，可以为0，表示存储在<encoding>中的小整数
        uint32_t headerSize;        //记录当前entry中header的长度，等于prevrawlensize + lensize
        uint8_t encoding;         //记录当前entry中数据的编码方式
        uint8_t *p;               //保存指向当前entry起始位置的指针，而这个指针指向的是当前节点的<prevlen>字段
    };
    union DataType
    {
        struct
        {
            uint8_t *val;
            size_t len;
        } str;
        int64_t val;
    };
    enum class DataTypeEnum
    {
        STRING,
        INTEGER,
        ERROR
    };
protected:
    // 尝试将字符串编码成数字，并填充encoding
    static inline bool zipTryEncode (uint8_t *s, uint32_t len, int64_t &value, uint8_t &encoding)
    {
        int64_t v;
        std::string str(reinterpret_cast<char *>(s), len);
        constexpr static int32_t
            int24Min = std::numeric_limits <int32_t>::min() >> sizeof(uint8_t) * 8;
        constexpr static int32_t
            int24Max = std::numeric_limits <int32_t>::max() >> sizeof(uint8_t) * 8;
        if (Utils::StringHelper::stringIsLL(str, reinterpret_cast<long long *>(&v)))
        {
            if (v >= 0 && v <= 12)
                encoding = ZIP_INT_IMM_MIN + v;
            else if (v >= std::numeric_limits <int8_t>::min()
                && v <= std::numeric_limits <int8_t>::max())
                encoding = ZIP_INT_8B;
            else if (v >= std::numeric_limits <int16_t>::min()
                && v <= std::numeric_limits <int16_t>::max())
                encoding = ZIP_INT_16B;
            else if (v >= int24Min && v <= int24Max)
                encoding = ZIP_INT_24B;
            else if (v >= std::numeric_limits <int32_t>::min()
                && v <= std::numeric_limits <int32_t>::max())
                encoding = ZIP_INT_32B;
            else if (v >= std::numeric_limits <int64_t>::min()
                && v <= std::numeric_limits <int64_t>::max())
                encoding = ZIP_INT_64B;

            value = v;
            return true;
        }
        return false;
    }
    static inline uint32_t zipEncodeLenSize (uint8_t encoding)
    {
        if (encoding == ZIP_INT_16B || encoding == ZIP_INT_32B ||
            encoding == ZIP_INT_24B || encoding == ZIP_INT_64B ||
            encoding == ZIP_INT_8B)
            return 1;
        if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX)
            return 1;
        if (encoding == ZIP_STR_06B)
            return 1;
        if (encoding == ZIP_STR_14B)
            return 2;
        if (encoding == ZIP_STR_32B)
            return 5;
        return ZIP_ENCODING_SIZE_INVALID;
    }

    // 根据encoding获取data 所占字节数
    static inline uint32_t zipIntSize (uint8_t encoding)
    {
        switch (encoding)
        {
            case ZIP_INT_8B:
                return 1;
            case ZIP_INT_16B:
                return 2;
            case ZIP_INT_24B:
                return 3;
            case ZIP_INT_32B:
                return 4;
            case ZIP_INT_64B:
                return 8;
            default:
                break;
        }
        if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX)
            return 0;
        return 0;
    }
    // 判断data是否存储的字符串
    static inline bool zipIsStr (uint8_t encoding) noexcept
    {
        return (encoding & ZIP_STR_MASK) < ZIP_STR_MASK;
    }
    // 将entry data 解码成数字 并赋值给data.val
    static inline bool zipDecodeInteger (const Entry &entry, DataType &data) noexcept
    {
        auto *p = entry.p + entry.headerSize;
        if (entry.encoding == ZIP_INT_8B)
            data.val = static_cast<int64_t>(*reinterpret_cast<int8_t *>(p)); //NOLINT
        else if (entry.encoding == ZIP_INT_16B)
            memcpy(&data.val, p, sizeof(int16_t));
        else if (entry.encoding == ZIP_INT_24B)
            // 空出8个字节，复制24个字节
            memcpy(reinterpret_cast<uint8_t *>(&data.val) + 1, p, sizeof(int32_t) - sizeof(int8_t));
        else if (entry.encoding == ZIP_INT_32B)
            memcpy(&data.val, p, sizeof(int32_t));
        else if (entry.encoding == ZIP_INT_64B)
            memcpy(&data.val, p, sizeof(int64_t));
        else if (entry.encoding >= ZIP_INT_IMM_MIN && entry.encoding <= ZIP_INT_IMM_MAX)
            data.val = (*p & 0xf) - 1; // 1111xxxx & 00001111 取出 xxxx - 1
        else
        {
            PRINT_ERROR("encoding error : %d", entry.encoding);
            return false;
        }
        return true;
    }
    // 根据encoding，编码data所占用长度
    // buf 至少需要5字节
    static inline uint32_t zipEncodeEncoding (uint8_t *buf, uint8_t encoding, uint32_t dataSize)
    {
        uint32_t len;
        if (zipIsStr(encoding))
        {
            if (dataSize < (1 << 6)) // 头两位用于编码00，只有6位可用，最大长度 (1 << 6) - 1
            {
                len = 1;
                buf[0] = dataSize;
            }
            else if (dataSize < (1 << 14)) // 头两位用于编码01，双字节，有14位可用，最大长度 (1 << 14) - 1
            {
                len = 2;
                buf[0] = ZIP_STR_14B | (dataSize >> 8); // 高6位
                buf[1] = dataSize & 0xff;  // 低8位
            }
            else
            {
                len = 5;
                buf[0] = ZIP_STR_32B;
                *reinterpret_cast<uint32_t *>(buf + 1) = dataSize;
            }
        }
        else
        {
            len = 1;
            buf[0] = encoding;
        }

        return len;
    }

    static inline uint32_t zipDecodePrevLenSize (uint8_t prevLenSize)
    {
        if (prevLenSize < ZIP_BIG_PREV_LEN)
            return 1;
        return 5;
    }
};

class ZipListBase : public ZipListHelper
{
public:
    ZipListBase ()
        : zipList(static_cast<uint8_t *>(malloc(HEAD_SIZE + sizeof(uint8_t))))
    {
        head->size = intConv32(HEAD_SIZE + sizeof(uint8_t));
        // 全部用小端储存
        head->tailOffset = intConv32(HEAD_SIZE);
        head->count = 0;
        zipList[HEAD_SIZE] = ZIP_END;
    }

protected:
    inline DataTypeEnum getEntryVal (DataType &data, off_t offset, Entry *entryPtr = nullptr)
    {
        Entry entry = zipEntry(offset);
        if (entryPtr) *entryPtr = entry;

        if (zipIsStr(entry.encoding))
        {
            data.str.len = entry.len;
            data.str.val = entry.p + entry.headerSize;
            return DataTypeEnum::STRING;
        }
        else if (zipDecodeInteger(entry, data))
            return DataTypeEnum::INTEGER;

        return DataTypeEnum::ERROR;
    }

    int zipListInsert (uint8_t *s, uint32_t len, uint32_t prevLen, off_t offset) noexcept
    {
        int64_t v;
        uint8_t encoding, buf[5];
        size_t reqLen;
        int nextDiff = 0;
        if (zipTryEncode(s, len, v, encoding))
            reqLen = zipIntSize(encoding);
        else
            reqLen = len;

        // prev
        reqLen += ZIP_PREV_LEN_SIZE(prevLen);
        reqLen += zipEncodeEncoding(buf, encoding, len);

        bool forceLarge = false;
        auto *p = zipList + offset;
        if (*p != ZIP_END)
        {
            // 当前添加节点总长度所需prevLenSize字节数 - 当前添加节点后置节点的prevLenSize字节数
            // 判断prevLenSize是否需要扩容/缩容
            nextDiff = ZIP_PREV_LEN_SIZE(reqLen) - ZipListHelper::zipDecodePrevLenSize(*p);
            // 之前节点需要缩容4字节，并且当前节点长度<4，则不做prevLenSize缩容，只做更新
            // 必须保证reqLen + nextDiff >= 0，不然realloc会进行缩容，导致数据丢失
            if (nextDiff == -4 && reqLen < 4)
            {
                nextDiff = 0;
                forceLarge = true;
            }
        }

        zipList = reinterpret_cast<uint8_t *>(realloc(
            zipList,
            intConv32(head->size) + nextDiff + reqLen
        ));
    }

protected:
    inline bool zipEntrySafe (
        off_t offset,
        Entry &entry,
        bool validatePrevLen = false
    ) const noexcept
    {
        uint8_t *zipFirst = zipList + HEAD_SIZE;
        uint8_t *zipLast = zipList + intConv32(head->size) - ZIP_END_SIZE;
#define OUT_OF_RANGE(p) unlikely((p) < zipFirst || (p) > zipLast)

        uint8_t *p = zipList + offset;
        entry.p = p;
        if (p >= zipFirst && p + 10 < zipLast)
        {
            entry = zipEntry(offset);
            if (unlikely(entry.lenSize == 0))
                return false;
            if (OUT_OF_RANGE(p + entry.len + entry.headerSize))
                return false;
            if (validatePrevLen && OUT_OF_RANGE(p - entry.prevLen))
                return false;

            return true;
        }

        if (OUT_OF_RANGE(p))
            return false;
        zipDecodePrevLenSize(entry, offset);
        if (OUT_OF_RANGE(p + entry.prevLenSize))
            return false;

        zipDecodeEncoding(entry, offset);
        entry.lenSize = zipEncodeLenSize(entry.encoding);
        if (entry.lenSize == ZIP_ENCODING_SIZE_INVALID)
            return false;
        if (OUT_OF_RANGE(entry.prevLenSize + entry.lenSize + p))
            return false;

        zipDecodePrev(entry, offset);
        zipDecodeLen(entry, offset);
        entry.headerSize = entry.prevLenSize + entry.lenSize;

        if (OUT_OF_RANGE(entry.headerSize + entry.len + p))
            return false;
        if (validatePrevLen && OUT_OF_RANGE(p - entry.prevLen))
            return false;

        return true;
#undef OUT_OF_RANGE
    }
    inline Entry zipEntry (off_t offset) const noexcept
    {
        Entry entry {};
        entry.p = zipList + offset;
        zipDecodePrev(entry, offset);
        zipDecodeEncoding(entry, offset);
        zipDecodeLen(entry, offset);
        entry.headerSize = entry.prevLenSize + entry.lenSize;
    }

    inline void zipDecodePrevLenSize (Entry &entry, off_t &offset) const noexcept
    {
        uint8_t *p = zipList + offset;
        entry.prevLenSize = ZipListHelper::zipDecodePrevLenSize(*p);
        if (entry.prevLenSize > 5)
            offset++;
    }

    inline void zipDecodePrev (Entry &entry, off_t &offset) const noexcept
    {
        zipDecodePrevLenSize(entry, offset);
        uint8_t *p = zipList + offset;
        if (entry.prevLenSize == 1)
            entry.prevLen = *p;
        else
        {
            entry.prevLenSize = *reinterpret_cast<uint32_t *>(p + 1);
            offset += 4;
        }
    }

    inline void zipDecodeEncoding (Entry &entry, off_t &offset) const noexcept
    {
        uint8_t *p = zipList + offset;
        entry.encoding = *p;
        // 此处为占用2个bit的encoding
        if (entry.encoding < ZIP_STR_MASK)
            // &= 0xc0，只保留最高2位
            entry.encoding &= ZIP_STR_MASK;
    }

    inline void zipDecodeLen (Entry &entry, off_t &offset) const noexcept
    {
        uint8_t *p = zipList + offset++;
        if (entry.encoding < ZIP_STR_MASK)
        {
            if (entry.encoding == ZIP_STR_06B) // 00
            {
                entry.lenSize = 1;
                entry.len = entry.encoding & ZIP_CONTENT_MASK; // 去掉头两位
            }
            else if (entry.encoding == ZIP_STR_14B) // 01
            {
                entry.lenSize = 2;
                entry.len = entry.encoding & ZIP_CONTENT_MASK + *(p + 1);
                offset++;
            }
            else if (entry.encoding == ZIP_STR_32B) // 10
            {
                entry.lenSize = 5;
                entry.len = *reinterpret_cast<uint32_t *>(p + 1);
                offset += 4;
            }
            else
            {
                entry.lenSize = 0;
                entry.len = 0;
                PRINT_ERROR("zipList encode error : %p , offset : %l", zipList, offset - 1);
            }
        }
        else
        {
            entry.lenSize = 1;
            switch (entry.encoding)
            {
                case ZIP_INT_8B:
                    entry.len = 1;
                    break;
                case ZIP_INT_16B:
                    entry.len = 2;
                    break;;
                case ZIP_INT_24B:
                    entry.len = 3;
                    break;
                case ZIP_INT_32B:
                    entry.len = 4;
                    break;
                case ZIP_INT_64B:
                    entry.len = 8;
                    break;
                default:
                    entry.len = 0;
                    // 1111xxxx	1 byte	4 bit 无符号整数，介于 0 至 12 之间
                    if (entry.encoding < ZIP_INT_IMM_MIN || entry.encoding > ZIP_INT_IMM_MAX)
                    {
                        entry.lenSize = 0;
                        PRINT_ERROR("4 bit unsigned number must be 0 ~ 12, check %d",
                            entry.encoding & 0xf);
                    }
            }
        }
    }

    uint8_t *zipList;
    static constexpr int HEAD_SIZE = sizeof(ZipListHead);
    ZipListHead *head = reinterpret_cast<ZipListHead *>(zipList);
};
#endif //LINUX_SERVER_KV_STORE_SERVER_DATA_STRUCTURE_KV_ZIPLIST_BASE_H_
