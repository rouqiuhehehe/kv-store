//
// Created by 115282 on 2023/10/11.
//

#ifndef LINUX_SERVER_KV_STORE_SERVER_DATA_STRUCTURE_KV_ZIPLIST_BASE_H_
#define LINUX_SERVER_KV_STORE_SERVER_DATA_STRUCTURE_KV_ZIPLIST_BASE_H_

#include "util/endian.h"
#include "util/global.h"
#include <type_traits>

class KvZipList;
namespace __KV_PRIVATE__
{
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

#define ZIP_PREV_LEN_SIZE(prevLen) ((prevLen) < ZIP_BIG_PREV_LEN ? 1 : (sizeof(int32_t) + 1))

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
            const uint8_t *p;               //保存指向当前entry起始位置的指针，而这个指针指向的是当前节点的<prevlen>字段
        };
        union DataType
        {
            struct
            {
                const uint8_t *s;
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
        struct IteratorDataType
        {
            DataType data;
            DataTypeEnum mode;
        };
        class Iterator
        {
            friend class ZipListBase;
            friend class ::KvZipList;
        protected:
            explicit Iterator (const uint8_t *const p) {
                if (!p)
                    entry.p = p;
                else if (*p == ZIP_END)
                    entry.p = nullptr;
                else
                    itData.mode = ZipListHelper::getEntryVal(itData.data, p, &entry);
            }
        public:
            const IteratorDataType &operator* () const noexcept {
                return itData;
            }
            const IteratorDataType *operator-> () const noexcept {
                return &itData;
            }
            Iterator &operator++ () noexcept {
                const uint8_t *p = entry.p + entry.headerSize + entry.len;
                if (*p == ZIP_END)
                    entry.p = nullptr;
                else
                    itData.mode = ZipListHelper::getEntryVal(itData.data, p, &entry);
                return *this;
            }
            Iterator operator++ (int) noexcept // NOLINT
            {
                auto old = *this;
                ++*this;
                return old;
            }
            Iterator &operator+ (uint32_t c) {
                while (c-- > 0)
                    ++*this;

                return *this;
            }
            bool operator== (const Iterator &r) const noexcept {
                return entry.p == r.entry.p;
            }
            bool operator!= (const Iterator &r) const noexcept {
                return !(*this == r);
            }
            explicit operator bool () const noexcept {
                return entry.p != nullptr;
            }

        private:
            Iterator (const IteratorDataType &itData, const Entry &entry)
                : itData(itData), entry(entry) {}
            IteratorDataType itData {};
            Entry entry {};
        };
    protected:
        // 尝试将字符串编码成数字，并填充encoding
        static inline bool zipTryEncodeInteger (
            const uint8_t *s,
            uint32_t len,
            int64_t &value,
            uint8_t &encoding
        ) {
            if (len >= 32 || len == 0) return false;
            int64_t v;
            StringType str(reinterpret_cast<const char *>(s), len);
            if (Utils::StringHelper::stringIsLL(str, reinterpret_cast<long long *>(&v))) {
                encoding = zipEncodeIntegerEncoding(v);

                value = v;
                return true;
            }
            return false;
        }

        static inline uint8_t zipEncodeIntegerEncoding (int64_t v) {
            uint8_t encoding;
            constexpr static int32_t
                int24Min = std::numeric_limits <int32_t>::min() >> sizeof(uint8_t) * 8;
            constexpr static int32_t
                int24Max = std::numeric_limits <int32_t>::max() >> sizeof(uint8_t) * 8;

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
            else
                PRINT_ERROR("error value to encoding : %ll", v);

            return encoding;
        }

        static inline uint32_t zipEncodeLenSize (uint8_t encoding) {
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
        static inline uint32_t zipIntSize (uint8_t encoding) {
            switch (encoding) {
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
        static inline bool zipIsStr (uint8_t encoding) noexcept {
            return (encoding & ZIP_STR_MASK) < ZIP_STR_MASK;
        }
        static inline void zipDecodeInteger (uint8_t *p, int64_t value, uint8_t encoding) {
            if (encoding == ZIP_INT_8B)
                *p = static_cast<uint8_t>(value);
            else if (encoding == ZIP_INT_16B) {
                memcpy(p, &value, sizeof(uint16_t));
                memConv16(p);
            }
            else if (encoding == ZIP_INT_24B) {
                // 先转成小端序
                uint32_t rValue = intConv32(static_cast<int32_t>(value << 8));
                memcpy(
                    p,
                    reinterpret_cast<uint8_t *>(&rValue) + 1,
                    sizeof(int32_t) - sizeof(int8_t));
            }
            else if (encoding == ZIP_INT_32B) {
                memcpy(p, &value, sizeof(int32_t));
                memConv32(p);
            }
            else if (encoding == ZIP_INT_64B) {
                memcpy(p, &value, sizeof(int64_t));
                memConv64(p);
            }
            else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX) {
                // 不用存数据，数据存在encoding中
            }
            else
                PRINT_ERROR("zipDecodeInteger error, encoding : %d", encoding);
        }
        // 将entry data 解码成数字 并赋值给data.val
        static inline bool zipEncodeInteger (const Entry &entry, int64_t &data) noexcept {
            auto *p = entry.p + entry.headerSize;
            if (entry.encoding == ZIP_INT_8B)
                data = static_cast<int64_t>(*reinterpret_cast<const int8_t *>(p)); //NOLINT
            else if (entry.encoding == ZIP_INT_16B) {
                memcpy(&data, p, sizeof(int16_t));
                memConv16(&data);
            }
            else if (entry.encoding == ZIP_INT_24B) {
                // 空出8个字节，复制24个字节
                memcpy(
                    reinterpret_cast<uint8_t *>(&data) + 1,
                    p,
                    sizeof(int32_t) - sizeof(int8_t));
                // 如果是大端需要转换，小端不需要
                memConv32(&data);
                // 小端低位存低地址，假如储存的是0xfff，memcpy后 值为 0xfff0（指针指向低位），所以需要右移一位
                data >>= 1;
            }
            else if (entry.encoding == ZIP_INT_32B) {
                memcpy(&data, p, sizeof(int32_t));
                memConv32(&data);
            }
            else if (entry.encoding == ZIP_INT_64B) {
                memcpy(&data, p, sizeof(int64_t));
                memConv64(&data);
            }
            else if (entry.encoding >= ZIP_INT_IMM_MIN && entry.encoding <= ZIP_INT_IMM_MAX)
                data = (entry.encoding & 0xf) - 1; // 1111xxxx & 00001111 取出 xxxx - 1
            else {
                PRINT_ERROR("encoding error : %d", entry.encoding);
                return false;
            }
            return true;
        }

        // 根据encoding，编码data所占用长度
        // buf 至少需要5字节
        static inline uint32_t zipEncodeEncoding (uint8_t encoding, uint32_t dataSize) {
            uint32_t len;
            if (zipIsStr(encoding)) {
                if (dataSize < (1 << 6)) // 头两位用于编码00，只有6位可用，最大长度 (1 << 6) - 1
                    len = 1;
                else if (dataSize < (1 << 14)) // 头两位用于编码01，双字节，有14位可用，最大长度 (1 << 14) - 1
                    len = 2;
                else
                    len = 5;
            }
            else
                len = 1;

            return len;
        }

        // 此函数会填充buf
        // 根据encoding，编码data所占用长度
        // buf 至少需要5字节
        static inline uint32_t zipEncodeEncoding (uint8_t *buf, uint8_t encoding, uint32_t dataSize) {
            uint32_t len;
            if (zipIsStr(encoding)) {
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
                else {
                    len = 5;
                    buf[0] = ZIP_STR_32B;
                    *reinterpret_cast<uint32_t *>(buf + 1) =
                        intConv32(static_cast<int32_t>(dataSize));
                }
            }
            else {
                len = 1;
                buf[0] = encoding;
            }

            return len;
        }

        static inline uint32_t zipDecodePrevLenSize (uint8_t prevLenSize) {
            if (prevLenSize < ZIP_BIG_PREV_LEN)
                return 1;
            return 5;
        }

        static inline size_t zipDecodePrev (Entry &entry, const uint8_t *p) noexcept {
            entry.prevLenSize = zipDecodePrevLenSize(*p);
            if (entry.prevLenSize == 1)
                entry.prevLen = *p;
            else
                entry.prevLen = *reinterpret_cast<const uint32_t *>(p + 1);

            return entry.prevLenSize;
        }

        static inline void zipDecodeEncoding (Entry &entry, const uint8_t *p) noexcept {
            entry.encoding = *p;
            // 此处为占用2个bit的encoding
            if (entry.encoding < ZIP_STR_MASK)
                // &= 0xc0，只保留最高2位
                entry.encoding &= ZIP_STR_MASK;
        }

        static inline size_t zipDecodeLen (Entry &entry, const uint8_t *p) noexcept {
            size_t ret = 1;
            if (entry.encoding < ZIP_STR_MASK) {
                if (entry.encoding == ZIP_STR_06B) // 00
                {
                    entry.lenSize = 1;
                    entry.len = *p & ZIP_CONTENT_MASK; // 去掉头两位
                }
                else if (entry.encoding == ZIP_STR_14B) // 01
                {
                    entry.lenSize = 2;
                    entry.len = ((*p & ZIP_CONTENT_MASK) << 8) | *(p + 1);
                    ret++;
                }
                else if (entry.encoding == ZIP_STR_32B) // 10
                {
                    entry.lenSize = 5;
                    entry.len = intConv32(*reinterpret_cast<const int32_t *>(p + 1));
                    ret += 4;
                }
                else {
                    entry.lenSize = 0;
                    entry.len = 0;
                    PRINT_ERROR("zipList encode error : %p", p);
                }
            }
            else {
                entry.lenSize = 1;
                switch (entry.encoding) {
                    case ZIP_INT_8B:
                        entry.len = 1;
                        break;
                    case ZIP_INT_16B:
                        entry.len = 2;
                        break;
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
                        if (entry.encoding < ZIP_INT_IMM_MIN || entry.encoding > ZIP_INT_IMM_MAX) {
                            entry.lenSize = 0;
                            PRINT_ERROR("4 bit unsigned number must be 0 ~ 12, check %d",
                                entry.encoding & 0xf);
                        }
                }
            }

            return ret;
        }

        static inline Entry zipEntry (const uint8_t *p) noexcept {
            Entry entry {};
            entry.p = p;
            p += zipDecodePrev(entry, p);
            zipDecodeEncoding(entry, p);
            zipDecodeLen(entry, p);
            entry.headerSize = entry.prevLenSize + entry.lenSize;

            return entry;
        }

        static inline DataTypeEnum getEntryVal (
            DataType &data,
            const uint8_t *p,
            Entry *entryPtr = nullptr
        ) {
            Entry entry = zipEntry(p);
            if (entryPtr) *entryPtr = entry;

            if (zipIsStr(entry.encoding)) {
                data.str.len = entry.len;
                data.str.s = entry.p + entry.headerSize;
                return DataTypeEnum::STRING;
            }
            else if (zipEncodeInteger(entry, data.val))
                return DataTypeEnum::INTEGER;

            return DataTypeEnum::ERROR;
        }
    };

    class ZipListBase : public ZipListHelper
    {
    protected:
        ZipListBase () // NOLINT
        {
            clear();
        }

        ~ZipListBase () noexcept {
            free(zipList);
        }

        ZipListBase (const ZipListBase &rhs) // NOLINT
            : zipList(nullptr) {
            operator=(rhs);
        }

        ZipListBase (ZipListBase &&rhs) noexcept // NOLINT
            : zipList(nullptr) {
            operator=(std::move(rhs));
        }

        ZipListBase &operator= (const ZipListBase &rhs) {
            if (this == &rhs)
                return *this;
            free(zipList);

            uint32_t size = intConv32(static_cast<int32_t>(rhs.head->size));
            zipList = static_cast<uint8_t *>(malloc(size));
            resetHead();
            memcpy(zipList, rhs.zipList, size);

            return *this;
        }

        ZipListBase &operator= (ZipListBase &&rhs) noexcept {
            if (this == &rhs)
                return *this;

            free(zipList);

            zipList = rhs.zipList;
            head = rhs.head;
            rhs.zipList = nullptr;
            rhs.head = nullptr;

            return *this;
        }

        void clear () noexcept {
            zipList = static_cast<uint8_t *>(malloc(HEAD_SIZE + sizeof(uint8_t)));
            resetHead();
            head->size = intConv32(HEAD_SIZE + sizeof(uint8_t));
            // 全部用小端储存
            head->tailOffset = intConv32(HEAD_SIZE);
            head->count = 0;
            zipList[HEAD_SIZE] = ZIP_END;
        }

        Iterator begin () const noexcept {
            return Iterator(zipList + HEAD_SIZE);
        }

        Iterator end () const noexcept // NOLINT
        {
            return Iterator(nullptr);
        }

        inline size_t size () const noexcept {
            size_t count = intConv16(static_cast<int16_t>(head->count));
            if (count >= std::numeric_limits <uint16_t>::max()) {
                count = 0;
                off_t offset = HEAD_SIZE;
                Entry entry {};
                while (*(zipList + offset) != ZIP_END) {
                    zipEntrySafe(offset, entry, false);
                    offset += static_cast<off_t>(entry.headerSize + entry.len);
                    count++;
                }
            }

            return count;
        }

        // 数据，数据长度，需要插入位置的上一个节点长度，需要插入的位置偏移量
        bool zipListInsert (const uint8_t *s, uint32_t len, uint32_t prevLen, off_t offset) noexcept {
            int64_t v;
            uint8_t encoding = 0;
            size_t reqLen;
            if (zipTryEncodeInteger(s, len, v, encoding))
                reqLen = zipIntSize(encoding);
            else
                reqLen = len;

            // prev
            reqLen += ZIP_PREV_LEN_SIZE(prevLen);
            reqLen += ZipListHelper::zipEncodeEncoding(encoding, len);

            if (!zipListInsertPrivate(reqLen, offset))
                return false;

            offset = zipEncodePrevLenSize(prevLen, offset);
            offset = zipEncodeEncoding(offset, encoding, len);
            if (zipIsStr(encoding))
                memcpy(zipList + offset, s, len);
            else
                zipDecodeInteger(zipList + offset, v, encoding);

            zipListIncrLen();

            return true;
        }

        // 数据，数据长度，需要插入位置的上一个节点长度，需要插入的位置偏移量
        template <class T, class = typename std::enable_if <std::is_integral <T>::value, T>::type>
        bool zipListInsert (T &&val, uint32_t prevLen, off_t offset) noexcept {
            // 数字不需要穿dataSize
            constexpr uint32_t DATA_SIZE = 0;
            size_t reqLen;

            uint8_t encoding = zipEncodeIntegerEncoding(val);
            reqLen = zipIntSize(encoding);

            // prev
            reqLen += ZIP_PREV_LEN_SIZE(prevLen);

            reqLen += ZipListHelper::zipEncodeEncoding(encoding, DATA_SIZE);

            if (!zipListInsertPrivate(reqLen, offset))
                return false;

            offset = zipEncodePrevLenSize(prevLen, offset);
            offset = zipEncodeEncoding(offset, encoding, DATA_SIZE);
            zipDecodeInteger(zipList + offset, val, encoding);

            zipListIncrLen();

            return true;
        }

        size_t zipListDelete (off_t offset, size_t num) {
            size_t deleted = 0;
            if (*(zipList + offset) == ZIP_END || num == 0)
                return deleted;

            // 第一个需要删除的元素
            // 不需要使用save，offset需外层校验传入
            Entry first = zipEntry(offset);
            Entry entry {};
            const uint8_t *p = first.p + first.headerSize + first.len;
            deleted++;

            for (size_t i = 1; i < num && *p != ZIP_END; ++i) {
                if (!zipEntrySafe(p - zipList, entry, false)) {
                    PRINT_ERROR("zip encode error on offset %d", p - zipList);
                    return 0;
                }
                deleted++;
                p += entry.headerSize + entry.len;
            }

            return zipListDeletePrivate(p, first, deleted);
        }

        Iterator zipListDelete (const Iterator &first, const Iterator &last) {
            size_t deleted = 0;
            if ((!first.entry.p && !last.entry.p) || (last.entry.p && first.entry.p > last.entry.p))
                return end();

            // 第一个需要删除的元素
            // 不需要使用save，offset需外层校验传入
            const uint8_t *p;
            if (!last.entry.p)
                p = zipList + intConv32(static_cast<int32_t>(head->size)) - 1;
            else
                p = last.entry.p + last.entry.headerSize + last.entry.len;

            Iterator it = first;
            while (++it != last)
                deleted++;

            off_t offset = first.entry.p - zipList;
            if (zipListDeletePrivate(p, first.entry, deleted))
                return Iterator(zipList + offset);

            return end();
        }

        Iterator zipListFind (const uint8_t *s, size_t len) const noexcept {
            Entry entry {};
            uint8_t *p = zipList + HEAD_SIZE;
            int64_t val;
            uint8_t encode = 0;
            while (*p != ZIP_END) {
                if (!zipEntrySafe(p - zipList, entry, true)) {
                    PRINT_ERROR("zip encode error on offset %d", p - zipList);
                    return end();
                }

                if (zipIsStr(entry.encoding)) {
                    if (len == entry.len && memcmp(entry.p + entry.headerSize, s, len) == 0) {
                        return Iterator(p);
                    }
                }
                else {
                    // 只讲字符串转一次数字，如果上方if能够找到，就不需要转数字
                    if (encode == 0) {
                        if (!zipTryEncodeInteger(s, len, val, encode))
                            encode = ZIP_INT_8B + 1;
                    }
                    // 字符串可以转成数字
                    if (encode != ZIP_INT_8B + 1) {
                        int64_t v;
                        zipEncodeInteger(entry, v);
                        if (v == val)
                            return Iterator(p);
                    }
                }

                p += entry.headerSize + entry.len;
            }

            return end();
        }

        template <class T, class = typename std::enable_if <std::is_integral <T>::value, T>::type>
        Iterator zipListFind (T &&val) const noexcept {
            Entry entry {};
            uint8_t *p = zipList + HEAD_SIZE;
            while (*p != ZIP_END) {
                if (!zipEntrySafe(p - zipList, entry, true)) {
                    PRINT_ERROR("zip encode error on offset %d", p - zipList);
                    return end();
                }

                if (!zipIsStr(entry.encoding)) {
                    int64_t v;
                    zipEncodeInteger(entry, v);
                    if (v == val)
                        return Iterator(p);
                }

                p += entry.headerSize + entry.len;
            }

            return end();
        }

        inline bool zipEntrySafe (
            off_t offset,
            Entry &entry,
            bool validatePrevLen = false
        ) const noexcept {
            uint8_t *zipFirst = zipList + HEAD_SIZE;
            uint8_t *zipLast = zipList + intConv32(static_cast<int32_t>(head->size)) - ZIP_END_SIZE;
#define OUT_OF_RANGE(p) unlikely((p) < zipFirst || (p) > zipLast)

            uint8_t *p = zipList + offset;
            entry.p = p;
            if (p >= zipFirst && p + 10 < zipLast) {
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
            entry.prevLenSize = zipDecodePrevLenSize(*p);
            if (OUT_OF_RANGE(p + entry.prevLenSize))
                return false;

            zipDecodeEncoding(entry, offset + entry.prevLenSize);
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

        inline Entry zipEntry (off_t offset) const noexcept {
            uint8_t *p = zipList + offset;
            return ZipListHelper::zipEntry(p);
        }

    private:
        // reqLen插入的节点所占字节数
        bool zipListInsertPrivate (size_t reqLen, off_t offset) {
            int nextDiff = 0;
            Entry tailEntry {};

            bool forceLarge = false;
            auto *p = zipList + offset;
            if (*p != ZIP_END) {
                // 当前添加节点总长度所需prevLenSize字节数 - 当前添加节点后置节点的prevLenSize字节数
                // 判断prevLenSize是否需要扩容/缩容
                nextDiff = ZIP_PREV_LEN_SIZE(reqLen) - ZipListHelper::zipDecodePrevLenSize(*p);
                // 之前节点需要缩容4字节，并且当前节点长度<4，则不做prevLenSize缩容，只做更新
                // 必须保证reqLen + nextDiff >= 0，不然realloc会进行缩容，导致数据丢失
                if (nextDiff == -4 && reqLen < 4) {
                    nextDiff = 0;
                    forceLarge = true;
                }
            }

            size_t listLen = static_cast<int32_t>(head->size);
            if (!zipListResize(intConv32(static_cast<int32_t>(head->size)) + nextDiff + reqLen))
                return false;

            p = zipList + offset; // 指针重新赋值

            if (*p != ZIP_END) {
                // nextDiff 为需要扩容/缩容的字节数 4 0 -4 ，> 0 时为扩容
                // realloc时 扩/缩了nextDiff 个字节，所以此处需要减去nextDiff，用于预留的缩容/扩容
                // intConv32(head->size) + nextDiff + reqLen
                memmove(
                    p + reqLen,
                    p - nextDiff,
                    listLen - offset - 1 + nextDiff
                );
                if (forceLarge)
                    zipEncodePrevLenSize5(reqLen, static_cast<off_t>(offset + reqLen));
                else
                    zipEncodePrevLenSize(reqLen, static_cast<off_t>(offset + reqLen));

                head->tailOffset = intConv32(
                    intConv32(static_cast<int32_t>(head->tailOffset))
                        + static_cast<int32_t>(reqLen));

                if (!zipEntrySafe(static_cast<off_t>(offset + reqLen), tailEntry, true)) {
                    PRINT_ERROR("zip encode error");
                    return false;
                }
                // 检查当前插入的元素是否为倒数第二个元素
                // 因为 扩容缩容对应的是插入节点的后一个节点，如果后一个节点为最后一个节点，则不需要通过nextDiff做偏移，比如nextDiff为4，则对应下一个节点指针偏移需要+4，即tail指针
                // 指向的节点长度应该+4，所以不需要通过nextDiff做偏移
                // 如果不是倒数第二个元素，则需要通过nextDiff做偏移，因为nextDiff仅影响了后一个节点的长度
                if (*(p + tailEntry.headerSize + tailEntry.len + reqLen) != ZIP_END)
                    head->tailOffset =
                        intConv32(intConv32(static_cast<int32_t>(head->tailOffset)) + nextDiff);
            }
            else
                // 如果是添加到最后，直接更新到offset
                head->tailOffset = intConv32(static_cast<int32_t>(offset));

            // 批量更新后置节点的prevLenSize prevLen
            if (nextDiff != 0)
                zipCascadeUpdate(static_cast<off_t>(offset + reqLen));

            return true;
        }

        // 不需要删除的第一个节点，需要删除的第一个节点，需要删除的个数
        size_t zipListDeletePrivate (const uint8_t *p, const Entry &first, size_t deleted) {
            Entry entry {};
            size_t totLen = p - first.p;
            uint32_t setTailOff;
            int nextDiff = 0;
            if (*p != ZIP_END) {
                // 前一个节点总长度所需prevLenSize字节数 - 删除后一个节点的prevLenSize字节数
                // 判断prevLenSize是否需要扩容/缩容
                nextDiff =
                    static_cast<int>(first.prevLenSize - ZipListHelper::zipDecodePrevLenSize(*p));
                // nextDiff = 4时需要扩容4，删除时少删除4个字节
                // nextDiff = -4时需要缩容4，删除时少删除4个字节
                p -= nextDiff;
                zipEncodePrevLenSize(first.prevLenSize, p - zipList);
                // 设置尾部为原尾部 - 需要删除的长度
                setTailOff = intConv32(static_cast<int32_t>(head->tailOffset)) - totLen;
                if (!zipEntrySafe(p - zipList, entry, true)) {
                    PRINT_ERROR("zip encode error on offset %d", p - zipList);
                    return 0;
                }
                // 如果删除的不是倒数第二个元素，则修正尾部指针需要+nextDiff
                // nextDiff 扩/缩的是 p所在节点的字节数，如果p是最后一个元素，不需要+nextDiff
                // 因为head->tailOffset 和 totLen 都是基于未修正的p指针, 如果为最后一个，则等于first.p - zipList
                // memmove 将从p处（即修正后的指针处）移动节点至first处
                if (*(p + entry.headerSize + entry.len) != ZIP_END)
                    setTailOff += nextDiff;

                memmove(
                    const_cast<uint8_t *>(first.p),
                    p,
                    intConv32(static_cast<int32_t>(head->size)) - (p - zipList) - 1
                );
            }
            else
                // 删除整个后部元素
                // 第一个元素的指针 - list开头指针 - 第一个元素上一个节点所占的字节数
                setTailOff = first.p - zipList - first.prevLen;

            off_t offset = first.p - zipList;
            if (!zipListResize(intConv32(static_cast<int32_t>(head->size)) - totLen + nextDiff))
                return 0;

            zipListIncrLen(static_cast<int16_t>(-deleted));
            head->tailOffset = intConv32(static_cast<int32_t>(setTailOff));
            if (nextDiff != 0)
                if (!zipCascadeUpdate(offset))
                    return 0;

            return deleted;
        }

        // 循环更新节点的prevLen
        // offset传入第一个不需要更新的节点偏移量
        // 即offset处节点的后置节点可能触发连续更新
        bool zipCascadeUpdate (off_t offset) {
            constexpr uint8_t delta = 5 - 1;
            auto p = zipList + offset;
            if (*p == ZIP_END) return true;

            // 此处不需要用zipEntrySafe，因为外层传入的offset是已经校验过的
            Entry currentEntry = zipEntry(offset);
            // 用于merge
            uint32_t firstLen;
            uint32_t prevLen = firstLen = currentEntry.headerSize + currentEntry.len;
            uint32_t prevLenSize = ZIP_PREV_LEN_SIZE(prevLen);
            off_t prevOffset = offset;
            size_t extra = 0;

            // 移到后一个节点
            offset += prevLen;
            p = zipList + offset;
            while (*p != ZIP_END) {
                if (!zipEntrySafe(offset, currentEntry)) {
                    PRINT_ERROR("zip encode error on offset %d", offset);
                    return false;
                }
                // 不需要改变
                if (currentEntry.prevLen == prevLen) break;

                // 如果当前prevLenSize >= 上一个节点需要的prevLenSize，则不需要再更新，退出循环
                if (currentEntry.prevLenSize >= prevLenSize) {
                    if (currentEntry.prevLenSize == prevLenSize)
                        zipEncodePrevLenSize(prevLen, offset);
                    else
                        // 避免后一个收缩，以防止后续的多次收缩，以空间换时间
                        zipEncodePrevLenSize5(prevLen, offset);

                    break;
                }

                // 如果不是头节点，并且当前节点的prevLenSize+4（即需要扩容）!= 前一个节点需要的prevLenSize，出错
                if (currentEntry.prevLen != 0 && currentEntry.prevLenSize + delta != prevLenSize) {
                    PRINT_ERROR("update prevLenSize error");
                    return false;
                }

                prevLen = currentEntry.headerSize + currentEntry.len + delta;
                prevLenSize = ZIP_PREV_LEN_SIZE(prevLen);
                prevOffset = offset;
                offset += currentEntry.headerSize + currentEntry.len;
                p = zipList + offset;
                extra += delta;
            }

            // 不需要更新
            if (extra == 0) return true;

            // 如果更新到list尾部
            if (intConv32(static_cast<int32_t>(head->tailOffset)) == offset) {
                // 如果当前节点插入的不是倒数第二个节点
                // 如果是倒数第二个节点，扩充的为最后一个节点的长度，不影响tailOffset的指向
                if (extra != delta)
                    // - delta  因为最后一个节点扩充  不影响tailOffset
                    head->tailOffset =
                        intConv32(static_cast<int32_t>(head->tailOffset + extra - delta));

            }
            else
                head->tailOffset = intConv32(static_cast<int32_t>(head->tailOffset + extra));

            size_t actualLen = intConv32(static_cast<int32_t>(head->size));
            zipListResize(actualLen + extra);
            // 讲不需要扩充prevLenSize的节点后移
            memmove(p + extra, p, actualLen - offset - 1);

            // 将指针移动不需要更新的节点前
            p += extra;
            // 从后往前更新
            while (extra) {
                currentEntry = zipEntry(prevOffset);
                // p指向不需要更新的第一个节点
                // prevOffset指向需要更新的最后一个节点
                // 中间空余内存为 刚拓展的extra字节
                // 将需要更新的最后一个节点除去prevLen的部分移动到不需要更新的第一个节点
                memmove(
                    p - currentEntry.headerSize - currentEntry.len + currentEntry.prevLenSize,
                    zipList + prevOffset + currentEntry.prevLenSize,
                    currentEntry.headerSize + currentEntry.len - currentEntry.prevLenSize
                );

                // 修正指针p指向移动完成的节点的头部
                p -= (currentEntry.headerSize + currentEntry.len + delta);
                // currentEntry指向前一个节点
                // 如果前一个节点是列表的第一个节点
                if (currentEntry.prevLen == 0)
                    zipEncodePrevLenSize(firstLen, p - zipList);
                else
                    zipEncodePrevLenSize(currentEntry.prevLen + delta, p - zipList);

                prevOffset -= currentEntry.prevLen;
                extra -= delta;
            }

            return true;
        }

        inline off_t zipEncodePrevLenSize5 (uint32_t len, off_t offset) {
            *(zipList + offset) = ZIP_BIG_PREV_LEN;
            *reinterpret_cast<uint32_t *>(zipList + offset + 1) =
                intConv32(static_cast<int32_t>(len));
            return offset + 5;
        }

        inline off_t zipEncodePrevLenSize (uint32_t len, off_t offset) {
            if (len < ZIP_BIG_PREV_LEN) {
                *(zipList + offset) = len;
                return offset + 1;
            }
            else
                return zipEncodePrevLenSize5(len, offset);
        }

        inline off_t zipEncodeEncoding (off_t offset, uint8_t encoding, uint32_t dataSize) {
            uint8_t *p = zipList + offset;

            return offset + ZipListHelper::zipEncodeEncoding(p, encoding, dataSize);
        }

        // inline void zipDecodePrevLenSize (Entry &entry, off_t &offset) const noexcept
        // {
        //     uint8_t *p = zipList + offset;
        //     entry.prevLenSize = ZipListHelper::zipDecodePrevLenSize(*p);
        //     if (entry.prevLenSize > 5)
        //         offset++;
        // }

        inline void zipDecodePrev (Entry &entry, off_t &offset) const noexcept {
            uint8_t *p = zipList + offset;
            offset += static_cast<off_t>(ZipListHelper::zipDecodePrev(entry, p));
        }

        inline void zipDecodeEncoding (Entry &entry, off_t offset) const noexcept {
            uint8_t *p = zipList + offset;
            ZipListHelper::zipDecodeEncoding(entry, p);
        }

        inline void zipDecodeLen (Entry &entry, off_t &offset) const noexcept {
            uint8_t *p = zipList + offset;
            offset += static_cast<off_t>(ZipListHelper::zipDecodeLen(entry, p));
        }

        inline bool zipListResize (size_t len) {
            // if (len < static_cast<size_t>(intConv32(static_cast<int32_t>(head->size))))
            // {
            //     PRINT_ERROR("resize length (%d) must greater than old length (%d)",
            //         len,
            //         intConv32(static_cast<int32_t>(head->size)));
            //     return false;
            // }

            zipList = reinterpret_cast<uint8_t *>(realloc(zipList, len));
            resetHead();
            head->size = intConv32(static_cast<int32_t>(len));
            zipList[len - 1] = ZIP_END;
            return true;
        }

        inline void resetHead () {
            head = reinterpret_cast<ZipListHead *>(zipList);
        }

        inline void zipListIncrLen (int16_t incr = 1) {
            if (static_cast<uint16_t>(intConv16(static_cast<int16_t>(head->count)))
                < std::numeric_limits <uint16_t>::max())
                head->count = head->count + incr;
        }

    protected:

        uint8_t *zipList;
        static constexpr int HEAD_SIZE = sizeof(ZipListHead);
        ZipListHead *head;
    };
}
#endif //LINUX_SERVER_KV_STORE_SERVER_DATA_STRUCTURE_KV_ZIPLIST_BASE_H_
