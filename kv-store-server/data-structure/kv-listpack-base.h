//
// Created by 115282 on 2023/11/22.
//

#ifndef KV_STORE_KV_STORE_SERVER_DATA_STRUCTURE_KV_LISTPACK_BASE_H_
#define KV_STORE_KV_STORE_SERVER_DATA_STRUCTURE_KV_LISTPACK_BASE_H_

#include "util/string-helper.h"
class KvListPack;
namespace __KV_PRIVATE__
{
    /*
     * string
     * 10|xxxxxx <string-data> 6位字符串长度，后6位为字符串长度，再之后则是字符串内容（0 ~ 63 bytes）
     * 1110|xxxx yyyyyyyy -- string with length up to 4095，即12位长度的字符串，后4位及下个字节为字符串长度，再之后的为字符串数据。
     * 1111|0000 <4 bytes len> <large string>，即32位长度字符串，后4字节为字符串长度，再之后为字符串内容
     */

    /*
     * integer
     * 0|xxxxxxx 表示7位无符号整型，后7位为数据。
     * 110|xxxxx yyyyyyyy -- 13 bit signed integer，即13位整型，后5位及下个字节为数据内容
     * 1111|0001 <16 bits signed integer>，即16位整型，后2字节为数据
     * 1111|0010 <24 bits signed integer>，即24位整型，后3字节为数据
     * 1111|0011 <32 bits signed integer>，即32位整型，后4字节为数据
     * 1111|0100 <64 bits signed integer>，即64位整型，后8字节为数据
     */
    class QuickListNode;
    class QuickListHelper;
    class QuickListBase;
#define LP_ENCODING_UINT_7BIT 0
#define LP_ENCODING_UINT_7BIT_MASK 0x80
#define LP_ENCODING_UINT_7BIT_MIN 0
#define LP_ENCODING_UINT_7BIT_MAX (~0x80 & 0xff)
#define LP_ENCODING_IS_UINT_7BIT(encoding) ((encoding) & LP_ENCODING_UINT_7BIT_MASK) == LP_ENCODING_UINT_7BIT
#define LP_ENCODING_UINT_7BIT_ENTRY_SIZE 2

#define LP_ENCODING_INT_13BIT 0xc0
#define LP_ENCODING_INT_13BIT_MASK 0xe0
#define LP_ENCODING_INT_13BIT_MIN ~LP_ENCODING_INT_13BIT_MAX
#define LP_ENCODING_INT_13BIT_MAX ((0x1f << 8 | 0xff) >> 1)
#define LP_ENCODING_IS_INT_13BIT(encoding) ((encoding) & LP_ENCODING_INT_13BIT_MASK) == LP_ENCODING_INT_13BIT
#define LP_ENCODING_INT_13BIT_ENTRY_SIZE 3

#define LP_ENCODING_INT_16BIT 0xf1
#define LP_ENCODING_INT_16BIT_MASK 0xff
#define LP_ENCODING_INT_16BIT_MIN ~LP_ENCODING_INT_16BIT_MAX
#define LP_ENCODING_INT_16BIT_MAX (0xffff >> 1)
#define LP_ENCODING_IS_INT_16BIT(encoding) ((encoding) & LP_ENCODING_INT_16BIT_MASK) == LP_ENCODING_INT_16BIT
#define LP_ENCODING_INT_16BIT_ENTRY_SIZE 4

#define LP_ENCODING_INT_24BIT 0xf2
#define LP_ENCODING_INT_24BIT_MASK 0xff
#define LP_ENCODING_INT_24BIT_MIN ~LP_ENCODING_INT_24BIT_MAX
#define LP_ENCODING_INT_24BIT_MAX (0xffffff >> 1)
#define LP_ENCODING_IS_INT_24BIT(encoding) ((encoding) & LP_ENCODING_INT_24BIT_MASK) == LP_ENCODING_INT_24BIT
#define LP_ENCODING_INT_24BIT_ENTRY_SIZE 5

#define LP_ENCODING_INT_32BIT 0xf3
#define LP_ENCODING_INT_32BIT_MASK 0xff
#define LP_ENCODING_INT_32BIT_MIN ~LP_ENCODING_INT_32BIT_MAX
#define LP_ENCODING_INT_32BIT_MAX (0xffffffffll >> 1)
#define LP_ENCODING_IS_INT_32BIT(encoding) ((encoding) & LP_ENCODING_INT_32BIT_MASK) == LP_ENCODING_INT_32BIT
#define LP_ENCODING_INT_32BIT_ENTRY_SIZE 6

#define LP_ENCODING_INT_64BIT 0xf4
#define LP_ENCODING_INT_64BIT_MASK 0xff
#define LP_ENCODING_INT_64BIT_MIN ~LP_ENCODING_INT_64BIT_MAX
#define LP_ENCODING_INT_64BIT_MAX (0xffffffffffffffffll >> 1)
#define LP_ENCODING_IS_INT_64BIT(encoding) ((encoding) & LP_ENCODING_INT_64BIT_MASK) == LP_ENCODING_INT_64BIT
#define LP_ENCODING_INT_64BIT_ENTRY_SIZE 10

#define LP_ENCODING_INT_LEN(ENCODING) ((ENCODING) - 1)

#define LP_ENCODING_STR_6BIT 0x80
#define LP_ENCODING_STR_6BIT_MASK 0xc0
#define LP_ENCODING_STR_6BIT_MAX (0xff >> 2)
#define LP_STR_6BIT_LEN 1
#define LP_ENCODING_IS_STR_6BIT(encoding) ((encoding) & LP_ENCODING_STR_6BIT_MASK) == LP_ENCODING_STR_6BIT

#define LP_ENCODING_STR_12BIT 0xe0
#define LP_ENCODING_STR_12BIT_MASK 0xf0
#define LP_ENCODING_STR_12BIT_MAX (0xf << 8 | 0xff)
#define LP_STR_12BIT_LEN 2
#define LP_ENCODING_IS_STR_12BIT(encoding) ((encoding) & LP_ENCODING_STR_12BIT_MASK) == LP_ENCODING_STR_12BIT

#define LP_ENCODING_STR_32BIT 0xf0
#define LP_ENCODING_STR_32BIT_MASK 0xff
#define LP_ENCODING_STR_32BIT_MAX 0xffffffff
#define LP_STR_32BIT_LEN 5
#define LP_ENCODING_IS_STR_32BIT(encoding) ((encoding) & LP_ENCODING_STR_32BIT_MASK) == LP_ENCODING_STR_32BIT

#define LP_END 0xff

#define LP_ENCODING_6BIT_STR_LEN(p) *(p) & 0x3f
#define LP_ENCODING_12BIT_STR_LEN(p) (((*(p) & 0xf) << 8) | (*((p) + 1)))
// 存储解码直接使用单字节，可以避免大小端区别引起的差异性
#define LP_ENCODING_32BIT_STR_LEN(p) ((*((p) + 1)) << 0 | (*((p) + 2)) << 8 | (*((p) + 3)) << 16 | (*((p) + 4)) << 24)

    /*
    * coding 每个字节只有7个bits有效，第一位bit是是否结束的标识，1表示未结束，0表示结束
    * 储存从左至右储存高位至低位
    * 0xxxxxxx <8 bits> 0 ~ 0x7f
    * 0xxxxxxx|1xxxxxxx <16 bits> 0x80 ~ 0x7f << 7 | 0x7f
    * 0xxxxxxx|1xxxxxxx|1xxxxxxx <24 bits> 0x7f << 7 | 0x7f ~ 0x7f << 14 | 0x7f << 7 | 0x7f
    * 0xxxxxxx|1xxxxxxx|1xxxxxxx|1xxxxxxx <32 bits>
    * 0xxxxxxx|1xxxxxxx|1xxxxxxx|1xxxxxxx|1xxxxxxx <40 bits>
    * */
#define BACK_LEN_8BIT_MAX 0x7fll
#define BACK_LEN_16BIT_MAX (0x7fll << 7 | BACK_LEN_8BIT_MAX)
#define BACK_LEN_24BIT_MAX (0x7fll << 14 | BACK_LEN_16BIT_MAX)
#define BACK_LEN_32BIT_MAX (0x7fll << 21 | BACK_LEN_24BIT_MAX)
#define BACK_LEN_40BIT_MAX (0x7fll << 28 | BACK_LEN_32BIT_MAX)

#define LP_HEAD_SIZE 6
#define LP_MAX_BACK_LEN_SIZE 5
#define LP_MAX_INT_ENCODING_SIZE (LP_ENCODING_INT_64BIT_ENTRY_SIZE - 1)

#define OUT_OF_RANGE(p) ((p) < listPack + LP_HEAD_SIZE || (p) > listPack + listPackGetHeadTotalSize() - 1)

    class ListPackHelper
    {
    public:
        union DataType // NOLINT
        {
            struct // NOLINT
            {
                const char *s;
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
        class IteratorBase
        {
            friend class ListPack;
            friend class ::KvListPack;
        public:
            virtual inline bool operator== (const IteratorBase &it) const noexcept
            {
                if (!p && !it.p)
                    return true;
                if (p != it.p)
                    return false;
                if (data.mode != it.data.mode)
                    return false;
                if (data.mode == DataTypeEnum::INTEGER && data.data.val != it.data.data.val)
                    return false;
                if (data.mode == DataTypeEnum::STRING && (data.data.str.len != it.data.data.str.len
                    || memcmp(data.data.str.s, it.data.data.str.s, data.data.str.len) != 0))
                    return false;

                return true;
            }
            virtual inline bool operator!= (const IteratorBase &it) const noexcept
            {
                return !(*this == it);
            }
            virtual inline explicit operator bool () const noexcept
            {
                return p != nullptr && data.mode != DataTypeEnum::ERROR;
            }

            virtual inline const IteratorDataType &operator* () const noexcept
            {
                return data;
            }
            virtual inline const IteratorDataType *operator-> () const noexcept
            {
                return &data;
            }

        protected:
            explicit IteratorBase (const uint8_t *ptr)
                : p(ptr) {}
            const uint8_t *p {};
            IteratorDataType data {};
            size_t entrySize {};
        };

        class Iterator : public IteratorBase
        {
            friend class ListPack;
            friend class ::KvListPack;
            friend class QuickListBase;
        public:
            virtual inline Iterator &operator++ ()
            {
                p += entrySize;

                getVal();
                return *this;
            }
            inline Iterator operator++ (int) // NOLINT
            {
                auto old = *this;
                ++(*this);
                return old;
            }
        protected:
            explicit Iterator (const uint8_t *ptr)
                : IteratorBase(ptr)
            {
                if (p)
                    getVal();
            }

            void getVal ()
            {
                if (*p == LP_END)
                    p = nullptr;
                else
                    data = ListPackHelper::listPackGetValue(p, &entrySize);
            }
        };

        class ReverseIterator : public IteratorBase
        {
            friend class ListPack;
            friend class ::KvListPack;
            friend class QuickListBase;
        public:
            virtual inline ReverseIterator &operator++ ()
            {
                p -= entrySize;
                if (p == firstEntry)
                    p = nullptr;
                else
                    getValue();

                return *this;
            }
            inline ReverseIterator operator++ (int) // NOLINT
            {
                auto old = *this;
                ++(*this);
                return old;
            }
        protected:
            explicit ReverseIterator (const uint8_t *listPack, const uint8_t *ptr)
                : IteratorBase(ptr), firstEntry(listPack + LP_HEAD_SIZE)
            {
                if (ptr <= firstEntry) p = nullptr;
                if (p)
                    getValue();
            }

            inline void getValue ()
            {
                uint8_t backLenSize;
                entrySize = listPackDecodingBackLen(p, &backLenSize);
                entrySize += backLenSize;
                data = listPackGetValue(p - entrySize);
            }

            const uint8_t *firstEntry {};
        };
    protected:
        struct IntegerCodingStruct
        {
            uint8_t coding[LP_MAX_INT_ENCODING_SIZE];
            uint8_t len;
        };
        struct BackLenStruct
        {
            uint8_t coding[LP_MAX_BACK_LEN_SIZE];
            uint8_t len;
        };
        using StringCodingStruct = BackLenStruct;

        // 获取int 数据编码和编码长度
        template <class T>
        static IntegerCodingStruct listPackEncodeInteger (T v) noexcept
        {
#define TRANSFORM_NEGATIVE(bit) if((v) < 0) (v) += (1LL << (bit))
#define GET_LOW_8BIT(v) ((v) & 0xff)
            IntegerCodingStruct integerCoding {};
            if (v >= LP_ENCODING_UINT_7BIT_MIN && v <= LP_ENCODING_UINT_7BIT_MAX)
            {
                integerCoding.coding[0] = v;
                integerCoding.len = LP_ENCODING_INT_LEN(LP_ENCODING_UINT_7BIT_ENTRY_SIZE);
            }
            else if (v >= LP_ENCODING_INT_13BIT_MIN && v <= LP_ENCODING_INT_13BIT_MAX)
            {
                TRANSFORM_NEGATIVE(13);
                integerCoding.coding[0] = LP_ENCODING_INT_13BIT | (v >> 8);
                integerCoding.coding[1] = GET_LOW_8BIT(v);
                integerCoding.len = LP_ENCODING_INT_LEN(LP_ENCODING_INT_13BIT_ENTRY_SIZE);
            }
            else if (v >= LP_ENCODING_INT_16BIT_MIN && v <= LP_ENCODING_INT_16BIT_MAX)
            {
                TRANSFORM_NEGATIVE(16);
                integerCoding.coding[0] = LP_ENCODING_INT_16BIT;
                integerCoding.coding[1] = GET_LOW_8BIT(v);
                integerCoding.coding[2] = v >> 8;
                integerCoding.len = LP_ENCODING_INT_LEN(LP_ENCODING_INT_16BIT_ENTRY_SIZE);
            }
            else if (v >= LP_ENCODING_INT_24BIT_MIN && v <= LP_ENCODING_INT_24BIT_MAX)
            {
                TRANSFORM_NEGATIVE(24);
                integerCoding.coding[0] = LP_ENCODING_INT_24BIT;
                integerCoding.coding[1] = GET_LOW_8BIT(v);
                integerCoding.coding[2] = GET_LOW_8BIT(v >> 8);
                integerCoding.coding[3] = v >> 16;
                integerCoding.len = LP_ENCODING_INT_LEN(LP_ENCODING_INT_24BIT_ENTRY_SIZE);
            }
            else if (v >= LP_ENCODING_INT_32BIT_MIN && v <= LP_ENCODING_INT_32BIT_MAX)
            {
                TRANSFORM_NEGATIVE(32);
                integerCoding.coding[0] = LP_ENCODING_INT_32BIT;
                integerCoding.coding[1] = GET_LOW_8BIT(v);
                integerCoding.coding[2] = GET_LOW_8BIT(v >> 8);
                integerCoding.coding[3] = GET_LOW_8BIT(v >> 16);
                integerCoding.coding[4] = v >> 24;
                integerCoding.len = LP_ENCODING_INT_LEN(LP_ENCODING_INT_32BIT_ENTRY_SIZE);
            }
            else
            {
                uint64_t uv = v;
                integerCoding.coding[0] = LP_ENCODING_INT_64BIT;
                integerCoding.coding[1] = GET_LOW_8BIT(uv);
                integerCoding.coding[2] = GET_LOW_8BIT(uv >> 8);
                integerCoding.coding[3] = GET_LOW_8BIT(uv >> 16);
                integerCoding.coding[4] = GET_LOW_8BIT(uv >> 24);
                integerCoding.coding[5] = GET_LOW_8BIT(uv >> 32);
                integerCoding.coding[6] = GET_LOW_8BIT(uv >> 40);
                integerCoding.coding[7] = GET_LOW_8BIT(uv >> 48);
                integerCoding.coding[8] = uv >> 56;
                integerCoding.len = LP_ENCODING_INT_LEN(LP_ENCODING_INT_64BIT_ENTRY_SIZE);
            }

            return integerCoding;
#undef TRANSFORM_NEGATIVE
#undef GET_LOW_8BIT
        }

        static StringCodingStruct listPackEncodeString (const StringType &s) noexcept
        {
            StringCodingStruct stringCodingStruct {};

            auto size = s.size();
            if (size <= LP_ENCODING_STR_6BIT_MAX)
            {
                stringCodingStruct.coding[0] = size | LP_ENCODING_STR_6BIT;
                stringCodingStruct.len = 1;
            }
            else if (size <= LP_ENCODING_STR_12BIT_MAX)
            {
                stringCodingStruct.coding[0] = (size >> 8) | LP_ENCODING_STR_12BIT;
                stringCodingStruct.coding[1] = size & 0xff;
                stringCodingStruct.len = 2;
            }
            else if (size <= LP_ENCODING_STR_32BIT_MAX)
            {
                stringCodingStruct.coding[0] = LP_ENCODING_INT_32BIT;
                stringCodingStruct.coding[1] = size >> 24;
                stringCodingStruct.coding[2] = (size >> 16) & 0xff;
                stringCodingStruct.coding[3] = (size >> 8) & 0xff;
                stringCodingStruct.coding[4] = size & 0xff;
                stringCodingStruct.len = 5;
            }
            else
            {
                PRINT_ERROR(
                    "out of range in string size, Maximum allowed length : %d, string size : %d",
                    LP_ENCODING_STR_32BIT_MAX,
                    size);
                memset(stringCodingStruct.coding, 0, LP_MAX_BACK_LEN_SIZE);
                stringCodingStruct.len = 0;
            }
            return stringCodingStruct;
        }

        // 获取节点backLen 编码及编码长度
        static BackLenStruct listPackEncodingBackLen (size_t v) noexcept
        {
#define GET_LOW_7BIT_AND_FILL_HIGHEST(v) (((v) & 0x7f) | 0x80)
            BackLenStruct backLenStruct {};
            if (v <= BACK_LEN_8BIT_MAX)
            {
                backLenStruct.coding[0] = v;
                backLenStruct.len = 1;
            }
            else if (v <= BACK_LEN_16BIT_MAX)
            {
                backLenStruct.coding[0] = v >> 7;
                backLenStruct.coding[1] = GET_LOW_7BIT_AND_FILL_HIGHEST(v);
                backLenStruct.len = 2;
            }
            else if (v <= BACK_LEN_24BIT_MAX)
            {
                backLenStruct.coding[0] = v >> 14;
                backLenStruct.coding[1] = GET_LOW_7BIT_AND_FILL_HIGHEST(v >> 7);
                backLenStruct.coding[2] = GET_LOW_7BIT_AND_FILL_HIGHEST(v);
                backLenStruct.len = 3;
            }
            else if (v <= BACK_LEN_32BIT_MAX)
            {
                backLenStruct.coding[0] = v >> 21;
                backLenStruct.coding[1] = GET_LOW_7BIT_AND_FILL_HIGHEST(v >> 14);
                backLenStruct.coding[2] = GET_LOW_7BIT_AND_FILL_HIGHEST(v >> 7);
                backLenStruct.coding[3] = GET_LOW_7BIT_AND_FILL_HIGHEST(v);
                backLenStruct.len = 4;
            }
            else
            {
                backLenStruct.coding[0] = v >> 28;
                backLenStruct.coding[1] = GET_LOW_7BIT_AND_FILL_HIGHEST(v >> 21);
                backLenStruct.coding[2] = GET_LOW_7BIT_AND_FILL_HIGHEST(v >> 14);
                backLenStruct.coding[3] = GET_LOW_7BIT_AND_FILL_HIGHEST(v >> 7);
                backLenStruct.coding[4] = GET_LOW_7BIT_AND_FILL_HIGHEST(v);
                backLenStruct.len = 5;
            }
            return backLenStruct;
#undef GET_LOW_7BIT_AND_FILL_HIGHEST
        }

        static size_t listPackDecodingBackLen (const uint8_t *p, uint8_t *backLenSize = nullptr)
        {
            size_t shift = 0;
            size_t val = 0;
            auto *temp = p - 1;

            while (shift <= 28)
            {
                val |= ((*temp & 127) << shift);
                if (!(*temp & 128)) break;
                temp--;
                shift += 7;
            }
            if (backLenSize) *backLenSize = (p - temp) / sizeof(uint8_t);

            return val;
        }

        static IteratorDataType listPackGetValue (const uint8_t *p, size_t *entrySize = nullptr)
        {
            IteratorDataType data {};
            BackLenStruct backLenStruct {};
#define FILL_STR_FIELD(bit) \
    do {data.mode = DataTypeEnum::STRING; \
    data.data.str.len = LP_ENCODING_##bit##BIT_STR_LEN(p); \
    data.data.str.s = reinterpret_cast<const char *>(p) + LP_STR_##bit##BIT_LEN;           \
    if (entrySize) {        \
        backLenStruct = listPackEncodingBackLen(data.data.str.len + LP_STR_##bit##BIT_LEN); \
        *entrySize = data.data.str.len + LP_STR_##bit##BIT_LEN + backLenStruct.len;   \
    }} while(0)
#define FILL_INTEGER_ENTRY_SIZE_AND_MODEL(v, size) \
    data.data.val = (v);  \
    data.mode = DataTypeEnum::INTEGER;  \
    if (entrySize) *entrySize = (size)
#define FILL_INTEGER_FIELD(v, bit) \
    do{                            \
        FILL_INTEGER_ENTRY_SIZE_AND_MODEL(v, LP_ENCODING_INT_##bit##BIT_ENTRY_SIZE); \
        if (data.data.val > LP_ENCODING_INT_##bit##BIT_MAX)  \
        {   \
            data.data.val -= 1ll << (bit);  \
            data.data.val |= 1ll << 63; \
        }} while(0)

            if (!p)
            {
                PRINT_ERROR("params : point is null");
                data.mode = DataTypeEnum::ERROR;
            }
            else
            {
                if (LP_ENCODING_IS_STR_6BIT(*p))
                {
                    FILL_STR_FIELD(6);
                }
                else if (LP_ENCODING_IS_STR_12BIT(*p))
                {
                    FILL_STR_FIELD(12);
                }
                else if (LP_ENCODING_IS_STR_32BIT(*p))
                {
                    FILL_STR_FIELD(32);
                }
                else if (LP_ENCODING_IS_UINT_7BIT(*p))
                {
                    FILL_INTEGER_ENTRY_SIZE_AND_MODEL(*p & 0x7f, LP_ENCODING_UINT_7BIT_ENTRY_SIZE);
                }
                else if (LP_ENCODING_IS_INT_13BIT(*p))
                {
                    FILL_INTEGER_FIELD((p[0] & 0x1f) << 8 | p[1], 13);
                }
                else if (LP_ENCODING_IS_INT_16BIT(*p))
                {
                    FILL_INTEGER_FIELD(p[1] | p[2] << 8, 16);
                }
                else if (LP_ENCODING_IS_INT_24BIT(*p))
                {
                    FILL_INTEGER_FIELD((uint64_t)p[1] | (uint64_t)p[2] << 8 | (uint64_t)p[3] << 16,
                        24);
                }
                else if (LP_ENCODING_IS_INT_32BIT(*p))
                {
                    FILL_INTEGER_FIELD(
                        (uint64_t)p[1] | (uint64_t)p[2] << 8 | (uint64_t)p[3] << 16
                            | (uint64_t)p[3] << 24,
                        32);
                }
                else if (LP_ENCODING_IS_INT_64BIT(*p))
                {
                    FILL_INTEGER_ENTRY_SIZE_AND_MODEL(
                        (uint64_t)p[1] | (uint64_t)p[2] << 8 | (uint64_t)p[3] << 16
                            | (uint64_t)p[4] << 24 | (uint64_t)p[5] << 32 | (uint64_t)p[6] << 40
                            | (uint64_t)p[7] << 48 | (uint64_t)p[8] << 56,
                        LP_ENCODING_INT_64BIT_ENTRY_SIZE);
                }
                else
                {
                    PRINT_ERROR("listPack encoding error");
                    data.mode = DataTypeEnum::ERROR;
                }
            }

            return data;
#undef FILL_STR_FIELD
#undef FILL_INTEGER_ENTRY_SIZE_AND_MODEL
#undef FILL_INTEGER_FIELD
        }

        static void listPackFillStringMemory (
            uint8_t *p,
            const StringType &s,
            const StringCodingStruct &stringCodingStruct,
            const BackLenStruct &backLenStruct
        )
        {
            auto *temp = p;
            // 填充encoding
            memcpy(temp, stringCodingStruct.coding, stringCodingStruct.len);
            temp += stringCodingStruct.len;
            // 填充字符串
            memcpy(temp, s.c_str(), s.size());
            temp += s.size();
            // 填充backLen
            memcpy(temp, backLenStruct.coding, backLenStruct.len);
        }

        static void listPackFillIntegerMemory (
            uint8_t *p,
            const IntegerCodingStruct &integerCodingStruct,
            const BackLenStruct &backLenStruct
        )
        {
            memcpy(p, integerCodingStruct.coding, integerCodingStruct.len);
            memcpy(p + integerCodingStruct.len, backLenStruct.coding, backLenStruct.len);
        }
    };

    class ListPack : protected ListPackHelper
    {
    protected:
        ListPack ()
        {
            init();
        }

        ~ListPack () noexcept
        {
            free(listPack);
        }

        ListPack (const ListPack &r)
        {
            operator=(r);
        }

        ListPack (ListPack &&r) noexcept
        {
            operator=(std::move(r));
        }

        ListPack &operator= (const ListPack &r)
        {
            if (this == &r)
                return *this;

            auto size = r.listPackGetHeadTotalSize();
            listPack = static_cast<uint8_t *>(realloc(listPack, size));
            memcpy(listPack, r.listPack, size);

            return *this;
        }

        ListPack &operator= (ListPack &&r) noexcept
        {
            if (this == &r)
                return *this;

            listPack = r.listPack;
            r.listPack = nullptr;

            return *this;
        }

        void clear () noexcept
        {
            free(listPack);
            listPack = nullptr;
        }

        void init()
        {
            listPack = static_cast<uint8_t *>(malloc(LP_HEAD_SIZE + 1));
            if (!listPack)
            {
                PRINT_ERROR("malloc error");
                return;
            }

            listPackSetHeadTotalSize(LP_HEAD_SIZE + 1);
            listPackSetHeadElementsSize(0);
            *(listPack + LP_HEAD_SIZE) = LP_END;
        }

        size_t listPackMerge (const ListPack &r)
        {
            auto rListSize = r.listPackGetHeadTotalSize() - LP_HEAD_SIZE - 1;
            auto rEle = r.listPackGetHeadElementsSize();
            auto size = listPackGetHeadTotalSize();
            if (!listPackResize(size + rListSize))
                return 0;

            memcpy(listPack + size - 1, r.listPack + LP_HEAD_SIZE, rListSize + 1);

            listPackSetHeadTotalSize(size + rListSize);
            listPackSetHeadElementsSize(listPackGetHeadElementsSize() + rEle);

            return size + rListSize;
        }

        size_t listPackInsert (off_t offset, const StringType &s)
        {
            long long v;
            if (Utils::StringHelper::stringIsLL(s, &v))
                return listPackInsert(offset, v);

            return listPackFillString(offset, s);
        }

        template <class T, class = typename std::enable_if <
            std::is_integral <typename std::remove_reference <T>::type>::value, T>::type>
        size_t listPackInsert (off_t offset, T val)
        {
            auto encodingStruct = listPackEncodeInteger(val);
            auto backLenStruct = listPackEncodingBackLen(encodingStruct.len);

            size_t newSize = listPackGetHeadTotalSize() + encodingStruct.len + backLenStruct.len;
            if (!listPackOperationPrivate(offset, newSize, 1))
                return 0;

            listPackFillIntegerMemory(listPack + offset, encodingStruct, backLenStruct);

            return encodingStruct.len + backLenStruct.len;
        }

        template <class T, class = typename std::enable_if <
            std::is_integral <typename std::remove_reference <T>::type>::value, T>::type>
        size_t listPackInsert (off_t offset, std::initializer_list <T> val)
        {
            auto size = val.size();
            IntegerCodingStruct integerCodingStruct[size];
            BackLenStruct backLenStruct[size];
            auto it = val.begin();
            size_t newSize = listPackGetHeadTotalSize();
            for (size_t i = 0; i < size; ++i)
            {
                integerCodingStruct[i] = listPackEncodeInteger(*it);
                backLenStruct[i] = listPackEncodingBackLen(integerCodingStruct[i].len);
                newSize += integerCodingStruct[i].len + backLenStruct[i].len;
            }

            if (!listPackOperationPrivate(offset, newSize, size))
                return 0;

            off_t oldOff = offset;
            for (size_t i = 0; i < size; ++i)
            {
                listPackFillIntegerMemory(
                    listPack + offset,
                    integerCodingStruct[i],
                    backLenStruct[i]
                );
                offset += integerCodingStruct[i].len + backLenStruct[i].len;
            }

            return offset - oldOff;
        }

        size_t listPackDelete (off_t offset, size_t num)
        {
            auto *p = listPack + offset;
            if (OUT_OF_RANGE(p))
            {
                PRINT_ERROR("out of range in listPack, listPack : %p, p : %p", listPack, p);
                return 0;
            }

            size_t deleteNum = 0;
            size_t entrySize;
            for (size_t i = 0; i < num; ++i)
            {
                if (*p == LP_END)
                    break;

                listPackGetValue(p, &entrySize);
                p += entrySize;
                deleteNum++;
            }

            return listPackDeleteSafe(offset, p - listPack, deleteNum);
        }

        size_t listPackDeleteSafe (off_t begin, off_t end, size_t num)
        {
            if (listPackOperationPrivate(end, listPackGetHeadTotalSize() - end + begin, -num))
                return num;

            return 0;
        }

        bool listPackReplace (off_t offset, const StringType &s)
        {
            long long val;
            if (Utils::StringHelper::stringIsLL(s, &val))
                return listPackReplace(offset, val);

            size_t entrySize;
            listPackGetValue(listPack + offset, &entrySize);

            auto stringCodingStruct = listPackEncodeString(s);
            auto backLenStruct = listPackEncodingBackLen(stringCodingStruct.len + s.size());

            int diff = stringCodingStruct.len + s.size() + backLenStruct.len - entrySize;
            if (diff != 0 && !listPackOperationPrivate(
                offset + entrySize,
                listPackGetHeadTotalSize() + diff,
                0
            ))
                return false;

            listPackFillStringMemory(listPack + offset, s, stringCodingStruct, backLenStruct);
            return true;
        }

        template <class T, class = typename std::enable_if <
            std::is_integral <typename std::remove_reference <T>::type>::value, T>::type>
        bool listPackReplace (off_t offset, T val)
        {
            size_t entrySize;
            listPackGetValue(listPack + offset, &entrySize);

            auto integerStruct = listPackEncodeInteger(val);
            auto backLenStruct = listPackEncodingBackLen(integerStruct.len);

            int diff = integerStruct.len + backLenStruct.len - entrySize;
            if (diff != 0 && !listPackOperationPrivate(
                offset + entrySize,
                listPackGetHeadTotalSize() + diff,
                0
            ))
                return false;

            listPackFillIntegerMemory(listPack + offset, integerStruct, backLenStruct);
            return true;
        }

        template <class T, class = typename std::enable_if <
            std::is_integral <typename std::remove_reference <T>::type>::value, T>::type>
        Iterator find (const T val) const
        {
            for (auto it = begin(); it != end(); ++it)
            {
                if (it->mode == DataTypeEnum::INTEGER && it->data.val == val)
                    return it;
            }

            return end();
        }

        Iterator find (const StringType &s) const
        {
            long long val;
            if (Utils::StringHelper::stringIsLL(s, &val))
                return find(val);

            for (auto it = begin(); it != end(); ++it)
            {
                if (it->mode == DataTypeEnum::STRING && it->data.str.len == s.size()
                    && memcmp(it->data.str.s, s.c_str(), it->data.str.len) == 0)
                    return it;
            }

            return end();
        }

        off_t getLastBitOffset () const noexcept
        {
            return static_cast<off_t>(listPackGetHeadTotalSize() - 1);
        }

        Iterator begin () const noexcept
        {
            return Iterator(listPack + LP_HEAD_SIZE);
        }

        Iterator end () const noexcept
        {
            return Iterator(nullptr);
        }

        ReverseIterator rbegin () const noexcept
        {
            return ReverseIterator(listPack, listPack + getLastBitOffset());
        }

        ReverseIterator rend () const noexcept
        {
            return ReverseIterator(listPack, nullptr);
        }

        const uint8_t *getListPackPtr () const noexcept
        {
            return listPack;
        }

        inline size_t listPackGetHeadElementsSize () const noexcept
        {
            return *(listPack + 4) << 0 | *(listPack + 5) << 8;
        }
        inline size_t listPackGetHeadTotalSize () const noexcept
        {
            return (*(listPack)) << 0 | (*(listPack + 1)) << 8 | (*(listPack + 2)) << 16
                | (*(listPack + 3)) << 24;
        }

    private:
        bool listPackFillString (
            off_t offset,
            const std::initializer_list <StringType> &list
        ) noexcept
        {
            auto size = list.size();
            StringCodingStruct stringCodingStruct[size];
            BackLenStruct backLenStruct[size];
            auto it = list.begin();
            size_t totLen = 0;
            auto *p = listPack + offset;
            for (size_t i = 0; i < size; ++i)
            {
                stringCodingStruct[i] = listPackEncodeString(*it);
                if (!stringCodingStruct[i].len)
                    return false;

                backLenStruct[i] = listPackEncodingBackLen(stringCodingStruct[i].len + it->size());
                totLen += stringCodingStruct[i].len + backLenStruct[i].len;

                it++;
            }

            if (!listPackResize(totLen))
                return false;
            memmove(
                listPack + offset + totLen,
                listPack + offset,
                listPackGetHeadTotalSize() - offset
            );

            it = list.begin();
            for (size_t i = 0; i < size; ++i)
            {
                // 填充encoding
                memcpy(p, stringCodingStruct[i].coding, stringCodingStruct[i].len);
                p += stringCodingStruct[i].len;
                // 填充字符串
                memcpy(p, it->c_str(), it->size());
                p += size;
                // 填充backLen
                memcpy(p, backLenStruct[i].coding, backLenStruct[i].len);
            }

            listPackSetHeadTotalSize(totLen);
            listPackSetHeadElementsSize(listPackGetHeadElementsSize() + size);
            return true;
        }

        size_t listPackFillString (off_t offset, const StringType &s) noexcept
        {
            auto stringEncodingStruct = listPackEncodeString(s);
            if (!stringEncodingStruct.len)
                return false;

            auto size = s.size();
            auto backLenStruct = listPackEncodingBackLen(stringEncodingStruct.len + size);
            auto newSize =
                listPackGetHeadTotalSize() + size + stringEncodingStruct.len + backLenStruct.len;
            if (!listPackOperationPrivate(offset, newSize, 1))
                return false;

            listPackFillStringMemory(listPack + offset, s, stringEncodingStruct, backLenStruct);
            return true;
        }

        // 新长度，操作节点数
        bool listPackOperationPrivate (off_t offset, size_t newSize, int count)
        {
            auto oldSize = listPackGetHeadTotalSize();
            if (newSize >= oldSize && !listPackResize(newSize))
                return false;
            listPackSetHeadTotalSize(newSize);
            if (count)
                listPackSetHeadElementsSize(listPackGetHeadElementsSize() + count);

            // 移动后置节点
            memmove(
                listPack + offset + (newSize - oldSize),
                listPack + offset,
                oldSize - offset
            );

            if (newSize < oldSize && !listPackResize(newSize))
                return false;
            return true;
        }
        bool listPackResize (size_t size)
        {
            // 头部最大允许的size长度是4字节，超过抛错
            if (size > std::numeric_limits <uint32_t>::max())
            {
                PRINT_ERROR(
                    "out of range in listPack size, Maximum allowed length : %d, resize : %d",
                    std::numeric_limits <uint32_t>::max(),
                    size);
                return false;
            }
            listPack = static_cast<uint8_t *>(realloc(listPack, size));
            if (!listPack)
            {
                PRINT_ERROR("realloc error");
                return false;
            }

            return true;
        }

        inline void listPackSetHeadTotalSize (size_t v) noexcept
        {
            *listPack = (v) & 0xff;
            *(listPack + 1) = ((v) >> 8) & 0xff;
            *(listPack + 2) = ((v) >> 16) & 0xff;
            *(listPack + 3) = ((v) >> 24) & 0xff;
        }

        inline void listPackSetHeadElementsSize (size_t v) noexcept
        {
            if (v <= std::numeric_limits <uint16_t>::max())
            {
                *(listPack + 4) = v & 0xff;
                *(listPack + 5) = (v >> 8) & 0xff;
            }
        }

        uint8_t *listPack = nullptr;
    };

#undef OUT_OF_RANGE
}
#endif //KV_STORE_KV_STORE_SERVER_DATA_STRUCTURE_KV_LISTPACK_BASE_H_
