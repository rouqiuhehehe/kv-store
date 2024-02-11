//
// Created by 115282 on 2024/1/10.
//

#ifndef KV_STORE_KV_STORE_SERVER_DATA_STRUCTURE_KV_INTSET_H_
#define KV_STORE_KV_STORE_SERVER_DATA_STRUCTURE_KV_INTSET_H_

#include "util/endian.h"
namespace __KV_PRIVATE__
{
#define INT_SET_INT16 (sizeof(int16_t))
#define INT_SET_INT32 (sizeof(int32_t))
#define INT_SET_INT64 (sizeof(int64_t))

#define DEFINE_PARAMS_IS_INTEGRAL(fnName, ret) template <class T>    \
    typename std::enable_if <std::is_integral <typename std::remove_reference <T>::type>::value, ret>::type \
    fnName (T &&val)

    using namespace Utils::Endian;
    class IntSetBase;
    class IntSetHelper
    {
    protected:
        static int64_t intSetGetVal (const uint8_t *p, uint8_t encoding) {
            switch (encoding) {
                case INT_SET_INT16:
                    return intConv16(*reinterpret_cast<const int16_t *>(p));
                case INT_SET_INT32:
                    return intConv32(*reinterpret_cast<const int32_t *>(p));
                case INT_SET_INT64:
                    return intConv64(*reinterpret_cast<const int64_t *>(p));
                default:
                    unreachable();
            }
        }
        static inline uint32_t intSetGetEncoding (int64_t entry) noexcept {
            if (entry <= std::numeric_limits <int16_t>::max() && entry >= std::numeric_limits <int16_t>::min())
                return INT_SET_INT16;
            else if (entry <= std::numeric_limits <int32_t>::max() && entry >= std::numeric_limits <int32_t>::min())
                return INT_SET_INT32;
            else
                return INT_SET_INT64;
        }
    };
    class IntSetIterator : protected IntSetHelper
    {
        friend class IntSetBase;
        uint8_t encoding;
        uint8_t *p;
        uint8_t *end;

    public:
        FORWARD_ITERATOR_TYPE(int64_t);
        IntSetIterator (uint8_t *p, uint8_t *end, uint8_t encoding)
            : encoding(encoding), p(p), end(end) {}
        inline bool operator== (const IntSetIterator &it) const {
            return encoding == it.encoding && p == it.p;
        }
        inline bool operator!= (const IntSetIterator &it) const {
            return !(*this == it);
        }
        inline explicit operator bool () const noexcept {
            return p != nullptr && encoding != 0;
        }
        IntSetIterator &operator++ () {
            p += encoding;
            if (p >= end) {
                p = nullptr;
                encoding = 0;
            }
            return *this;
        }
        IntSetIterator operator++ (int) // NOLINT
        {
            auto old = *this;
            ++*this;
            return old;
        }
        int64_t operator* () const {
            return intSetGetVal(p, encoding);
        }
    };
    class IntSetBase : protected IntSetHelper
    {
    protected:
        uint32_t size = 0;
        uint8_t encoding = 0;
        uint8_t *contents = nullptr;

        IntSetBase () = default;
        ~IntSetBase () noexcept {
            clear();
        }
        IntSetBase (const IntSetBase &r) {
            operator=(r);
        }
        IntSetBase (IntSetBase &&r) noexcept {
            operator=(std::move(r));
        }
        IntSetBase &operator= (const IntSetBase &r) {
            if (this == &r) return *this;

            encoding = r.encoding;
            size = r.size;
            contents = new uint8_t[size * encoding];
            memcpy(contents, r.contents, size * encoding);

            return *this;
        }
        IntSetBase &operator= (IntSetBase &&r) noexcept {
            if (this == &r) return *this;
            encoding = r.encoding;
            size = r.size;
            contents = r.contents;
            r.clear();

            return *this;
        }
        using Iterator = IntSetIterator;
        using RetType = std::pair <bool, Iterator>;

        void clear () {
            encoding = 0;
            size = 0;
            delete[] contents;
            contents = nullptr;
        }
        DEFINE_PARAMS_IS_INTEGRAL(intSetInsert, Iterator) {
            auto code = intSetGetEncoding(std::forward <T>(val));
            if (code > encoding)
                return intSetUpgradeAndAdd(val);
            auto ret = intSetSearch(std::forward <T>(val));
            if (ret.first) return end();

            uint8_t *pos;
            size_t offset;
            if (ret.second) {
                auto it = ret.second;
                if (it.p != it.end) {
                    offset = it.p - contents;
                    size_t endOff = it.end - contents;
                    intSetResize((intSetGetSize() + 1) * encoding);
                    memmove(contents + offset + encoding, contents + offset, endOff - offset);
                    pos = contents + offset;
                }
            } else {
                intSetResize((intSetGetSize() + 1) * encoding);
                if (val < intSetGetMin()) {
                    pos = contents;
                    memmove(contents + encoding, contents, intSetGetSize() * encoding);
                } else
                    pos = intSetGetEnd();
            }

            intSetSet(val, pos);
            intSetSetSize(intSetGetSize() + 1);
            return { contents + offset, intSetGetEnd(), encoding };
        }

        Iterator intSetRemove (const Iterator &it, size_t num) {
            if (!it || !num) return end();
            if (num * encoding < static_cast<size_t>((it.end - it.p)))
                memcpy(it.p, it.p + it.encoding * num, it.end - it.p - it.encoding * num);

            intSetSetSize(intSetGetSize() - num);
            auto offset = it.p - contents;
            intSetResize(intSetGetSize() * encoding);

            return { contents + offset, intSetGetEnd(), encoding };
        }

        DEFINE_PARAMS_IS_INTEGRAL(intSetSearch, RetType) {
            if (intSetGetSize() == 0) return { false, end() };
            auto max = intSetGetMax();
            auto min = intSetGetMin();
            auto maxIdx = intSetGetSize() - 1;
            uint32_t minIdx = 0;
            uint32_t midIdx;
            int64_t cur;

            if (val > max || val < min) return { false, end() };
            while (maxIdx >= minIdx) {
                midIdx = (maxIdx + minIdx) >> 1;
                cur = intSetGetVal(contents + midIdx * encoding, encoding);
                if (val > cur)
                    minIdx = midIdx + 1;
                else if (val < cur)
                    maxIdx = midIdx - 1;
                else
                    break;
            }

            if (val == cur)
                return { true, Iterator(contents + midIdx * encoding, intSetGetEnd(), encoding) };

            return { false, Iterator(contents + minIdx * encoding, intSetGetEnd(), encoding) };
        }

        Iterator begin () const noexcept { return { contents, intSetGetEnd(), encoding }; }
        Iterator end () const noexcept { return { nullptr, nullptr, 0 }; }

        inline void intSetUpgrade (uint8_t newEncoding) {
            auto oldCoding = encoding;
            encoding = newEncoding;
            auto len = intSetGetSize();
            while (len--) {
                auto v = intSetGetVal(contents + len * oldCoding, oldCoding);
                intSetSet(v, contents + len * encoding);
            }
        }

        DEFINE_PARAMS_IS_INTEGRAL(intSetUpgradeAndAdd, Iterator) {
            auto newEncoding = intSetGetEncoding(std::forward <T>(val));
            // 是否添加到头，此函数只有当添加的字段宽度比当前encoding大时才会触发
            // 所以val只会存在于头或尾
            bool isHead = val < 0;

            intSetResize(newEncoding * (intSetGetSize() + 1));
            intSetUpgrade(newEncoding);

            if (isHead)
                intSetSet(val, contents);
            else
                intSetSet(val, contents + intSetGetSize() * encoding);
            intSetSetSize(intSetGetSize() + 1);

            return { isHead ? contents : (contents + (intSetGetSize() - 1) * encoding), intSetGetEnd(), encoding };
        }
        template <class T>
        typename std::enable_if <std::is_integral <typename std::remove_reference <T>::type>::value,
                                 void>::type intSetSet (T &&val, uint8_t *p) {
            if (encoding == INT_SET_INT64) {
                *reinterpret_cast<int64_t *>(p) = val;
                memConv64(p);
            } else if (encoding == INT_SET_INT32) {
                *reinterpret_cast<int32_t *>(p) = val;
                memConv32(p);
            } else {
                *reinterpret_cast<int16_t *>(p) = val;
                memConv16(p);
            }
        }
        inline void intSetResize (size_t s) {
            auto *newContents = new uint8_t[s];
            memcpy(newContents, contents, intSetGetSize() * encoding);
            delete[] contents;
            contents = newContents;
        }
        inline void intSetSetSize (uint32_t s) noexcept {
            size = s;
            memConv32(&size);
        }
        inline uint32_t intSetGetSize () const noexcept {
            return intConv32(static_cast<int32_t>(size));
        }

        inline int64_t intSetGetMin () const noexcept {
            if (intSetGetSize() == 0) return 0;
            return intSetGetVal(contents, encoding);
        }
        inline int64_t intSetGetMax () const noexcept {
            if (intSetGetSize() == 0) return 0;
            return intSetGetVal(contents + ((intSetGetSize() - 1) * encoding), encoding);
        }
        inline uint8_t *intSetGetEnd () const noexcept {
            return contents + intSetGetSize() * encoding;
        }
    };
}

class KvIntSet : __KV_PRIVATE__::IntSetBase
{
public:
    using __KV_PRIVATE__::IntSetBase::Iterator;
    using __KV_PRIVATE__::IntSetBase::begin;
    using __KV_PRIVATE__::IntSetBase::end;
    using __KV_PRIVATE__::IntSetBase::clear;

    KvIntSet () = default;
    ~KvIntSet () noexcept = default;
    KvIntSet (const KvIntSet &) = default;
    KvIntSet (KvIntSet &&) noexcept = default;
    KvIntSet &operator= (const KvIntSet &) = default;
    KvIntSet &operator= (KvIntSet &&) noexcept = default;

    inline void merge (const KvIntSet &r) {
        for (const int64_t &v : r) {
            intSetInsert(v);
        }
    }

    template <class T, class ...Arg>
    typename std::enable_if <std::is_integral <typename std::remove_reference <T>::type>::value, void>::type
    insert (T &&val, Arg ...arg) {
        insert(val);
        insert(std::forward <Arg>(arg)...);
    }

    DEFINE_PARAMS_IS_INTEGRAL(insert, Iterator) {
        return intSetInsert(val);
    }

    DEFINE_PARAMS_IS_INTEGRAL(find, Iterator) {
        auto ret = intSetSearch(val);
        if (!ret.first) return end();

        return ret.second;
    }

    DEFINE_OVERRIDE_STRING_FN(Iterator, find) {
        long long val;
        if (!Utils::StringHelper::stringIsLL(param, &val))
            return end();

        return find(val);
    }

    DEFINE_PARAMS_IS_INTEGRAL(erase, Iterator) {
        auto it = find(val);
        return intSetRemove(it, 1);
    }

    Iterator erase (const Iterator &it) {
        return erase(it, 1);
    }

    Iterator erase (const Iterator &it, size_t num) {
        return intSetRemove(it, num);
    }

    inline size_t size () const noexcept { return __KV_PRIVATE__::IntSetBase::intSetGetSize(); }
    inline bool empty () const noexcept { return size() == 0; }
    inline size_t capacity () const noexcept { return intSetGetSize() * encoding; }
};
#endif //KV_STORE_KV_STORE_SERVER_DATA_STRUCTURE_KV_INTSET_H_
