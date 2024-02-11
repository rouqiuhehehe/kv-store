//
// Created by 115282 on 2023/11/22.
//
#ifndef KV_STORE_KV_STORE_SERVER_DATA_STRUCTURE_KV_LISTPACK_H_
#define KV_STORE_KV_STORE_SERVER_DATA_STRUCTURE_KV_LISTPACK_H_

#include "kv-listpack-base.h"

enum class INSERT_POSITION : uint8_t
{
    BEFORE,
    AFTER
};
class KvListPack : __KV_PRIVATE__::ListPack
{
    friend class __KV_PRIVATE__::QuickListNode;
    friend class __KV_PRIVATE__::QuickListBase;
    friend class __KV_PRIVATE__::QuickListHelper;
    friend class KvQuickList;
public:
    using __KV_PRIVATE__::ListPack::begin;
    using __KV_PRIVATE__::ListPack::end;
    using __KV_PRIVATE__::ListPack::rbegin;
    using __KV_PRIVATE__::ListPack::rend;
    using __KV_PRIVATE__::ListPack::find;
    using __KV_PRIVATE__::ListPack::findReverse;
    using __KV_PRIVATE__::ListPackHelper::DataType;
    using __KV_PRIVATE__::ListPackHelper::DataTypeEnum;
    using __KV_PRIVATE__::ListPackHelper::IteratorDataType;
    using __KV_PRIVATE__::ListPackHelper::Iterator;
    using __KV_PRIVATE__::ListPackHelper::ReverseIterator;

    KvListPack () = default;
    ~KvListPack () = default;
    KvListPack (KvListPack &&) = default;
    KvListPack &operator= (KvListPack &&) = default;
    KvListPack (const KvListPack &) = default;
    KvListPack &operator= (const KvListPack &) = default;

    inline IteratorDataType findIndex (IntegerType index) const {
        if (index >= 0) return *findIndexForward(index);
        return *findIndexReverse(index);
    }

    inline void popFront () {
        listPackDelete(LP_HEAD_SIZE, 1);
    }

    inline void popBack () {
        off_t endOff = getLastBitOffset();
        uint8_t backLenSize;
        size_t entrySize = listPackDecodingBackLen(getListPackPtr() + endOff, &backLenSize);
        entrySize += backLenSize;
        listPackDeleteSafe(endOff - static_cast<off_t>(entrySize), endOff, 1);
    }

    inline IteratorDataType front () const {
        if (empty()) {
            IteratorDataType data {};
            data.mode = __KV_PRIVATE__::ListPackHelper::DataTypeEnum::ERROR;
            return data;
        }
        return ListPackHelper::listPackGetValue(getListPackPtr() + LP_HEAD_SIZE);
    }

    inline IteratorDataType back () const {
        if (empty()) {
            IteratorDataType data {};
            data.mode = __KV_PRIVATE__::ListPackHelper::DataTypeEnum::ERROR;
            return data;
        }
        off_t endOff = getLastBitOffset();
        uint8_t backLenSize;
        size_t entrySize = listPackDecodingBackLen(getListPackPtr() + endOff, &backLenSize);
        entrySize += backLenSize;
        return ListPackHelper::listPackGetValue(getListPackPtr() + endOff - entrySize);
    }

    bool pushBack (const StringType &s) {
        return !!listPackInsert(getLastBitOffset(), s);
    }

    template <int size>
    bool pushBack (const char (&s)[size]) {
        StringType str(s, size - 1);
        return pushBack(str);
    }

    bool pushBack (const char *s, size_t size) {
        StringType str(s, size);
        return pushBack(str);
    }

    template <class T, class = typename std::enable_if <
        std::is_integral <typename std::remove_reference <T>::type>::value, T>::type>
    bool pushBack (T val) {
        return listPackInsert(getLastBitOffset(), val);
    }

    // 用于兼容 bool pushBack (const char *s, size_t size)
    template <class T, class ...Arg>
    typename std::enable_if <
        !std::is_same <typename std::decay <T>::type, char *>::value, bool
    >::type pushBack (T s, Arg &&...val) {
        if (!pushBack(s))
            return false;

        return pushBack(std::forward <Arg>(val)...);
    }

    template <int size, class ...Arg>
    bool pushBack (const char (&s)[size], Arg &&...val) {
        if (!pushBack(s))
            return false;

        return pushBack(std::forward <Arg>(val)...);
    }

    bool pushFront (const StringType &s) {
        return !!listPackInsert(LP_HEAD_SIZE, s);
    }

    template <int size>
    bool pushFront (const char (&s)[size]) {
        StringType str(s, size - 1);
        return pushFront(str);
    }

    bool pushFront (const char *s, size_t size) {
        StringType str(s, size);
        return pushFront(str);
    }

    template <class T, class = typename std::enable_if <
        std::is_integral <typename std::remove_reference <T>::type>::value, T>::type>
    bool pushFront (T val) {
        return listPackInsert(LP_HEAD_SIZE, val);
    }

    Iterator insert (const Iterator &posIt, const StringType &s, INSERT_POSITION pos) {
        if (!posIt) {
            pushBack(s);
            return end();
        }
        off_t offset = posIt.p - getListPackPtr();
        if (pos == INSERT_POSITION::AFTER)
            offset += static_cast<off_t>(posIt.entrySize);

        listPackInsert(offset, s);
        return Iterator(getListPackPtr() + offset);
    }

    Iterator insert (const Iterator &posIt, const char *s, size_t len, INSERT_POSITION pos) {
        return insert(posIt, StringType(s, len), pos);
    }

    template <class T, class = typename std::enable_if <std::is_integral <typename std::remove_reference <
        T>::type>::value, T>::type>
    Iterator insert (const Iterator &posIt, T &&val, INSERT_POSITION pos) {
        if (!posIt)
            return pushBack(val);

        off_t offset = posIt.p - getListPackPtr();
        if (pos == INSERT_POSITION::AFTER)
            offset += static_cast<off_t>(posIt.entrySize);

        listPackInsert(offset, val);
        return Iterator(getListPackPtr() + offset);
    }

    Iterator erase (const Iterator &it) {
        return erase(it, 1);
    }

    Iterator erase (const Iterator &begin, const Iterator &end) {
        if (begin == end || begin == __KV_PRIVATE__::ListPack::end())
            return __KV_PRIVATE__::ListPack::end();

        off_t beginOff = begin.p - getListPackPtr();
        auto it = begin;
        size_t num = 0;
        while (it != end) {
            num++;
            ++it;
        }
        off_t endOff = end.p ? end.p - getListPackPtr() : getLastBitOffset();

        if (listPackDeleteSafe(beginOff, endOff, num) != num)
            return __KV_PRIVATE__::ListPack::end();

        return Iterator(getListPackPtr() + beginOff);
    }

    Iterator erase (const Iterator &it, size_t num) {
        if (!it.p || num == 0) return end();
        auto *p = it.p;
        if (listPackDelete(p - getListPackPtr(), num) == 0)
            return end();

        return Iterator(it.p);
    }

    ReverseIterator erase (const ReverseIterator &it) {
        return erase(it, 1);
    }

    ReverseIterator erase (const ReverseIterator &it, size_t num) {
        if (!it.p || num == 0) return rend();
        auto *p = it.p - it.entrySize;
        if (listPackDelete(p - getListPackPtr(), num) == 0)
            return rend();

        return ReverseIterator(getListPackPtr(), it.p - it.entrySize);
    }

    Iterator replace (const Iterator &it, const StringType &s) {
        if (!it.p) return end();
        auto offset = it.p - getListPackPtr();
        if (!listPackReplace(offset, s))
            return end();

        return Iterator(getListPackPtr() + offset);
    }

    template <class T>
    typename std::enable_if <
        std::is_integral <typename std::remove_reference <T>::type>::value, Iterator>::type
    replace (const Iterator &it, T s) {
        if (!it.p) return end();
        auto offset = it.p - getListPackPtr();
        if (!listPackReplace(offset, s))
            return end();

        return Iterator(getListPackPtr() + offset);
    }

    size_t merge (const KvListPack &r) {
        return listPackMerge(r);
    }

    inline size_t size () const noexcept {
        return listPackGetHeadElementsSize();
    }

    inline size_t capacity () const noexcept {
        return listPackGetHeadTotalSize();
    }

    inline bool empty () const noexcept {
        return size() == 0;
    }

    inline Iterator findIndexForward (IntegerType index) const noexcept {
        if (index >= static_cast<IntegerType>(size())) return end();

        auto it = begin();
        IntegerType i = 0;
        while (i++ < index) ++it;

        return it;
    }

    inline ReverseIterator findIndexReverse (IntegerType index) const noexcept {
        if (index + size() < 0) return rend();
        auto it = rbegin();
        while (++index < 0) ++it;
        return it;
    }
};
#endif //KV_STORE_KV_STORE_SERVER_DATA_STRUCTURE_KV_LISTPACK_H_
