//
// Created by 115282 on 2023/10/13.
//

#ifndef LINUX_SERVER_KV_STORE_SERVER_DATA_STRUCTURE_KV_ZIPLIST_H_
#define LINUX_SERVER_KV_STORE_SERVER_DATA_STRUCTURE_KV_ZIPLIST_H_

#include "kv-ziplist-base.h"
#include <list>
class KvZipList : public __KV_PRIVATE__::ZipListBase
{
public:
    using DataTypeEnum = __KV_PRIVATE__::ZipListHelper::DataTypeEnum;
    using __KV_PRIVATE__::ZipListBase::begin;
    using __KV_PRIVATE__::ZipListBase::end;
    using __KV_PRIVATE__::ZipListBase::size;
    using __KV_PRIVATE__::ZipListBase::clear;

    KvZipList () = default;
    KvZipList (const KvZipList &) = default;
    KvZipList (KvZipList &&) = default;

    template <int SIZE>
    Iterator insert (const Iterator &pos, const char (&s)[SIZE]) noexcept {
        return insert(pos, s, SIZE - 1);
    }

    // 插入pos之前，返回插入元素的迭代器
    template <class T, class = typename std::enable_if <std::is_integral <T>::value, T>::type>
    Iterator insert (const Iterator &pos, T &&val) noexcept {
        // 迭代器为end
        if (!pos.entry.p) {
            if (pushBack(std::forward <T>(val)))
                return Iterator(
                    zipList + __KV_PRIVATE__::intConv32(static_cast<int32_t>(head->tailOffset)));

            return end();
        }
        else {
            off_t prevOffset = pos.entry.p - pos.entry.prevLen - zipList;
            if (zipListInsert(
                std::forward <T>(val),
                pos.entry.prevLen,
                pos.entry.p - zipList
            ))
                return Iterator(zipList + prevOffset);

            return end();
        }
    }

    // 插入pos之前，返回插入元素的迭代器
    Iterator insert (const Iterator &pos, const char *s, size_t len) noexcept {
        // 迭代器为end
        if (!pos.entry.p) {
            if (pushBack(s, len))
                return Iterator(
                    zipList + __KV_PRIVATE__::intConv32(static_cast<int32_t>(head->tailOffset)));

            return end();
        }
        else {
            off_t prevOffset = pos.entry.p - pos.entry.prevLen - zipList;
            if (zipListInsert(
                reinterpret_cast<const uint8_t *>(s),
                len,
                pos.entry.prevLen,
                pos.entry.p - zipList
            ))
                return Iterator(zipList + prevOffset);

            return end();
        }
    }

    Iterator insert (const Iterator &pos, const StringType &str) noexcept {
        return insert(pos, str.c_str(), str.size());
    }

    template <class T, class = typename std::enable_if <std::is_integral <T>::value, T>::type>
    bool pushBack (T &&val) {
        if (!head->count)
            return pushFront(std::forward <T>(val));

        Entry entry {};
        if (!zipEntrySafe(
            __KV_PRIVATE__::intConv32(static_cast<int32_t>(head->tailOffset)),
            entry
        )) {
            PRINT_ERROR("error");
            return false;
        }

        return zipListInsert(
            std::forward <T>(val),
            entry.headerSize + entry.len,
            entry.headerSize + entry.len
                + __KV_PRIVATE__::intConv32(static_cast<int32_t>(head->tailOffset)));
    }

    template <int SIZE>
    bool pushBack (const char (&s)[SIZE]) {
        return pushBack(s, SIZE - 1);
    }

    bool pushBack (const char *s, size_t len) noexcept {
        if (!head->count)
            return pushFront(s, len);

        Entry entry {};
        if (!zipEntrySafe(
            __KV_PRIVATE__::intConv32(static_cast<int32_t>(head->tailOffset)),
            entry
        )) {
            PRINT_ERROR("error");
            return false;
        }
        return zipListInsert(
            reinterpret_cast<const uint8_t *>(s),
            len,
            entry.headerSize + entry.len,
            entry.headerSize + entry.len
                + __KV_PRIVATE__::intConv32(static_cast<int32_t>(head->tailOffset)));
    }

    bool pushBack (const StringType &string) noexcept {
        return pushBack(string.c_str(), string.size());
    }

    template <int SIZE>
    bool pushFront (const char (&s)[SIZE]) {
        return pushFront(s, SIZE - 1);
    }

    bool pushFront (const char *s, size_t len) noexcept {
        return zipListInsert(reinterpret_cast<const uint8_t *>(s), len, 0, HEAD_SIZE);
    }

    template <class T, class = typename std::enable_if <std::is_integral <T>::value, T>::type>
    bool pushFront (T &&val) {
        return zipListInsert(std::forward <T>(val), 0, HEAD_SIZE);
    }

    size_t erase (const StringType &string) noexcept {
        return erase(string.c_str(), string.size());
    }

    size_t erase (const char *s, size_t len) noexcept {
        auto it = find(s, len);
        if (erase(it) != end())
            return 1;

        return 0;
    }

    template <int SIZE>
    size_t erase (const char (&s)[SIZE]) noexcept {
        return erase(s, SIZE - 1);
    }

    template <class T, class = typename std::enable_if <std::is_integral <T>::value, T>::type>
    size_t erase (T &&val) noexcept {
        auto it = find(std::forward <T>(val));
        if (erase(it) != end())
            return 1;

        return 0;
    }

    Iterator erase (const Iterator &it) noexcept {
        return erase(it, 1);
    }

    Iterator erase (const Iterator &first, const Iterator &last) noexcept {
        return zipListDelete(first, last);
    }

    Iterator erase (const Iterator &first, size_t num) noexcept {
        auto offset = first.entry.p - zipList;
        zipListDelete(offset, num);

        return Iterator(zipList + offset);
    }

    Iterator find (const char *s, size_t len) const noexcept {
        return ZipListBase::zipListFind(reinterpret_cast<const uint8_t *>(s), len);
    }

    Iterator find (const StringType &string) const noexcept {
        return find(string.c_str(), string.size());
    }

    template <int SIZE>
    Iterator find (const char (&s)[SIZE]) const noexcept {
        return find(s, SIZE);
    }

    template <class T, class = typename std::enable_if <std::is_integral <T>::value, T>::type>
    Iterator find (T &&val) const noexcept {
        return ZipListBase::zipListFind(std::forward <T>(val));
    }
};
#endif //LINUX_SERVER_KV_STORE_SERVER_DATA_STRUCTURE_KV_ZIPLIS_H_
