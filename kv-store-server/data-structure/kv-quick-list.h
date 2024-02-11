//
// Created by 115282 on 2023/11/29.
//

#ifndef KV_STORE_KV_STORE_SERVER_DATA_STRUCTURE_KV_QUICK_LIST_H_
#define KV_STORE_KV_STORE_SERVER_DATA_STRUCTURE_KV_QUICK_LIST_H_

#include "kv-quick-list-base.h"
#include "util/global.h"

#define DEFINE_OVERRIDE_STRING_FN(ret, name) \
    inline ret name (const char *s, size_t len) { \
        StringType str(s, len); \
        return name(str);   \
    }                                 \
    inline ret name (const StringType &param)

#define DEFINE_OVERRIDE_INTEGER_FN(ret, name) \
    template <class T, class = typename std::enable_if <std::is_integral <typename std::remove_reference < \
        T>::type>::value, T>::type>           \
    inline ret name (T param)

#define DEFINE_OVERRIDE_STRING_FN_CONST(ret, name) \
    inline ret name (const char *s, size_t len) const { \
        StringType str(s, len); \
        return name(str);   \
    }                                 \
    inline ret name (const StringType &param) const

#define DEFINE_OVERRIDE_INTEGER_FN_CONST(ret, name) \
    template <class T, class = typename std::enable_if <std::is_integral <typename std::remove_reference < \
        T>::type>::value, T>::type>           \
    inline ret name (T param) const

class KvQuickList : public __KV_PRIVATE__::QuickListBase
{
public:
    using __KV_PRIVATE__::QuickListBase::begin;
    using __KV_PRIVATE__::QuickListBase::end;
    using __KV_PRIVATE__::QuickListBase::rbegin;
    using __KV_PRIVATE__::QuickListBase::rend;
    using __KV_PRIVATE__::QuickListBase::size;
    using __KV_PRIVATE__::QuickListBase::empty;
    using __KV_PRIVATE__::QuickListBase::popFront;
    using __KV_PRIVATE__::QuickListBase::popBack;
    using __KV_PRIVATE__::QuickListBase::front;
    using __KV_PRIVATE__::QuickListBase::back;

    KvQuickList () = default;
    ~KvQuickList () = default;
    KvQuickList (const KvQuickList &) = default;
    KvQuickList (KvQuickList &&) noexcept = default;
    KvQuickList &operator= (const KvQuickList &) = default;
    KvQuickList &operator= (KvQuickList &&) = default;

    explicit KvQuickList (KvListPack *listPack)
        : __KV_PRIVATE__::QuickListBase(listPack) {}

    inline KvListPack::IteratorDataType findIndex (IntegerType index) const {
        if (index >= 0) return *findIndexForward(index);
        return *findIndexReverse(index);
    }

    DEFINE_OVERRIDE_STRING_FN(Iterator, pushFront) {
        auto ret = quickListPushFront(param);
        return { ret.first, ret.second };
    }
    DEFINE_OVERRIDE_INTEGER_FN(Iterator, pushFront) {
        auto ret = quickListPushFront(param);
        return { ret.first, ret.second };
    }

    DEFINE_OVERRIDE_STRING_FN(Iterator, pushBack) {
        auto ret = quickListPushBack(param);
        return { ret.first, ret.second };
    }
    DEFINE_OVERRIDE_INTEGER_FN(Iterator, pushBack) {
        auto ret = quickListPushBack(param);
        return { ret.first, ret.second };
    }

    inline Iterator insert (const Iterator &posIt, const char *s, size_t len, INSERT_POSITION pos) {
        auto ret = quickListInsert(posIt, s, len, pos);
        return { ret.first, ret.second };
    }
    inline Iterator insert (const Iterator &posIt, const StringType &param, INSERT_POSITION pos) {
        return insert(posIt, param.c_str(), param.size(), pos);
    }
    template <class T, class = typename std::enable_if <std::is_integral <typename std::remove_reference <
        T>::type>::value, T>::type>
    inline Iterator insert (const Iterator &posIt, T &&param, INSERT_POSITION pos) {
        auto ret = quickListInsert(posIt, param, sizeof(T), pos);
        return { ret.first, ret.second };
    }

    DEFINE_OVERRIDE_STRING_FN_CONST(Iterator, find) {
        auto ret = quickListFind(param);
        return { ret.first, ret.second };
    }

    DEFINE_OVERRIDE_INTEGER_FN_CONST(Iterator, find) {
        auto ret = quickListFind(param);
        return { ret.first, ret.second };
    }

    DEFINE_OVERRIDE_STRING_FN_CONST(ReverseIterator, findReverse) {
        auto ret = quickListFind(param);
        return { ret.first, ret.second };
    }

    DEFINE_OVERRIDE_INTEGER_FN_CONST(ReverseIterator, findReverse) {
        auto ret = quickListFind(param);
        return { ret.first, ret.second };
    }

    Iterator erase (const Iterator &it) {
        return quickListErase(it, 1);
    }

    Iterator erase (const Iterator &it, size_t num) {
        return quickListErase(it, num);
    }

    Iterator erase (const Iterator &begin, const Iterator &end) {
        auto it = begin;
        size_t num = 0;
        while (it != end && it) {
            num++;
            it++;
        }
        if (it != end) return this->end();

        if (!end) {
            quickListErase(begin, num);
            return this->end();
        }

        auto *node = end.node;
        off_t offset = end.p - end.node->data.listPack->getListPackPtr();
        quickListErase(begin, num);
        return { node, offset };
    }

    ReverseIterator erase (const ReverseIterator &it) {
        return quickListErase(it, 1);
    }

    ReverseIterator erase (const ReverseIterator &it, size_t num) {
        return quickListErase(it, num);
    }

    ReverseIterator erase (const ReverseIterator &rbegin, const ReverseIterator &rend) {
        auto it = rbegin;
        size_t num = 0;
        while (it != rend && it) {
            num++;
            it++;
        }
        if (it != rend) return this->rend();

        if (!rend) {
            quickListErase(rbegin, num);
            return this->rend();
        }

        auto *node = rend.node;
        off_t offset = rend.p - rend.node->data.listPack->getListPackPtr();
        quickListErase(rbegin, num);
        return { node, offset };
    }

    Iterator replace (const Iterator &it, const StringType &s) {
        if (!it.p) return end();
        it.node->decompressForUse();
        auto listPack = it.node->data.listPack;
        auto offset = it.p - listPack->getListPackPtr();
        if (!listPack->listPackReplace(offset, s))
            return end();

        return { it.node, offset };
    }

    inline KvQuickList::Iterator findIndexForward (IntegerType index) const noexcept {
        if (index >= static_cast<IntegerType>(size())) return end();

        auto it = begin();
        IntegerType i = 0;
        while (i++ < index) ++it;

        return it;
    }

    inline KvQuickList::ReverseIterator findIndexReverse (IntegerType index) const noexcept {
        if (index + size() < 0) return rend();
        auto it = rbegin();
        while (++index < 0) ++it;
        return it;
    }
};
#endif //KV_STORE_KV_STORE_SERVER_DATA_STRUCTURE_KV_QUICK_LIST_H_
