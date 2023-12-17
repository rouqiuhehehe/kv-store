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

    KvQuickList () = default;
    ~KvQuickList () = default;

    DEFINE_OVERRIDE_STRING_FN(Iterator, pushFront)
    {
        auto ret = quickListPushFront(param);
        return Iterator(ret.first, ret.second);
    }
    DEFINE_OVERRIDE_INTEGER_FN(Iterator, pushFront)
    {
        auto ret = quickListPushFront(param);
        return Iterator(ret.first, ret.second);
    }

    DEFINE_OVERRIDE_STRING_FN(Iterator, pushBack)
    {
        auto ret = quickListPushBack(param);
        return Iterator(ret.first, ret.second);
    }
    DEFINE_OVERRIDE_INTEGER_FN(Iterator, pushBack)
    {
        auto ret = quickListPushBack(param);
        return Iterator(ret.first, ret.second);
    }

    inline Iterator insert (const Iterator &posIt, const char *s, size_t len, INSERT_POSITION pos)
    {
        auto ret = quickListInsert(posIt, s, len, pos);
        return Iterator(ret.first, ret.second);
    }
    inline Iterator insert (const Iterator &posIt, const StringType &param, INSERT_POSITION pos)
    {
        return insert(posIt, param.c_str(), param.size(), pos);
    }
    template <class T, class = typename std::enable_if <std::is_integral <typename std::remove_reference <
        T>::type>::value, T>::type>
    inline Iterator insert (const Iterator &posIt, T &&param, INSERT_POSITION pos)
    {
        auto ret = quickListInsert(posIt, param, sizeof(T), pos);
        return Iterator(ret.first, ret.second);
    }

    DEFINE_OVERRIDE_STRING_FN_CONST(Iterator, find)
    {
        auto ret = quickListFind(param);
        return Iterator(ret.first, ret.second);
    }

    DEFINE_OVERRIDE_INTEGER_FN_CONST(Iterator, find)
    {
        auto ret = quickListFind(param);
        return Iterator(ret.first, ret.second);
    }

    size_t erase (const Iterator &it, size_t num)
    {
        return quickListErase(it, num);
    }

    Iterator erase (const Iterator &begin, const Iterator &end)
    {
        auto it = begin;
        size_t num = 0;
        while (it != end && it)
        {
            num++;
            it++;
        }
        if (it != end) return this->end();

        if (!end)
        {
            quickListErase(begin, num);
            return this->end();
        }

        auto *node = end.node;
        off_t offset = end.p - end.node->data.listPack->getListPackPtr();
        quickListErase(begin, num);
        return Iterator(node, offset);
    }

    size_t erase (const ReverseIterator &it, size_t num)
    {
        return quickListErase(it, num);
    }

    ReverseIterator erase (const ReverseIterator &rbegin, const ReverseIterator &rend)
    {
        auto it = rbegin;
        size_t num = 0;
        while (it != rend && it)
        {
            num++;
            it++;
        }
        if (it != rend) return this->rend();

        if (!rend)
        {
            quickListErase(rbegin, num);
            return this->rend();
        }

        auto *node = rend.node;
        off_t offset = rend.p - rend.node->data.listPack->getListPackPtr();
        quickListErase(rbegin, num);
        return ReverseIterator(node, offset);
    }
};
#endif //KV_STORE_KV_STORE_SERVER_DATA_STRUCTURE_KV_QUICK_LIST_H_
