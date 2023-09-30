//
// Created by Yoshiki on 2023/8/3.
//

#ifndef LINUX_SERVER_LIB_INCLUDE_SKIPLIST_H_
#define LINUX_SERVER_LIB_INCLUDE_SKIPLIST_H_

#include <functional>
#include <memory>
#include "util/util.h"
#include <chrono>
template <class T, int MaxLevel = 32, class Compare = std::less <T>>
class SkipList
{
    static_assert(MaxLevel <= 32 && MaxLevel > 1, "MaxLevel must between 2~32");
    class SkipListNode;
    typedef SkipListNode *SkipListNodePtr;
    typedef SkipList <T, MaxLevel, Compare> ClassType;

    class SkipListNode
    {
    public:
        template <class R>
        SkipListNode (int level, R &&value)
            : value(std::forward <R>(value))
        {
            for (int i = 0; i < level; ++i)
                nextArr[i] = nullptr;
        }
        ~SkipListNode () noexcept = default;
        inline bool operator== (const T &r) const noexcept
        {
            return value == r;
        }
        inline bool operator!= (const T &r) const noexcept
        {
            return !operator==(r);
        }
        inline bool operator== (const SkipListNode &r) const noexcept
        {
            return value == r.value;
        }
        inline bool operator!= (const SkipListNode &r) const noexcept
        {
            return operator==(r);
        }
        inline bool operator< (const SkipListNode &r) const noexcept
        {
            return Compare()(value, r.value);
        }
        inline bool operator< (const T &r) const noexcept
        {
            return Compare()(value, r);
        }
        inline bool operator> (const SkipListNode &r) const noexcept
        {
            return !operator<(r);
        }
        bool operator> (const T &r) const noexcept
        {
            return !operator<(r);
        }
        void *operator new (size_t size, int level)
        {
            return ::operator new(size + level * sizeof(SkipListNodePtr)); // NOLINT
        }

        T value;
        // 1 2 3 4
        SkipListNodePtr nextArr[];
    };

    class SkipListIterator
    {
    public:
        explicit SkipListIterator (const SkipListNodePtr &node)
            : node(node) {}
        ~SkipListIterator () noexcept = default;
        explicit operator bool () const noexcept
        {
            return node != nullptr;
        }
        const T &operator* () const noexcept
        {
            return node->value;
        }
        const T *operator-> () const noexcept
        {
            return Utils::addressOf(node->value);
        }
        bool operator== (const SkipListIterator &r) const noexcept
        {
            return node == r.node;
        }
        bool operator!= (const SkipListIterator &r) const noexcept
        {
            return !operator==(r);
        }
        SkipListIterator &operator++ () noexcept
        {
            // 取最低层级的next
            node = node->nextArr[0];
            return *this;
        }
        SkipListIterator operator++ (int) noexcept  // NOLINT(cert-dcl21-cpp)
        {
            auto old = *this;
            ++*this;
            return old;
        }

    private:
        SkipListNodePtr node;
    };

public:
    using Iterator = SkipListIterator;
    SkipList () = default;
    ~SkipList () noexcept
    {
        for (SkipListNodePtr i = nil->nextArr[0], old = nil->nextArr[0]; i != nullptr; old = i)
        {
            i = old->nextArr[0];
            delete old;
        }
    };

    Iterator begin () const
    {
        return Iterator(nil->nextArr[0]);
    }

    Iterator end () const noexcept
    {
        return Iterator(nullptr);
    }

    inline size_t size () const noexcept
    {
        return size_;
    }

    template <class R>
    void insert (R &&value)
    {
        int level = getRandomLevel();
        if (level > maxLevel_)
            maxLevel_ = level;
        auto *node = new(level) SkipListNode(level, std::forward <R>(value));

        int i = maxLevel_ - 1;
        SkipListNodePtr it = nil;

        for (; i >= 0; --i)
        {
            while (it->nextArr[i] && *(it->nextArr[i]) < *node)
                it = it->nextArr[i];

            if (i < level)
            {
                node->nextArr[i] = it->nextArr[i];
                it->nextArr[i] = node;
            }
        }

        if ((*tail)->nextArr[0])
            tail = &(*tail)->nextArr[0];

        size_++;
    }

    bool erase (const T &value)
    {
        int i = maxLevel_ - 1;
        SkipListNodePtr *it = &nil;
        SkipListNodePtr old;

        for (; i >= 0; --i)
        {
            while ((*it)->nextArr[i] && *((*it)->nextArr[i]) < value)
                it = &(*it)->nextArr[i];

            if ((*it)->nextArr[i] && *((*it)->nextArr[i]) == value)
            {
                if (i == 0)
                {
                    if (tail == &(*it)->nextArr[i])
                        tail = it;
                    old = (*it)->nextArr[i];
                    (*it)->nextArr[i] = (*it)->nextArr[i]->nextArr[i];
                    delete old;
                    size_--;
                    return true;
                }
                (*it)->nextArr[i] = (*it)->nextArr[i]->nextArr[i];
            }
        }

        return false;
    }

    Iterator find (const T &value) const noexcept
    {
        int i = maxLevel_ - 1;
        SkipListNodePtr ptr = nil;
        for (; i >= 0; --i)
        {
            while (ptr->nextArr[i] && *(ptr->nextArr[i]) < value)
                ptr = ptr->nextArr[i];

            if (*(ptr->nextArr[i]) == value)
                return Iterator(ptr->nextArr[i]);
        }

        return end();
    }

private:
    static inline int getRandomLevel () noexcept
    {
        int level = 1;
        int time;
        while (level <= MaxLevel)
        {
            if (Utils::getRandomNum(0, 3) == 0)
                level++;
            else
                return level;
        }
        return level;
    }

    // 1 2 3 4
    SkipListNodePtr nil = new(MaxLevel) SkipListNode(MaxLevel, T {});
    // 尾部不做层级
    SkipListNodePtr *tail = &nil->nextArr[0];
    size_t size_ = 0;
    int maxLevel_ = 1;
};
#endif //LINUX_SERVER_LIB_INCLUDE_SKIPLIST_H_
