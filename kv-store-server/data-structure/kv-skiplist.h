//
// Created by Yoshiki on 2023/8/3.
//

#ifndef LINUX_SERVER_LIB_INCLUDE_SKIPLIST_H_
#define LINUX_SERVER_LIB_INCLUDE_SKIPLIST_H_

#include <functional>
#include <memory>
#include "util/util.h"
#include <chrono>
template <class T, int MaxLevel = 32>
class SkipList
{
    static_assert(MaxLevel <= 32 && MaxLevel > 1, "MaxLevel must between 2~32");
    class SkipListNode;
    typedef SkipListNode *SkipListNodePtr;
    typedef SkipList <T, MaxLevel> ClassType;

    class SkipListNode
    {
    public:
        template <class R>
        SkipListNode (int level, R &&value)
            : value(std::forward <R>(value)), level(level) {
            for (int i = 0; i < level; ++i)
                nextArr[i] = nullptr;
        }
        ~SkipListNode () noexcept = default;
        template <class R>
        inline bool operator== (const R &r) const noexcept {
            return value == r;
        }
        template <class R>
        inline bool operator!= (const R &r) const noexcept {
            return !operator==(r);
        }
        inline bool operator== (const SkipListNode &r) const noexcept {
            return value == r.value;
        }
        inline bool operator!= (const SkipListNode &r) const noexcept {
            return operator==(r);
        }
        inline bool operator< (const SkipListNode &r) const noexcept {
            return value < r.value;
        }
        template <class R>
        inline bool operator< (const R &r) const noexcept {
            return value < r;
        }
        inline bool operator> (const SkipListNode &r) const noexcept {
            return value > r.value;
        }
        template <class R>
        bool operator> (const R &r) const noexcept {
            return value > r;
        }
        void *operator new (size_t size, int level) {
            return ::operator new(size + level * sizeof(SkipListNodePtr)); // NOLINT
        }

        T value;
        int level;
        SkipListNodePtr prev {};
        // 1 2 3 4
        SkipListNodePtr nextArr[];
    };

    class SkipListIterator
    {
        friend class SkipList;
    public:
        FORWARD_ITERATOR_TYPE(T);
        explicit SkipListIterator (const SkipListNodePtr &node)
            : node(node) {}
        ~SkipListIterator () noexcept = default;
        explicit operator bool () const noexcept {
            return node != nullptr;
        }
        const T &operator* () const noexcept {
            return node->value;
        }
        const T *operator-> () const noexcept {
            return Utils::addressOf(node->value);
        }
        bool operator== (const SkipListIterator &r) const noexcept {
            return node == r.node;
        }
        bool operator!= (const SkipListIterator &r) const noexcept {
            return !operator==(r);
        }
        virtual SkipListIterator &operator++ () noexcept {
            // 取最低层级的next
            node = node->nextArr[0];
            return *this;
        }
        SkipListIterator &operator-- () noexcept {
            // NIL节点
            if (node->prev == nullptr) node = nullptr;
            else node = node->prev;
        }
        SkipListIterator &operator-- (int) noexcept { // NOLINT
            auto old = *this;
            --(*this);
            return old;
        }
        SkipListIterator operator++ (int) noexcept  // NOLINT(cert-dcl21-cpp)
        {
            auto old = *this;
            ++*this;
            return old;
        }

    protected:
        SkipListNodePtr node;
    };

    class SkipListReverseIterator : public SkipListIterator
    {
    public:
        SkipListReverseIterator (const SkipListNodePtr &node, SkipListNodePtr nil)
            : SkipListIterator(node), nil(nil) {}
        SkipListReverseIterator &operator++ () noexcept override {
            this->node = this->node->prev;
            if (this->node == nil) this->node = nullptr;
            return *this;
        }
        SkipListReverseIterator operator++ (int) noexcept {
            auto old = *this;
            ++*this;
            return old;
        }

    protected:
        SkipListNodePtr nil;
    };

public:
    using Iterator = SkipListIterator;
    using ReverseIterator = SkipListReverseIterator;
    enum class FIND_RETURN_MODE
    {
        HEAD,
        TAIL,
        PREV,
        FOUND
    };
    SkipList ()
        : nil(new(MaxLevel) SkipListNode(MaxLevel, T {})), tail(&nil->nextArr[0]) {}
    ~SkipList () noexcept {
        clear();
    };
    SkipList (const SkipList &r)
        : nil(nullptr), tail(nullptr) {
        operator=(r);
    }
    SkipList (SkipList &&r) noexcept
        : nil(nullptr), tail(nullptr) {
        operator=(std::move(r));
    }

    SkipList &operator= (const SkipList &r) {
        if (this == &r) return *this;

        delete nil;
        nil = new(MaxLevel) SkipListNode(MaxLevel, T {});
        tail = &nil->nextArr[0];
        maxLevel_ = r.maxLevel_;

        SkipListNodePtr p = r.nil;
        while ((p = p->nextArr[0])) insertPrivate(p->value, p->level);

        return *this;
    }

    SkipList &operator= (SkipList &&r) noexcept {
        if (this == &r) return *this;

        size_ = r.size_;
        maxLevel_ = r.maxLevel_;
        nil = r.nil;
        tail = r.tail;

        r.nil = nullptr;
        r.tail = nullptr;
        r.size_ = 0;
        r.maxLevel_ = 0;

        return *this;
    }

    void clear () noexcept {
        if (nil) {
            for (SkipListNodePtr i = nil->nextArr[0], old = nil->nextArr[0]; i != nullptr; old = i) {
                i = old->nextArr[0];
                delete old;
            }
        }
        delete nil;
        nil = nullptr;
        tail = nullptr;
        size_ = 0;
        maxLevel_ = 0;
    }

    Iterator begin () const {
        return Iterator(nil->nextArr[0]);
    }

    Iterator end () const noexcept {
        return Iterator(nullptr);
    }

    ReverseIterator rbegin () const {
        return ReverseIterator(*tail, nil);
    }

    ReverseIterator rend () const noexcept {
        return ReverseIterator(nullptr, nil);
    }

    inline size_t size () const noexcept {
        return size_;
    }

    template <class R, class ...Arg>
    void insert (R &&value, Arg ...arg) {
        insert(std::forward <R>(value));
        insert(std::forward <Arg>(arg)...);
    }

    template <class R>
    void insert (R &&value) {
        int level = getRandomLevel();
        insertPrivate(std::forward <R>(value), level);
    }

    bool erase (const T &value) {
        int i = maxLevel_ - 1;
        SkipListNodePtr *it = &nil;
        SkipListNodePtr old;

        for (; i >= 0; --i) {
            while ((*it)->nextArr[i] && *((*it)->nextArr[i]) < value)
                it = &(*it)->nextArr[i];

            if ((*it)->nextArr[i] && *((*it)->nextArr[i]) == value) {
                if (i == 0) {
                    if (tail == &(*it)->nextArr[i])
                        tail = it;
                    old = (*it)->nextArr[i];
                    if ((*it)->nextArr[i]->nextArr[i])
                        (*it)->nextArr[i]->nextArr[i]->prev = *it;
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

    template <class F>
    std::pair <FIND_RETURN_MODE, Iterator> find (const F &value) const noexcept {
        int i = maxLevel_ - 1;
        SkipListNodePtr ptr = nil;
        for (; i >= 0; --i) {
            while (ptr->nextArr[i] && *(ptr->nextArr[i]) < value)
                ptr = ptr->nextArr[i];

            if (!ptr->nextArr[i]) continue;

            if (*(ptr->nextArr[i]) == value)
                return { FIND_RETURN_MODE::FOUND, Iterator(ptr->nextArr[i]) };
        }

        if (!ptr) return { FIND_RETURN_MODE::TAIL, end() };
        else if (ptr == nil) return { FIND_RETURN_MODE::HEAD, end() };
        else return { FIND_RETURN_MODE::PREV, Iterator(ptr) };
    }

    std::pair <FIND_RETURN_MODE, ReverseIterator> findReverseIterator (const T &value) const noexcept {
        auto ret = find(value);
        return { ret.first, { ret.second.node, nil }};
    }

private:
    static inline int getRandomLevel () noexcept {
        int level = 1;
        while (level <= MaxLevel) {
            if (Utils::Random::getRandomNum(0, 3) == 0)
                level++;
            else
                return level;
        }
        return level;
    }

    template <class R>
    void insertPrivate (R &&value, int level) {
        auto *node = new(level) SkipListNode(level, std::forward <R>(value));
        if (level > maxLevel_) maxLevel_ = level;
        int i = maxLevel_ - 1;
        SkipListNodePtr it = nil;

        for (; i >= 0; --i) {
            while (it->nextArr[i] && *(it->nextArr[i]) < *node)
                it = it->nextArr[i];

            if (i < node->level) {
                if (i == 0) {
                    if (it->nextArr[i])
                        it->nextArr[i]->prev = node;
                    node->prev = it;
                }

                node->nextArr[i] = it->nextArr[i];
                it->nextArr[i] = node;
            }
        }

        if ((*tail)->nextArr[0])
            tail = &(*tail)->nextArr[0];

        size_++;
    }

    // 1 2 3 4
    SkipListNodePtr nil;
    // 尾部不做层级
    SkipListNodePtr *tail;
    size_t size_ = 0;
    int maxLevel_ = 1;
};
#endif //LINUX_SERVER_LIB_INCLUDE_SKIPLIST_H_
