//
// Created by 115282 on 2023/11/28.
//

#ifndef KV_STORE_KV_STORE_SERVER_DATA_STRUCTURE_KV_QUICK_LIST_BASE_H_
#define KV_STORE_KV_STORE_SERVER_DATA_STRUCTURE_KV_QUICK_LIST_BASE_H_

#include "kv-listpack.h"
#include "lzf.h"

class KvQuickList;
namespace __KV_PRIVATE__
{
    class QuickListNode
    {
    public:
        friend class QuickListBase;
        friend class QuickListHelper;
        friend class ::KvListPack;
        enum class ENCODING_MODE : uint8_t
        {
            RAW,
            LZF
        };
        enum class CONTAINER_TYPE : uint8_t
        {
            LIST_PACK,
            PLAIN
        };
        struct ListPackLzf
        {
            void *operator new (size_t s, size_t bufLen)
            {
                return ::operator new(sizeof(ListPackLzf) + sizeof(uint8_t) * bufLen);
            }
            void operator delete (void *p) noexcept
            {
                ::operator delete(p);
            }
            size_t decodeSize {};
            size_t size {};
            uint8_t compress[];
        };
        union DataUnion
        {
            KvListPack *listPack;
            ListPackLzf *lzf;
        };

        QuickListNode ()
        {
            data.listPack = new KvListPack;
        }
        explicit QuickListNode (KvListPack *listPack)
        {
            data.listPack = listPack;
            encoding = ENCODING_MODE::RAW;
        }
        explicit QuickListNode (const KvListPack &listPack)
        {
            data.listPack = new KvListPack(listPack);
            encoding = ENCODING_MODE::RAW;
        }
        ~QuickListNode () noexcept
        {
            switch (encoding)
            {
                case ENCODING_MODE::RAW:
                    delete data.listPack;
                    break;
                case ENCODING_MODE::LZF:
                    delete data.lzf;
                    break;
            }
        }

        inline bool compressForRecompress ()
        {
            if (recompress)
                return compress();

            return false;
        }
        bool compress ()
        {
            // 头尾节点不需要压缩
            // if (!prev || !next) return false;
            if (encoding != ENCODING_MODE::RAW) return false;

            recompress = false;
            size_t totalSize = data.listPack->listPackGetHeadTotalSize() - LP_HEAD_SIZE;
            // 小节点不需要压缩
            if (totalSize < MIN_COMPRESS_BYTES) return false;

            auto *lzf = new(totalSize) ListPackLzf;
            // 头6个字节不压缩
            lzf->size = lzf_compress(
                data.listPack->getListPackPtr() + LP_HEAD_SIZE,
                totalSize,
                lzf->compress,
                totalSize
            );
            // 如果压缩失败，或者压缩字节过少（少于8字节）
            if (lzf->size == 0 || lzf->size + MIN_COMPRESS_IMPROVE >= totalSize)
            {
                delete lzf;
                return false;
            }
            lzf->decodeSize = totalSize;

            auto *old = lzf;
            lzf = new(lzf->size + LP_HEAD_SIZE) ListPackLzf;
            lzf->decodeSize = old->decodeSize;
            lzf->size = old->size;
            memcpy(lzf->compress, data.listPack->getListPackPtr(), LP_HEAD_SIZE);
            memcpy(lzf->compress + LP_HEAD_SIZE, old->compress, old->size);
            delete old;

            delete data.listPack;
            data.lzf = lzf;
            encoding = ENCODING_MODE::LZF;

            return true;
        }

        bool decompress ()
        {
            if (encoding != ENCODING_MODE::LZF) return false;

            recompress = false;
            uint8_t *decompressed =
                static_cast<uint8_t *>(malloc(data.lzf->decodeSize + LP_HEAD_SIZE));
            if (lzf_decompress(
                data.lzf->compress + LP_HEAD_SIZE,
                data.lzf->size,
                decompressed + LP_HEAD_SIZE,
                data.lzf->decodeSize
            ) == 0)
            {
                delete decompressed;
                return false;
            }
            memcpy(decompressed, data.lzf->compress, LP_HEAD_SIZE);
            delete data.lzf;

            data.listPack =
                new KvListPack(std::move(*reinterpret_cast<KvListPack *>(&decompressed)));
            encoding = ENCODING_MODE::RAW;
            return true;
        }

        inline bool isPlainNode () const { return containerType == CONTAINER_TYPE::PLAIN; }

        inline bool decompressForUse ()
        {
            if (encoding == ENCODING_MODE::LZF)
            {
                recompress = true;
                return decompress();
            }
            return true;
        }

        QuickListNode *prev = nullptr;
        QuickListNode *next = nullptr;
        DataUnion data {};
        ENCODING_MODE encoding = ENCODING_MODE::RAW;
        CONTAINER_TYPE containerType = CONTAINER_TYPE::LIST_PACK;  // 是否为单独节点 如果数据过大则定义为单独节点
        bool recompress = false;  // 是否暂时解压

        static constexpr uint8_t MIN_COMPRESS_BYTES = 48;
        static constexpr uint8_t MIN_COMPRESS_IMPROVE = 8;
    };

    class QuickListHelper
    {
    protected:
        // 节点指针，偏移量
        using RetType = std::pair <QuickListNode *, off_t>;
        class Iterator : public ListPackHelper::Iterator
        {
            friend class QuickListBase;
            friend class ::KvQuickList;
        public:
            Iterator (QuickListNode *node, off_t offset)
                : ListPackHelper::Iterator(
                node ? (node->data.listPack->getListPackPtr() + offset) : nullptr
            ), node(node) {}

            bool operator== (const Iterator &it) const noexcept
            {
                return node == it.node && IteratorBase::operator==(it);
            }

            bool operator!= (const Iterator &it) const noexcept
            {
                return !(*this == it);
            }

            explicit operator bool () const noexcept override
            {
                return node && node->encoding == QuickListNode::ENCODING_MODE::RAW
                    && IteratorBase::operator bool();
            }

            Iterator &operator++ () override
            {
                ListPackHelper::Iterator::operator++();
                if (!p)
                {
                    if (node->next)
                    {
                        node = node->next;
                        p = node->data.listPack->getListPackPtr() + LP_HEAD_SIZE;
                        getVal();
                    }
                    else
                        node = nullptr;
                }

                return *this;
            }
            Iterator operator++ (int) // NOLINT
            {
                auto old = *this;
                ++(*this);
                return old;
            }
            const ListPackHelper::IteratorDataType &operator* () const noexcept override
            {
                node->decompressForUse();
                return IteratorBase::operator*();
            }
            const ListPackHelper::IteratorDataType *operator-> () const noexcept override
            {
                node->decompressForUse();
                return IteratorBase::operator->();
            }

        private:
            QuickListNode *node {};
        };
        class ReverseIterator : public ListPackHelper::ReverseIterator
        {
            friend class QuickListBase;
            friend class ::KvQuickList;
        public:
            ReverseIterator (QuickListNode *node, off_t offset)
                : ListPackHelper::ReverseIterator(
                node ? node->data.listPack->getListPackPtr() : nullptr,
                node ? (node->data.listPack->getListPackPtr() + offset) : nullptr
            ), node(node) {}
            bool operator== (const ReverseIterator &it) const noexcept
            {
                return node == it.node && IteratorBase::operator==(it);
            }

            bool operator!= (const ReverseIterator &it) const noexcept
            {
                return !(*this == it);
            }
            explicit operator bool () const noexcept override
            {
                return node && node->encoding == QuickListNode::ENCODING_MODE::RAW
                    && IteratorBase::operator bool();
            }

            ReverseIterator &operator++ () override
            {
                ListPackHelper::ReverseIterator::operator++();
                if (!p)
                {
                    if (node->prev)
                    {
                        node = node->prev;
                        p = node->data.listPack->getListPackPtr()
                            + node->data.listPack->getLastBitOffset();
                        firstEntry = node->data.listPack->getListPackPtr() + LP_HEAD_SIZE;
                        getValue();
                    }
                    else
                        node = nullptr;
                }

                return *this;
            }
            ReverseIterator operator++ (int) // NOLINT
            {
                auto old = *this;
                ++(*this);
                return old;
            }
            const ListPackHelper::IteratorDataType &operator* () const noexcept override
            {
                node->decompressForUse();
                return IteratorBase::operator*();
            }
            const ListPackHelper::IteratorDataType *operator-> () const noexcept override
            {
                node->decompressForUse();
                return IteratorBase::operator->();
            }

        private:
            QuickListNode *node {};
        };

        static inline bool nodeAllowInsert (const QuickListNode *node, size_t valSize) noexcept
        {
            if (unlikely(!node) || unlikely(node->isPlainNode())
                || unlikely(isLargeElement(valSize)))
                return false;

            size_t newSize = valSize + node->data.listPack->listPackGetHeadTotalSize()
                + LIST_PACK_ENTRY_OVERHEAD;
            if (unlikely(nodeExceedsLimit(newSize, node->data.listPack->size() + 1)))
                return false;

            return true;
        }

        static inline bool nodeAllowMerge (const QuickListNode *a, const QuickListNode *b) noexcept
        {
            if (!a || !b) return false;
            if (unlikely(a->isPlainNode() || b->isPlainNode())) return false;
            // 总长度 = a的listPack长度 + b的listPack长度 - b的listPack头长 和 LP_END 长度
            size_t mergeSize = a->data.listPack->listPackGetHeadTotalSize()
                + b->data.listPack->listPackGetHeadTotalSize() - LP_HEAD_SIZE - 1;
            uint16_t mergeCount = a->data.listPack->listPackGetHeadElementsSize()
                + b->data.listPack->listPackGetHeadElementsSize();

            return !nodeExceedsLimit(mergeSize, mergeCount);
        }

        // 是否超出限制
        static inline bool nodeExceedsLimit (size_t newSize, uint16_t newCount)
        {
            ListMaxListPackSize listMaxListPackSize = KvConfig::getConfig().listMaxListPackSize;
            switch (listMaxListPackSize.type)
            {
                case ListMaxListPackSizeType::TYPE_NUM:
                    if (!checkSizeMeetsSafetyLimit(newSize)) return false;
                    return newCount > listMaxListPackSize.number;
                case ListMaxListPackSizeType::TYPE_SIZE:
                    return newSize > listMaxListPackSize.number;
            }

            KV_ASSERT(false);
            unreachable();
        }

        static inline bool checkSizeMeetsSafetyLimit (size_t size)
        {
            return size <= SIZE_SAFETY_LIMIT;
        }

        static inline bool isLargeElement (size_t size) noexcept
        {
            return size >= PACKED_THRESHOLD;
        }

        // 检查当前长度是否需要压缩node
        // 判断config listCompressDepth不等于0 并且 当前node数量 >  listCompressDepth * 2
        // 配置为双端不需要压缩的节点数
        static inline bool needCompress (size_t len)
        {
            auto listCompressDepth = KvConfig::getConfig().listCompressDepth;
            return listCompressDepth != 0 && len > listCompressDepth * 2;
        }

        template <class T>
        static inline off_t checkTypeAndListPackPush (
            KvListPack *listPack,
            off_t offset,
            T &&val,
            size_t size,
            INSERT_POSITION pos
        )
        {
            switch (pos)
            {
                case INSERT_POSITION::BEFORE:
                    break;
                case INSERT_POSITION::AFTER:
                    size_t entrySize;
                    listPack->listPackGetValue(listPack->getListPackPtr() + offset, &entrySize);
                    offset += entrySize;
                    break;
            }

            if (std::is_integral <typename std::remove_reference <
                T>::type>::value)
                listPack->listPackInsert(offset, val);
            else
                listPack->listPackInsert(offset, StringType(val, size));
            return offset;
        }

        // PLAIN 节点阈值
        static constexpr size_t PACKED_THRESHOLD = 1 << 30;
        // 我的理解是10，redis源码定义的是8，8就8把
        static constexpr uint8_t LIST_PACK_ENTRY_OVERHEAD = 8;
        // 安全的listPack最大长度，当listMaxListPackSize 配置TYPE 为 NUM 即最大允许个数时
        // 当listPack 超出此长度，即使个数未达到配置的最大个数，也会不允许进行插入
        static constexpr int SIZE_SAFETY_LIMIT = 8192;
    };
    class QuickListBase : protected QuickListHelper
    {
    public:
        ~QuickListBase () noexcept
        {
            auto *cur = head;
            QuickListNode *next;
            while (len--)
            {
                next = cur->next;
                delete cur;
                cur = next;
            }
        }

    protected:
        RetType quickListPushFront (const StringType &s)
        {
            RetType ret;
            if (quickListCheckAndInsertPlainNode(s, head, INSERT_POSITION::BEFORE, ret))
                return ret;

            if (likely(nodeAllowInsert(head, s.size())))
                head->data.listPack->pushFront(s);
            else
            {
                auto *node = new QuickListNode;
                node->data.listPack->pushFront(s);
                quickListInsertNode(node, head, INSERT_POSITION::BEFORE);
            }

            count++;
            ret.first = head;
            ret.second = LP_HEAD_SIZE;
            return ret;
        }
        template <class T, class = typename std::enable_if <std::is_integral <typename std::remove_reference <
            T>::type>::value, T>::type>
        RetType quickListPushFront (T val)
        {
            RetType ret;
            if (likely(nodeAllowInsert(head, sizeof(T))))
                head->data.listPack->pushFront(val);
            else
            {
                auto *node = new QuickListNode;
                node->data.listPack->pushFront(val);
                quickListInsertNode(node, head, INSERT_POSITION::BEFORE);
            }

            count++;
            ret.first = head;
            ret.second = LP_HEAD_SIZE;
            return ret;
        }

        RetType quickListPushBack (const StringType &s)
        {
            RetType ret;
            if (quickListCheckAndInsertPlainNode(s, tail, INSERT_POSITION::AFTER, ret))
                return ret;

            size_t entrySize;
            if (likely(nodeAllowInsert(tail, s.size())))
                entrySize =
                    tail->data.listPack->listPackInsert(tail->data.listPack->getLastBitOffset(), s);
            else
            {
                auto *node = new QuickListNode;
                entrySize =
                    node->data.listPack->listPackInsert(LP_HEAD_SIZE, s);
                quickListInsertNode(node, tail, INSERT_POSITION::AFTER);
            }

            count++;
            ret.first = tail;
            ret.second = tail->data.listPack->getLastBitOffset() - entrySize;
            return ret;
        }

        template <class T, class = typename std::enable_if <std::is_integral <typename std::remove_reference <
            T>::type>::value, T>::type>
        RetType quickListPushBack (T val)
        {
            RetType ret;
            size_t entrySize;
            if (likely(nodeAllowInsert(tail, sizeof(T))))
                entrySize = tail->data.listPack->
                    listPackInsert(tail->data.listPack->getLastBitOffset(), val);
            else
            {
                auto *node = new QuickListNode;
                entrySize = node->data.listPack->listPackInsert(LP_HEAD_SIZE, val);
                quickListInsertNode(node, tail, INSERT_POSITION::AFTER);
            }

            count++;
            ret.first = tail;
            ret.second = tail->data.listPack->getLastBitOffset() - entrySize;
            return ret;
        }

        template <class T>
        RetType quickListInsert (const Iterator &posIt, T &&val, size_t size, INSERT_POSITION pos)
        {
            bool full = false;
            bool isTail = false;
            bool isHead = false;
            bool availNext = false;
            bool availPrev = false;
            RetType ret;
            auto *listPack = posIt.node->data.listPack;
            off_t offset = posIt.p - listPack->getListPackPtr();
            auto *node = posIt.node;
            QuickListNode *newNode;
            // 如果没有node节点
            if (unlikely(!posIt.p))
            {
                PRINT_WARNING("no position iterator, so we create a new node and insert in tail");
                if (std::is_integral <typename std::remove_reference <
                    T>::type>::value)
                    return quickListPushBack(val);
                else
                    return quickListPushBack(StringType(val, size));
            }

            // 如果node节点已满
            if (!nodeAllowInsert(posIt.node, size))
            {
                PRINT_DEBUG("current node is full with count %d, with size %d",
                    posIt.node->data.listPack->size(),
                    posIt.node->data.listPack->listPackGetHeadTotalSize());
                full = true;
            }

            switch (pos)
            {
                case INSERT_POSITION::BEFORE:
                    // 如果插入的是当前entry的第一个节点之前，或者当前entry的头部
                    if (posIt.p == posIt.node->data.listPack->getListPackPtr() + LP_HEAD_SIZE
                        || posIt.p + KvListPack::listPackDecodingBackLen(posIt.p, nullptr)
                            == posIt.node->data.listPack->getListPackPtr() + LP_HEAD_SIZE)
                    {
                        PRINT_DEBUG("insert at head in listPack, listPack : %p",
                            posIt.node->data.listPack->getListPackPtr());
                        isHead = true;
                        if (nodeAllowInsert(posIt.node->prev, size))
                        {
                            PRINT_DEBUG("prev node is available, node : %p", posIt.node->prev);
                            availPrev = true;
                        }
                    }
                    break;
                case INSERT_POSITION::AFTER:
                    // 如果插入的是最后，或者是倒数第二个节点之后
                    if (*posIt.p == LP_END || *(posIt.p + posIt.entrySize) == LP_END)
                    {
                        PRINT_DEBUG("insert at tail in listPack, listPack : %p",
                            posIt.node->data.listPack->getListPackPtr());
                        isTail = true;
                        if (nodeAllowInsert(posIt.node->next, size))
                        {
                            PRINT_DEBUG("next node is available, node : %p", posIt.node->next);
                            availNext = true;
                        }
                    }
                    break;
            }
            // 如果新节点是plain节点
            if (unlikely(isLargeElement(size)))
            {
                // 如果当前节点是Plain节点，或者插入的是最后一个节点之后，或者插入的是第一个节点之前
                if (posIt.node->isPlainNode() || isTail || isHead)
                    // 大节点只可能是字符串
                    quickListInsertPlainNode(StringType(val, size), posIt.node, pos, ret);
                else
                {
                    posIt.node->decompressForUse();
                    newNode = quickListSplitNode(posIt, pos);
                    node = quickListCreatePlainNode(StringType(val, size));
                    // 将创建的节点添加到posNode 的 pos处
                    quickListInsertNode(node, posIt.node, pos);
                    // 将分割出来的节点添加到创建节点的pos处
                    quickListInsertNode(newNode, node, pos);
                    count++;
                }
                return ret;
            }

            // 如果没满
            if (!full)
            {
                PRINT_DEBUG("node not full, insert into this %s current position",
                    pos == INSERT_POSITION::BEFORE ? "before" : "after");
                node->decompressForUse();
                offset = checkTypeAndListPackPush(listPack, offset, val, size, pos);
                node->compressForRecompress();
            }
            else if ((isHead && availPrev) || (isTail && availNext))
            {
                // 如果满了，但是插入的是头/尾 并且前/后一个节点是可插入的
                PRINT_DEBUG("node is full, but %s node is not full,  insert into this %s node %s",
                    pos == INSERT_POSITION::BEFORE ? "before" : "after",
                    pos == INSERT_POSITION::BEFORE ? "before" : "after",
                    pos == INSERT_POSITION::BEFORE ? "tail" : "head");

                node->compressForRecompress();
                switch (pos)
                {
                    case INSERT_POSITION::BEFORE:
                        node = node->prev;
                        offset = node->data.listPack->getLastBitOffset();
                        break;
                    case INSERT_POSITION::AFTER:
                        node = node->next;
                        offset = LP_HEAD_SIZE;
                        break;
                }
                node->decompressForUse();
                checkTypeAndListPackPush(node->data.listPack, offset, val, size, pos);
                node->compressForRecompress();
            }
            else if (isHead || isTail)
            {
                // 如果满了，并且添加的是节点的头/尾部，并且前/后一个节点处于不可插入状态
                // 因为上面已经判断了isHead && availPrev，所以此处不需要判断 isHead && !availPrev
                PRINT_DEBUG("node is full, and %s node is also full, insert new node",
                    pos == INSERT_POSITION::BEFORE ? "before" : "after");
                newNode = new QuickListNode;
                // pushBack
                checkTypeAndListPackPush(
                    newNode->data.listPack,
                    LP_HEAD_SIZE,
                    val,
                    size,
                    INSERT_POSITION::BEFORE
                );
                quickListInsertNode(newNode, node, pos);
                node = newNode;
                offset = LP_HEAD_SIZE;
            }
            else
            {
                // 如果当前节点是满的，并且插入的是中间位置，需要分割节点
                PRINT_DEBUG("node is full, and insert position is mid, split node");
                node->decompressForUse();
                newNode = quickListSplitNode(posIt, pos);
                listPack = newNode->data.listPack;
                switch (pos)
                {
                    case INSERT_POSITION::BEFORE:
                        offset = listPack->getLastBitOffset();
                        break;
                    case INSERT_POSITION::AFTER:
                        offset = LP_HEAD_SIZE;
                        break;
                }
                checkTypeAndListPackPush(
                    listPack,
                    offset,
                    val,
                    size,
                    INSERT_POSITION::BEFORE
                );
                quickListInsertNode(newNode, node, pos);
                // 此处node、newNode为分割后的节点，长度不饱和
                // 检查前后节点，进行listPack合并
                quickMergeNodes(node);
                node = newNode;
            }

            count++;
            ret.first = node;
            ret.second = offset;
            return ret;
        }

        template <class T>
        RetType quickListFind (T &&val) const
        {
            if (empty())
                return { nullptr, 0 };

            auto *cur = head;
            while (cur)
            {
                cur->decompressForUse();
                auto it = cur->data.listPack->find(val);
                if (it)
                    return { cur, it.p - cur->data.listPack->getListPackPtr() };
                cur = cur->next;
            }

            return { nullptr, 0 };
        }

        size_t quickListErase (const Iterator &begin, size_t num)
        {
            auto it = begin;
            auto *listPack = it.node->data.listPack;
            auto *curNode = it.node;
            size_t deleted = 0, curDel = 0;
            auto beginOff = it.p - listPack->getListPackPtr();
            while (num && it.p)
            {
                uint16_t ele = listPack->listPackGetHeadElementsSize();
                if (beginOff == LP_HEAD_SIZE && num >= ele)
                {
                    auto *nxtNode = curNode->next;
                    quickListDelNode(curNode);
                    num -= ele;
                    deleted += ele;
                    if (!nxtNode) break;
                    it = Iterator(nxtNode, LP_HEAD_SIZE);
                }
                else
                {
                    curDel = 0;
                    while (++curDel < num && (++it).node == curNode);
                    listPack->listPackDeleteSafe(
                        beginOff,
                        it.node == curNode
                        ? it.p + it.entrySize - listPack->getListPackPtr()
                        : listPack->getLastBitOffset(),
                        curDel
                    );
                    num -= curDel;
                    deleted += curDel;
                    beginOff = LP_HEAD_SIZE;
                    count -= curDel;

                    if (!it.node) break;
                }

                curNode = it.node;
                curNode->decompressForUse();
                listPack = curNode->data.listPack;
            }

            return deleted;
        }

        size_t quickListErase (const ReverseIterator &rbegin, size_t num)
        {
            auto it = rbegin;
            auto *listPack = it.node->data.listPack;
            auto *curNode = it.node;
            size_t deleted = 0, curDel = 0;
            auto endOff = it.p - listPack->getListPackPtr();
            while (num && it.p)
            {
                uint16_t ele = listPack->listPackGetHeadElementsSize();
                if (*(listPack->getListPackPtr() + endOff) == LP_END && num >= ele)
                {
                    auto *prevNode = curNode->prev;
                    quickListDelNode(curNode);
                    num -= ele;
                    deleted += ele;
                    if (!prevNode) break;
                    it = ReverseIterator(prevNode, prevNode->data.listPack->getLastBitOffset());
                }
                else
                {
                    curDel = 0;
                    while (++curDel < num && (++it).node == curNode);
                    listPack->listPackDeleteSafe(
                        it.node == curNode
                        ? it.p - it.entrySize - listPack->getListPackPtr()
                        : LP_HEAD_SIZE,
                        endOff,
                        curDel
                    );
                    num -= curDel;
                    deleted += curDel;
                    count -= curDel;

                    if (!it.node) break;
                }

                curNode = it.node;
                curNode->decompressForUse();
                listPack = curNode->data.listPack;
                endOff = listPack->getLastBitOffset();
            }

            return deleted;
        }

        inline Iterator begin () const noexcept
        {
            KV_ASSERT(head->encoding == QuickListNode::ENCODING_MODE::RAW && !head->recompress);
            return Iterator(head, LP_HEAD_SIZE);
        }
        inline Iterator end () const noexcept
        {
            return Iterator(nullptr, 0);
        }
        inline ReverseIterator rbegin () const noexcept
        {
            KV_ASSERT(tail->encoding == QuickListNode::ENCODING_MODE::RAW && !tail->recompress);
            return ReverseIterator(tail, tail->data.listPack->getLastBitOffset());
        }
        inline ReverseIterator rend () const noexcept
        {
            return ReverseIterator(nullptr, 0);
        }
        inline size_t size () const noexcept { return count; }
        inline bool empty () const noexcept { return !count; }

        void quickListInsertNode (
            QuickListNode *newNode,
            QuickListNode *posNode,
            INSERT_POSITION pos
        )
        {
            switch (pos)
            {
                case INSERT_POSITION::BEFORE:
                    newNode->next = posNode;
                    if (posNode)
                    {
                        newNode->prev = posNode->prev;
                        if (posNode->prev) posNode->prev->next = newNode;
                        if (head == posNode) head = newNode;
                        posNode->prev = newNode;
                    }
                    break;
                case INSERT_POSITION::AFTER:
                    newNode->prev = posNode;
                    if (posNode)
                    {
                        newNode->next = posNode->next;
                        if (posNode->next) posNode->next->prev = newNode;
                        if (tail == posNode) tail = newNode;
                        posNode->next = newNode;
                    }
                    break;
            }

            if (!len) head = tail = newNode;
            len++;

            if (posNode) quickListNodeCompress(posNode);
            quickListNodeCompress(newNode);
        }
    private:
        /*
         *   - (center->prev->prev, center->prev) 检查前节点和前前节点能否合并
         *   - (center->next, center->next->next) 检查后节点和后后节点能否合并
         *   - (center->prev, center) 检查前节点和当前节点能否合并
         *   - (center, center->next) 检查当前节点和后节点能否合并
         */
        void quickMergeNodes (QuickListNode *centerNode)
        {
            PRINT_DEBUG("check nodes need to be merged");
            QuickListNode *prev, *prevPrev, *next, *nextNext;
            prev = prevPrev = next = nextNext = nullptr;

            if (centerNode->prev)
            {
                prev = centerNode->prev;
                if (prev->prev)
                    prevPrev = prev->prev;
            }

            if (centerNode->next)
            {
                next = centerNode->next;
                if (next->next)
                    nextNext = next->next;
            }

            if (nodeAllowMerge(prev, prevPrev))
                quickListListPackMerge(prev, prevPrev);
            if (nodeAllowMerge(next, nextNext))
                quickListListPackMerge(next, nextNext);

            if (nodeAllowMerge(centerNode, prev))
                quickListListPackMerge(centerNode, prev);

            if (nodeAllowMerge(centerNode, next))
                quickListListPackMerge(centerNode, next);
        }
        // a merge b
        void quickListListPackMerge (QuickListNode *a, QuickListNode *b)
        {
            a->decompress();
            b->decompress();
            PRINT_DEBUG("merge a : %p, b : %p",
                a->data.listPack->getListPackPtr(),
                b->data.listPack->getListPackPtr());

            a->data.listPack->listPackMerge(*b->data.listPack);
            // 此处只重新压了一次临界节点
            quickListDelNode(b);
            // 因为长度改变需要重新检查一下节点是否需要压缩
            // quickListCompress 只会压缩临界节点 以及检查传入的节点
            quickListNodeCompress(a);
        }

        void quickListDelNode (QuickListNode *node)
        {
            if (node->next)
                node->next->prev = node->prev;
            if (node->prev)
                node->prev->next = node->next;
            if (node == tail)
                tail = node->prev;
            if (node == head)
                head = node->next;

            len--;
            count -= node->data.listPack->listPackGetHeadElementsSize();
            // 删除节点后需要重新进行整体压缩检查
            quickListCompress(nullptr);
            delete node;
        }

        inline QuickListNode *quickListSplitNode (const Iterator &posIt, INSERT_POSITION pos) const
        {
            auto *newNode = new QuickListNode(*posIt.node->data.listPack);
            auto *listPack = posIt.node->data.listPack;
            off_t begin = LP_HEAD_SIZE;
            off_t end = listPack->getLastBitOffset();

            auto it = Iterator(posIt.node, LP_HEAD_SIZE);
            off_t splitPos = posIt.p - listPack->getListPackPtr();

            KV_ASSERT(!(pos == INSERT_POSITION::BEFORE && splitPos == begin)
                && !(pos == INSERT_POSITION::AFTER && splitPos == end));

            size_t num = 0;
            if (it != posIt)
                while (++it != posIt) num++;

            switch (pos)
            {
                case INSERT_POSITION::BEFORE:
                    listPack->listPackDeleteSafe(begin, splitPos, num);
                    num = newNode->data.listPack->listPackGetHeadElementsSize() - num;
                    newNode->data.listPack->listPackDeleteSafe(splitPos, end, num);
                    break;
                case INSERT_POSITION::AFTER:
                    // 强行调用父类虚函数，如果为listPack end话，将p置nullptr
                    it.ListPackHelper::Iterator::operator++();
                    num++;
                    splitPos = it.p ? (it.p - listPack->getListPackPtr()) : end;
                    newNode->data.listPack->listPackDeleteSafe(begin, splitPos, num);
                    num = newNode->data.listPack->listPackGetHeadElementsSize() - num;
                    listPack->listPackDeleteSafe(splitPos, end, num);
                    break;
            }

            PRINT_DEBUG("after split length : origin %d, new %d",
                listPack->listPackGetHeadElementsSize(),
                newNode->data.listPack->listPackGetHeadElementsSize());
            return newNode;
        }
        inline bool quickListNodeCompress (QuickListNode *node) const
        {
            // 如果是暂时解压的节点，直接压缩
            if (node->recompress)
                return node->compress();
                // 整个quickList重新进行压缩，判断node是否在需要压缩的节点中
            else
                return quickListCompress(node);
        }
        inline bool quickListCompress (QuickListNode *node) const
        {
            // 此处做了长度检查，所以下方指针一定不为nullptr
            if (!len || !needCompress(len)) return false;
            KV_ASSERT(!head->recompress && !tail->recompress);

            auto *forward = head;
            auto *reverse = tail;
            uint32_t depth = 0;
            bool nodeNeedCompress = true;
            auto listCompressDepth = KvConfig::getConfig().listCompressDepth;
            // 双端查找，通过配置查找不需要压缩的节点并解压
            while (depth++ < listCompressDepth)
            {
                forward->decompress();
                reverse->decompress();
                if (forward == node || reverse == node) nodeNeedCompress = false;
                // 没有节点需要压缩
                if (forward == reverse || forward->next == reverse) return false;
                forward = forward->next;
                reverse = reverse->prev;
            }

            if (node && nodeNeedCompress) node->compress();
            // 只需压缩一次临界节点，因为中间节点一定是之前已经压缩过的（能压缩的情况下）
            forward->compress();
            reverse->compress();
            return true;
        }
        inline bool quickListCheckAndInsertPlainNode (
            const StringType &s,
            QuickListNode *posNode,
            INSERT_POSITION pos,
            RetType &ret
        )
        {
            if (unlikely(isLargeElement(s.size())))
            {
                quickListInsertPlainNode(s, posNode, pos, ret);
                return true;
            }

            return false;
        }

        inline void quickListInsertPlainNode (
            const StringType &s,
            QuickListNode *posNode,
            INSERT_POSITION pos,
            RetType &ret
        )
        {
            auto *node = quickListCreatePlainNode(s);
            quickListInsertNode(node, posNode, pos);
            ret.first = node;
            ret.second = LP_HEAD_SIZE;
        }

        static inline QuickListNode *quickListCreatePlainNode (const StringType &s)
        {
            auto *node = new QuickListNode;
            node->containerType = QuickListNode::CONTAINER_TYPE::PLAIN;
            node->data.listPack->pushBack(s);
            return node;
        }

        QuickListNode *head {};
        QuickListNode *tail {};
        size_t count {}; // 所有listpack中entry的总数
        size_t len {}; // quicklistNode数量 即 链表长度
    };
}
#endif //KV_STORE_KV_STORE_SERVER_DATA_STRUCTURE_KV_QUICK_LIST_BASE_H_
