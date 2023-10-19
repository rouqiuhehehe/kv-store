//
// Created by 115282 on 2023/10/13.
//

#ifndef LINUX_SERVER_KV_STORE_SERVER_DATA_STRUCTURE_KV_ZIPLIST_H_
#define LINUX_SERVER_KV_STORE_SERVER_DATA_STRUCTURE_KV_ZIPLIST_H_

#include "kv-ziplist-base.h"
#include <list>
class KvZipList : public ZipListBase
{
public:
    void pushBack (u_char *s, uint32_t len) noexcept
    {
        if (!head->count)
            pushFront(s, len);
        else
        {
            Entry entry {};
            if (!zipEntrySafe(intConv32(head->tailOffset), entry))
                return PRINT_ERROR("error");

            zipListInsert(
                s,
                len,
                entry.prevLen,
                entry.headerSize + entry.len + intConv32(head->tailOffset));
        }
    }

    void pushFront (u_char *s, uint32_t len) noexcept
    {
        zipListInsert(s, len, 0, HEAD_SIZE);
    }

};
#endif //LINUX_SERVER_KV_STORE_SERVER_DATA_STRUCTURE_KV_ZIPLIS_H_
