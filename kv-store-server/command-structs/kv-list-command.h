//
// Created by 115282 on 2023/10/11.
//

#ifndef LINUX_SERVER_KV_STORE_SERVER_COMMAND_STRUCTS_KV_LIST_COMMAND_H_
#define LINUX_SERVER_KV_STORE_SERVER_COMMAND_STRUCTS_KV_LIST_COMMAND_H_

#include "data-structure/kv-ziplist-base.h"
class ListCommandHandler : public CommandCommon {
    friend class BaseCommandHandler;
public:
    void clear () noexcept override
    {

    }
    size_t delKey (const std::string &string) noexcept override
    {
        return 0;
    }

};
#endif //LINUX_SERVER_KV_STORE_SERVER_COMMAND_STRUCTS_KV_LIST_COMMAND_H_
