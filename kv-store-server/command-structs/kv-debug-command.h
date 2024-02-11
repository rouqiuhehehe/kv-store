//
// Created by 115282 on 2024/1/20.
//

#ifndef KV_STORE_KV_STORE_SERVER_COMMAND_STRUCTS_KV_DEBUG_COMMAND_H_
#define KV_STORE_KV_STORE_SERVER_COMMAND_STRUCTS_KV_DEBUG_COMMAND_H_

#include <circle/kv-server.h>

class KvDebug : public CommandCommonWithServerPtr
{
public:
    using CommandCommonWithServerPtr::CommandCommonWithServerPtr;
    bool handlerBefore (ParamsType &params) override {
        return true;
    }

    void handlerDebugCommand (ParamsType &params) {
        Utils::StringHelper::stringTolower(params.commandParams.key);
        if (params.commandParams.key == "object")
            return handlerDebugObject(params);
        else
            return params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::WRONG_NUMBER);
    }

private:
    void handlerDebugObject (ParamsType &params) {
        if (params.commandParams.params.size() != 1) return setDebugError(params);

        auto it = server->keyMap.find(params.commandParams.params[0]);
        if (it == server->keyMap.end())
            return params.resValue
                         .setErrorStr(
                             params.commandParams,
                             ResValueType::ErrorType::NO_SUCH_KEY
                         );

        StringType name;
        void *p;
        size_t size;
        DataStructureType type;
        if (it->second.structType == StructType::STRING) {
            p = &static_cast<StringCommandHandler *>(it->second.handler)->keyValues; // NOLINT
            size = 1;
            type = DataStructureType::STRING;
        } else if (it->second.structType == StructType::LIST) {
            auto keyValue = &static_cast<ListCommandHandler *>(it->second.handler)->keyValues; // NOLINT
            p = keyValue->data.listPackKeyValues;
            size = keyValue->size();
            type = keyValue->dataStructureType;
        } else if (it->second.structType == StructType::HASH) {
            auto keyValue = &static_cast<HashCommandHandler *>(it->second.handler)->keyValues; // NOLINT
            p = keyValue->data.listPackKeyValues;
            size = keyValue->size();
            type = keyValue->dataStructureType;
        } else if (it->second.structType == StructType::SET) {
            auto keyValue = &static_cast<SetCommandHandler *>(it->second.handler)->keyValues; // NOLINT
            p = keyValue->data.listPackKeyValues;
            size = keyValue->size();
            type = keyValue->dataStructureType;
        } else if (it->second.structType == StructType::ZSET) {
            auto keyValue = &static_cast<ZSetCommandHandler *>(it->second.handler)->keyValues; // NOLINT
            p = keyValue->data.listPackKeyValues;
            size = keyValue->size();
            type = keyValue->dataStructureType;
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }

        switch (type) {
            case DataStructureType::INT_SET:
                name = "intset";
                break;
            case DataStructureType::HASH_TABLE:
                name = "hashtable";
                break;
            case DataStructureType::SKIP_LIST:
                name = "skiplist";
                break;
            case DataStructureType::LIST_PACK:
                name = "listpack";
                break;
            case DataStructureType::QUICK_LIST:
                name = "quicklist";
                break;
            case DataStructureType::STRING:
                name = "string";
                break;
            case DataStructureType::STREAM:
                name = "stream";
                break;
            case DataStructureType::NIL:
                break;
        }

        std::stringstream stringStream;
        stringStream << "Value at : " << p << "\ndata struct : " << name << "\nstruct size : " << size;
        params.resValue.setStringValue(stringStream.str());
    }

    static inline void setDebugError (ParamsType &params) {
        params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::DEBUG_ERROR);
    }
};
#endif //KV_STORE_KV_STORE_SERVER_COMMAND_STRUCTS_KV_DEBUG_COMMAND_H_
