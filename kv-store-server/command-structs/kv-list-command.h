//
// Created by 115282 on 2023/10/11.
//

#ifndef LINUX_SERVER_KV_STORE_SERVER_COMMAND_STRUCTS_KV_LIST_COMMAND_H_
#define LINUX_SERVER_KV_STORE_SERVER_COMMAND_STRUCTS_KV_LIST_COMMAND_H_

#include <circle/kv-server.h>

#include "data-structure/kv-ziplist-base.h"

class ListCommandHandler : public CommandCommonWithServerPtr
{
    enum class BlockPopModel
    {
        LEFT,
        RIGHT
    };
    friend class BaseCommandHandler;
    friend class KvDebug;
    class ListDataStructure
    {
    public:
        ListDataStructure () {
            data.listPackKeyValues = new KvListPack;
            dataStructureType = DataStructureType::LIST_PACK;
        }
        ListDataStructure (const ListDataStructure &r) {
            operator=(r);
        };
        ListDataStructure (ListDataStructure &&r) noexcept {
            operator=(std::move(r));
        };
        ListDataStructure &operator= (const ListDataStructure &r) {
            if (this == &r) return *this;
            dataStructureType = r.dataStructureType;
            if (dataStructureType == DataStructureType::LIST_PACK) {
                if (data.listPackKeyValues)
                    *data.listPackKeyValues = *r.data.listPackKeyValues;
                else
                    data.listPackKeyValues = new KvListPack(*r.data.listPackKeyValues);
            } else if (dataStructureType == DataStructureType::QUICK_LIST) {
                if (data.quickListKeyValues)
                    *data.quickListKeyValues = *r.data.quickListKeyValues;
                else
                    data.quickListKeyValues = new KvQuickList(*r.data.quickListKeyValues);
            } else {
                VALUE_DATA_STRUCTURE_ERROR;
            }

            return *this;
        }
        ListDataStructure &operator= (ListDataStructure &&r) noexcept {
            if (this == &r) return *this;
            dataStructureType = r.dataStructureType;
            if (dataStructureType == DataStructureType::LIST_PACK) {
                if (data.listPackKeyValues)
                    *data.listPackKeyValues = std::move(*r.data.listPackKeyValues);
                else
                    data.listPackKeyValues = new KvListPack(std::move(*r.data.listPackKeyValues));
            } else if (dataStructureType == DataStructureType::QUICK_LIST) {
                if (data.quickListKeyValues)
                    *data.quickListKeyValues = std::move(*r.data.quickListKeyValues);
                else
                    data.quickListKeyValues =
                        new KvQuickList(std::move(*r.data.quickListKeyValues));
            } else {
                VALUE_DATA_STRUCTURE_ERROR;
            }

            return *this;
        }
        ~ListDataStructure () noexcept {
            if (dataStructureType == DataStructureType::LIST_PACK) {
                delete data.listPackKeyValues;
                data.listPackKeyValues = nullptr;
            } else if (dataStructureType == DataStructureType::QUICK_LIST) {
                delete data.quickListKeyValues;
                data.quickListKeyValues = nullptr;
            } else {
                VALUE_DATA_STRUCTURE_ERROR;
            }
        }
        bool checkStructureConvert (size_t addSize, size_t addCount) const {
            if (dataStructureType != DataStructureType::QUICK_LIST) {
                size_t count = data.listPackKeyValues->size() + addCount;
                if (checkOverflow(addSize, count))
                    return true;
            }
            return false;
        }
        void structureConvert () {
            data.quickListKeyValues = new KvQuickList(data.listPackKeyValues);
            dataStructureType = DataStructureType::QUICK_LIST;
        }
        size_t size () const {
            return dataStructureType == DataStructureType::LIST_PACK
                   ? data.listPackKeyValues->size()
                   : data.quickListKeyValues->size();
        }
        bool empty () const {
            return size() == 0;
        }
        // 检测字节是否超出
        static inline bool checkOverflow (size_t size, size_t count) noexcept {
            auto listPackConfig = KvConfig::getConfig().listMaxListPackSize;
            switch (listPackConfig.type) {
                case ListMaxListPackSizeType::TYPE_NUM:
                    return count > listPackConfig.number;
                case ListMaxListPackSizeType::TYPE_SIZE:
                    return size > listPackConfig.number;
            }
            unreachable();
        }
        union DataType
        {
            ~DataType () = default;
            KvListPack *listPackKeyValues = nullptr;
            KvQuickList *quickListKeyValues;
        } data {};
        DataStructureType dataStructureType {};
    };
public:
    using CommandCommonWithServerPtr::CommandCommonWithServerPtr;
    bool handlerBefore (ParamsType &params) override {
        // 将一个或多个值插入到已存在的列表头部，列表不存在时操作无效。
        // 不需要判断，如果不存在key会直接走外层的emptyFn
        // if (params.commandParams.command == "lpushx" || params.commandParams.command == "rpushx")
        //     if (params.arg->isNewKey) return false;
        return true;
    }
public:
    void handlerLIndex (ParamsType &params) // NOLINT
    {
        IntegerType index = GET_ARG_VALUE(IntegerType);
        KvListPack::IteratorDataType data;
        switch (keyValues.dataStructureType) {
            case DataStructureType::LIST_PACK:
                data = keyValues.data.listPackKeyValues->findIndex(index);
                break;
            case DataStructureType::QUICK_LIST:
                data = keyValues.data.quickListKeyValues->findIndex(index);
                break;
            default:
            VALUE_DATA_STRUCTURE_ERROR;
        }
        if (data.mode == KvListPack::DataTypeEnum::ERROR)
            params.resValue.setNilFlag();
        else
            params.resValue.setStringValue(data.toString());
    }
    void handlerLPush (ParamsType &params) {
        checkPushStructureConvert(params.commandParams.params);

        if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            for (const auto &v : params.commandParams.params)
                keyValues.data.listPackKeyValues->pushFront(v);

            params.resValue.setIntegerValue(keyValues.data.listPackKeyValues->size());
        } else if (keyValues.dataStructureType == DataStructureType::QUICK_LIST) {
            for (const auto &v : params.commandParams.params)
                keyValues.data.quickListKeyValues->pushFront(v);

            params.resValue.setIntegerValue(keyValues.data.quickListKeyValues->size());
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }
    }
    void handlerLPushX (ParamsType &params) {
        // 不需要额外处理，在KvServer::bindAllCommand处理了空KEY情况
        handlerLPush(params);
    }
    // lpop key [count]
    void handlerLPop (ParamsType &params) {
        IntegerType count = GET_ARG_VALUE(IntegerType);

        if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            auto it = keyValues.data.listPackKeyValues->begin();
            while (count-- && !keyValues.data.listPackKeyValues->empty()) {
                params.resValue.setVectorValue(it->toString());
                it = keyValues.data.listPackKeyValues->erase(it);
            }
            if (keyValues.data.listPackKeyValues->empty())
                delKeyValue(params.commandParams.key);
        } else if (keyValues.dataStructureType == DataStructureType::QUICK_LIST) {
            auto it = keyValues.data.quickListKeyValues->begin();
            while (count-- && !keyValues.data.quickListKeyValues->empty()) {
                params.resValue.setVectorValue(it->toString());
                it = keyValues.data.quickListKeyValues->erase(it);
            }
            if (keyValues.data.quickListKeyValues->empty())
                delKeyValue(params.commandParams.key);
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }
    }
    void handlerRPush (ParamsType &params) {
        checkPushStructureConvert(params.commandParams.params);

        if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            for (const auto &v : params.commandParams.params)
                keyValues.data.listPackKeyValues->pushFront(v);

            params.resValue.setIntegerValue(keyValues.data.listPackKeyValues->size());
        } else if (keyValues.dataStructureType == DataStructureType::QUICK_LIST) {
            for (const auto &v : params.commandParams.params)
                keyValues.data.quickListKeyValues->pushFront(v);

            params.resValue.setIntegerValue(keyValues.data.quickListKeyValues->size());
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }
    }
    void handlerRPushX (ParamsType &params) {
        // 不需要额外处理，在KvServer::bindAllCommand处理了空KEY情况
        handlerRPush(params);
    }
    void handlerRPop (ParamsType &params) {
        IntegerType count = GET_ARG_VALUE(IntegerType);
        if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            auto it = keyValues.data.listPackKeyValues->rbegin();
            while (count-- && !keyValues.data.listPackKeyValues->empty()) {
                params.resValue.setVectorValue(it->toString());
                it = keyValues.data.listPackKeyValues->erase(it);
            }
            if (keyValues.data.listPackKeyValues->empty())
                delKeyValue(params.commandParams.key);
        } else if (keyValues.dataStructureType == DataStructureType::QUICK_LIST) {
            auto it = keyValues.data.quickListKeyValues->rbegin();
            while (count-- && !keyValues.data.quickListKeyValues->empty()) {
                params.resValue.setVectorValue(it->toString());
                it = keyValues.data.quickListKeyValues->erase(it);
            }
            if (keyValues.data.quickListKeyValues->empty())
                delKeyValue(params.commandParams.key);
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }
    }
    // linsert key before [position value] [insert value]
    void handlerLInsert (ParamsType &params) {
        INSERT_POSITION position = GET_ARG_VALUE(INSERT_POSITION);

        if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            auto it = keyValues.data.listPackKeyValues->find(params.commandParams.params[1]);
            if (it == keyValues.data.listPackKeyValues->end()) {
                params.resValue.setIntegerValue(-1);
                return;
            }

            if (keyValues.checkStructureConvert(params.commandParams.params[2].size(), 1))
                keyValues.structureConvert();

            keyValues.data.listPackKeyValues->insert(it, params.commandParams.params[2], position);
            params.resValue.setIntegerValue(keyValues.data.listPackKeyValues->size());
        } else if (keyValues.dataStructureType == DataStructureType::QUICK_LIST) {
            auto it = keyValues.data.quickListKeyValues->find(params.commandParams.params[1]);
            if (it == keyValues.data.quickListKeyValues->end()) {
                params.resValue.setIntegerValue(-1);
                return;
            }

            keyValues.data.quickListKeyValues->insert(it, params.commandParams.params[2], position);
            params.resValue.setIntegerValue(keyValues.data.quickListKeyValues->size());
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }
    }
    void handlerLSet (ParamsType &params) {
        IntegerType index = GET_ARG_VALUE(IntegerType);
        auto size = static_cast<IntegerType>(keyValues.size());
        if (index < 0) index += size;
        if (index >= size) {
            params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::INDEX_OUT_OF_RANGE);
            return;
        }
        if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            auto it = keyValues.data.listPackKeyValues->findIndexForward(index);
            keyValues.data.listPackKeyValues->replace(it, params.commandParams.params[2]);
        } else if (keyValues.dataStructureType == DataStructureType::QUICK_LIST) {
            auto it = keyValues.data.quickListKeyValues->findIndexForward(index);
            keyValues.data.quickListKeyValues->replace(it, params.commandParams.params[2]);
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }

    }
    void handlerLRange (ParamsType &params) {
        IntegerRange integerRange = GET_ARG_VALUE(IntegerRange);
        auto size = keyValues.size();
        if (integerRange.min < 0)
            integerRange.min = static_cast<int>(size + integerRange.min);
        if (integerRange.min > static_cast<int>(size - 1)) {
            params.resValue.setEmptyVectorValue();
            return;
        }
        if (integerRange.max < 0)
            integerRange.max = static_cast<int>(size + integerRange.max);
        if (integerRange.max > static_cast<int>(size - 1))
            integerRange.max = static_cast<int>(size - 1);

        if (integerRange.min > integerRange.max) {
            params.resValue.setEmptyVectorValue();
            return;
        }

        if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            auto it = keyValues.data.listPackKeyValues->findIndexForward(integerRange.min);
            params.resValue.setVectorValue(it->toString());

            while (integerRange.min++ != integerRange.max) {
                ++it;
                params.resValue.setVectorValue(it->toString());
            }
        } else if (keyValues.dataStructureType == DataStructureType::QUICK_LIST) {
            auto it = keyValues.data.quickListKeyValues->findIndexForward(integerRange.min);
            params.resValue.setVectorValue(it->toString());

            while (integerRange.min++ != integerRange.max) {
                ++it;
                params.resValue.setVectorValue(it->toString());
            }
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }
    }
    void handlerRPopLPush (ParamsType &params) {
        listRPopLPush(params, params.commandParams.key);
    }

    /**
     * LREM KEY_NAME COUNT VALUE
     * count > 0 : 从表头开始向表尾搜索，移除与 VALUE 相等的元素，数量为 COUNT 。
     * count < 0 : 从表尾开始向表头搜索，移除与 VALUE 相等的元素，数量为 COUNT 的绝对值。
     * count = 0 : 移除表中所有与 VALUE 相等的值。
     * */
    void handlerLRem (ParamsType &params) {
        IntegerType count = GET_ARG_VALUE(IntegerType);
        IntegerType res = 0;
        if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            if (count > 0) {
                for (auto it = keyValues.data.listPackKeyValues->begin();
                     res < count && it != keyValues.data.listPackKeyValues->end();) {
                    if (it->toString() == params.commandParams.params[1]) {
                        it = keyValues.data.listPackKeyValues->erase(it);
                        res++;

                        if (it == keyValues.data.listPackKeyValues->end()) break;
                    } else ++it;
                }
            } else if (count == 0) {
                for (auto it = keyValues.data.listPackKeyValues->begin();
                     it != keyValues.data.listPackKeyValues->end();) {
                    if (it->toString() == params.commandParams.params[1]) {
                        it = keyValues.data.listPackKeyValues->erase(it);
                        res++;

                        if (it == keyValues.data.listPackKeyValues->end()) break;
                    } else ++it;
                }
            } else {
                count = -count;
                for (auto it = keyValues.data.listPackKeyValues->rbegin();
                     res < count && it != keyValues.data.listPackKeyValues->rend();) {
                    if (it->toString() == params.commandParams.params[1]) {
                        it = keyValues.data.listPackKeyValues->erase(it);
                        res++;

                        if (it == keyValues.data.listPackKeyValues->rend()) break;
                    } else ++it;
                }
            }
        } else if (keyValues.dataStructureType == DataStructureType::QUICK_LIST) {
            if (count > 0) {
                for (auto it = keyValues.data.quickListKeyValues->begin();
                     res < count && it != keyValues.data.quickListKeyValues->end();) {
                    if (it->toString() == params.commandParams.params[1]) {
                        it = keyValues.data.quickListKeyValues->erase(it);
                        res++;

                        if (it == keyValues.data.quickListKeyValues->end()) break;
                    } else ++it;
                }
            } else if (count == 0) {
                for (auto it = keyValues.data.quickListKeyValues->begin();
                     it != keyValues.data.quickListKeyValues->end();) {
                    if (it->toString() == params.commandParams.params[1]) {
                        it = keyValues.data.quickListKeyValues->erase(it);
                        res++;

                        if (it == keyValues.data.quickListKeyValues->end()) break;
                    } else ++it;
                }
            } else {
                count = -count;
                for (auto it = keyValues.data.quickListKeyValues->rbegin();
                     res < count && it != keyValues.data.quickListKeyValues->rend();) {
                    if (it->toString() == params.commandParams.params[1]) {
                        it = keyValues.data.quickListKeyValues->erase(it);
                        res++;

                        if (it == keyValues.data.quickListKeyValues->rend()) break;
                    } else ++it;
                }
            }
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }
        params.resValue.setIntegerValue(res);
    }
    void handlerLLen (ParamsType &params) {
        params.resValue.setIntegerValue(keyValues.size());
    }
    void handlerLTrim (ParamsType &params) {
        IntegerRange integerRange = GET_ARG_VALUE(IntegerRange);
        auto size = keyValues.size();
        if (integerRange.min < 0)
            integerRange.min = static_cast<int>(size + integerRange.min);
        if (integerRange.min > static_cast<int>(size - 1))
            goto delKey;

        if (integerRange.max < 0)
            integerRange.max = static_cast<int>(size + integerRange.max);
        if (integerRange.max > static_cast<int>(size - 1))
            integerRange.max = static_cast<int>(size - 1);

        if (integerRange.min > integerRange.max)
            goto delKey;

        if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            keyValues.data.listPackKeyValues->erase(keyValues.data.listPackKeyValues->begin(), integerRange.min);
            keyValues.data
                     .listPackKeyValues
                     ->erase(keyValues.data.listPackKeyValues->rbegin(), integerRange.max + 1 - size);
        } else if (keyValues.dataStructureType == DataStructureType::QUICK_LIST) {
            keyValues.data.quickListKeyValues->erase(keyValues.data.quickListKeyValues->begin(), integerRange.min);
            keyValues.data
                     .quickListKeyValues
                     ->erase(keyValues.data.quickListKeyValues->rbegin(), integerRange.max + 1 - size);
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }
        goto end;

    delKey:
        delKeyValue(params.commandParams.key);
    end:
        params.resValue.setOKFlag();
    }

    void handlerBrPopLPush (ParamsType &params) {
        std::chrono::milliseconds ms = GET_ARG_VALUE(std::chrono::milliseconds);
        StringType data;
        // 没有数据弹出，阻塞io
        auto it = server->keyMap.find(params.commandParams.key);
        if (it != server->keyMap.end()) {
            if (!server->checkKeyType(params.commandParams, it, StructType::LIST, params.resValue))
                return;

            auto *listCommandHandler = static_cast<ListCommandHandler *>(it->second.handler); // NOLINT
            data = brCommandPopList(listCommandHandler);
            if (listCommandHandler->keyValues.empty()) server->keyMap.erase(it);

            blockListHandlerLPush(params, std::move(data));
            return;
        }
        params.reactorParams->circleEvents.addEvent(
            params.commandParams.key,
            &params,
            ms,
            StructType::LIST,
            std::bind(
                &ListCommandHandler::blockListRPopLPushCallback,
                this,
                std::placeholders::_1,
                std::placeholders::_2,
                std::placeholders::_3
            ),
            std::bind(
                &ListCommandHandler::blockListPopTimeoutCallback,
                this,
                std::placeholders::_1,
                std::placeholders::_2
            ));

        params.setBlock();
    }
    void handlerBlPop (ParamsType &params) {
        std::chrono::milliseconds ms = GET_ARG_VALUE(std::chrono::milliseconds);
        handlerBlockPopCommon(params, ms, BlockPopModel::LEFT);
    }
    void handlerBrPop (ParamsType &params) {
        std::chrono::milliseconds ms = GET_ARG_VALUE(std::chrono::milliseconds);
        handlerBlockPopCommon(params, ms, BlockPopModel::RIGHT);
    }

private:
    // block events
    void handlerBlockPopCommon (
        ParamsType &params,
        std::chrono::milliseconds ms,
        BlockPopModel model
    ) {
        if (blockPopDel(params.commandParams.key, params, model))
            return;
        for (auto vecIt = params.commandParams.params.begin(); vecIt != params.commandParams.params.end() - 1; ++vecIt)
            if (blockPopDel(*vecIt, params, model)) return;

        blockSocketAndAddEvent(params, ms, model);
    }
    void blockSocketAndAddEvent (
        ParamsType &params,
        const std::chrono::milliseconds &ms,
        BlockPopModel model
    ) {
        ArrayType <StringType> keys;
        keys.emplace_back(params.commandParams.key);
        std::copy(params.commandParams.params.begin(), params.commandParams.params.end() - 1, std::back_inserter(keys));

        params.reactorParams->circleEvents.addEvent(
            keys,
            &params,
            ms,
            StructType::LIST,
            std::bind(
                &ListCommandHandler::blockListPopCallback,
                this,
                std::placeholders::_1,
                std::placeholders::_2,
                std::placeholders::_3,
                model
            ),
            std::bind(
                &ListCommandHandler::blockListPopTimeoutCallback,
                this,
                std::placeholders::_1,
                std::placeholders::_2
            ));

        params.setBlock();
    }
    bool blockListPopCallback (
        ParamsType &params,
        CirCleEventType &cirCleEvent,
        const StringType &key,
        BlockPopModel model
    ) {
        auto it = server->keyMap.find(key);
        KV_ASSERT(it != server->keyMap.end()); // NOLINT
        auto *listCommandHandler = static_cast<ListCommandHandler *>(it->second.handler); // NOLINT
        blockPopAndSetRes(params, key, listCommandHandler, model);

        params.unBlock();
        // 清空过期时间，事件处理完成会被删除
        cirCleEvent.clearExpire();
        return true;
    }

    void listRPopLPush (ParamsType &params, const StringType &key) {
        auto it = server->keyMap.find(key);
        KV_ASSERT(it != server->keyMap.end());
        auto *listCommandHandler = static_cast<ListCommandHandler *>(it->second.handler); // NOLINT
        StringType data = brCommandPopList(listCommandHandler);
        if (listCommandHandler->keyValues.empty())
            server->keyMap.erase(key);

        blockListHandlerLPush(params, std::move(data));
    }
    bool blockListRPopLPushCallback (ParamsType &params, CirCleEventType &cirCleEvent, const StringType &key) {
        listRPopLPush(params, key);
        params.unBlock();
        // 清空过期时间，事件处理完成会被删除
        cirCleEvent.clearExpire();
        return true;
    }
    bool blockListPopTimeoutCallback (ParamsType &params, CirCleEventType &cirCleEvent) // NOLINT
    {
        params.resValue.setNilFlag();
        params.unBlock();
        // 清空过期时间，时间结束会删除
        cirCleEvent.clearExpire();
        return true;
    }
    inline void blockPopAndSetRes (
        ParamsType &params, const StringType &key, ListCommandHandler *listCommandHandler, BlockPopModel model
    ) {
        StringType data {};
        switch (model) {
            case BlockPopModel::LEFT:
                data = blCommandPopList(listCommandHandler);
                break;
            case BlockPopModel::RIGHT:
                data = brCommandPopList(listCommandHandler);
                break;
        }
        if (listCommandHandler->keyValues.empty())
            server->keyMap.erase(key);

        params.resValue.setVectorValue(key);
        params.resValue.setVectorValue(data);
    }
    inline bool blockPopDel (
        const KeyType &key,
        ParamsType &params,
        BlockPopModel model
    ) {
        auto it = server->keyMap.find(key);
        if (it != server->keyMap.end()) {
            if (!server->checkKeyType(params.commandParams, it, StructType::LIST, params.resValue))
                return false;

            auto *listCommandHandler = static_cast<ListCommandHandler *>(it->second.handler); // NOLINT
            blockPopAndSetRes(params, key, listCommandHandler, model);

            return true;
        }

        return false;
    }

    void blockListHandlerLPush (ParamsType &params, StringType &&data) {
        StringType pushKey = params.commandParams.params[0];
        auto pushIt = server->keyMap.find(pushKey);
        if (pushIt == server->keyMap.end()) {
            KeyOfValue newKey(StructType::LIST, server);
            pushIt = server->keyMap.emplace(pushKey, std::move(newKey)).first;
        }
        params.commandParams.params.clear();
        params.commandParams.params.emplace_back(std::move(data));
        static_cast<ListCommandHandler *>(pushIt->second.handler)->handlerLPush(params); // NOLINT
        params.resValue.setStringValue(std::move(params.commandParams.params[0]));
    }
    static inline StringType blCommandPopList (ListCommandHandler *listCommandHandler) {
        StringType data {};
        switch (listCommandHandler->keyValues.dataStructureType) {
            case DataStructureType::LIST_PACK:
                data = listCommandHandler->keyValues.data.listPackKeyValues->front().toString();
                listCommandHandler->keyValues.data.listPackKeyValues->popFront();
                break;
            case DataStructureType::QUICK_LIST:
                data = listCommandHandler->keyValues.data.quickListKeyValues->front().toString();
                listCommandHandler->keyValues.data.quickListKeyValues->popFront();
                break;
            default:
            VALUE_DATA_STRUCTURE_ERROR;
        }
        return data;
    }

    static inline StringType brCommandPopList (ListCommandHandler *listCommandHandler) {
        StringType data {};
        switch (listCommandHandler->keyValues.dataStructureType) {
            case DataStructureType::LIST_PACK:
                data = listCommandHandler->keyValues.data.listPackKeyValues->back().toString();
                listCommandHandler->keyValues.data.listPackKeyValues->popBack();
                break;
            case DataStructureType::QUICK_LIST:
                data = listCommandHandler->keyValues.data.quickListKeyValues->back().toString();
                listCommandHandler->keyValues.data.quickListKeyValues->popBack();
                break;
            default:
            VALUE_DATA_STRUCTURE_ERROR;
        }
        return data;
    }

    void checkPushStructureConvert (const ArrayType <StringType> &pushKeys) {
        size_t maxSize = 0;
        std::for_each(
            pushKeys.begin(), pushKeys.end(), [&maxSize] (const StringType &s) {
                if (s.size() > maxSize) maxSize = s.size();
            }
        );
        size_t addCount = pushKeys.size();
        if (keyValues.checkStructureConvert(maxSize, addCount))
            keyValues.structureConvert();
    }

    ListDataStructure keyValues {};
};
#endif //LINUX_SERVER_KV_STORE_SERVER_COMMAND_STRUCTS_KV_LIST_COMMAND_H_
