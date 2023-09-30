//
// Created by Yoshiki on 2023/8/23.
//

#ifndef LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_HASH_COMMAND_H_
#define LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_HASH_COMMAND_H_

#include "kv-command-common.h"

class HashCommandHandler : public CommandCommon
{
public:
    using HashType = std::unordered_map <KeyType, ValueType>;
    enum Commands
    {
        NIL = -1,
        HSET,   // 将哈希表 key 中的字段 field 的值设为 value 。
        HGET,  // 获取存储在哈希表中指定字段的值
        HGETALL, // 获取在哈希表中指定 key 的所有字段和值
        HEXISTS, // 查看哈希表 key 中，指定的字段是否存在。
        HINCRBY, // 为哈希表 key 中的指定字段的整数值加上增量 increment 。
        HINCRBYFLOAT, // 为哈希表 key 中的指定字段的浮点数值加上增量 increment 。
        HLEN, // 获取哈希表中字段的数量
        HDEL, // 删除一个或多个哈希表字段
        HVALS,  // 获取哈希表中所有值
        HKEYS, // 获取所有哈希表中的字段
        HSETNX, // 只有在字段 field 不存在时，设置哈希表字段的值。
        END
    };

    FIND_COMMAND

    inline ResValueType handlerCommand (
        const CommandParams &commandParams,
        Commands cmd
    )
    {
        ResValueType resValue;
        // 检查是否有key
        bool success = checkKeyIsValid(commandParams, resValue);
        if (!success)
            return resValue;

        switch (cmd)
        {
            case Commands::HSET:
                handlerHSet(commandParams, resValue);
                break;
            case Commands::HGET:
                handlerHGet(commandParams, resValue);
                break;
            case Commands::HGETALL:
                handlerHGetAll(commandParams, resValue);
                break;
            case HEXISTS:
                handlerHExists(commandParams, resValue);
                break;
            case HINCRBY:
                handlerHIncrBy(commandParams, resValue);
                break;
            case HINCRBYFLOAT:
                handlerHIncrByFloat(commandParams, resValue);
                break;
            case HLEN:
                handlerHLen(commandParams, resValue);
                break;
            case HDEL:
                handlerHDel(commandParams, resValue);
                break;
            case HVALS:
                handlerHVals(commandParams, resValue);
                break;
            case HKEYS:
                handlerHKeys(commandParams, resValue);
                break;
            case HSETNX:
                handlerHSetNx(commandParams, resValue);
                break;
            case Commands::NIL:
            case Commands::END:
                break;
        }

        return resValue;
    }

    inline void clear () noexcept override
    {
        keyValues.clear();
    }
    inline size_t delKey (const std::string &key) noexcept override
    {
        return keyValues.erase(key);
    }

private:
    void handlerHSet (const CommandParams &commandParams, ResValueType &resValueType)
    {
        // 检查是否有参数
        if (!checkHasParams(commandParams, resValueType, 1, true))
            return;
        // 检查参数是否为偶数，key:value
        if ((commandParams.params.size() & 1) == 1)
        {
            resValueType.setErrorStr(commandParams, ResValueType::ErrorType::WRONG_NUMBER);
            return;
        }

        setHashValue(commandParams, resValueType);
    }
    void handlerHGet (const CommandParams &commandParams, ResValueType &resValueType)
    {
        if (!checkHasParams(commandParams, resValueType, 1))
            return;

        auto it = keyValues.find(commandParams.key);
        if (it == end)
        {
            resValueType.setNilFlag();
            return;
        }

        auto &map = it->second;
        auto hashIt = map.find(commandParams.params[0]);
        if (hashIt == map.end())
        {
            resValueType.setNilFlag();
            return;
        }

        resValueType.setStringValue(hashIt->second);
    }

    void handlerHGetAll (const CommandParams &commandParams, ResValueType &resValueType)
    {
        if (!checkHasParams(commandParams, resValueType, 0))
            return;

        auto it = keyValues.find(commandParams.key);
        if (it == end)
        {
            resValueType.setEmptyVectorValue();
            return;
        }

        auto &map = it->second;
        for (auto &value : map)
            resValueType.setVectorValue(value.first, value.second);
    }

    void handlerHExists (const CommandParams &commandParams, ResValueType &resValueType)
    {
        if (!checkHasParams(commandParams, resValueType, 1))
            return;

        auto it = keyValues.find(commandParams.key);
        int found = 0;
        if (it != end && it->second.find(commandParams.params[0]) != it->second.end())
            found++;

        resValueType.setIntegerValue(found);
    }

    void handlerHIncrBy (const CommandParams &commandParams, ResValueType &resValue)
    {
        if (!checkHasParams(commandParams, resValue, 2))
            return;

        IntegerType step;
        if (!checkValueIsLongLong(commandParams, commandParams.params[1], resValue, &step))
            return;

        auto it = keyValues.find(commandParams.key);
        if (it == end)
        {
            defaultHashMap.emplace(commandParams.params[0], commandParams.params[1]);
            keyValues.emplace(commandParams.key, std::move(defaultHashMap));
            setNewKeyValue(commandParams.key, StructType::HASH);

            resValue.setIntegerValue(step);
        }
        else
        {
            auto &map = it->second;
            auto mapIt = map.find(commandParams.params[0]);
            if (mapIt == map.end())
            {
                map.emplace(commandParams.params[0], commandParams.params[1]);

                resValue.setIntegerValue(step);
            }
            else
            {
                IntegerType integer;
                if (!Utils::StringHelper::stringIsLL(mapIt->second, &integer))
                    resValue.setErrorStr(
                        commandParams,
                        ResValueType::ErrorType::HASH_VALUE_NOT_INTEGER
                    );
                else
                {
                    if (Utils::MathHelper::integerPlusOverflow(integer, step))
                        resValue.setErrorStr(
                            commandParams,
                            ResValueType::ErrorType::INCR_OR_DECR_OVERFLOW
                        );
                    else
                    {
                        mapIt->second = std::to_string(integer + step);
                        resValue.setIntegerValue(integer + step);
                    }
                }
            }
        }
    }

    void handlerHIncrByFloat (const CommandParams &commandParams, ResValueType &resValue)
    {
        if (!checkHasParams(commandParams, resValue, 2))
            return;

        FloatType step;
        if (!Utils::StringHelper::stringIsDouble(commandParams.params[1], &step))
        {
            resValue.setErrorStr(commandParams, ResValueType::ErrorType::VALUE_NOT_FLOAT);
            return;
        }

        auto it = keyValues.find(commandParams.key);
        if (it == end)
        {
            defaultHashMap.emplace(commandParams.params[0], commandParams.params[1]);
            keyValues.emplace(commandParams.key, std::move(defaultHashMap));
            setNewKeyValue(commandParams.key, StructType::HASH);

            resValue.setIntegerValue(step);
        }
        else
        {
            auto &map = it->second;
            auto mapIt = map.find(commandParams.params[0]);
            if (mapIt == map.end())
            {
                map.emplace(commandParams.params[0], commandParams.params[1]);

                resValue.setStringValue(commandParams.params[1]);
            }
            else
            {
                FloatType floatValue;
                if (!Utils::StringHelper::stringIsDouble(mapIt->second, &floatValue))
                    resValue.setErrorStr(
                        commandParams,
                        ResValueType::ErrorType::HASH_VALUE_NOT_FLOAT
                    );
                else
                {
                    if (Utils::MathHelper::doubleCalculateWhetherOverflow <std::plus <FloatType>>(
                        floatValue,
                        step
                    ))
                        resValue.setErrorStr(
                            commandParams,
                            ResValueType::ErrorType::INCR_OR_DECR_OVERFLOW
                        );
                    else
                    {
                        // 不用to_string，to_string 会吧 5转成5.000000
                        // mapIt->second = std::to_string(floatValue + step);
                        mapIt->second = std::move(Utils::StringHelper::toString(floatValue + step));
                        resValue.setStringValue(mapIt->second);
                    }
                }
            }
        }
    }

    void handlerHLen (const CommandParams &commandParams, ResValueType &resValue)
    {
        if (!checkHasParams(commandParams, resValue, 1))
            return;

        auto it = keyValues.find(commandParams.key);
        if (it == end)
            resValue.setIntegerValue(0);
        else
            resValue.setIntegerValue(it->second.size());
    }

    void handlerHDel (const CommandParams &commandParams, ResValueType &resValue)
    {
        if (!checkHasParams(commandParams, resValue, 1, true))
            return;

        auto it = keyValues.find(commandParams.key);
        if (it == end)
            resValue.setIntegerValue(0);
        else
        {
            int count = 0;
            for (auto &v : commandParams.params)
                count += it->second.erase(v);

            if (it->second.empty())
                // 这里不用在keyValues里删除，CommandHandler::delKeyEvent 会调用 HashCommandHandler::delKey
                delKeyValue(commandParams.key);

            resValue.setIntegerValue(count);
        }
    }

    void handlerHVals (const CommandParams &commandParams, ResValueType &resValueType)
    {

    }
    void handlerHKeys (const CommandParams &commandParams, ResValueType &resValueType)
    {

    }
    void handlerHSetNx (const CommandParams &commandParams, ResValueType &resValueType)
    {

    }

private:
    void setHashValue (const CommandParams &commandParams, ResValueType &resValueType)
    {
        auto it = keyValues.find(commandParams.key);
        HashType *hashMap;
        int count = 0;
        if (it == end)
            hashMap = &defaultHashMap;
        else
            hashMap = &it->second;

        for (size_t i = 0; i < commandParams.params.size();)
        {
            // c++ 函数参数加载顺序是未定义的，所以不能写成i++ 因为有可能是第二个参数先加载
            auto hashIt = hashMap->emplace(commandParams.params[i], commandParams.params[i + 1]);

            if (hashIt.second)
                count++;
            else
                hashIt.first->second = commandParams.params[i + 1];

            i += 2;
        }

        if (it == end)
        {
            keyValues.emplace(commandParams.key, std::move(*hashMap));
            defaultHashMap.clear();
            setNewKeyValue(commandParams.key, StructType::HASH);
        }

        resValueType.setIntegerValue(count);
    }

private:
    KvHashTable <KeyType, HashType> keyValues {};
    KvHashTable <KeyType, HashType>::iterator end = keyValues.end();
    HashType defaultHashMap {};

    static constexpr const char *commands[] {
        "hset",
        "hget",
        "hgetall",
        "hexists",
        "hincrby",
        "hincrbyfloat",
        "hlen",
        "hdel",
        "hvals",
        "hkeys",
        "hsetnx"
    };
};

constexpr const char *HashCommandHandler::commands[];
#endif //LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_HASH_COMMAND_H_
