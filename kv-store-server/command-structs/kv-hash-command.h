//
// Created by Yoshiki on 2023/8/23.
//

#ifndef LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_HASH_COMMAND_H_
#define LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_HASH_COMMAND_H_

class HashCommandHandler : public CommandCommon
{
    friend class BaseCommandHandler;
public:
    using ValueType = KvHashTable <KeyType, ::ValueType>;
    using PairType = std::pair <KeyType, ::ValueType>;

    inline void clear () noexcept override
    {
        keyValues.clear();
    }
    inline size_t delKey (const std::string &key) noexcept override
    {
        return keyValues.erase(key);
    }

private:
    void handlerHSet (ParamsType &params)
    {
        // 检查是否有参数
        if (!checkHasParams(params.commandParams, params.resValue, 1, true))
            return;
        // 检查参数是否为偶数，key:value
        if ((params.commandParams.params.size() & 1) == 1)
        {
            params.resValue
                  .setErrorStr(params.commandParams, ResValueType::ErrorType::WRONG_NUMBER);
            return;
        }

        setHashValue(params.commandParams, params.resValue);
    }
    void handlerHGet (ParamsType &params)
    {
        if (!checkHasParams(params.commandParams, params.resValue, 1))
            return;

        auto it = keyValues.find(params.commandParams.key);
        if (it == end)
        {
            params.resValue.setNilFlag();
            return;
        }

        auto &map = it->second;
        auto hashIt = map.find(params.commandParams.params[0]);
        if (hashIt == map.end())
        {
            params.resValue.setNilFlag();
            return;
        }

        params.resValue.setStringValue(hashIt->second);
    }

    void handlerHExists (ParamsType &params)
    {
        if (!checkHasParams(params.commandParams, params.resValue, 1))
            return;

        auto it = keyValues.find(params.commandParams.key);
        int found = 0;
        if (it != end && it->second.find(params.commandParams.params[0]) != it->second.end())
            found++;

        params.resValue.setIntegerValue(found);
    }

    void handlerHIncrBy (ParamsType &params)
    {
        if (!checkHasParams(params.commandParams, params.resValue, 2))
            return;

        IntegerType step;
        if (!checkValueIsLongLong(
            params.commandParams,
            params.commandParams.params[1],
            params.resValue,
            &step
        ))
            return;

        auto it = keyValues.find(params.commandParams.key);
        if (it == end)
        {
            defaultHashMap.emplace(params.commandParams.params[0], params.commandParams.params[1]);
            keyValues.emplace(params.commandParams.key, std::move(defaultHashMap));
            setNewKeyValue(params.commandParams.key, StructType::HASH);

            params.resValue.setIntegerValue(step);
        }
        else
        {
            auto &map = it->second;
            auto mapIt = map.find(params.commandParams.params[0]);
            if (mapIt == map.end())
            {
                map.emplace(params.commandParams.params[0], params.commandParams.params[1]);

                params.resValue.setIntegerValue(step);
            }
            else
            {
                IntegerType integer;
                if (!Utils::StringHelper::stringIsLL(mapIt->second, &integer))
                    params.resValue.setErrorStr(
                        params.commandParams,
                        ResValueType::ErrorType::HASH_VALUE_NOT_INTEGER
                    );
                else
                {
                    if (Utils::MathHelper::integerPlusOverflow(integer, step))
                        params.resValue.setErrorStr(
                            params.commandParams,
                            ResValueType::ErrorType::INCR_OR_DECR_OVERFLOW
                        );
                    else
                    {
                        mapIt->second = std::to_string(integer + step);
                        params.resValue.setIntegerValue(integer + step);
                    }
                }
            }
        }
    }

    void handlerHIncrByFloat (ParamsType &params)
    {
        if (!checkHasParams(params.commandParams, params.resValue, 2))
            return;

        FloatType step;
        if (!Utils::StringHelper::stringIsDouble(params.commandParams.params[1], &step))
        {
            params.resValue
                  .setErrorStr(params.commandParams, ResValueType::ErrorType::VALUE_NOT_FLOAT);
            return;
        }

        auto it = keyValues.find(params.commandParams.key);
        if (it == end)
        {
            defaultHashMap.emplace(params.commandParams.params[0], params.commandParams.params[1]);
            keyValues.emplace(params.commandParams.key, std::move(defaultHashMap));
            setNewKeyValue(params.commandParams.key, StructType::HASH);

            params.resValue.setIntegerValue(static_cast<IntegerType>(step));
        }
        else
        {
            auto &map = it->second;
            auto mapIt = map.find(params.commandParams.params[0]);
            if (mapIt == map.end())
            {
                map.emplace(params.commandParams.params[0], params.commandParams.params[1]);

                params.resValue.setStringValue(params.commandParams.params[1]);
            }
            else
            {
                FloatType floatValue;
                if (!Utils::StringHelper::stringIsDouble(mapIt->second, &floatValue))
                    params.resValue.setErrorStr(
                        params.commandParams,
                        ResValueType::ErrorType::HASH_VALUE_NOT_FLOAT
                    );
                else
                {
                    if (Utils::MathHelper::doubleCalculateWhetherOverflow <std::plus <FloatType>>(
                        floatValue,
                        step
                    ))
                        params.resValue.setErrorStr(
                            params.commandParams,
                            ResValueType::ErrorType::INCR_OR_DECR_OVERFLOW
                        );
                    else
                    {
                        // 不用to_string，to_string 会吧 5转成5.000000
                        // mapIt->second = std::to_string(floatValue + step);
                        mapIt->second = Utils::StringHelper::toString(floatValue + step);
                        params.resValue.setStringValue(mapIt->second);
                    }
                }
            }
        }
    }

    void handlerHLen (ParamsType &params)
    {
        if (!checkHasParams(params.commandParams, params.resValue, 1))
            return;

        auto it = keyValues.find(params.commandParams.key);
        if (it == end)
            params.resValue.setIntegerValue(0);
        else
            params.resValue.setIntegerValue(it->second.size());
    }

    void handlerHDel (ParamsType &params)
    {
        if (!checkHasParams(params.commandParams, params.resValue, 1, true))
            return;

        auto it = keyValues.find(params.commandParams.key);
        if (it == end)
            params.resValue.setIntegerValue(0);
        else
        {
            size_t count = 0;
            for (auto &v : params.commandParams.params)
                count += it->second.erase(v);

            if (it->second.empty())
                // 这里不用在keyValues里删除，CommandHandler::delKeyEvent 会调用 HashCommandHandler::delKey
                delKeyValue(params.commandParams.key);

            params.resValue.setIntegerValue(static_cast<IntegerType>(count));
        }
    }

    void handlerHGetAll (ParamsType &params)
    {
        hGetCommon(
            params, false, [&] (const PairType &pair) {
                params.resValue.setVectorValue(pair.first, pair.second);
            }
        );
    }

    void handlerHVals (ParamsType &params)
    {
        hGetCommon(
            params, true, [&] (const PairType &pair) {
                params.resValue.setVectorValue(pair.second);
            }
        );
    }
    void handlerHKeys (ParamsType &params)
    {
        hGetCommon(
            params, true, [&] (const PairType &pair) {
                params.resValue.setVectorValue(pair.first);
            }
        );
    }
    void handlerHSetNx (ParamsType &params)
    {
        if (checkHasParams(params.commandParams, params.resValue, 2))
            return;

        auto it = keyValues.find(params.commandParams.key);
        if (it != end)
        {
            params.resValue.setIntegerValue(0);
            return;
        }

        setHashValue(params.commandParams, params.resValue);
    }

private:
    void setHashValue (const CommandParams &commandParams, ResValueType &resValueType)
    {
        auto it = keyValues.find(commandParams.key);
        ValueType *hashMap;
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

    template <class F>
    void hGetCommon (ParamsType &params, bool needKey, F &&fn)
    {
        if (!checkHasParams(params.commandParams, params.resValue, static_cast<int>(needKey)))
            return;

        auto it = keyValues.find(params.commandParams.key);
        if (it == end)
        {
            params.resValue.setEmptyVectorValue();
            return;
        }

        auto &map = it->second;
        for (auto &value : map)
            fn(value);
    }

private:
    KvHashTable <KeyType, ValueType> keyValues {};
    KvHashTable <KeyType, ValueType>::iterator end = keyValues.end();
    ValueType defaultHashMap {};
};

#endif //LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_HASH_COMMAND_H_
