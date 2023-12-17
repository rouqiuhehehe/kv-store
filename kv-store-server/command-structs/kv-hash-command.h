//
// Created by Yoshiki on 2023/8/23.
//

#ifndef LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_HASH_COMMAND_H_
#define LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_HASH_COMMAND_H_

class HashCommandHandler : public CommandCommon
{
    friend class BaseCommandHandler;
public:
    using HashType = KvHashTable <KeyType, ::ValueType>;
    class HashDataStructure
    {
    public:
        HashDataStructure (size_t addSize, size_t addCount)
            : dataStructureType(DataStructureType::LIST_PACK)
        {
            if (checkSizeOverflow(addSize) || checkEntriesCountOverflow(addCount))
            {
                data.hashKeyValues = new HashType(addCount);
                dataStructureType = DataStructureType::HASH_TABLE;
            }
            else
            {
                data.listPackKeyValues = new KvListPack;
                dataStructureType = DataStructureType::LIST_PACK;
            }
        }
        HashDataStructure (size_t size, KeyType key, ::ValueType value)
        {
            if (checkSizeOverflow(size))
            {
                data.hashKeyValues = new HashType;
                dataStructureType = DataStructureType::HASH_TABLE;
                data.hashKeyValues->emplace(key, value);
            }
            else
            {
                data.listPackKeyValues = new KvListPack;
                dataStructureType = DataStructureType::LIST_PACK;
                data.listPackKeyValues->pushBack(key, value);
            }
        }
        HashDataStructure (const HashDataStructure &r)
        {
            operator=(r);
        };
        HashDataStructure (HashDataStructure &&r) noexcept
        {
            operator=(std::move(r));
        };
        HashDataStructure &operator= (const HashDataStructure &r)
        {
            if (this == &r) return *this;
            dataStructureType = r.dataStructureType;
            if (dataStructureType == DataStructureType::LIST_PACK)
            {
                if (data.listPackKeyValues)
                    *data.listPackKeyValues = *r.data.listPackKeyValues;
                else
                    data.listPackKeyValues = new KvListPack(*r.data.listPackKeyValues);
            }
            else
            {
                if (data.hashKeyValues)
                    *data.hashKeyValues = *r.data.hashKeyValues;
                else
                    data.hashKeyValues = new HashType(*r.data.hashKeyValues);
            }

            return *this;
        }
        HashDataStructure &operator= (HashDataStructure &&r) noexcept
        {
            if (this == &r) return *this;
            dataStructureType = r.dataStructureType;
            if (dataStructureType == DataStructureType::LIST_PACK)
            {
                if (data.listPackKeyValues)
                    *data.listPackKeyValues = std::move(*r.data.listPackKeyValues);
                else
                    data.listPackKeyValues = new KvListPack(std::move(*r.data.listPackKeyValues));
            }
            else
            {
                if (data.hashKeyValues)
                    *data.hashKeyValues = std::move(*r.data.hashKeyValues);
                else
                    data.hashKeyValues = new HashType(std::move(*r.data.hashKeyValues));
            }

            return *this;
        }
        ~HashDataStructure () noexcept
        {
            if (dataStructureType == DataStructureType::LIST_PACK)
            {
                delete data.listPackKeyValues;
                data.listPackKeyValues = nullptr;
            }
            else
            {
                delete data.hashKeyValues;
                data.hashKeyValues = nullptr;
            }
        }
        bool checkStructureConvert (size_t addSize, size_t addCount)
        {
            if (dataStructureType != DataStructureType::HASH_TABLE)
            {
                size_t count = data.listPackKeyValues->size() / 2 + addCount;
                size_t size = data.listPackKeyValues->capacity() + addSize;
                if (checkSizeOverflow(size) || checkEntriesCountOverflow(count))
                    return true;
            }
            return false;
        }
        void structureConvert ()
        {
            auto *hashTable = new HashType(data.listPackKeyValues->size());
            for (auto it = data.listPackKeyValues->begin();
                 it != data.listPackKeyValues->end();
                 ++it)
            {
                auto key = listPackValuesFillString(*it);
                it++;
                auto val = listPackValuesFillString(*it);
                hashTable->emplace(key, val);
            }

            delete data.listPackKeyValues;
            data.hashKeyValues = hashTable;
            dataStructureType = DataStructureType::HASH_TABLE;
        }
        inline StringType listPackValuesFillString (const KvListPack::IteratorDataType &itData)
        {
            if (itData.mode == KvListPack::DataTypeEnum::INTEGER)
                return Utils::StringHelper::toString(itData.data.val);
            else
                return StringType(itData.data.str.s, itData.data.str.len);
        }
        // 检测字节是否超出
        static inline bool checkSizeOverflow (size_t size) noexcept
        {
            return size > KvConfig::getConfig().hashMaxListPackValue;
        }
        // 检测entry条目数是否超出
        static inline bool checkEntriesCountOverflow (size_t count) noexcept
        {
            return count > KvConfig::getConfig().hashMaxListPackEntries;
        }
        union DataType
        {
            ~DataType () = default;
            HashType *hashKeyValues = nullptr;
            KvListPack *listPackKeyValues;
        } data {};
        DataStructureType dataStructureType {};
    };
    using ValueType = HashDataStructure;
    using PairType = std::pair <KeyType, ::ValueType>;

    inline bool handlerBefore (ParamsType &params) override
    {
        return true;
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

        size_t count = 0;
        auto it = keyValues.find(params.commandParams.key);
        // 忽略重复的key，猜测所有的key都以新增处理
        // redis中也是这么做的
        size_t addSize = std::accumulate(
            params.commandParams.params.begin(),
            params.commandParams.params.end(),
            0,
            [] (size_t a, const StringType &s) {
                return a + s.size();
            }
        );
        size_t addCount = params.commandParams.params.size() / 2;
        HashDataStructure *hashDataStructurePtr;
        if (it != keyValues.end())
        {
            if (it->second.checkStructureConvert(addSize, addCount))
                it->second.structureConvert();

            hashDataStructurePtr = &it->second;
        }
        else
        {
            HashDataStructure hashDataStructure(addSize, addCount);
            auto ret = keyValues.emplace(params.commandParams.key, std::move(hashDataStructure));
            KV_ASSERT(ret.second);
            hashDataStructurePtr = &ret.first->second;
        }

        for (auto paramIt = params.commandParams.params.begin();
             paramIt != params.commandParams.params.end(); ++paramIt)
        {
            const auto &key = *paramIt;
            paramIt++;
            const auto &val = *paramIt;

            if (hashDataStructurePtr->dataStructureType == DataStructureType::LIST_PACK)
            {
                auto findIt = hashDataStructurePtr->data.listPackKeyValues->find(key);
                if (findIt == hashDataStructurePtr->data.listPackKeyValues->end())
                {
                    hashDataStructurePtr->data.listPackKeyValues->pushBack(key, val);
                    count++;
                }
                else
                    hashDataStructurePtr->data.listPackKeyValues->replace(findIt, val);
            }
            else
            {
                auto findIt = hashDataStructurePtr->data.hashKeyValues->find(key);
                if (findIt == hashDataStructurePtr->data.hashKeyValues->end())
                {
                    hashDataStructurePtr->data.hashKeyValues->emplace(key, val);
                    count++;
                }
                else
                    findIt->second = val;
            }
        }

        params.resValue.setIntegerValue(count);
    }
    void handlerHGet (ParamsType &params)
    {
        if (!checkHasParams(params.commandParams, params.resValue, 1))
            return;

        auto it = keyValues.find(params.commandParams.key);
        if (it == keyValues.end())
        {
            params.resValue.setNilFlag();
            return;
        }

        auto &map = it->second;
        if (map.dataStructureType == DataStructureType::LIST_PACK)
        {
            auto hashIt = map.data.listPackKeyValues->find(params.commandParams.params[0]);
            if (hashIt == map.data.listPackKeyValues->end())
            {
                params.resValue.setNilFlag();
                return;
            }
            hashIt++;
            if (hashIt->mode == KvListPack::DataTypeEnum::INTEGER)
                params.resValue.setStringValue(Utils::StringHelper::toString(hashIt->data.val));
            else
                params.resValue
                      .setStringValue(StringType(hashIt->data.str.s, hashIt->data.str.len));
        }
        else
        {
            auto hashIt = map.data.hashKeyValues->find(params.commandParams.params[0]);
            if (hashIt == map.data.hashKeyValues->end())
            {
                params.resValue.setNilFlag();
                return;
            }

            params.resValue.setStringValue(hashIt->second);
        }
    }

    void handlerHExists (ParamsType &params)
    {
        if (!checkHasParams(params.commandParams, params.resValue, 1))
            return;

        auto it = keyValues.find(params.commandParams.key);
        int found = 0;
        if (it != keyValues.end() && (
            (it->second.dataStructureType == DataStructureType::LIST_PACK
                && it->second.data.listPackKeyValues->find(params.commandParams.params[0])
                    != it->second.data.listPackKeyValues->end()) ||
                (it->second.dataStructureType == DataStructureType::HASH_TABLE
                    && it->second.data.hashKeyValues->find(params.commandParams.params[0])
                        != it->second.data.hashKeyValues->end())
        ))
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
        // 如果外层key不存在
        if (it == keyValues.end())
        {
            auto &key = params.commandParams.params[0];
            auto &val = params.commandParams.params[1];
            HashDataStructure hashDataStructure(key.size() + val.size(), key, val);
            keyValues.emplace(params.commandParams.key, std::move(hashDataStructure));

            setNewKeyValue(params.commandParams.key, StructType::HASH);

            params.resValue.setIntegerValue(step);
        }
        else
        {
            auto &map = it->second;
            IntegerType integer;
            if (map.dataStructureType == DataStructureType::LIST_PACK)
            {
                auto mapIt = map.data.listPackKeyValues->find(params.commandParams.params[0]);
                // 如果内层key不存在
                if (mapIt == map.data.listPackKeyValues->end())
                {
                    map.data
                       .listPackKeyValues
                       ->pushBack(params.commandParams.params[0], params.commandParams.params[1]);

                    params.resValue.setIntegerValue(step);
                    return;
                }
                // 如果内层key存在，mapIt++ 取得是val的储存iterator
                mapIt++;
                KV_ASSERT(mapIt != map.data.listPackKeyValues->end());

                if (mapIt->mode == KvListPack::DataTypeEnum::STRING)
                {
                    auto ret = checkInvalidIncrInteger(
                        params,
                        StringType(mapIt->data.str.s, mapIt->data.str.len),
                        step
                    );
                    if (!ret.first) return;
                    integer = ret.second;
                }
                else
                {
                    if (!checkInvalidIncrInteger(params, mapIt->data.val, step)) return;
                    integer = mapIt->data.val + step;
                }
                map.data.listPackKeyValues->replace(mapIt, integer);
            }
            else
            {
                auto mapIt = map.data.hashKeyValues->find(params.commandParams.params[0]);
                // 如果内层key不存在
                if (mapIt == map.data.hashKeyValues->end())
                {
                    map.data
                       .hashKeyValues
                       ->emplace(params.commandParams.params[0], params.commandParams.params[1]);

                    integer = step;
                }
                else
                {
                    auto ret = checkInvalidIncrInteger(params, mapIt->second, step);
                    if (!ret.first) return;

                    integer = ret.second;
                    mapIt->second = Utils::StringHelper::toString(integer);
                }
            }
            params.resValue.setIntegerValue(integer);
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
        if (it == keyValues.end())
        {
            auto &key = params.commandParams.params[0];
            auto &val = params.commandParams.params[1];
            HashDataStructure hashDataStructure(key.size() + val.size(), key, val);
            keyValues.emplace(params.commandParams.key, std::move(hashDataStructure));
            setNewKeyValue(params.commandParams.key, StructType::HASH);

            params.resValue.setStringValue(Utils::StringHelper::toString(step));
        }
        else
        {
            auto &map = it->second;
            FloatType floatValue;
            StringType strFloatVal;
            if (map.dataStructureType == DataStructureType::LIST_PACK)
            {
                auto mapIt = map.data.listPackKeyValues->find(params.commandParams.params[0]);
                // 如果内层key不存在
                if (mapIt == map.data.listPackKeyValues->end())
                {
                    map.data
                       .listPackKeyValues
                       ->pushBack(params.commandParams.params[0], params.commandParams.params[1]);

                    params.resValue.setStringValue(Utils::StringHelper::toString(step));
                    return;
                }
                mapIt++;
                KV_ASSERT(mapIt != map.data.listPackKeyValues->end());

                if (mapIt->mode == KvListPack::DataTypeEnum::STRING)
                {
                    auto ret = checkInvalidIncrFloat(
                        params,
                        StringType(mapIt->data.str.s, mapIt->data.str.len),
                        step
                    );
                    if (!ret.first) return;
                    floatValue = ret.second;
                }
                else
                {
                    if (!checkInvalidIncrFloat(params, mapIt->data.val, step)) return;
                    floatValue = mapIt->data.val + step;
                }
                strFloatVal = Utils::StringHelper::toString(floatValue);
                map.data.listPackKeyValues->replace(mapIt, strFloatVal);
            }
            else
            {
                auto mapIt = map.data.hashKeyValues->find(params.commandParams.params[0]);
                // 如果内层key不存在
                if (mapIt == map.data.hashKeyValues->end())
                {
                    map.data
                       .hashKeyValues
                       ->emplace(params.commandParams.params[0], params.commandParams.params[1]);

                    strFloatVal = params.commandParams.params[1];
                }
                else
                {
                    auto ret = checkInvalidIncrFloat(params, mapIt->second, step);
                    if (!ret.first) return;

                    strFloatVal = Utils::StringHelper::toString(ret.second);
                    mapIt->second = strFloatVal;
                }
            }
            params.resValue.setStringValue(strFloatVal);
        }
    }

    void handlerHLen (ParamsType &params)
    {
        if (!checkHasParams(params.commandParams, params.resValue, 1))
            return;

        auto it = keyValues.find(params.commandParams.key);
        if (it == keyValues.end())
            params.resValue.setIntegerValue(0);
        else
            params.resValue.setIntegerValue(
                it->second.dataStructureType == DataStructureType::LIST_PACK
                ? it->second.data.listPackKeyValues->size() / 2
                : it->second.data.hashKeyValues->size()
            );
    }

    void handlerHDel (ParamsType &params)
    {
        if (!checkHasParams(params.commandParams, params.resValue, 1, true))
            return;

        auto it = keyValues.find(params.commandParams.key);
        if (it == keyValues.end())
            params.resValue.setIntegerValue(0);
        else
        {
            size_t count = 0;
            if (it->second.dataStructureType == DataStructureType::LIST_PACK)
            {
                for (auto &v : params.commandParams.params)
                {
                    auto delIt = it->second.data.listPackKeyValues->find(v);
                    if (delIt != it->second.data.listPackKeyValues->end())
                    {
                        // 删除两个，find找的是key，后一个存的是value
                        it->second.data.listPackKeyValues->erase(delIt, 2);
                        count++;
                    }
                }

                if (it->second.data.listPackKeyValues->empty())
                    // 这里不用在keyValues里删除，CommandHandler::delKeyEvent 会调用 HashCommandHandler::delKey
                    delKeyValue(params.commandParams.key);
            }
            else
            {
                for (auto &v : params.commandParams.params)
                    count += it->second.data.hashKeyValues->erase(v);

                if (it->second.data.hashKeyValues->empty())
                    // 这里不用在keyValues里删除，CommandHandler::delKeyEvent 会调用 HashCommandHandler::delKey
                    delKeyValue(params.commandParams.key);
            }

            params.resValue.setIntegerValue(count);
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
            params, false, [&] (const PairType &pair) {
                params.resValue.setVectorValue(pair.second);
            }
        );
    }
    void handlerHKeys (ParamsType &params)
    {
        hGetCommon(
            params, false, [&] (const PairType &pair) {
                params.resValue.setVectorValue(pair.first);
            }
        );
    }
    void handlerHSetNx (ParamsType &params)
    {
        if (!checkHasParams(params.commandParams, params.resValue, 2))
            return;

        auto it = keyValues.find(params.commandParams.key);
        // 如果外层key存在
        if (it != keyValues.end())
        {
            // 检查内存key是否存在，如果存在不允许插入
            if ((it->second.dataStructureType == DataStructureType::LIST_PACK
                && it->second.data.listPackKeyValues->find(params.commandParams.params[0])
                    != it->second.data.listPackKeyValues->end()) ||
                (it->second.dataStructureType == DataStructureType::HASH_TABLE
                    && it->second.data.hashKeyValues->find(params.commandParams.params[0])
                        != it->second.data.hashKeyValues->end()))
            {
                params.resValue.setIntegerValue(0);
                return;
            }
            if (it->second.dataStructureType == DataStructureType::LIST_PACK)
            {
                if (it->second.data.listPackKeyValues->find(params.commandParams.params[0])
                    != it->second.data.listPackKeyValues->end())
                {
                    params.resValue.setIntegerValue(0);
                    return;
                }
                auto addSize =
                    params.commandParams.params[0].size() + params.commandParams.params[1].size();
                // 检测是否需要进行数据结构转换
                if (it->second.checkStructureConvert(addSize, 1))
                {
                    it->second.structureConvert();
                    it->second
                      .data
                      .hashKeyValues
                      ->emplace(params.commandParams.params[0], params.commandParams.params[1]);
                }
                else
                    it->second
                      .data
                      .listPackKeyValues
                      ->pushBack(params.commandParams.params[0], params.commandParams.params[1]);
            }
            else
                it->second
                  .data
                  .hashKeyValues
                  ->emplace(params.commandParams.params[0], params.commandParams.params[1]);
        }
        else
        {
            size_t size =
                params.commandParams.params[0].size() + params.commandParams.params[1].size();
            HashDataStructure hashDataStructure
                (size, params.commandParams.params[0], params.commandParams.params[1]);
            keyValues.emplace(params.commandParams.key, std::move(hashDataStructure));
        }
        params.resValue.setIntegerValue(1);
    }

private:
    template <class F>
    void hGetCommon (ParamsType &params, bool needKey, F &&fn)
    {
        if (!checkHasParams(params.commandParams, params.resValue, static_cast<int>(needKey)))
            return;

        auto it = keyValues.find(params.commandParams.key);
        if (it == keyValues.end())
        {
            params.resValue.setEmptyVectorValue();
            return;
        }

        auto &map = it->second;
        if (map.dataStructureType == DataStructureType::LIST_PACK)
        {
            std::pair <StringType, StringType> param;
            for (auto valIt = map.data.listPackKeyValues->begin();
                 valIt != map.data.listPackKeyValues->end(); ++valIt)
            {
                param.first = map.listPackValuesFillString(*valIt);
                ++valIt;
                param.second = map.listPackValuesFillString(*valIt);

                fn(param);
            }
            return;
        }

        for (auto &value : *map.data.hashKeyValues)
            fn(value);

    }

    static inline std::pair <bool, IntegerType> checkInvalidIncrInteger (
        ParamsType &params,
        const StringType &s,
        IntegerType step
    )
    {
        IntegerType integer;
        if (!Utils::StringHelper::stringIsLL(s, &integer))
        {
            params.resValue.setErrorStr(
                params.commandParams,
                ResValueType::ErrorType::HASH_VALUE_NOT_INTEGER
            );
            return { false, 0 };
        }

        if (Utils::MathHelper::integerPlusOverflow(integer, step))
        {
            params.resValue.setErrorStr(
                params.commandParams,
                ResValueType::ErrorType::INCR_OR_DECR_OVERFLOW
            );
            return { false, 0 };
        }

        return { true, integer + step };
    }

    static inline bool checkInvalidIncrInteger (
        ParamsType &params,
        IntegerType integer,
        IntegerType step
    )
    {
        if (Utils::MathHelper::integerPlusOverflow(integer, step))
        {
            params.resValue.setErrorStr(
                params.commandParams,
                ResValueType::ErrorType::INCR_OR_DECR_OVERFLOW
            );
            return false;
        }

        return true;
    }

    static inline std::pair <bool, FloatType> checkInvalidIncrFloat (
        ParamsType &params,
        const StringType &s,
        FloatType step
    )
    {
        FloatType floatValue;
        if (!Utils::StringHelper::stringIsDouble(s, &floatValue))
        {
            params.resValue.setErrorStr(
                params.commandParams,
                ResValueType::ErrorType::HASH_VALUE_NOT_FLOAT
            );
            return { false, 0 };
        }
        if (Utils::MathHelper::doubleCalculateWhetherOverflow <std::plus <FloatType>>(
            floatValue,
            step
        ))
        {
            params.resValue.setErrorStr(
                params.commandParams,
                ResValueType::ErrorType::INCR_OR_DECR_OVERFLOW
            );
            return { false, 0 };
        }
        // 不用to_string，to_string 会吧 5转成5.000000
        return { true, floatValue + step };
    }

    static inline bool checkInvalidIncrFloat (
        ParamsType &params,
        FloatType s,
        FloatType step
    )
    {
        if (Utils::MathHelper::doubleCalculateWhetherOverflow <std::plus <FloatType>>(
            s,
            step
        ))
        {
            params.resValue.setErrorStr(
                params.commandParams,
                ResValueType::ErrorType::INCR_OR_DECR_OVERFLOW
            );
            return false;
        }
        // 不用to_string，to_string 会吧 5转成5.000000
        return true;
    }

private:
    KvHashTable <KeyType, ValueType> keyValues {};
};
#endif //LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_HASH_COMMAND_H_
