//
// Created by Yoshiki on 2023/8/23.
//

#ifndef LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_HASH_COMMAND_H_
#define LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_HASH_COMMAND_H_

class HashCommandHandler : public CommandCommonWithServerPtr
{
    friend class KvDebug;
    using HashType = KvHashTable <KeyType, ::ValueType>;
    class HashDataStructure
    {
    public:
        HashDataStructure ()
        {
            data.listPackKeyValues = new KvListPack;
            dataStructureType = DataStructureType::LIST_PACK;
        }
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
        HashDataStructure (size_t size, const KeyType &key, const ::ValueType &value)
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
            else if (dataStructureType == DataStructureType::HASH_TABLE)
            {
                if (data.hashKeyValues)
                    *data.hashKeyValues = *r.data.hashKeyValues;
                else
                    data.hashKeyValues = new HashType(*r.data.hashKeyValues);
            }
            else
            {
                VALUE_DATA_STRUCTURE_ERROR;
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
            else if (dataStructureType == DataStructureType::HASH_TABLE)
            {
                if (data.hashKeyValues)
                    *data.hashKeyValues = std::move(*r.data.hashKeyValues);
                else
                    data.hashKeyValues = new HashType(std::move(*r.data.hashKeyValues));
            }
            else
            {
                VALUE_DATA_STRUCTURE_ERROR;
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
            else if (dataStructureType == DataStructureType::HASH_TABLE)
            {
                delete data.hashKeyValues;
                data.hashKeyValues = nullptr;
            }
            else
            {
                VALUE_DATA_STRUCTURE_ERROR;
            }
        }
        bool checkStructureConvert (size_t addSize, size_t addCount) const
        {
            if (dataStructureType != DataStructureType::HASH_TABLE)
            {
                size_t count = data.listPackKeyValues->size() / 2 + addCount;
                if (checkSizeOverflow(addSize) || checkEntriesCountOverflow(count))
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
                auto key = it->toString();
                it++;
                auto val = it->toString();
                hashTable->emplace(key, val);
            }

            delete data.listPackKeyValues;
            data.hashKeyValues = hashTable;
            dataStructureType = DataStructureType::HASH_TABLE;
        }
        size_t size () const
        {
            return dataStructureType == DataStructureType::LIST_PACK
                   ? data.listPackKeyValues->size() / 2 // key : value
                   : data.hashKeyValues->size();
        }
        bool empty () const
        {
            return size() == 0;
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
    using PairType = std::pair <KeyType, ::ValueType>;
public:
    using CommandCommonWithServerPtr::CommandCommonWithServerPtr;
    inline bool handlerBefore (ParamsType &params) override
    {
        if (params.commandParams.command == "hsetnx")
        {
            if (params.arg)
            {
                // 已存在不让操作
                if (!params.arg->isNewKey)
                {
                    if ((keyValues.dataStructureType == DataStructureType::LIST_PACK
                        && keyValues.data.listPackKeyValues->find(params.commandParams.params[0])
                            != keyValues.data.listPackKeyValues->end()) ||
                        (keyValues.dataStructureType == DataStructureType::HASH_TABLE
                            && keyValues.data.hashKeyValues->find(params.commandParams.params[0])
                                != keyValues.data.hashKeyValues->end()))
                    {
                        params.resValue.setZeroIntegerValue();
                        return false;
                    }
                }
                params.arg = nullptr;
            }
        }
        return true;
    }

public:
    void handlerHSet (ParamsType &params)
    {
        size_t count = 0;
        // 忽略重复的key，猜测所有的key都以新增处理
        // redis中也是这么做的
        size_t maxSize = 0;
        std::for_each(
            params.commandParams.params.begin(), params.commandParams.params.end(), [&maxSize] (const StringType &s) {
                if (s.size() > maxSize) maxSize = s.size();
            }
        );
        size_t addCount = params.commandParams.params.size() / 2;
        if (keyValues.checkStructureConvert(maxSize, addCount))
            keyValues.structureConvert();

        if (keyValues.dataStructureType == DataStructureType::LIST_PACK)
        {
            for (auto paramIt = params.commandParams.params.begin();
                 paramIt != params.commandParams.params.end(); ++paramIt)
            {
                const auto &key = *paramIt;
                paramIt++;
                const auto &val = *paramIt;
                auto findIt = keyValues.data.listPackKeyValues->find(key);
                if (findIt == keyValues.data.listPackKeyValues->end())
                {
                    keyValues.data.listPackKeyValues->pushBack(key, val);
                    count++;
                }
                else
                    keyValues.data.listPackKeyValues->replace(findIt, val);
            }
        }
        else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE)
        {
            for (auto paramIt = params.commandParams.params.begin();
                 paramIt != params.commandParams.params.end(); ++paramIt)
            {
                const auto &key = *paramIt;
                paramIt++;
                const auto &val = *paramIt;
                auto findIt = keyValues.data.hashKeyValues->find(key);
                if (findIt == keyValues.data.hashKeyValues->end())
                {
                    keyValues.data.hashKeyValues->emplace(key, val);
                    count++;
                }
                else
                    findIt->second = val;
            }
        }
        else
        {
            VALUE_DATA_STRUCTURE_ERROR;
        }

        params.resValue.setIntegerValue(count);
    }
    void handlerHGet (ParamsType &params) // NOLINT
    {
        if (keyValues.dataStructureType == DataStructureType::LIST_PACK)
        {
            auto hashIt = keyValues.data.listPackKeyValues->find(params.commandParams.params[0]);
            if (hashIt == keyValues.data.listPackKeyValues->end())
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
        else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE)
        {
            auto hashIt = keyValues.data.hashKeyValues->find(params.commandParams.params[0]);
            if (hashIt == keyValues.data.hashKeyValues->end())
            {
                params.resValue.setNilFlag();
                return;
            }

            params.resValue.setStringValue(hashIt->second);
        }
        else
        {
            VALUE_DATA_STRUCTURE_ERROR;
        }
    }

    void handlerHExists (ParamsType &params) // NOLINT
    {
        int found = 0;
        if ((keyValues.dataStructureType == DataStructureType::LIST_PACK
            && keyValues.data.listPackKeyValues->find(params.commandParams.params[0])
                != keyValues.data.listPackKeyValues->end()) ||
            (keyValues.dataStructureType == DataStructureType::HASH_TABLE
                && keyValues.data.hashKeyValues->find(params.commandParams.params[0])
                    != keyValues.data.hashKeyValues->end()))
            found++;

        params.resValue.setIntegerValue(found);
    }

    void handlerHIncrBy (ParamsType &params)
    {
        auto &key = params.commandParams.params[0];
        auto &val = params.commandParams.params[1];
        if (keyValues.empty())
        {
            if (keyValues.checkStructureConvert(0, 1))
                keyValues.structureConvert();
        }

        IntegerType step = *static_cast<IntegerType *>(params.arg->arg);
        IntegerType integer;
        if (keyValues.dataStructureType == DataStructureType::LIST_PACK)
        {
            auto mapIt = keyValues.data.listPackKeyValues->find(params.commandParams.params[0]);
            // 如果内层key不存在
            if (mapIt == keyValues.data.listPackKeyValues->end())
            {
                keyValues.data.listPackKeyValues->pushBack(key, val);

                params.resValue.setIntegerValue(step);
                return;
            }
            // 如果内层key存在，mapIt++ 取得是val的储存iterator
            mapIt++;
            KV_ASSERT(mapIt != keyValues.data.listPackKeyValues->end());

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
            keyValues.data.listPackKeyValues->replace(mapIt, integer);
        }
        else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE)
        {
            auto mapIt = keyValues.data.hashKeyValues->find(params.commandParams.params[0]);
            // 如果内层key不存在
            if (mapIt == keyValues.data.hashKeyValues->end())
            {
                keyValues.data
                         .hashKeyValues
                         ->emplace(key, val);

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
        else
        {
            VALUE_DATA_STRUCTURE_ERROR;
        }
        params.resValue.setIntegerValue(integer);
    }

    void handlerHIncrByFloat (ParamsType &params)
    {
        auto &key = params.commandParams.params[0];
        auto &val = params.commandParams.params[1];
        if (keyValues.empty())
        {
            if (keyValues.checkStructureConvert(0, 1))
                keyValues.structureConvert();
        }

        FloatType step = *reinterpret_cast<FloatType *>(&params.arg->arg);
        FloatType floatValue;
        StringType strFloatVal;
        if (keyValues.dataStructureType == DataStructureType::LIST_PACK)
        {
            auto mapIt = keyValues.data.listPackKeyValues->find(key);
            // 如果内层key不存在
            if (mapIt == keyValues.data.listPackKeyValues->end())
            {
                keyValues.data.listPackKeyValues->pushBack(key, val);

                params.resValue.setStringValue(Utils::StringHelper::toString(step));
                return;
            }
            mapIt++;
            KV_ASSERT(mapIt != keyValues.data.listPackKeyValues->end());

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
                if (!checkInvalidIncrFloat(
                    params,
                    static_cast<double>(mapIt->data.val),
                    step
                ))
                    return;
                floatValue = static_cast<double>(mapIt->data.val) + step;
            }
            strFloatVal = Utils::StringHelper::toString(floatValue);
            keyValues.data.listPackKeyValues->replace(mapIt, strFloatVal);
        }
        else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE)
        {
            auto mapIt = keyValues.data.hashKeyValues->find(params.commandParams.params[0]);
            // 如果内层key不存在
            if (mapIt == keyValues.data.hashKeyValues->end())
            {
                keyValues.data.hashKeyValues->emplace(key, val);

                strFloatVal = key;
            }
            else
            {
                auto ret = checkInvalidIncrFloat(params, mapIt->second, step);
                if (!ret.first) return;

                strFloatVal = Utils::StringHelper::toString(ret.second);
                mapIt->second = strFloatVal;
            }
        }
        else
        {
            VALUE_DATA_STRUCTURE_ERROR;
        }
        params.resValue.setStringValue(strFloatVal);
    }

    void handlerHLen (ParamsType &params)
    {
        params.resValue.setIntegerValue(keyValues.size());
    }

    void handlerHDel (ParamsType &params)
    {
        size_t count = 0;
        if (keyValues.dataStructureType == DataStructureType::LIST_PACK)
        {
            for (auto &v : params.commandParams.params)
            {
                auto delIt = keyValues.data.listPackKeyValues->find(v);
                if (delIt != keyValues.data.listPackKeyValues->end())
                {
                    // 删除两个，find找的是key，后一个存的是value
                    keyValues.data.listPackKeyValues->erase(delIt, 2);
                    count++;
                }
            }

            if (keyValues.data.listPackKeyValues->empty())
                // 这里不用在keyValues里删除，CommandHandler::delKeyEvent 会调用 HashCommandHandler::delKey
                delKeyValue(params.commandParams.key);
        }
        else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE)
        {
            for (auto &v : params.commandParams.params)
                count += keyValues.data.hashKeyValues->erase(v);

            if (keyValues.data.hashKeyValues->empty())
                // 这里不用在keyValues里删除，CommandHandler::delKeyEvent 会调用 HashCommandHandler::delKey
                delKeyValue(params.commandParams.key);
        }
        else
        {
            VALUE_DATA_STRUCTURE_ERROR;
        }

        params.resValue.setIntegerValue(count);
    }

    void handlerHGetAll (ParamsType &params)
    {
        hGetCommon(
            params, [&] (const PairType &pair) {
                params.resValue.setVectorValue(pair.first, pair.second);
            }
        );
    }

    void handlerHVals (ParamsType &params)
    {
        hGetCommon(
            params, [&] (const PairType &pair) {
                params.resValue.setVectorValue(pair.second);
            }
        );
    }
    void handlerHKeys (ParamsType &params)
    {
        hGetCommon(
            params, [&] (const PairType &pair) {
                params.resValue.setVectorValue(pair.first);
            }
        );
    }
    void handlerHSetNx (ParamsType &params)
    {
        if (keyValues.dataStructureType == DataStructureType::LIST_PACK)
        {
            auto addSize =
                params.commandParams.params[0].size() + params.commandParams.params[1].size();
            // 检测是否需要进行数据结构转换
            if (keyValues.checkStructureConvert(addSize, 1))
            {
                keyValues.structureConvert();
                keyValues
                    .data
                    .hashKeyValues
                    ->emplace(params.commandParams.params[0], params.commandParams.params[1]);
            }
            else
                keyValues
                    .data
                    .listPackKeyValues
                    ->pushBack(params.commandParams.params[0], params.commandParams.params[1]);
        }
        else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE)
            keyValues
                .data
                .hashKeyValues
                ->emplace(params.commandParams.params[0], params.commandParams.params[1]);
        else
        {
            VALUE_DATA_STRUCTURE_ERROR;
        }
        params.resValue.setIntegerValue(1);
    }

private:
    template <class F>
    void hGetCommon (ParamsType &params, F &&fn)
    {
        if (keyValues.dataStructureType == DataStructureType::LIST_PACK)
        {
            std::pair <StringType, StringType> param;
            for (auto valIt = keyValues.data.listPackKeyValues->begin();
                 valIt != keyValues.data.listPackKeyValues->end(); ++valIt)
            {
                param.first = valIt->toString();
                ++valIt;
                param.second = valIt->toString();

                fn(param);
            }
        }
        else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE)
        {
            for (auto &value : *keyValues.data.hashKeyValues)
                fn(value);
        }
        else
        {
            VALUE_DATA_STRUCTURE_ERROR;
        }

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
    HashDataStructure keyValues {};
};
#endif //LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_HASH_COMMAND_H_
