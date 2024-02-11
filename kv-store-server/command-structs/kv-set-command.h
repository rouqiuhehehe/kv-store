//
// Created by 115282 on 2024/1/12.
//

#ifndef KV_STORE_KV_STORE_SERVER_COMMAND_STRUCTS_KV_SET_COMMAND_H_
#define KV_STORE_KV_STORE_SERVER_COMMAND_STRUCTS_KV_SET_COMMAND_H_
class SetCommandHandler final : public CommandCommonWithServerPtr
{
    friend class KvDebug;
    using HashType = KvHashSet <::ValueType>;
    using HandlerSetStoreCommandParamsHandlerIteratorType = decltype(decltype(ParamsType::commandParams)::params)::iterator;
    using HandlerSetStoreCommandParamsHandlerType = SetCommandHandler *(SetCommandHandler::*) (
        ParamsType &params,
        HandlerSetStoreCommandParamsHandlerIteratorType begin,
        const HandlerSetStoreCommandParamsHandlerIteratorType &end
    );
    class SetDataStructure
    {
    public:
        SetDataStructure () {
            data.intSetKeyValues = new KvIntSet;
            dataStructureType = DataStructureType::INT_SET;
        }
        SetDataStructure (const SetDataStructure &r) {
            operator=(r);
        };
        SetDataStructure (SetDataStructure &&r) noexcept {
            operator=(std::move(r));
        };
        SetDataStructure &operator= (const SetDataStructure &r) {
            if (this == &r) return *this;
            dataStructureType = r.dataStructureType;
            if (dataStructureType == DataStructureType::INT_SET) {
                if (data.intSetKeyValues)
                    *data.intSetKeyValues = *r.data.intSetKeyValues;
                else
                    data.intSetKeyValues = new KvIntSet(*r.data.intSetKeyValues);
            } else if (dataStructureType == DataStructureType::LIST_PACK) {
                if (data.listPackKeyValues)
                    *data.listPackKeyValues = *r.data.listPackKeyValues;
                else
                    data.listPackKeyValues = new KvListPack(*r.data.listPackKeyValues);
            } else if (dataStructureType == DataStructureType::HASH_TABLE) {
                if (data.hashKeyValues)
                    *data.hashKeyValues = *r.data.hashKeyValues;
                else
                    data.hashKeyValues = new HashType(*r.data.hashKeyValues);
            } else {
                VALUE_DATA_STRUCTURE_ERROR;
            }

            return *this;
        }
        SetDataStructure &operator= (SetDataStructure &&r) noexcept {
            if (this == &r) return *this;
            dataStructureType = r.dataStructureType;
            if (dataStructureType == DataStructureType::INT_SET) {
                if (data.intSetKeyValues)
                    *data.intSetKeyValues = std::move(*r.data.intSetKeyValues);
                else
                    data.intSetKeyValues = new KvIntSet(std::move(*r.data.intSetKeyValues));
            } else if (dataStructureType == DataStructureType::LIST_PACK) {
                if (data.listPackKeyValues)
                    *data.listPackKeyValues = std::move(*r.data.listPackKeyValues);
                else
                    data.listPackKeyValues = new KvListPack(std::move(*r.data.listPackKeyValues));
            } else if (dataStructureType == DataStructureType::HASH_TABLE) {
                if (data.hashKeyValues)
                    *data.hashKeyValues = std::move(*r.data.hashKeyValues);
                else
                    data.hashKeyValues = new HashType(std::move(*r.data.hashKeyValues));
            } else {
                VALUE_DATA_STRUCTURE_ERROR;
            }

            return *this;
        }
        ~SetDataStructure () noexcept {
            if (dataStructureType == DataStructureType::INT_SET) {
                delete data.intSetKeyValues;
                data.intSetKeyValues = nullptr;
            } else if (dataStructureType == DataStructureType::LIST_PACK) {
                delete data.listPackKeyValues;
                data.listPackKeyValues = nullptr;
            } else if (dataStructureType == DataStructureType::HASH_TABLE) {
                delete data.hashKeyValues;
                data.hashKeyValues = nullptr;
            } else {
                VALUE_DATA_STRUCTURE_ERROR;
            }
        }
        template <class Iterator>
        void checkStructureConvert (
            size_t addSize,
            size_t addCount,
            Iterator begin,
            const Iterator &end,
            ArrayType <int64_t> &resVec
        ) {
            if (dataStructureType == DataStructureType::INT_SET) {
                long long val;
                bool success = true;
                for (; begin != end; ++begin) {
                    if (!checkStringIsLL(*begin, &val)) {
                        success = false;
                        break;
                    }
                    resVec.emplace_back(val);
                }

                size_t count = data.intSetKeyValues->size() + addCount;
                // 如果有非数字 或者 entry数量超出
                // 结构转换
                if (!success || checkIntSetEntriesCountOverflow(count)) {
                    // 如果数量超出配置的LIST_PACK最大数量，直接转成Hashtable
                    if (checkListPackEntriesCountOverflow(count)) structureConvertIntSetToHashTable();
                    else {
                        // 否则转成ListPack
                        structureConvertIntSetToListPack();
                        // 转换后检查size，如果超出，转换成Hashtable
                        if (checkListPackSizeOverflow(addSize))
                            structureConvertListPackToHashTable();
                    }
                }
            } else if (dataStructureType == DataStructureType::LIST_PACK) {
                if (checkListPackStructureConvert(addSize, addCount))
                    structureConvertListPackToHashTable();
            }
        }
        void structureConvertIntSetToHashTable () {
            auto *hashTable = new HashType(data.intSetKeyValues->size());
            for (const auto &v: *data.intSetKeyValues)
                hashTable->emplace(Utils::StringHelper::toString(v));

            delete data.intSetKeyValues;
            data.hashKeyValues = hashTable;
            dataStructureType = DataStructureType::HASH_TABLE;
        }
        void structureConvertIntSetToListPack () {
            auto *listPack = new KvListPack();
            for (const auto &v: *data.intSetKeyValues)
                listPack->pushBack(v);

            delete data.intSetKeyValues;
            data.listPackKeyValues = listPack;
            dataStructureType = DataStructureType::LIST_PACK;
        }
        void structureConvertListPackToHashTable () {
            auto *hashTable = new HashType(data.listPackKeyValues->size());
            for (auto it = data.listPackKeyValues->begin(); it != data.listPackKeyValues->end(); ++it) {
                auto val = it->toString();
                hashTable->emplace(val);
            }

            delete data.listPackKeyValues;
            data.hashKeyValues = hashTable;
            dataStructureType = DataStructureType::HASH_TABLE;
        }
        inline size_t size () const {
            switch (dataStructureType) {
                case DataStructureType::INT_SET:
                    return data.intSetKeyValues->size();
                case DataStructureType::HASH_TABLE:
                    return data.hashKeyValues->size();
                case DataStructureType::LIST_PACK:
                    return data.listPackKeyValues->size();
                default:
                VALUE_DATA_STRUCTURE_ERROR;
            }
        }
        bool empty () const {
            return size() == 0;
        }
        bool checkListPackStructureConvert (size_t addSize, size_t addCount) const {
            size_t count = data.listPackKeyValues->size() + addCount;
            if (checkListPackSizeOverflow(addSize) || checkListPackEntriesCountOverflow(count))
                return true;

            return false;
        }
        static inline bool checkIntSetEntriesCountOverflow (size_t count) noexcept {
            return count > KvConfig::getConfig().setMaxIntSetEntries;
        }
        // 检测字节是否超出
        static inline bool checkListPackSizeOverflow (size_t size) noexcept {
            return size > KvConfig::getConfig().setMaxListPackValue;
        }
        // 检测entry条目数是否超出
        static inline bool checkListPackEntriesCountOverflow (size_t count) noexcept {
            return count > KvConfig::getConfig().setMaxListPackEntries;
        }

        static inline bool checkStringIsLL (const StringType &s, long long *val) {
            return Utils::StringHelper::stringIsLL(s, val);
        }

        template <class T,
                  class = typename std::enable_if <Utils::TypeHelper::CheckTypeIsInteger <T>::value, bool>::type>
        static inline bool checkStringIsLL (const T &v, long long *val) {
            *val = v;
            return true;
        }

        union DataType
        {
            ~DataType () = default;
            KvIntSet *intSetKeyValues = nullptr;
            KvListPack *listPackKeyValues;
            HashType *hashKeyValues;
        } data {};
        DataStructureType dataStructureType {};
    };

public:
    using CommandCommonWithServerPtr::CommandCommonWithServerPtr;
    inline bool handlerBefore (ParamsType &params) override {
        return true;
    }

public:
    /*
     * 向集合添加一个或多个成员
     * sadd key member1 [member2 ...]
     * 返回值：被添加到集合中的新元素的数量，不包括被忽略的元素。
     */
    void handlerSAdd (ParamsType &params) {
        size_t maxSize = 0;
        std::for_each(
            params.commandParams.params.begin(), params.commandParams.params.end(), [&maxSize] (const StringType &s) {
                if (s.size() > maxSize) maxSize = s.size();
            }
        );
        size_t addCount = params.commandParams.params.size();
        params.resValue.setIntegerValue(
            handlerSAddPrivate(
                maxSize, addCount, params.commandParams.params.begin(),
                params.commandParams.params.end()));
    }

    /*
     * 查询集合的成员
     * smember key
     * 返回值:集合中的所有成员
     */
    void handlerSMembers (ParamsType &params) { // NOLINT(readability-make-member-function-const)
        if (keyValues.dataStructureType == DataStructureType::INT_SET) {
            std::for_each(
                keyValues.data.intSetKeyValues->begin(),
                keyValues.data.intSetKeyValues->end(),
                [&params] (const int64_t &val) {
                    params.resValue.setVectorValue(Utils::StringHelper::toString(val));
                }
            );
        } else if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            std::for_each(
                keyValues.data.listPackKeyValues->begin(),
                keyValues.data.listPackKeyValues->end(),
                [&params] (const KvListPack::IteratorDataType &s) {
                    params.resValue.setVectorValue(s.toString());
                }
            );
        } else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE) {
            std::for_each(
                keyValues.data.hashKeyValues->begin(),
                keyValues.data.hashKeyValues->end(),
                [&params] (const StringType &s) {
                    params.resValue.setVectorValue(s);
                }
            );
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }
    }

    /*
     * 查询集合成员数量
     * scard key
     * 返回值：集合的数量。 当集合 key 不存在时，返回 0 。
     */
    void handlerSCard (ParamsType &params) {
        params.resValue.setIntegerValue(keyValues.size());
    }

    /*
     * 判断某元素是否存在
     * sismember key member
     * 返回值：如果成员元素是集合的成员，返回 1 。 如果成员元素不是集合的成员，或 key 不存在，返回 0 。
     */
    void handlerSIsMember (ParamsType &params) { // NOLINT(readability-make-member-function-const)
        params.resValue.setIntegerValue(handlerSIsMemberPrivate(params.commandParams.params[0]));
    }

    /*
     * 随机返回集合里面的成员
     * srandmember key [count]
     * 返回值：只提供集合 key 参数时，返回一个元素；如果集合为空，返回 nil 。 如果提供了 count 参数，那么返回一个数组；如果集合为空，返回空数组。
     */
    void handlerSRandMember (ParamsType &params) {
        IntegerType count = GET_ARG_VALUE(IntegerType);

        if (count == 0) {
            params.resValue.setEmptyVectorValue();
            return;
        }

        if (params.commandParams.params.empty()) {
            handlerSRandMemberSingle(params);
            return;
        }

        bool negative = false;
        if (count < 0) {
            negative = true;
            count = -count;
        }

        if (count > S_RAND_FIELD_RANDOM_SAMPLE_LIMIT) count = S_RAND_FIELD_RANDOM_SAMPLE_LIMIT;

        handlerSRandMemberMulti(params, negative, count);
    }

    /*
     * 将 member 元素从 source 集合移动到 destination 集合
     * smove source destination member
     * 返回值：如果成员元素被成功移除，返回 1 。 如果成员元素不是 source 集合的成员，并且没有任何操作对 destination 集合执行，那么返回 0 。
     */
    void handlerSMove (ParamsType &params) { // NOLINT(readability-make-member-function-const)
        StringType s;
        if (keyValues.dataStructureType == DataStructureType::INT_SET) {
            auto it = keyValues.data.intSetKeyValues->find(params.commandParams.params[1]);
            if (it == keyValues.data.intSetKeyValues->end()) {
                params.resValue.setZeroIntegerValue();
                return;
            }
            s = Utils::StringHelper::toString(*it);
            keyValues.data.intSetKeyValues->erase(it);
        } else if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            auto it = keyValues.data.listPackKeyValues->find(params.commandParams.params[1]);
            if (it == keyValues.data.listPackKeyValues->end()) {
                params.resValue.setZeroIntegerValue();
                return;
            }
            s = it->toString();
            keyValues.data.listPackKeyValues->erase(it);
        } else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE) {
            auto it = keyValues.data.hashKeyValues->find(params.commandParams.params[1]);
            if (it == keyValues.data.hashKeyValues->end()) {
                params.resValue.setZeroIntegerValue();
                return;
            }
            s = *it;
            keyValues.data.hashKeyValues->erase(it);
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }

        auto it = server->keyMap.find(params.commandParams.params[0]);
        if (it == server->keyMap.end()) {
            // 不存在需要添加的key，新建
            KeyOfValue newKey(StructType::SET, server);
            it = server->keyMap.emplace(params.commandParams.params[0], std::move(newKey)).first;
        }

        ArrayType <StringType> keys { s };
        static_cast<SetCommandHandler *>(it->second.handler)->handlerSAddPrivate( // NOLINT
            s.size(),
            1,
            keys.begin(),
            keys.end()); // NOLINT
        params.resValue.setIntegerValue(1);
    }

    /*
     * 移除并返回集合中的count个随机元素
     * spop key [count]
     * 返回值：被移除的随机元素。 当集合不存在或是空集时，返回 nil 。
     */
    void handlerSPop (ParamsType &params) {
        if (params.commandParams.params.empty()) {
            handlerSPopSingle(params);
            return;
        }

        size_t count = GET_ARG_VALUE(size_t);
        if (count >= keyValues.size()) {
            handlerSPopAll(params);
            return;
        }
        handlerSPopMulti(params, count);
    }

    /*
     * 移除集合中一个或多个成员
     * srem key member1 [member2 ...]
     * 返回值：被成功移除的元素的数量，不包括被忽略的元素
     */
    void handlerSRem (ParamsType &params) { // NOLINT(readability-make-member-function-const)
        size_t count = 0;

        if (keyValues.dataStructureType == DataStructureType::INT_SET) {
            IntegerType val;
            for (const StringType &v: params.commandParams.params) {
                if (Utils::StringHelper::stringIsLL(v, &val)) {
                    auto it = keyValues.data.intSetKeyValues->find(val);
                    if (it != keyValues.data.intSetKeyValues->end()) {
                        keyValues.data.intSetKeyValues->erase(it);
                        count++;
                    }
                }
            }
        } else if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            for (const StringType &v: params.commandParams.params) {
                auto it = keyValues.data.listPackKeyValues->find(v);
                if (it != keyValues.data.listPackKeyValues->end()) {
                    keyValues.data.listPackKeyValues->erase(it);
                    count++;
                }
            }
        } else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE) {
            for (const StringType &v: params.commandParams.params) {
                auto it = keyValues.data.hashKeyValues->find(v);
                if (it != keyValues.data.hashKeyValues->end()) {
                    keyValues.data.hashKeyValues->erase(it);
                    count++;
                }
            }
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }

        params.resValue.setIntegerValue(count);
    }

    /*
     * 返回给定所有集合相对于key1的差集
     * sdiff key1 [key2 ...]
     * 返回值：包含差集成员的列表。
     */
    void handlerSDiff (ParamsType &params) {
        auto *handler = handlerSDiffPrivate(
            params,
            params.commandParams.params.begin(),
            params.commandParams.params.end());
        delete handler;
    }

    /*
     * 返回给定所有集合相对于key1的差集并存储在 destination 中.
     * sdiffstore destination key1 [key2 ...]
     * 返回给定所有集合的差集并存储在 destination 中.
     * Redis sdiffstore 命令将给定集合之间的差集存储在指定的集合中。如果指定的集合 key 已存在，则会被覆盖。
     */
    void handlerSDiffStore (ParamsType &params) {
        handlerSetStoreCommon <decltype(params.commandParams.params.begin()), HandlerSetStoreCommandParamsHandlerType>(
            params,
            params.commandParams.params.begin() + 1,
            params.commandParams.params.end(),
            &SetCommandHandler::handlerSDiffPrivate
        );
    }

    /*
     * 返回给定所有集合的交集
     * sinter key1 [key2 ...]
     * Redis sinter 命令返回给定所有给定集合的交集。 不存在的集合 key 被视为空集。 当给定集合当中有一个空集时，结果也为空集(根据集合运算定律)。
     */
    void handlerSInter (ParamsType &params) {
        auto *handler = handlerSInterPrivate(
            params,
            params.commandParams.params.begin(),
            params.commandParams.params.end());
        delete handler;
    }

    /*
     * 返回给定所有集合的交集并存储在 destination 中.
     * sinterstore destination key1 [key2 ...]
     * Redis sinterstore 命令将给定集合之间的交集存储在指定的集合中。如果指定的集合已经存在，则将其覆盖。
     */
    void handlerSInterStore (ParamsType &params) {
        handlerSetStoreCommon <decltype(params.commandParams.params.begin()), HandlerSetStoreCommandParamsHandlerType>(
            params,
            params.commandParams.params.begin() + 1,
            params.commandParams.params.end(),
            &SetCommandHandler::handlerSInterPrivate
        );
    }

    /*
     * 返回所有给定集合的并集
     * sunion key1 [key2 ...]
     * Redis sunion 命令返回给定集合的并集。不存在的集合 key 被视为空集。
     */
    void handlerSUnion (ParamsType &params) {
        auto ret = getUnionHandlerAndBeginIterator(params, true);
        auto commandHandler = std::get <0>(ret);
        auto begin = std::get <1>(ret);
        auto *handler = commandHandler->handlerSUnionPrivate(
            params,
            begin,
            params.commandParams.params.end());

        delete handler;
    }

    /*
     * 所有给定集合的并集存储在 destination 集合中.
     * sunionstore destination key1 [key2 ...]
     * Redis sunionstore 命令将给定集合的并集存储在指定的集合 destination 中。如果 destination 已经存在，则将其覆盖。
     */
    void handlerSUnionStore (ParamsType &params) {
        auto ret = getUnionHandlerAndBeginIterator(params, false);
        auto commandHandler = std::get <0>(ret);
        auto begin = std::get <1>(ret);
        commandHandler->handlerSetStoreCommon <decltype(params.commandParams.params)::iterator,
                                               HandlerSetStoreCommandParamsHandlerType>(
            params,
            begin,
            params.commandParams.params.end(),
            &SetCommandHandler::handlerSUnionPrivate
        );
    }

    /*
     * 代办
     * SSCAN 命令用于遍历集合中键的元素.
     * SSCAN key cursor [MATCH pattern] [COUNT count]
     * cursor - 游标。
     * pattern - 匹配的模式。
     * count - 指定从数据集里返回多少元素，默认值为 10 。
     */
    // void handlerSScan (ParamsType &params)
    // {
    //
    // }

private:
    template <class Iterator>
    size_t handlerSAddPrivate (size_t addSize, size_t addCount, Iterator begin, const Iterator &end) {
        static ArrayType <int64_t> resVec;
        resVec.clear();

        keyValues.checkStructureConvert(addSize, addCount, begin, end, resVec);

        size_t count = 0;
        if (keyValues.dataStructureType == DataStructureType::INT_SET)
            std::for_each(
                resVec.begin(), resVec.end(), [this, &count] (const int64_t &v) {
                    auto it = keyValues.data.intSetKeyValues->insert(v);
                    if (it != keyValues.data.intSetKeyValues->end()) count++;
                }
            );
        else if (keyValues.dataStructureType == DataStructureType::LIST_PACK)
            std::for_each(
                begin,
                end,
                [this, &count] (const StringType &v) {
                    auto it = keyValues.data.listPackKeyValues->find(v);
                    if (it == keyValues.data.listPackKeyValues->end()) {
                        keyValues.data.listPackKeyValues->pushBack(v);
                        count++;
                    }
                }
            );
        else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE)
            std::for_each(
                begin,
                end,
                [this, &count] (const StringType &v) {
                    auto ret = keyValues.data.hashKeyValues->emplace(v);
                    if (ret.second)
                        count++;
                }
            );
        else {
            VALUE_DATA_STRUCTURE_ERROR;
        }

        return count;
    }

    void handlerSRandMemberSingle (ParamsType &params) {
        size_t idx = Utils::Random::getRandomNum(static_cast<size_t>(0), keyValues.size() - 1);

        if (keyValues.dataStructureType == DataStructureType::INT_SET) {
            auto it = keyValues.data.intSetKeyValues->begin();
            while (idx--) ++it;
            params.resValue.setStringValue(Utils::StringHelper::toString(*it));
        } else if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            auto it = keyValues.data.listPackKeyValues->begin();
            while (idx--) ++it;
            params.resValue.setStringValue(it->toString());
        } else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE) {
            auto it = keyValues.data.hashKeyValues->begin();
            while (idx--) ++it;
            params.resValue.setStringValue(*it);
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }
    }

    void handlerSRandMemberMulti (ParamsType &params, bool negative, size_t count) {
        size_t resSize;
        // 如果是非负数，需要检查是否大于当前结构长度
        if (!negative) {
            size_t size = keyValues.size();
            resSize = count > size ? size : count;
            handlerSRandMemberMultiPositive(params, resSize);
        } else {
            resSize = count;
            handlerSRandMemberMultiNegative(params, resSize);
        }
    }

    // 如果是负数，允许重复数据
    void handlerSRandMemberMultiNegative (ParamsType &params, size_t count) {
        params.resValue.setEmptyVectorValue();
        params.resValue.elements.resize(count);
        ArrayType <size_t> index(count);
        size_t size = keyValues.size();
        std::for_each(
            index.begin(), index.end(), [&size] (size_t &v) {
                v = Utils::Random::getRandomNum(static_cast<size_t>(0), size - 1);
            }
        );

        std::sort(index.begin(), index.end());
        size_t cursor = 0;
        auto cursorIt = index.begin();
        auto resIt = params.resValue.elements.begin();
        StringType s;
        if (keyValues.dataStructureType == DataStructureType::INT_SET) {
            for (const int64_t &v: *keyValues.data.intSetKeyValues) {
                if (cursor == *cursorIt) {
                    s = Utils::StringHelper::toString(v);
                    do {
                        (resIt++)->setStringValue(s);
                    } while (*++cursorIt == cursor);
                }
                cursor++;
            }
        } else if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            for (const KvListPack::IteratorDataType &v: *keyValues.data.listPackKeyValues) {
                if (cursor == *cursorIt) {
                    s = v.toString();
                    do {
                        (resIt++)->setStringValue(s);
                    } while (*++cursorIt == cursor);
                }
                cursor++;
            }
        } else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE) {
            for (const StringType &v: *keyValues.data.hashKeyValues) {
                while (*cursorIt == cursor) {
                    (resIt++)->setStringValue(v);
                    ++cursorIt;
                }
                cursor++;
            }
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }
    }

    void handlerSRandMemberMultiPositive (ParamsType &params, size_t count) {
        Utils::Random::Sample <StringType> sample {};
        params.resValue.setEmptyVectorValue();
        params.resValue.elements.resize(count);

        size_t size = keyValues.size();
        if (keyValues.dataStructureType == DataStructureType::INT_SET) {
            sample(
                keyValues.data.intSetKeyValues->begin(),
                keyValues.data.intSetKeyValues->end(),
                params.resValue.elements.begin(),
                size,
                count,
                [] (typename decltype(params.resValue.elements)::iterator it, const int64_t &val) {
                    it->setStringValue(Utils::StringHelper::toString(val));
                }
            );
        } else if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            sample(
                keyValues.data.listPackKeyValues->begin(),
                keyValues.data.listPackKeyValues->end(),
                params.resValue.elements.begin(),
                size,
                count,
                [] (typename decltype(params.resValue.elements)::iterator it, const KvListPack::IteratorDataType &val) {
                    it->setStringValue(val.toString());
                }
            );
        } else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE) {
            sample(
                keyValues.data.hashKeyValues->begin(),
                keyValues.data.hashKeyValues->end(),
                params.resValue.elements.begin(),
                size,
                count,
                [] (typename decltype(params.resValue.elements)::iterator it, const HashType::HashNodeType &val) {
                    it->setStringValue(val);
                }
            );
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }
    }

    void handlerSPopSingle (ParamsType &params) {
        size_t idx = Utils::Random::getRandomNum(static_cast<size_t>(0), keyValues.size() - 1);

        if (keyValues.dataStructureType == DataStructureType::INT_SET) {
            auto it = keyValues.data.intSetKeyValues->begin();
            while (idx--) ++it;
            params.resValue.setStringValue(Utils::StringHelper::toString(*it));
            keyValues.data.intSetKeyValues->erase(it);
        } else if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            auto it = keyValues.data.listPackKeyValues->begin();
            while (idx--) ++it;
            params.resValue.setStringValue(it->toString());
            keyValues.data.listPackKeyValues->erase(it);
        } else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE) {
            auto it = keyValues.data.hashKeyValues->begin();
            while (idx--) ++it;
            params.resValue.setStringValue(*it);
            keyValues.data.hashKeyValues->erase(it);
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }
    }

    void handlerSPopAll (ParamsType &params) {
        if (keyValues.dataStructureType == DataStructureType::INT_SET) {
            for (const int64_t &val: *keyValues.data.intSetKeyValues)
                params.resValue.setVectorValue(Utils::StringHelper::toString(val));
        } else if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            for (const KvListPack::IteratorDataType &val: *keyValues.data.listPackKeyValues)
                params.resValue.setVectorValue(val.toString());
        } else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE) {
            for (const StringType &val: *keyValues.data.hashKeyValues)
                params.resValue.setVectorValue(val);
        }

        delKeyValue(params.commandParams.key);
    }

    void handlerSPopMulti (ParamsType &params, size_t count) {
        params.resValue.elements.resize(count);
        Utils::Random::Sample <size_t> sample {};
        const size_t size = keyValues.size();
        auto index = sample(0, size - 1, count);
        std::sort(index.begin(), index.end());

        auto cursorIdx = index.begin();
        size_t cursor = 0;
        auto outIt = params.resValue.elements.begin();
        if (keyValues.dataStructureType == DataStructureType::INT_SET) {
            auto it = keyValues.data.intSetKeyValues->begin();
            while (cursor != size) {
                if (cursor == *cursorIdx) {
                    outIt++->setStringValue(Utils::StringHelper::toString(*it));
                    it = keyValues.data.intSetKeyValues->erase(it);
                    ++cursorIdx;
                } else ++it;
                ++cursor;
            }
        } else if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            auto it = keyValues.data.listPackKeyValues->begin();
            while (cursor != size) {
                if (cursor == *cursorIdx) {
                    outIt++->setStringValue(it->toString());
                    it = keyValues.data.listPackKeyValues->erase(it);
                    ++cursorIdx;
                } else ++it;
                ++cursor;
            }
        } else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE) {
            auto it = keyValues.data.hashKeyValues->begin();
            while (cursor != size) {
                if (cursor == *cursorIdx) {
                    outIt++->setStringValue(*it);
                    it = keyValues.data.hashKeyValues->erase(it);
                    ++cursorIdx;
                } else ++it;
                ++cursor;
            }
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }
    }

    template <class Iterator, class Fn>
    void handlerSetStoreCommon (ParamsType &params, Iterator begin, const Iterator &end, Fn &&fn) {
        auto it = server->keyMap.find(params.commandParams.params[0]);
        if (!server->checkKeyType(params.commandParams, it, StructType::SET, params.resValue)) return;
        auto *handler = (static_cast<SetCommandHandler *>(it->second.handler)->*fn)( // NOLINT
            params,
            begin,
            end
        );
        if (!handler) return;

        it = server->keyMap.find(params.commandParams.key);
        if (it == server->keyMap.end()) {
            KeyOfValue keyOfValue(StructType::SET, handler);
            server->keyMap.emplace(params.commandParams.key, std::move(keyOfValue));
        } else {
            delete it->second.handler;
            it->second.handler = handler;
        }
    }

    template <class Iterator>
    SetCommandHandler *handlerSDiffPrivate (ParamsType &params, Iterator begin, const Iterator &end) {
        auto *newSetHandler = new SetCommandHandler(*this);
        if (keyValues.dataStructureType == DataStructureType::INT_SET)
            newSetHandler->handlerSDiffIntSet(params, begin, end);
        else if (keyValues.dataStructureType == DataStructureType::LIST_PACK)
            newSetHandler->handlerSDiffListPack(params, begin, end);
        else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE)
            newSetHandler->handlerSDiffHash(params, begin, end);
        else {
            VALUE_DATA_STRUCTURE_ERROR;
        }

        return newSetHandler;
    }

    template <class Iterator>
    void handlerSDiffIntSet (ParamsType &params, Iterator begin, const Iterator &end) {
        for (; begin != end; ++begin) {
            auto ret = findSetCommand(params, *begin);
            if (!ret.first) continue; // 没找到
            if (!ret.second) return; // 找到了，但是结构不是SET
            auto compareSet = ret.second;
            if (compareSet->keyValues.dataStructureType == DataStructureType::INT_SET) {
                for (auto it = keyValues.data.intSetKeyValues->begin();
                     it != keyValues.data.intSetKeyValues->end();) {
                    if (compareSet->keyValues.data.intSetKeyValues->find(*it)
                        != compareSet->keyValues.data.intSetKeyValues->end())
                        it = keyValues.data.intSetKeyValues->erase(it);
                    else ++it;
                }
            } else if (compareSet->keyValues.dataStructureType == DataStructureType::LIST_PACK) {
                for (auto it = keyValues.data.intSetKeyValues->begin();
                     it != keyValues.data.intSetKeyValues->end();) {
                    if (compareSet->keyValues.data.listPackKeyValues->find(*it)
                        != compareSet->keyValues.data.listPackKeyValues->end())
                        it = keyValues.data.intSetKeyValues->erase(it);
                    else ++it;
                }
            } else if (compareSet->keyValues.dataStructureType == DataStructureType::HASH_TABLE) {
                for (auto it = keyValues.data.intSetKeyValues->begin();
                     it != keyValues.data.intSetKeyValues->end();) {
                    if (compareSet->keyValues.data.hashKeyValues->find(Utils::StringHelper::toString(*it))
                        != compareSet->keyValues.data.hashKeyValues->end())
                        it = keyValues.data.intSetKeyValues->erase(it);
                    else ++it;
                }
            } else {
                VALUE_DATA_STRUCTURE_ERROR;
            }
        }

        fillResValue <DataStructureType::INT_SET>(params.resValue);
    }

    template <class Iterator>
    void handlerSDiffListPack (ParamsType &params, Iterator begin, const Iterator &end) {
        for (; begin != end; ++begin) {
            auto ret = findSetCommand(params, *begin);
            if (!ret.first) continue; // 没找到
            if (!ret.second) return; // 找到了，但是结构不是SET
            auto compareSet = ret.second;
            if (compareSet->keyValues.dataStructureType == DataStructureType::INT_SET) {
                for (auto it = keyValues.data.listPackKeyValues->begin();
                     it != keyValues.data.listPackKeyValues->end();) {
                    // 字符串类型不需要检查
                    if (it->mode == DataTypeEnum::INTEGER
                        && compareSet->keyValues.data.intSetKeyValues->find(it->data.val)
                            != compareSet->keyValues.data.intSetKeyValues->end())
                        it = keyValues.data.listPackKeyValues->erase(it);
                    else ++it;
                }
            } else if (compareSet->keyValues.dataStructureType == DataStructureType::LIST_PACK) {
                for (auto it = keyValues.data.listPackKeyValues->begin();
                     it != keyValues.data.listPackKeyValues->end();) {
                    if ((it->mode == DataTypeEnum::INTEGER
                        && compareSet->keyValues.data.listPackKeyValues->find(it->data.val)
                            != compareSet->keyValues.data.listPackKeyValues->end()) || (it->mode == DataTypeEnum::STRING
                        && compareSet->keyValues.data.listPackKeyValues->find({ it->data.str.s, it->data.str.len })
                            != compareSet->keyValues.data.listPackKeyValues->end()))
                        it = keyValues.data.listPackKeyValues->erase(it);
                    else ++it;
                }
            } else if (compareSet->keyValues.dataStructureType == DataStructureType::HASH_TABLE) {
                for (auto it = keyValues.data.listPackKeyValues->begin();
                     it != keyValues.data.listPackKeyValues->end();) {
                    if (compareSet->keyValues.data.hashKeyValues->find(it->toString())
                        != compareSet->keyValues.data.hashKeyValues->end())
                        it = keyValues.data.listPackKeyValues->erase(it);
                    else ++it;
                }
            } else {
                VALUE_DATA_STRUCTURE_ERROR;
            }
        }

        fillResValue <DataStructureType::LIST_PACK>(params.resValue);
    }

    template <class Iterator>
    void handlerSDiffHash (ParamsType &params, Iterator begin, const Iterator &end) {
        for (; begin != end; ++begin) {
            auto ret = findSetCommand(params, *begin);
            if (!ret.first) continue; // 没找到
            if (!ret.second) return; // 找到了，但是结构不是SET
            auto compareSet = ret.second;
            if (compareSet->keyValues.dataStructureType == DataStructureType::INT_SET) {
                for (auto it = keyValues.data.hashKeyValues->begin();
                     it != keyValues.data.hashKeyValues->end();) {
                    if (compareSet->keyValues.data.intSetKeyValues->find(*it)
                        != compareSet->keyValues.data.intSetKeyValues->end())
                        it = keyValues.data.hashKeyValues->erase(it);
                    else ++it;
                }
            } else if (compareSet->keyValues.dataStructureType == DataStructureType::LIST_PACK) {
                for (auto it = keyValues.data.hashKeyValues->begin();
                     it != keyValues.data.hashKeyValues->end();) {
                    if (compareSet->keyValues.data.listPackKeyValues->find(*it)
                        != compareSet->keyValues.data.listPackKeyValues->end())
                        it = keyValues.data.hashKeyValues->erase(it);
                    else ++it;
                }
            } else if (compareSet->keyValues.dataStructureType == DataStructureType::HASH_TABLE) {
                for (auto it = keyValues.data.hashKeyValues->begin();
                     it != keyValues.data.hashKeyValues->end();) {
                    if (compareSet->keyValues.data.hashKeyValues->find(*it)
                        != compareSet->keyValues.data.hashKeyValues->end())
                        it = keyValues.data.hashKeyValues->erase(it);
                    else ++it;
                }
            } else {
                VALUE_DATA_STRUCTURE_ERROR;
            }
        }

        fillResValue <DataStructureType::HASH_TABLE>(params.resValue);
    }

    template <class Iterator>
    SetCommandHandler *handlerSInterPrivate (ParamsType &params, Iterator begin, const Iterator &end) {
        auto *newSetHandler = new SetCommandHandler(server);
        SetCommandHandler *compareSet = nullptr;
        decltype(findSetCommand(params, *begin)) ret;
        while (!compareSet) {
            ret = findSetCommand(params, *begin++);
            if (!ret.first) continue;
            if (!ret.second) {
                delete newSetHandler;
                return nullptr;
            }
            compareSet = ret.second;
        }
        ArrayType <DataUnion <>> values;
        DataUnion <> temp;

        // 先求出第一个set和第二个set的交集
        if (keyValues.dataStructureType == DataStructureType::INT_SET) {
            for (const int64_t &v: *keyValues.data.intSetKeyValues) {
                if (compareSet->handlerSIsMemberPrivate(v)) {
                    temp.setIntegerValue(v); // NOLINT
                    values.emplace_back(std::move(temp));
                }
            }
        } else if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            for (const KvListPack::IteratorDataType &v: *keyValues.data.listPackKeyValues) {
                if (v.mode == DataTypeEnum::INTEGER && compareSet->handlerSIsMemberPrivate(v.data.val)) {
                    temp.setIntegerValue(v.data.val); // NOLINT
                    values.emplace_back(std::move(temp));
                } else if (v.mode == DataTypeEnum::STRING
                    && compareSet->handlerSIsMemberPrivate(StringType(v.data.str.s, v.data.str.len))) {
                    temp.setStringValue(v.data.str.s, v.data.str.len); // NOLINT
                    values.emplace_back(std::move(temp));
                }
            }
        } else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE) {
            for (const StringType &v: *keyValues.data.hashKeyValues) {
                if (compareSet->handlerSIsMemberPrivate(v)) {
                    temp.setStringValue(v); // NOLINT
                    values.emplace_back(std::move(temp));
                }
            }
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }

        // 再求出之前求得的交集，和之后集合的交集
        for (; begin != end;) {
            ret = findSetCommand(params, *begin);
            if (!ret.first) continue; // 没找到
            if (!ret.second) return nullptr; // 找到了，但是结构不是SET
            compareSet = ret.second;

            if (compareSet->keyValues.dataStructureType == DataStructureType::INT_SET) {
                for (auto it = values.begin(); it != values.end();) {
                    if (it->mode == DataTypeEnum::INTEGER
                        && compareSet->keyValues.data.intSetKeyValues->find(it->data.val)
                            == compareSet->keyValues.data.intSetKeyValues->end())
                        it = values.erase(it);
                    else ++it;
                }
            } else if (compareSet->keyValues.dataStructureType == DataStructureType::LIST_PACK) {
                for (auto it = values.begin(); it != values.end();) {
                    if ((it->mode == DataTypeEnum::INTEGER
                        && compareSet->keyValues.data.listPackKeyValues->find(it->data.val)
                            != compareSet->keyValues.data.listPackKeyValues->end()) ||
                        (it->mode == DataTypeEnum::STRING
                            && compareSet->keyValues.data.listPackKeyValues->find(it->toString())
                                != compareSet->keyValues.data.listPackKeyValues->end()))
                        ++it;
                    else it = values.erase(it);
                }
            } else if (compareSet->keyValues.dataStructureType == DataStructureType::HASH_TABLE) {
                for (auto it = values.begin(); it != values.end();) {
                    if (compareSet->keyValues.data.hashKeyValues->find(it->toString())
                        == compareSet->keyValues.data.hashKeyValues->end())
                        it = values.erase(it);
                    else ++it;
                }
            } else {
                VALUE_DATA_STRUCTURE_ERROR;
            }

        }

        size_t maxSize = 0;
        std::for_each(
            values.begin(), values.end(), [&maxSize] (const DataUnion <> &s) {
                if (s.size() > maxSize) maxSize = s.size();
            }
        );

        newSetHandler->handlerSAddPrivate(maxSize, values.size(), values.begin(), values.end());
        newSetHandler->fillResValue(params.resValue);

        return newSetHandler;
    }

    template <class Iterator>
    SetCommandHandler *handlerSUnionPrivate (ParamsType &params, Iterator begin, const Iterator &end) {
        auto *newSetHandler = new SetCommandHandler(*this);
        size_t maxSize;
        size_t addCount;
        for (; begin != end; ++begin) {
            auto ret = findSetCommand(params, *begin);
            if (!ret.first) continue;
            if (!ret.second) {
                delete newSetHandler;
                return nullptr;
            }
            auto compareSet = ret.second;

            if (compareSet->keyValues.dataStructureType == DataStructureType::INT_SET) {
                ArrayType <int64_t> s;
                if (newSetHandler->keyValues.dataStructureType == DataStructureType::INT_SET) {
                    newSetHandler->keyValues.data.intSetKeyValues->merge(*compareSet->keyValues.data.intSetKeyValues);
                    newSetHandler->keyValues.checkStructureConvert(0, 0, end, end, s);
                } else if (newSetHandler->keyValues.dataStructureType == DataStructureType::LIST_PACK) {
                    for (const int64_t &v: *compareSet->keyValues.data.intSetKeyValues)
                        newSetHandler->keyValues.data.listPackKeyValues->pushBack(v);
                    newSetHandler->keyValues.checkStructureConvert(0, 0, end, end, s);
                } else if (newSetHandler->keyValues.dataStructureType == DataStructureType::HASH_TABLE) {
                    for (const int64_t &v: *compareSet->keyValues.data.intSetKeyValues)
                        newSetHandler->keyValues.data.hashKeyValues->emplace(Utils::StringHelper::toString(v));
                } else {
                    VALUE_DATA_STRUCTURE_ERROR;
                }
            } else if (compareSet->keyValues.dataStructureType == DataStructureType::LIST_PACK) {
                std::for_each(
                    compareSet->keyValues.data.listPackKeyValues->begin(),
                    compareSet->keyValues.data.listPackKeyValues->end(),
                    [&maxSize] (const DataUnion <> &s) {
                        if (s.size() > maxSize) maxSize = s.size();
                    }
                );
                addCount = compareSet->keyValues.data.listPackKeyValues->size();
                newSetHandler->handlerSAddPrivate(
                    maxSize,
                    addCount,
                    compareSet->keyValues.data.listPackKeyValues->begin(),
                    compareSet->keyValues.data.listPackKeyValues->end());
            } else if (compareSet->keyValues.dataStructureType == DataStructureType::HASH_TABLE) {
                // 仅用于数据结构转换
                // hash类型不会用到这两个参数
                maxSize = 0;
                addCount = 456;
                newSetHandler->handlerSAddPrivate(
                    maxSize,
                    addCount,
                    compareSet->keyValues.data.hashKeyValues->begin(),
                    compareSet->keyValues.data.hashKeyValues->end());
            } else {
                VALUE_DATA_STRUCTURE_ERROR;
            }
        }

        newSetHandler->fillResValue(params.resValue);
        return newSetHandler;
    }

    inline bool handlerSIsMemberPrivate (const StringType &val) const {
        bool find;
        if (keyValues.dataStructureType == DataStructureType::INT_SET)
            find = keyValues.data.intSetKeyValues->find(val)
                != keyValues.data.intSetKeyValues->end();
        else if (keyValues.dataStructureType == DataStructureType::LIST_PACK)
            find = keyValues.data.listPackKeyValues->find(val)
                != keyValues.data.listPackKeyValues->end();
        else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE)
            find = keyValues.data.hashKeyValues->find(val)
                != keyValues.data.hashKeyValues->end();
        else {
            VALUE_DATA_STRUCTURE_ERROR;
        }

        return find;
    }

    template <class T,
              class = typename std::enable_if <std::is_integral <typename std::remove_reference <T>::type>::value,
                                               void>::type>
    inline bool handlerSIsMemberPrivate (T &&val) const {
        bool find;
        if (keyValues.dataStructureType == DataStructureType::INT_SET)
            find = keyValues.data.intSetKeyValues->find(std::forward <T>(val))
                != keyValues.data.intSetKeyValues->end();
        else if (keyValues.dataStructureType == DataStructureType::LIST_PACK)
            find = keyValues.data.listPackKeyValues->find(std::forward <T>(val))
                != keyValues.data.listPackKeyValues->end();
        else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE)
            find = keyValues.data.hashKeyValues->find(Utils::StringHelper::toString(std::forward <T>(val)))
                != keyValues.data.hashKeyValues->end();
        else {
            VALUE_DATA_STRUCTURE_ERROR;
        }

        return find;
    }

    // 是否找到，如果第一个参数返回true则找到，如果找到的结构不是SET，则填充resValue，返回空指针
    inline std::pair <bool, SetCommandHandler *> findSetCommand (ParamsType &params, const StringType &key) const {
        auto setHandler = server->keyMap.find(key);
        // 如果没有找到，或者找到的handler和this相等
        if (setHandler == server->keyMap.end() || setHandler->second.handler == this) return { false, nullptr };

        if (setHandler->second.structType != StructType::SET) {
            params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::WRONGTYPE);
            return { true, nullptr };
        }
        return { true, static_cast<SetCommandHandler *>(setHandler->second.handler) }; // NOLINT
    }

    template <DataStructureType = DataStructureType::NIL>
    inline void fillResValue (ResValueType &resValue) const noexcept {
        if (keyValues.dataStructureType == DataStructureType::INT_SET) {
            fillResValue <DataStructureType::INT_SET>(resValue);
        } else if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            fillResValue <DataStructureType::LIST_PACK>(resValue);
        } else if (keyValues.dataStructureType == DataStructureType::HASH_TABLE) {
            fillResValue <DataStructureType::HASH_TABLE>(resValue);
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }
    }

    std::tuple <SetCommandHandler *, decltype(CommandParams::params)::iterator>
    getUnionHandlerAndBeginIterator (ParamsType &params, bool needCheckNewKey) {
        auto begin = params.commandParams.params.begin();
        auto end = params.commandParams.params.end();
        auto commandHandler = this;
        // 此处为了处理key不存在的情况
        if (needCheckNewKey && params.arg->isNewKey) {
            for (; begin != end; ++begin) {
                auto it = server->keyMap.find(*begin);
                if (it != server->keyMap.end()) {
                    commandHandler = static_cast<SetCommandHandler *>(it->second.handler); // NOLINT
                    ++begin;
                }
            }
            commandHandler = nullptr;
        }

        return { commandHandler, begin };
    }

private:
    SetDataStructure keyValues {};

    static constexpr int S_RAND_FIELD_RANDOM_SAMPLE_LIMIT = 1000;
};

template <>
inline void SetCommandHandler::fillResValue <DataStructureType::INT_SET> (ResValueType &resValue) const noexcept {
    for (const int64_t &v: *keyValues.data.intSetKeyValues)
        resValue.setVectorValue(Utils::StringHelper::toString(v));
}

template <>
inline void SetCommandHandler::fillResValue <DataStructureType::LIST_PACK> (ResValueType &resValue) const noexcept {
    for (const KvListPack::IteratorDataType &v: *keyValues.data.listPackKeyValues)
        resValue.setVectorValue(v.toString());
}

template <>
inline void SetCommandHandler::fillResValue <DataStructureType::HASH_TABLE> (ResValueType &resValue) const noexcept {
    for (const StringType &v: *keyValues.data.hashKeyValues)
        resValue.setVectorValue(v);
}
#endif //KV_STORE_KV_STORE_SERVER_COMMAND_STRUCTS_KV_SET_COMMAND_H_
