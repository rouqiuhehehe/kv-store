//
// Created by 115282 on 2024/1/23.
//

#ifndef KV_STORE_KV_STORE_SERVER_COMMAND_STRUCTS_KV_ZSET_COMMAND_EXE_
#define KV_STORE_KV_STORE_SERVER_COMMAND_STRUCTS_KV_ZSET_COMMAND_EXE_
#include <utility>
class ZSetCommandHandler final : public CommandCommonWithServerPtr
{
    friend class KvDebug;
    struct FindCommandReturnType
    {
        bool found = false;
        StructType structType = StructType::NIL;
        union
        {
            SetCommandHandler *setCommandHandler {};
            ZSetCommandHandler *zSetCommandHandler;
        } handler {};
    };
    struct ZSetData
    {
        FloatType score {};
        StringType member;

        ZSetData () = default;
        ZSetData (FloatType score, StringType member)
            : score(score), member(std::move(member)) {}
        ZSetData (const ZSetData &) = default;
        ZSetData &operator= (const ZSetData &) = default;

        ZSetData (ZSetData &&r) noexcept {
            operator=(std::move(r));
        }

        ZSetData &operator= (ZSetData &&r) noexcept {
            if (this == &r) return *this;
            score = r.score;
            member = std::move(r.member);

            r.score = 0;
            return *this;
        }

        inline bool operator< (const ZSetData &r) const noexcept {
            if (score == r.score) return member < r.member;
            return score < r.score;
        }

        inline bool operator== (const ZSetData &r) const noexcept {
            return score == r.score && member == r.member;
        }

        inline bool operator!= (const ZSetData &r) const noexcept {
            return !(*this == r);
        }

        inline bool operator> (const ZSetData &r) const noexcept {
            if (score == r.score) return member > r.member;
            return score > r.score;
        }

        inline bool operator< (FloatType r) const noexcept {
            return score < r;
        }
        inline bool operator== (FloatType r) const noexcept {
            return score == r;
        }
        inline bool operator> (FloatType r) const noexcept {
            return score > r;
        }
    };
    struct ZSetDataWithStringScore : public ZSetData
    {
        const char *stringScore {};
        size_t len {};

        ZSetDataWithStringScore () = default;
        ZSetDataWithStringScore (const ZSetDataWithStringScore &) = default;
        ZSetDataWithStringScore &operator= (const ZSetDataWithStringScore &) = default;

        ZSetDataWithStringScore (ZSetDataWithStringScore &&r) noexcept {
            operator=(std::move(r));
        }

        ZSetDataWithStringScore &operator= (ZSetDataWithStringScore &&r) noexcept {
            if (this == &r) return *this;
            stringScore = r.stringScore;
            len = r.len;
            ZSetData::operator=(std::move(r));

            return *this;
        }
    };
    using KvSkipList = SkipList <ZSetData, 32>;
    class ZSetDataStructure
    {
    public:
        union DataType
        {
            ~DataType () = default;
            KvListPack *listPackKeyValues = nullptr;
            KvSkipList *skipListKeyValue;
        } data {};
        DataStructureType dataStructureType {};

        ZSetDataStructure () {
            data.listPackKeyValues = new KvListPack;
            dataStructureType = DataStructureType::LIST_PACK;
        }
        ZSetDataStructure (const ZSetDataStructure &r) {
            operator=(r);
        };
        ZSetDataStructure (ZSetDataStructure &&r) noexcept {
            operator=(std::move(r));
        };
        ZSetDataStructure &operator= (const ZSetDataStructure &r) {
            if (this == &r) return *this;
            dataStructureType = r.dataStructureType;
            if (dataStructureType == DataStructureType::LIST_PACK) {
                if (data.listPackKeyValues)
                    *data.listPackKeyValues = *r.data.listPackKeyValues;
                else
                    data.listPackKeyValues = new KvListPack(*r.data.listPackKeyValues);
            } else if (dataStructureType == DataStructureType::SKIP_LIST) {
                if (data.skipListKeyValue)
                    *data.skipListKeyValue = *r.data.skipListKeyValue;
                else
                    data.skipListKeyValue = new KvSkipList(*r.data.skipListKeyValue);
            } else {
                VALUE_DATA_STRUCTURE_ERROR;
            }

            return *this;
        }
        ZSetDataStructure &operator= (ZSetDataStructure &&r) noexcept {
            if (this == &r) return *this;
            dataStructureType = r.dataStructureType;
            if (dataStructureType == DataStructureType::LIST_PACK) {
                if (data.listPackKeyValues)
                    *data.listPackKeyValues = std::move(*r.data.listPackKeyValues);
                else
                    data.listPackKeyValues = new KvListPack(std::move(*r.data.listPackKeyValues));
            } else if (dataStructureType == DataStructureType::SKIP_LIST) {
                if (data.skipListKeyValue)
                    *data.skipListKeyValue = std::move(*r.data.skipListKeyValue);
                else
                    data.skipListKeyValue = new KvSkipList(std::move(*r.data.skipListKeyValue));
            } else {
                VALUE_DATA_STRUCTURE_ERROR;
            }

            return *this;
        }
        ~ZSetDataStructure () noexcept {
            if (dataStructureType == DataStructureType::LIST_PACK) {
                delete data.listPackKeyValues;
                data.listPackKeyValues = nullptr;
            } else if (dataStructureType == DataStructureType::SKIP_LIST) {
                delete data.skipListKeyValue;
                data.skipListKeyValue = nullptr;
            } else {
                VALUE_DATA_STRUCTURE_ERROR;
            }
        }
        void checkStructureConvert (size_t maxSize, size_t addCount) {
            if (dataStructureType == DataStructureType::LIST_PACK) {
                // 储存 score - member
                size_t count = data.listPackKeyValues->size() / 2 + addCount;
                if (checkListPackSizeOverflow(maxSize) || checkListPackEntriesCountOverflow(count))
                    listPackStructureConvertSkipList();
            }
        }
        inline size_t size () const {
            switch (dataStructureType) {
                case DataStructureType::LIST_PACK:
                    return data.listPackKeyValues->size();
                case DataStructureType::SKIP_LIST:
                    return data.skipListKeyValue->size();
                default:
                VALUE_DATA_STRUCTURE_ERROR;
            }
        }
        bool empty () const {
            return size() == 0;
        }
        // 检测entry字节是否超出
        static inline bool checkListPackSizeOverflow (size_t size) noexcept {
            return size > KvConfig::getConfig().zsetMaxListPackValue;
        }
        // 检测entry条目数是否超出
        static inline bool checkListPackEntriesCountOverflow (size_t count) noexcept {
            return count > KvConfig::getConfig().zsetMaxListPackEntries;
        }

    private:
        void listPackStructureConvertSkipList () {
            auto *skipList = new KvSkipList;
            ZSetData zSetData;
            for (auto it = data.listPackKeyValues->begin(); it != data.listPackKeyValues->end(); ++it) {
                if (it->mode == DataTypeEnum::INTEGER) zSetData.score = static_cast<double>(it->data.val); // NOLINT
                else Utils::StringHelper::stringIsDouble(it->toString(), &zSetData.score);
                ++it;
                zSetData.member = it->toString();

                skipList->insert(std::move(zSetData));
            }

            delete data.listPackKeyValues;
            data.skipListKeyValue = skipList;
            dataStructureType = DataStructureType::SKIP_LIST;
        }
    };

    ZSetDataStructure keyValues {};
public:
    using CommandCommonWithServerPtr::CommandCommonWithServerPtr;
    inline bool handlerBefore (ParamsType &params) override {
        return true;
    }

    /*
     * 将一个或多个 member 元素及其 score 值加入到有序集 key 当中
     * ZADD key [NX|XX] [GT | LT] [CH] [INCR] score member [score member …]
     * XX： 仅更新已存在的元素。不要添加新元素。 NX：仅添加新元素。不要更新已存在的元素。
     * LT：仅当新分数小于当前分数时才更新现有元素。此标志不会阻止添加新元素。 GT：仅当新分数大于当前分数时才更新现有元素。此标志不会阻止添加新元素。
     * CH：将返回值从添加的新元素数量修改为更改的元素总数（CH是changed的缩写）。更改的元素是添加的新元素和已经存在的元素，更新了它们的分数。
     * INCR：当指定此选项时，ZADD的行为类似于ZINCRBY。在此模式中只能指定一个分数元素对。
     * 返回值：整数:被成功添加的新成员的数量，不包括那些被更新分数的、已经存在的成员。
     * 如果使用 INCR 选项，则返回 多行字符串:以字符串形式表示的 member 的 score 值(双精度浮点数)。(双精度浮点数) , 执行失败返回 nil (当使用 XX 或 NX 选项)。
     */
    void handlerZAdd (ParamsType &params) {
        ZSetParamsCheck::ZAddExtendArgType *zAddExtendArg = GET_ARG_VALUE(ZSetParamsCheck::ZAddExtendArgType *);
        auto begin = zAddExtendArg->begin;
        auto end = params.commandParams.params.end();
        // 检查数据结构转换，假设所有元素都可添加
        size_t maxSize = 0;
        size_t addCount = std::distance(begin, end) / 2;
        auto *zSetData = new ZSetDataWithStringScore[addCount];
        size_t cursor = 0;
        for (; begin != end; ++begin) {
            // score
            if (!Utils::StringHelper::stringIsDouble(*begin, &zSetData[cursor].score)) {
                params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::VALUE_NOT_FLOAT);
                return;
            }
            // 作浅拷贝，用于listPack做插入操作
            zSetData[cursor].stringScore = begin->c_str();
            zSetData[cursor].len = begin->size();
            ++begin;
            // member, 只检查key长度
            if (begin->size() > maxSize) maxSize = begin->size();
            zSetData[cursor].member = *begin;
            cursor++;
        }
        keyValues.checkStructureConvert(maxSize, addCount);

        handlerZAddPrivate(params.resValue, zSetData, addCount, zAddExtendArg);
        delete[] zSetData;
    }

    /*
     * 用于返回有序集的成员个数。
     * ZCARD key
     * 返回值：整数: 返回有序集的成员个数，当 key 不存在时，返回 0。
     */
    void handlerZCard (ParamsType &params) {
        params.resValue.setIntegerValue(keyValues.size());
    }

    /*
     * 用于返回成员的分数。
     * ZSCORE key member
     * 返回值：string: 成员的分数；NIL:key不存在
     */
    void handlerZScore (ParamsType &params) { // NOLINT
        if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            auto it = keyValues.data.listPackKeyValues->begin();
            const auto end = keyValues.data.listPackKeyValues->end();
            auto scoreIt = end;
            for (; it != end; ++it) {
                scoreIt = it;
                ++it;
                if (it->toString() == params.commandParams.params[0]) {
                    params.resValue.setStringValue(scoreIt->toString());
                    return;
                }
            }

            params.resValue.setNilFlag();
        } else if (keyValues.dataStructureType == DataStructureType::SKIP_LIST) {
            auto it = keyValues.data.skipListKeyValue->begin();
            const auto end = keyValues.data.skipListKeyValue->end();
            for (; it != end; ++it) {
                if (it->member == params.commandParams.params[0]) {
                    params.resValue.setStringValue(Utils::StringHelper::toString(it->score));
                    return;
                }
            }

            params.resValue.setNilFlag();
        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }
    }

    /*
     * ZCOUNT 返回有序集 key 中， score 值在 min 和 max 之间(默认包括 score 值等于 min 或 max )的成员的数量
     * ZCOUNT key min max [min, max]
     * ZCOUNT key (min (max (min, max)
     * ZCOUNT key -inf inf
     * 返回值：整数: score 值在 min 和 max 之间的成员的数量。
     */
    void handlerZCount (ParamsType &params) {
        auto *extendArg = GET_ARG_VALUE(ZSetParamsCheck::ZSetParamsCheck::ZRangeExtendArg *);
        handlerZRangeByScorePrivate(params, *extendArg);

        params.resValue.setIntegerValue(params.resValue.elements.size());
    }

    /*
     * ZINCRBY 为有序集 key 的成员 member 的 score 值加上增量 increment 。
     * ZINCRBY key score member
     * 可以通过传递一个负数值 increment ，让 score 减去相应的值
     * 返回值：多行字符串: 以字符串形式表示的 member 成员的新 score 值（双精度浮点数）。
     */
    void handlerZIncrBy (ParamsType &params) {
        ZSetDataWithStringScore data;
        if (!Utils::StringHelper::stringIsDouble(params.commandParams.params[0], &data.score)) {
            params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::VALUE_NOT_FLOAT);
            return;
        }
        data.stringScore = params.commandParams.params[0].c_str();
        data.len = params.commandParams.params[0].size();
        data.member = params.commandParams.params[1];
        ZSetParamsCheck::ZAddExtendArgType extendArg;
        extendArg.flag.setFlag(ZSetParamsCheck::ZSetParamsCheck::ZAddFlag::INCR);

        handlerZAddPrivate(params.resValue, &data, 1, &extendArg);
        params.resValue.setStringValue(Utils::StringHelper::toString(data.score));
    }

    /*
     * 计算 numkeys 个有序集合的交集，并且把结果放到 destination 中
     * ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
     * 返回值：整数:结果集 destination 中元素个数。
     * 详见zinter
     */
    void handlerZInterStore (ParamsType &params) {
    }

    /*
     * 计算 numkeys 个有序集合的交集，并且把结果放到 destination 中
     * ZDIFFSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
     * 返回值：整数:结果集 destination 中元素个数。
     * 详见zinter
     */
    void handlerZDiffStore (ParamsType &params) {
    }

    /*
     * 计算 numkeys 个有序集合的交集，并且把结果放到 destination 中
     * ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
     * 返回值：整数:结果集 destination 中元素个数。
     * 详见zinter
     */
    void handlerZUnionStore (ParamsType &params) {
    }

    /*
     * 命令计算有序集合中指定字典区间内成员数量
     * zlexcount key min max
     * [min 表示返回的结果中包含 min 值
     * [max 表示返回的结果中包含 max 值
     * (min 表示返回的结果中不包含 min 值
     * (max 表示返回的结果中不包含 max 值
     * [MIN, [MAX 可以用-,+ 代替，- 表示得分最小值的成员，+ 表示得分最大值的成员
     * 返回值：整数:被成功添加的新成员的数量，不包括那些被更新分数的、已经存在的成员。
     */
    void handlerZLexCount (ParamsType &params) {
    }

    /*
     * 返回存储在＜key＞中的排序集中指定的元素范围。 [BYSCORE | BYLEX] [REV] [LIMIT offset count] 为6.2.0以后支持
     * ZRANGE key start stop [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
     * 元素的顺序是从得分最低到得分最高。具有相同分数的元素按字典顺序排列
     * 可选的REV参数颠倒了排序，与 zrevrange zrevrangebylex(可选项bylex) zrevrangebyscore(可选项byscore) 一致
     * 可选的LIMIT参数可用于从匹配的元素中获取子范围（类似于SQL中的SELECT LIMIT偏移量，count）
     * 可选的WITHSCORES参数用返回的元素分数来补充命令的回复
     * 索引也可以是负数，表示与排序集末尾的偏移量，-1是排序集的最后一个元素，-2是倒数第二个元素，依此类推。超出范围的索引不会产生错误。如果＜start＞大于排序集的结束索引或＜stop＞，则返回一个空列表。如果＜stop＞大于排序集的结束索引，＜stop＞设置为排序集的结束索引
     * 当使用BYLEX选项时，该命令的行为与ZRANGEBYLEX类似，并返回<start>和<stop>字典式闭范围区间之间的排序集中的元素范围。当一个排序集中的所有元素都以相同的分数插入时，为了强制字典排序，此命令在key处返回排序集中的全部元素，其值介于min和max之间。如果排序集中的元素具有不同的分数，则返回的元素是未指定的。
     * 当提供BYSCORE选项时，该命令的行为与ZRANGEBYSCORE类似，并返回排序集中得分等于或介于＜start＞和＜stop＞之间的元素范围。
     * 返回值：整数:被成功添加的新成员的数量，不包括那些被更新分数的、已经存在的成员。
     */
    void handlerZRange (ParamsType &params) {
        if (keyValues.empty()) {
            params.resValue.setEmptyVectorValue();
            return;
        }
        auto *extendArg = GET_ARG_VALUE(ZSetParamsCheck::ZSetParamsCheck::ZRangeExtendArg *);

#if CHECK_VERSION_GREATER_THEN_OR_EQUAL_BUILD_SUPPORTED_REDIS_VERSION(6, 2, 0)
        if (extendArg->flag.hasFlag(ZSetParamsCheck::ZSetParamsCheck::ZRangeFlag::BY_LEX))
            handlerZRangeByLexPrivate(
                params,
                *extendArg
            );
        else if (extendArg->flag.hasFlag(ZSetParamsCheck::ZRangeFlag::BY_SCORE))
            handlerZRangeByScorePrivate(
                params,
                *extendArg
            );
        else if (extendArg->flag.hasFlag(ZSetParamsCheck::ZRangeFlag::REV))
            handlerZRangeRevDefaultPrivate(
                params,
                *extendArg
            );
        else
#endif
            handlerZRangeDefaultPrivate(params, *extendArg);
    }

    /*
     * 返回存储在关键字处的排序集中成员的排名，分数从低到高排序。排名（或索引）基于0，这意味着得分最低的成员的排名为0
     * ZRANK key member [WITHSCORE]
     * 返回值：整数:成员的排名。数组: 当使用可选项[WITHSCORE]时，返回1.整数:排名，2:score
     */
    void handlerZRank (ParamsType &params) {
    }

    /*
     * 从存储在键处的已排序集合中删除指定的成员。将忽略不存在的成员。
     * ZREM key member [member ...]
     * 返回值：整数:从已排序集合中删除的成员数，不包括不存在的成员。
     */
    void handlerZRem (ParamsType &params) {
    }

    /*
     * 当一个排序集中的所有元素都以相同的分数插入时，为了强制进行字典排序，此命令将删除存储在min和max指定的字典范围之间的关键字处的排序集中的全部元素。
     * ZREMRANGEBYLEX key min max
     * min max 含义同zrangebylex
     * 返回值：整数:从已排序集合中删除的成员数。
     */
    void handlerZRemRangeByLex (ParamsType &params) {
    }

    /*
     * 删除排序集中存储在关键字处的所有元素，该关键字的秩介于开始和停止之间。
     * ZREMRANGEBYRANK key start stop
     * start stop 含义同zrange 没有使用可选项 BYLEX BYSCORE REV 时的start stop
     * 返回值：整数:从已排序集合中删除的成员数。
     */
    void handlerZRemRangeByRank (ParamsType &params) {
    }

    /*
     * 删除存储在关键字处的排序集中得分在最小值和最大值（包括最小值）之间的所有元素。
     * ZREMRANGEBYSCORE key min max
     * min max 含义同zrangebyscore
     * 返回值：整数:从已排序集合中删除的成员数。
     */
    void handlerZRemRangeByScore (ParamsType &params) {
    }

    /*
     * 返回存储在键处的排序集中指定的元素范围。元素被认为是从最高得分到最低得分的顺序。降序字典顺序用于得分相等的元素。
     * 除了顺序相反之外，与zrange相同
     * ZREVRANGE key start stop [WITHSCORES]
     * 返回值：数组:指定分数范围内的元素列表。当使用可选项[WITHSCORES]，返回元素和分数列表
     */
    void handlerZRevRange (ParamsType &params) {
        auto *extendArg = GET_ARG_VALUE(ZSetParamsCheck::ZRangeExtendArg *);
        handlerZRangeRevDefaultPrivate(params, *extendArg);
    }

    /*
     * 当一个排序集中的所有元素都以相同的分数插入时，为了强制字典排序，此命令在key处返回排序集中的全部元素，其值介于max和min之间。
     * 除了顺序相反之外，ZREVRANGEBYLEX与ZRANGEBYLEX相似。
     * ZREVRANGEBYLEX key max min [LIMIT offset count]
     * 返回值：数组:指定分数范围内的元素列表。
     */
    void handlerZRevRangeByLex (ParamsType &params) {
        auto *extendArg = GET_ARG_VALUE(ZSetParamsCheck::ZRangeExtendArg *);
        handlerZRangeByLexPrivate(params, *extendArg);
    }

    /*
     * 返回排序集at key中得分在max和min之间的所有元素（包括得分等于max或min的元素）。与排序集的默认排序相反，对于该命令，元素被认为是从高分到低分排序的。
     * 除了反向排序之外，ZREVRANGEBYSCORE与ZRANGEBYSCORE相似。
     * ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
     * 返回值：数组:指定分数范围内的元素列表。当使用可选项[WITHSCORES]，返回元素和分数列表
     */
    void handlerZRevRangeByScore (ParamsType &params) {
        auto extendArg = GET_ARG_VALUE(ZSetParamsCheck::ZRangeExtendArg *);
        handlerZRangeByScorePrivate(params, *extendArg);
    }

    /*
     * 返回存储在关键字处的排序集中成员的级别，分数从高到低依次排列。排名（或索引）基于0，这意味着得分最高的成员的排名为0。
     * 除了反向排序之外，ZREVRANK与ZRANK相似。
     * ZREVRANK key member [WITHSCORE]
     * 返回值：整数:成员的排名。当使用可选项[WITHSCORES]，返回排名和分数
     */
    void handlerZRevRanK (ParamsType &params) {
    }

    // scan 代办
    // /*
    //  * ZSCAN key cursor [MATCH pattern] [COUNT count]
    //  */
    // void handlerZScan (ParamsType &params) {
    // }

    /*
     * 删除并返回存储在关键字处的排序集中得分最高的成员（最多计数）。
     * ZPOPMAX key [count]
     * 返回值：数组:弹出元素和分数的列表
     */
    void handlerZPopMax (ParamsType &params) {
    }

    /*
     * 删除并返回存储在关键字处的排序集中得分最低的成员（最多计数）。
     * ZPOPMIN key [count]
     * 返回值：数组:弹出元素和分数的列表
     */
    void handlerZPopMin (ParamsType &params) {
    }

    /*
     * 阻塞版本的zpopmax
     * BZPOPMAX key [key ...] timeout
     * 返回值：数组:弹出元素和分数的列表; NIL:超时
     */
    void handlerBZPopMax (ParamsType &params) {
    }

    /*
     * 阻塞版本的zpopmin
     * BZPOPMIN key [key ...] timeout
     * 返回值：数组:弹出元素和分数的列表; NIL:超时
     */
    void handlerBZPopMin (ParamsType &params) {
    }

    /*
     * 当一个排序集中的所有元素都以相同的分数插入时，为了强制字典排序，此命令在key处返回排序集中的全部元素，其值介于min和max之间。
     * [min 表示返回的结果中包含 min 值
     * [max 表示返回的结果中包含 max 值
     * (min 表示返回的结果中不包含 min 值
     * (max 表示返回的结果中不包含 max 值
     * [MIN, [MAX 可以用-,+ 代替，- 表示得分最小值的成员，+ 表示得分最大值的成员
     * ZRANGEBYLEX key min max [LIMIT offset count] (ex. ZRANGEBYLEX myzset - (c )
     * 当一个排序集中的所有元素都以相同的分数插入时，为了强制字典排序，此命令在key处返回排序集中的全部元素，其值介于min和max之间。如果排序集中的元素具有不同的分数，则返回的元素是未指定的。
     * 返回值：数组:指定分数范围内的元素列表。
     */
    void handlerZRangeByLex (ParamsType &params) {
        auto extendArg = GET_ARG_VALUE(ZSetParamsCheck::ZRangeExtendArg *);
        handlerZRangeByLexPrivate(params, *extendArg);
    }

    /*
     * 返回排序集中关键点处得分在最小值和最大值之间的所有元素。元素被认为是从低到高的顺序。
     * [min 表示返回的结果中包含 min 值
     * [max 表示返回的结果中包含 max 值
     * (min 表示返回的结果中不包含 min 值
     * (max 表示返回的结果中不包含 max 值
     * [MIN, [MAX 可以用-inf +inf 代替，-inf 表示得分最小值的成员，+inf 表示得分最大值的成员
     * ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count] (ex. ZRANGEBYSCORE myzset -inf (100 limit 0 1 )
     * 当一个排序集中的所有元素都以相同的分数插入时，为了强制字典排序，此命令在key处返回排序集中的全部元素，其值介于min和max之间。如果排序集中的元素具有不同的分数，则返回的元素是未指定的。
     * 返回值：数组:指定分数范围内的元素列表。
     */
    void handlerZRangeByScore (ParamsType &params) {
        auto extendArg = GET_ARG_VALUE(ZSetParamsCheck::ZRangeExtendArg *);
        handlerZRangeByScorePrivate(params, *extendArg);
    }

    /*
     * 计算 numkeys 个有序集合的交集。在给定要计算的 key 和其它参数之前，必须先给定 key 个数(numberkeys)。
     * ZINTER numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
     * 使用WEIGHTS选项，你可以为每个给定的有序集指定一个乘法因子，意思就是，每个给定有序集的所有成员的score值在传递给聚合函数之前都要先乘以该因子。如果WEIGHTS没有给定，默认是 1。
     * zinterstore zkey3 2 zkey1 zkey2 weights 10 1  (zkey3 score = (zkey1 score)*10 + (zkey2 score)*1 )
     * 使用AGGREGATE选项，你可以指定并集的结果集的聚合方式。默认使用的参数SUM，可以将所有集合中某个成员的score值之和作为结果集中该成员的score值。如果使用参数MIN或者MAX，结果集就是所有集合中该元素最小或最大score。
     * zinterstore zkey3 2 zkey1 zkey2 aggregate min (zkey3 score = min（ (zkey1 score)*1  ， (zkey2 score)*1 ）)
     * 可以传递 WITHSCORES 选项，以便将元素的分数与元素一起返回。这样返回的列表将包含 value1,score1,...,valueN,scoreN ，而不是 value1,...,valueN 。 客户端类库可以自由地返回更合适的数据类型（建议：具有值和得分的数组或元组）
     * 返回值：数组: 返回结合的交集（使用 WITHSCORES 参数时，可以返回scores ）
     */
    void handlerZInter (ParamsType &params) {
        auto *aggregateExtendArg = GET_ARG_VALUE(ZSetParamsCheck::AggregateExtendArg *);

        auto *newZSetHandler = new ZSetCommandHandler(server);
    }

    /*
     * 计算 numkeys 个有序集合的差集
     * ZDIFF numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
     * 返回值：数组: 返回结合的交集（使用 WITHSCORES 参数时，可以返回scores ）
     * 详见zinter
     */
    void handlerZDiff (ParamsType &params) {
    }

    /*
     * 计算 numkeys 个有序集合的差集
     * ZUNION numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
     * 返回值：数组: 返回结合的交集（使用 WITHSCORES 参数时，可以返回scores ）
     * 详见zinter
     */
    void handlerZUnion (ParamsType &params) {
    }

    /*
     * 返回与存储在关键字处的排序集中的指定成员相关联的分数，对于不存在的key，返回nil
     * ZMSCORE key member [member ...]
     * 返回值：数组:批量字符串回复成员分数的列表为双精度浮点数，对于不存在的key，返回nil。
     */
    void handlerZMScore (ParamsType &params) {
    }

    /*
     * 当仅使用key参数调用时，从存储在key处的排序集值中返回一个随机元素。
     * 如果提供的count参数为正，则返回一个不同元素的数组。数组的长度是计数或排序集的基数（ZCARD），以较低者为准。
     * 如果以负计数调用，则行为会发生更改，并且命令可以多次返回同一元素。在这种情况下，返回的元素数是指定计数的绝对值。
     * ZRANDMEMBER key [count [WITHSCORES]]
     * 返回值：字符串:未使用count选项，随即返回一个member；NIL:未使用count选项，key不存在
     * 数组:使用count选项，批量回复member，key不存在返回empty vector，使用[WITHSCORES]选项 批量回复member及对应score
     */
    void handlerZRandMember (ParamsType &params) {
    }

    /*
     * 将zrange的结果储存到 dst，详见zrange
     * ZRANGESTORE dst src min max [BYSCORE | BYLEX] [REV] [LIMIT offset count]
     * 返回值：整数:被成功添加的新成员的数量
     */
    void handlerZRangeStore (ParamsType &params) {
    }

    /*
     * 这个命令类似于ZINTER，但它不返回结果集，而是只返回结果的基数。
     * ZINTERCARD numkeys key [key ...] [LIMIT limit]
     * 返回值：整数:结果的基数
     * 如果使用 limit 选项，如果交集基数在计算的中途达到极限，则算法将退出并将LIMIT作为基数
     */
    void handlerZInterCard (ParamsType &params) {
    }

    /*
     * 从提供的关键字名称列表中的第一个非空排序集中弹出一个或多个元素
     * ZMPOP numkeys key [key ...] <MIN | MAX> [COUNT count]
     * 返回值：数组：一个双元素数组，第一个元素是从中弹出元素的键的名称，第二个元素是弹出元素的数组
     * <MIN | MAX> 最小值 | 最大值 ， [COUNT count] 可选项，弹出几个元素
     */
    void handlerZMPop (ParamsType &params) {
    }

    /*
     * 阻塞的zmpop
     * 从提供的关键字名称列表中的第一个非空排序集中弹出一个或多个元素
     * BZMPOP timeout numkeys key [key ...] <MIN | MAX> [COUNT count]
     * 返回值：数组：一个双元素数组，第一个元素是从中弹出元素的键的名称，第二个元素是弹出元素的数组
     * <MIN | MAX> 最小值 | 最大值 ， [COUNT count] 可选项，弹出几个元素
     */
    void handlerBZMPop (ParamsType &params) {
    }

private:
    void handlerZAddPrivate (
        ResValueType &resValue,
        ZSetDataWithStringScore *data,
        size_t count,
        ZSetParamsCheck::ZAddExtendArgType *extendArg
    ) {
        size_t retCount = 0;
        FloatType score;
        StringType temp;
        if (keyValues.dataStructureType == DataStructureType::LIST_PACK) {
            const auto end = keyValues.data.listPackKeyValues->end();
            auto scoreIt = end;
            auto posIt = end;
            INSERT_POSITION position = INSERT_POSITION::BEFORE;
            for (size_t i = 0; i < count; ++i) {
                auto begin = keyValues.data.listPackKeyValues->begin();
                // 取第二个值，member
                while (begin != end) {
                    scoreIt = begin;
                    ++begin;
                    temp = begin->toString();
                    ++begin;
                    if (temp == data[i].member) break;
                    if (posIt == end) {
                        // 找插入位置，处理元素不存在并且没有incr的情况
                        if (scoreIt->mode == DataTypeEnum::INTEGER) score = static_cast<double>(scoreIt->data.val);
                        else Utils::StringHelper::stringIsDouble(scoreIt->toString(), &score, false);

                        if (score < data[i].score) continue;
                        else if (score == data[i].score) {
                            if (temp < data[i].member) continue;
                            else posIt = scoreIt;
                        } else posIt = scoreIt;
                    }
                }
                // 找到了
                if (begin != end) {
                    // NX 标识 仅新增元素，不更新元素
                    if (extendArg->flag.hasFlag(ZSetParamsCheck::ZAddFlag::NX)) continue;
                    if (scoreIt->mode == DataTypeEnum::INTEGER) score = static_cast<double>(scoreIt->data.val);
                    else Utils::StringHelper::stringIsDouble(scoreIt->toString(), &score, false);

                    // 如果有自增标识
                    if (extendArg->flag.hasFlag(ZSetParamsCheck::ZAddFlag::INCR)) data[i].score += score;
                    // GT 新分数大于原分数才做更新  LT 新分数小于原分数才更新
                    if ((extendArg->flag.hasFlag(ZSetParamsCheck::ZAddFlag::GT) && data[i].score < score)
                        || (extendArg->flag.hasFlag(ZSetParamsCheck::ZAddFlag::LT) && data[i].score > score))
                        continue;

                    if (score != data[i].score) {
                        // 删除原 score member 重新添加
                        scoreIt = keyValues.data.listPackKeyValues->erase(scoreIt);
                        keyValues.data.listPackKeyValues->erase(scoreIt);
                        auto ret = findPosWithScore(data[i]);
                        KV_ASSERT(!std::get <0>(ret));
                        posIt = std::get <1>(ret);
                        position = std::get <2>(ret);
                        posIt = keyValues.data.listPackKeyValues->insert(
                            posIt,
                            Utils::StringHelper::toString(data[i].score),
                            position
                        );
                        keyValues.data.listPackKeyValues->insert(posIt, data[i].member, INSERT_POSITION::AFTER);

                        // 如果有 CH 标识，更新的元素也添加retCount
                        if (extendArg->flag.hasFlag(ZSetParamsCheck::ZAddFlag::CH)) ++retCount;
                    }
                } else {
                    // 元素不存在，新加元素
                    // XX标识 仅更新元素，不新增元素
                    if (extendArg->flag.hasFlag(ZSetParamsCheck::ZAddFlag::XX)) continue;
                    posIt = keyValues.data.listPackKeyValues->insert(
                        posIt,
                        data[i].stringScore,
                        data[i].len,
                        position
                    );
                    keyValues.data.listPackKeyValues->insert(posIt, data[i].member, INSERT_POSITION::AFTER);
                    ++retCount;
                }
            }
        } else if (keyValues.dataStructureType == DataStructureType::SKIP_LIST) {
            const auto end = keyValues.data.skipListKeyValue->end();
            for (size_t i = 0; i < count; ++i) {
                auto begin = keyValues.data.skipListKeyValue->begin();
                // 此处需要找member，无法用find 进行排序查找
                while (begin != end) {
                    if (begin->member == data[i].member) break;
                    ++begin;
                }
                // 已存在的值
                if (begin != end) {
                    // NX 标识 仅新增元素，不更新元素
                    if (extendArg->flag.hasFlag(ZSetParamsCheck::ZAddFlag::NX)) continue;

                    if (extendArg->flag.hasFlag(ZSetParamsCheck::ZAddFlag::INCR)) data[i].score += begin->score;
                    // GT 新分数大于原分数才做更新  LT 新分数小于原分数才更新
                    if ((extendArg->flag.hasFlag(ZSetParamsCheck::ZAddFlag::GT) && data[i].score < begin->score)
                        || (extendArg->flag.hasFlag(ZSetParamsCheck::ZAddFlag::LT) && data[i].score > begin->score))
                        continue;

                    if (data[i].score != begin->score) {
                        keyValues.data.skipListKeyValue->erase(*begin);
                        keyValues.data.skipListKeyValue->insert(std::move(data[i]));

                        if (extendArg->flag.hasFlag(ZSetParamsCheck::ZAddFlag::CH)) ++retCount;
                    }

                } else {
                    // 不存在的值
                    // XX标识 仅更新元素，不新增元素
                    if (extendArg->flag.hasFlag(ZSetParamsCheck::ZAddFlag::XX)) continue;
                    keyValues.data.skipListKeyValue->insert(std::move(data[i]));
                    ++retCount;
                }
            }

        } else {
            VALUE_DATA_STRUCTURE_ERROR;
        }

        resValue.setIntegerValue(retCount);
    }

    // 返回是否找到
    // 如果找到，返回的迭代器指向listPack中储存的当前元素的score部分
    // 如果未找到，返回的迭代器指向应当插入的位置的前一个元素的member部分 或 end()
    std::tuple <bool, KvListPack::Iterator, INSERT_POSITION> findPosWithScore (const ZSetData &data) const {
        FloatType val;
        auto it = keyValues.data.listPackKeyValues->begin();
        KvListPack::Iterator pos = it;
        StringType member;
        for (; it != keyValues.data.listPackKeyValues->end(); ++it) {
            if (it->mode == DataTypeEnum::INTEGER) val = static_cast<double>(it->data.val);
            else Utils::StringHelper::stringIsDouble(it->toString(), &val, false);
            // 小于直接下一次循环 比较score
            if (val < data.score) {
                pos = it;
                // 将pos赋值it，用于返回时插入，避免二次循环
                ++it;
                continue;
            } else if (val == data.score) {
                // 赋值，如果
                pos = it;
                // 如果等于的话，是通过member字典序来排列
                ++it;
                member = it->toString();
                if (member < data.member) continue;
                else if (member == data.member) return { true, pos, {}};
                else return { false, pos, INSERT_POSITION::BEFORE };
                // 如果score > data.score 还没有找到，直接返回end
            } else return { false, ++pos, INSERT_POSITION::AFTER };
        }

        return { false, keyValues.data.listPackKeyValues->end(), INSERT_POSITION::AFTER };
    }

    inline bool handlerDefaultRange (int &start, int &stop) const noexcept {
        int size = static_cast<int>(keyValues.size());
        if (keyValues.dataStructureType == DataStructureType::LIST_PACK) size /= 2;
        if (start < 0) start += size;
        if (stop < 0) stop += size;

        if (stop >= size) stop = size - 1;
        if (start >= stop || start >= size) return false;

        return true;
    }

    template <class Iterator>
    inline void handlerZRangeDefaultByListPack (
        ResValueType &resValue,
        int start,
        int stop,
        bool withScores,
        bool rev,
        Iterator it
    ) {
        StringType score;
        int cursor = 0;
        while (cursor != start) {
            ++it;
            ++it;
            ++cursor;
        }
        for (; cursor <= stop; ++cursor) {
            if (rev) {
                resValue.setVectorValue(it->toString());
                ++it;
                if (withScores) resValue.setVectorValue(it->toString()); // NOLINT
                ++it;
            } else {
                if (withScores) score = it->toString();
                ++it;
                resValue.setVectorValue(it->toString());
                if (withScores) resValue.setVectorValue(std::move(score)); // NOLINT
                ++it;
            }

        }
    }

    template <class Iterator>
    inline void handlerZRangeDefaultBySkipList (
        ResValueType &resValue,
        int start,
        int stop,
        bool withScores,
        Iterator it
    ) {
        int cursor = 0;
        while (cursor != start) {
            ++cursor;
            ++it;
        }
        for (; cursor <= stop; ++cursor) {
            resValue.setVectorValue(it->member);
            if (withScores) resValue.setVectorValue(Utils::StringHelper::toString(it->score));
            ++it;
        }
    }

    inline void handlerZRangeByLexFormListPack (
        ParamsType &params,
        ZSetParamsCheck::ZSetParamsCheck::ZSetByLexMinMaxType range,
        IntegerType offset,
        IntegerType limit,
        bool rev
    ) const {
        if (rev) {
            auto temp = std::move(range.min);
            range.min = std::move(range.max);
            range.max = std::move(temp);
        }
        if (range.min.intervalModel == ZSetParamsCheck::IntervalModel::MAX
            || range.max.intervalModel == ZSetParamsCheck::IntervalModel::MIN)
            return;

        auto begin = keyValues.data.listPackKeyValues->begin();
        auto rbegin = keyValues.data.listPackKeyValues->rbegin();
        const auto end = keyValues.data.listPackKeyValues->end();
        const auto rend = keyValues.data.listPackKeyValues->rend();
        StringType temp;
        // 如果最小值不是-号，则先找到最小值的迭代器
        if (!rev) {
            if (range.min.intervalModel != ZSetParamsCheck::IntervalModel::MIN) {
                auto scoreIt = end;
                while (begin != end) {
                    scoreIt = begin;
                    ++begin;
                    temp = begin->toString();
                    if (temp == range.min.val) {
                        begin = scoreIt;
                        // 如果是开区间 则不包含当前member 将member后移
                        if (range.min.intervalModel == ZSetParamsCheck::IntervalModel::OPEN) {
                            ++begin;
                            ++begin;
                        }
                        break;
                    } else if (temp > range.min.val) return;
                    ++begin;
                }
            }

            while (begin != end && offset--) {
                ++begin;
                ++begin;
            }

            while (begin != end && limit--) {
                ++begin;
                temp = begin->toString();
                if (range.max.intervalModel != ZSetParamsCheck::IntervalModel::MAX) {
                    if (temp == range.max.val) {
                        if (range.max.intervalModel == ZSetParamsCheck::IntervalModel::CLOSED)
                            params.resValue
                                  .setVectorValue(
                                      temp
                                  );
                        return;
                        // 没有找到最大值member，返回空数组
                    } else if (temp > range.max.val) {
                        params.resValue.setEmptyVectorValue();
                        return;
                    }
                }
                params.resValue.setVectorValue(temp);
                ++begin;
            }

            // 如果最大值不是+号，并且没有找到的情况下，返回空数组
            if (begin == end && range.max.intervalModel != ZSetParamsCheck::IntervalModel::MAX)
                params.resValue
                      .setEmptyVectorValue();
        } else {
            if (range.max.intervalModel != ZSetParamsCheck::IntervalModel::MAX) {
                while (rbegin != rend) {
                    temp = rbegin->toString();
                    if (temp == range.max.val) {
                        // 如果是开区间 则不包含当前member 将member后移
                        if (range.max.intervalModel == ZSetParamsCheck::IntervalModel::OPEN) {
                            ++rbegin;
                            ++rbegin;
                        }
                        break;
                        // 没找到
                    } else if (temp < range.max.val) return;
                    ++rbegin;
                    ++rbegin;
                }
            }

            // 计算 limit可选项 offset值的偏移
            while (rbegin != rend && offset--) {
                ++rbegin;
                ++rbegin;
            }

            while (rbegin != rend && limit--) {
                temp = rbegin->toString();
                if (range.min.intervalModel != ZSetParamsCheck::IntervalModel::MIN) {
                    if (temp == range.min.val) {
                        if (range.min.intervalModel == ZSetParamsCheck::IntervalModel::CLOSED)
                            params.resValue
                                  .setVectorValue(
                                      temp
                                  );
                        return;
                        // 没有找到最小值member，返回空数组
                    } else if (temp < range.min.val) {
                        params.resValue.setEmptyVectorValue();
                        return;
                    }
                }
                params.resValue.setVectorValue(temp);
                ++rbegin;
                ++rbegin;
            }

            // 如果最小值不是-号，并且没有找到的情况下，返回空数组
            if (rbegin == rend && range.min.intervalModel != ZSetParamsCheck::IntervalModel::MIN)
                params.resValue
                      .setEmptyVectorValue();
        }
    };

    inline void handlerZRangeByLexFormSkipList (
        ParamsType &params,
        ZSetParamsCheck::ZSetByLexMinMaxType range,
        IntegerType offset,
        IntegerType limit,
        bool rev
    ) const {
        if (rev) {
            auto temp = range.min;
            range.min = range.max;
            range.max = temp;
        }
        if (range.min.intervalModel == ZSetParamsCheck::ZSetParamsCheck::IntervalModel::MAX
            || range.max.intervalModel == ZSetParamsCheck::IntervalModel::MIN)
            return;

        auto begin = keyValues.data.skipListKeyValue->begin();
        auto rbegin = keyValues.data.skipListKeyValue->rbegin();
        const auto end = keyValues.data.skipListKeyValue->end();
        const auto rend = keyValues.data.skipListKeyValue->rend();
        // 如果最小值不是-号，则先找到最小值的迭代器
        if (!rev) {
            if (range.min.intervalModel != ZSetParamsCheck::IntervalModel::MIN) {
                while (begin != end) {
                    if (begin->member == range.min.val) {
                        // 如果是开区间 则不包含当前member 将member后移
                        if (range.min.intervalModel == ZSetParamsCheck::IntervalModel::OPEN) ++begin;
                        break;
                        // 没找到，返回空数组
                    } else if (begin->member > range.min.val) return;
                    ++begin;
                }
            }

            while (begin != end && offset--) ++begin;

            while (begin != end && limit--) {
                if (range.max.intervalModel != ZSetParamsCheck::IntervalModel::MAX) {
                    if (begin->member == range.max.val) {
                        if (range.max.intervalModel == ZSetParamsCheck::IntervalModel::CLOSED)
                            params.resValue.setVectorValue(begin->member);
                        return;
                        // 没有找到最大值member，返回空数组
                    } else if (begin->member > range.max.val) {
                        params.resValue.setEmptyVectorValue();
                        return;
                    }
                }
                params.resValue.setVectorValue(begin->member);
                ++begin;
            }

            // 如果最大值不是+号，并且没有找到的情况下，返回空数组
            if (begin == end && range.max.intervalModel != ZSetParamsCheck::IntervalModel::MAX)
                params.resValue
                      .setEmptyVectorValue();
        } else {
            if (range.max.intervalModel != ZSetParamsCheck::IntervalModel::MAX) {
                while (rbegin != rend) {
                    if (rbegin->member == range.max.val) {
                        // 如果是开区间 则不包含当前member 将member后移
                        if (range.max.intervalModel == ZSetParamsCheck::IntervalModel::OPEN) ++rbegin;
                        break;
                        // 没找到，返回空数组
                    } else if (rbegin->member < range.max.val) return;
                    ++rbegin;
                }
            }

            // 计算 limit可选项 offset值的偏移
            while (rbegin != rend && offset--) ++rbegin;

            while (rbegin != rend && limit--) {
                if (range.min.intervalModel != ZSetParamsCheck::IntervalModel::MIN) {
                    if (rbegin->member == range.min.val) {
                        if (range.min.intervalModel == ZSetParamsCheck::IntervalModel::CLOSED)
                            params.resValue.setVectorValue(rbegin->member);
                        return;
                        // 没有找到最小值member，返回空数组
                    } else if (rbegin->member < range.min.val) {
                        params.resValue.setEmptyVectorValue();
                        return;
                    }
                }
                params.resValue.setVectorValue(rbegin->member);
                ++rbegin;
            }

            // 如果最小值不是-号，并且没有找到的情况下，返回空数组
            if (rbegin == rend && range.min.intervalModel != ZSetParamsCheck::IntervalModel::MIN)
                params.resValue
                      .setEmptyVectorValue();
        }
    };

    inline void handlerZRangeByScoreFormSkipList (
        ParamsType &params,
        ZSetParamsCheck::ZSetByScoreMinMaxType range,
        IntegerType offset,
        IntegerType limit,
        bool withScores,
        bool rev
    ) const {
        if (rev) {
            auto temp = range.min;
            range.min = range.max;
            range.max = temp;
        }
        if (range.min.intervalModel == ZSetParamsCheck::IntervalModel::MAX
            || range.max.intervalModel == ZSetParamsCheck::IntervalModel::MIN
            || range.min.val >= range.max.val)
            return;

        auto begin = keyValues.data.skipListKeyValue->begin();
        const auto end = keyValues.data.skipListKeyValue->end();
        auto ret = keyValues.data.skipListKeyValue->find(rev ? range.max.val : range.min.val);
        if (ret.first == KvSkipList::FIND_RETURN_MODE::TAIL) return;
            // 没有找到，但是最小值/最大值在跳表中的score区间中，返回离最小值/最大值最近的前一个节点
        else if (ret.first == KvSkipList::FIND_RETURN_MODE::PREV && !rev) begin = ++ret.second;
            // 找到了
        else if (ret.first == KvSkipList::FIND_RETURN_MODE::FOUND) {
            begin = ret.second;
            if (!rev && range.min.intervalModel == ZSetParamsCheck::IntervalModel::OPEN) {
                while (begin->score == range.min.val && begin != end) ++begin;
            } else if (rev && range.max.intervalModel == ZSetParamsCheck::IntervalModel::OPEN) {
                while (begin->score == range.max.val && begin != end) --begin;
            }
        }
        // 处理offset
        while (begin != end && offset--)
            if (rev) --begin;
            else ++begin;

        if (!rev) {
            while (begin != end && limit--) {
                if (begin->score > range.max.val
                    || (begin->score == range.max.val
                        && range.max.intervalModel == ZSetParamsCheck::IntervalModel::OPEN))
                    return;
                params.resValue.setVectorValue(begin->member);
                if (withScores) params.resValue.setVectorValue(Utils::StringHelper::toString(begin->score));
                ++begin;
            }
        } else {
            while (begin != end && limit--) {
                if (begin->score < range.min.val
                    || (begin->score == range.min.val
                        && range.min.intervalModel == ZSetParamsCheck::IntervalModel::OPEN))
                    return;
                params.resValue.setVectorValue(begin->member);
                if (withScores) params.resValue.setVectorValue(Utils::StringHelper::toString(begin->score));
                --begin;
            }
        }
    }

    inline void handlerZRangeByScoreFormListPack (
        ParamsType &params,
        ZSetParamsCheck::ZSetByScoreMinMaxType range,
        IntegerType offset,
        IntegerType limit,
        bool withScores,
        bool rev
    ) const {
        if (rev) {
            auto temp = range.min;
            range.min = range.max;
            range.max = temp;
        }
        if (range.min.intervalModel == ZSetParamsCheck::IntervalModel::MAX
            || range.max.intervalModel == ZSetParamsCheck::IntervalModel::MIN
            || range.min.val >= range.max.val)
            return;

        if (!rev) {
            auto begin = keyValues.data.listPackKeyValues->begin();
            const auto end = keyValues.data.listPackKeyValues->end();
            auto scoreIt = begin;

            while (begin != end) {
                if (begin->toDouble() == range.min.val) {
                    // 如果是开区间 则不包含当前member 将member后移
                    if (range.min.intervalModel == ZSetParamsCheck::IntervalModel::OPEN) {
                        while (begin != end && begin->toDouble() == range.min.val) {
                            ++begin;
                            ++begin;
                        }
                    }
                    break;
                } else if (begin->toDouble() > range.min.val) break;
                ++begin;
                ++begin;
            }
            while (begin != end && offset--) {
                ++begin;
                ++begin;
            }
            while (begin != end && limit--) {
                if (begin->toDouble() > range.max.val
                    || (begin->toDouble() == range.max.val
                        && range.max.intervalModel == ZSetParamsCheck::IntervalModel::OPEN))
                    return;

                scoreIt = begin++;
                params.resValue.setVectorValue(begin->toString());
                if (withScores) params.resValue.setVectorValue(scoreIt->toString());
                ++begin;
            }
        } else {
            auto rbegin = keyValues.data.listPackKeyValues->rbegin();
            const auto rend = keyValues.data.listPackKeyValues->rend();
            auto memberIt = rbegin;
            while (rbegin != rend) {
                memberIt = rbegin;
                ++rbegin;
                if (rbegin->toDouble() == range.max.val) {
                    // 如果是开区间 则不包含当前member 将member后移
                    if (range.max.intervalModel == ZSetParamsCheck::IntervalModel::OPEN) {
                        while (rbegin->toDouble() == range.max.val) {
                            ++rbegin;
                            if (rbegin == rend) {
                                memberIt = rend;
                                break;
                            }
                            ++rbegin;
                        }
                    }
                    break;
                } else if (rbegin->toDouble() < range.max.val) break;
                ++rbegin;
            }
            rbegin = memberIt;
            while (rbegin == rend && offset--) {
                ++rbegin;
                ++rbegin;
            }
            while (rbegin == rend && limit--) {
                memberIt = rbegin++;
                if (rbegin->toDouble() < range.min.val
                    || (rbegin->toDouble() == range.min.val
                        && range.min.intervalModel == ZSetParamsCheck::IntervalModel::OPEN))
                    return;

                params.resValue.setVectorValue(memberIt->toString());
                if (withScores) params.resValue.setVectorValue(rbegin->toString());
                ++rbegin;
            }
        }
    }

    void handlerZRangeRevDefaultPrivate (
        ParamsType &params,
        const ZSetParamsCheck::ZSetParamsCheck::ZRangeExtendArg &extendArg
    ) {
        params.resValue.setEmptyVectorValue();
        int start = extendArg.start, stop = extendArg.stop;
        if (!handlerDefaultRange(start, stop)) return;

        auto withScores = extendArg.flag.hasFlag(ZSetParamsCheck::ZRangeFlag::WITH_SCORES);
        if (keyValues.dataStructureType == DataStructureType::LIST_PACK)
            handlerZRangeDefaultByListPack(
                params.resValue,
                start,
                stop,
                withScores,
                true,
                keyValues.data.listPackKeyValues->rbegin());
        else if (keyValues.dataStructureType == DataStructureType::SKIP_LIST)
            handlerZRangeDefaultBySkipList(
                params.resValue,
                start,
                stop,
                withScores,
                keyValues.data.skipListKeyValue->rbegin());
        else {
            VALUE_DATA_STRUCTURE_ERROR;
        }
    }
    void handlerZRangeDefaultPrivate (ParamsType &params, const ZSetParamsCheck::ZRangeExtendArg &extendArg) {
        params.resValue.setEmptyVectorValue();
        int start = extendArg.start, stop = extendArg.stop;
        if (!handlerDefaultRange(start, stop)) return;

        auto withScores = extendArg.flag.hasFlag(ZSetParamsCheck::ZRangeFlag::WITH_SCORES);
        StringType score;
        if (keyValues.dataStructureType == DataStructureType::LIST_PACK)
            handlerZRangeDefaultByListPack(
                params.resValue,
                start,
                stop,
                withScores,
                false,
                keyValues.data.listPackKeyValues->begin());
        else if (keyValues.dataStructureType == DataStructureType::SKIP_LIST)
            handlerZRangeDefaultBySkipList(
                params.resValue,
                start,
                stop,
                withScores,
                keyValues.data.skipListKeyValue->begin());
        else {
            VALUE_DATA_STRUCTURE_ERROR;
        }
    }
    void handlerZRangeByLexPrivate (ParamsType &params, const ZSetParamsCheck::ZRangeExtendArg &extendArg) {
        params.resValue.setEmptyVectorValue();
        IntegerType offset = 0, limit = std::numeric_limits <IntegerType>::max();
        if (extendArg.flag.hasFlag(ZSetParamsCheck::ZRangeFlag::LIMIT)) {
            offset = extendArg.limit.limitOff;
            limit = extendArg.limit.limitCount;
        }
        if (keyValues.dataStructureType == DataStructureType::LIST_PACK)
            handlerZRangeByLexFormListPack(
                params,
                extendArg.lexRange,
                offset,
                limit,
                extendArg.flag.hasFlag(ZSetParamsCheck::ZRangeFlag::REV));
        else if (keyValues.dataStructureType == DataStructureType::SKIP_LIST)
            handlerZRangeByLexFormSkipList(
                params,
                extendArg.lexRange,
                offset,
                limit,
                extendArg.flag.hasFlag(ZSetParamsCheck::ZRangeFlag::REV));
        else {
            VALUE_DATA_STRUCTURE_ERROR;
        }
    }
    void handlerZRangeByScorePrivate (ParamsType &params, const ZSetParamsCheck::ZRangeExtendArg &extendArg) {
        params.resValue.setEmptyVectorValue();
        IntegerType offset = 0, limit = std::numeric_limits <IntegerType>::max();
        if (extendArg.flag.hasFlag(ZSetParamsCheck::ZRangeFlag::LIMIT)) {
            offset = extendArg.limit.limitOff;
            limit = extendArg.limit.limitCount;
        }

        if (keyValues.dataStructureType == DataStructureType::LIST_PACK)
            handlerZRangeByScoreFormListPack(
                params,
                extendArg.scoreRange,
                offset,
                limit,
                extendArg.flag.hasFlag(ZSetParamsCheck::ZRangeFlag::WITH_SCORES),
                extendArg.flag.hasFlag(ZSetParamsCheck::ZRangeFlag::REV));
        else if (keyValues.dataStructureType == DataStructureType::SKIP_LIST)
            handlerZRangeByScoreFormSkipList(
                params,
                extendArg.scoreRange,
                offset,
                limit,
                extendArg.flag.hasFlag(ZSetParamsCheck::ZRangeFlag::WITH_SCORES),
                extendArg.flag.hasFlag(ZSetParamsCheck::ZRangeFlag::REV));
        else {
            VALUE_DATA_STRUCTURE_ERROR;
        }
    }

    // 是否找到，如果找到了则设置found为true，并检查structType
    // 如果不为SET或ZSET，设置structType为NIL，填充报错
    inline FindCommandReturnType findSetCommand (ParamsType &params, const StringType &key) const {
        FindCommandReturnType ret;
        auto handler = server->keyMap.find(key);
        // 如果没有找到，或者找到的handler和this相等
        if (handler == server->keyMap.end() || handler->second.handler == this) {
            ret.found = false;
            return ret;
        }

        ret.found = true;
        if (handler->second.structType != StructType::SET && handler->second.structType != StructType::ZSET) {
            params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::WRONGTYPE);
            ret.structType = StructType::NIL;
            return ret;
        }
        ret.structType = handler->second.structType;
        if (ret.structType == StructType::SET)
            ret.handler.setCommandHandler = static_cast<SetCommandHandler *>(handler->second.handler); // NOLINT
        else ret.handler.zSetCommandHandler = static_cast<ZSetCommandHandler *>(handler->second.handler); // NOLINT
        return ret;
    }
};
#endif //KV_STORE_KV_STORE_SERVER_COMMAND_STRUCTS_KV_ZSET_COMMAND_EXE_
