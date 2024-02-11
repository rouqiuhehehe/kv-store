//
// Created by 115282 on 2024/1/2.
//

#ifndef KV_STORE_KV_STORE_SERVER_COMMAND_STRUCTS_KV_CHECK_PARAMS_VALID_H_
#define KV_STORE_KV_STORE_SERVER_COMMAND_STRUCTS_KV_CHECK_PARAMS_VALID_H_

#define GET_ARG_VALUE(type) *reinterpret_cast<type *>(&params.arg->arg)

struct IntegerRange
{
    int min;
    int max;
};
#ifndef __x86_64__
static_assert(false, "暂不支持32位系统，请使用64位系统编译运行");
#endif
struct BaseParamsCheck : protected CommandCommon
{
    static inline bool handlerFlushAll (ParamsType &params) {
        if (!params.commandParams.key.empty() || !params.commandParams.params.empty()) {
            params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::SYNTAX_ERROR);
            return false;
        }
        return true;
    }

    static inline bool handlerAuth (ParamsType &params) {
        if (!checkKeyIsValid(params.commandParams, params.resValue)) return false;

        // 如果不需要输入密码
        if (!params.needAuth) {
            params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::WITHOUT_PASSWORD);
            return false;
        }

        return true;
    }
};
struct KeyParamsCheck : protected CommandCommon
{
    static inline bool handlerDel (ParamsType &params) {
        return checkKeyIsValid(params.commandParams, params.resValue);
    }

    static inline bool handlerExpire (ParamsType &params) {
        return expireCheckCommon(params);
    }

    static inline bool handlerPExpire (ParamsType &params) {
        return expireCheckCommon(params);
    }

    static inline bool handlerTTL (ParamsType &params) {
        return checkKeyIsValid(params.commandParams, params.resValue);
    }

    static inline bool handlerPTTL (ParamsType &params) {
        return checkKeyIsValid(params.commandParams, params.resValue);
    }

    static inline bool handlerKeys (ParamsType &params) {
        return checkKeyIsValid(params.commandParams, params.resValue);
    }

    static inline bool handlerExists (ParamsType &params) {
        return checkKeyIsValid(params.commandParams, params.resValue);
    }

    static inline bool handlerType (ParamsType &params) {
        return checkKeyIsValid(params.commandParams, params.resValue)
            && checkHasParams(params.commandParams, params.resValue, 0);
    }

private:
    static inline bool expireCheckCommon (ParamsType &params) {
        IntegerType expire;
        bool success = checkKeyIsValid(params.commandParams, params.resValue)
            && checkHasParams(params.commandParams, params.resValue, 1)
            && checkValueIsLongLong(params.commandParams, params.resValue, &expire);

        GET_ARG_VALUE(IntegerType) = expire;
        return success;
    }
};
struct StringParamsCheck : protected CommandCommon
{
    static inline bool handlerSet (ParamsType &params) {
        // 检查参数长度 是否缺少参数
        return checkHasParams(params.commandParams, params.resValue, -1);
    }
    static inline bool handlerGet (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 0);
    }
    static inline bool handlerIncr (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 0);
    }
    static inline bool handlerIncrBy (ParamsType &params) {
        if (!checkHasParams(params.commandParams, params.resValue, 1))
            return false;

        IntegerType integer;
        if (!checkValueIsLongLong(params.commandParams, params.resValue, &integer))
            return false;

        GET_ARG_VALUE(IntegerType) = integer;
        return true;
    }
    static inline bool handlerDecr (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 0);
    }
    static inline bool handlerDecrBy (ParamsType &params) {
        if (!checkHasParams(params.commandParams, params.resValue, 1))
            return false;

        IntegerType integer;
        if (!checkValueIsLongLong(params.commandParams, params.resValue, &integer))
            return false;

        GET_ARG_VALUE(IntegerType) = integer;
        return true;
    }
    static inline bool handlerAppend (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 1);
    }
    static inline bool handlerMSet (ParamsType &params) {
        // 因为第一个key被截出来了，所以commandParams.params.size()必须是奇数才是正确的
        if ((params.commandParams.params.size() & 1) == 0) {
            params.resValue
                  .setErrorStr(params.commandParams, ResValueType::ErrorType::WRONG_NUMBER);
            return false;
        }
        return true;
    }
    static inline bool handlerIncrByFloat (ParamsType &params) {
        if (!checkHasParams(params.commandParams, params.resValue, 1))
            return false;

        FloatType doubleValue;
        if (!Utils::StringHelper::stringIsDouble(params.commandParams.params[0], &doubleValue)) {
            params.resValue
                  .setErrorStr(params.commandParams, ResValueType::ErrorType::VALUE_NOT_FLOAT);
            return false;
        }

        GET_ARG_VALUE(FloatType) = doubleValue;
        return true;
    }
};
struct HashParamsCheck : protected CommandCommon
{
    static inline bool handlerHSet (ParamsType &params) {
        // 检查是否有参数
        if (!checkHasParams(params.commandParams, params.resValue, 1, true))
            return false;
        // 检查参数是否为偶数，key:value
        if ((params.commandParams.params.size() & 1) == 1) {
            params.resValue
                  .setErrorStr(params.commandParams, ResValueType::ErrorType::WRONG_NUMBER);
            return false;
        }
        return true;
    }
    static inline bool handlerHGet (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 1);
    }
    static inline bool handlerHExists (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 1);
    }
    static inline bool handlerHIncrBy (ParamsType &params) {
        if (!checkHasParams(params.commandParams, params.resValue, 2))
            return false;

        IntegerType step;
        if (!checkValueIsLongLong(params.commandParams, params.commandParams.params[1], params.resValue, &step))
            return false;

        GET_ARG_VALUE(IntegerType) = step;
        return true;
    }
    static inline bool handlerHIncrByFloat (ParamsType &params) {
        if (!checkHasParams(params.commandParams, params.resValue, 2))
            return false;

        FloatType step;
        if (!Utils::StringHelper::stringIsDouble(params.commandParams.params[1], &step)) {
            params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::VALUE_NOT_FLOAT);
            return false;
        }

        GET_ARG_VALUE(FloatType) = step;
        return true;
    }
    static inline bool handlerHLen (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 1);
    }
    static inline bool handlerHDel (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 1, true);
    }
    static inline bool handlerHGetAll (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 1);
    }
    static inline bool handlerHVals (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 1);
    }
    static inline bool handlerHKeys (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 1);
    }
    static inline bool handlerHSetNx (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 2);
    }

};
struct ListParamsCheck : protected CommandCommon
{
    static inline bool handlerLIndex (ParamsType &params) {
        if (!checkHasParams(params.commandParams, params.resValue, 1)) return false;
        IntegerType index;
        if (!checkValueIsLongLong(params.commandParams, params.resValue, &index)) return false;

        GET_ARG_VALUE(IntegerType) = index;
        return true;
    }
    static inline bool handlerLPush (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 1, true);
    }
    static inline bool handlerLPushX (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 1, true);
    }
    static inline bool handlerLPop (ParamsType &params) {
        GET_ARG_VALUE(IntegerType) = fillPopCountParams(params);
        if (GET_ARG_VALUE(IntegerType) == -1)
            return false;

        return true;
    }
    static inline bool handlerRPush (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 1, true);
    }
    static inline bool handlerRPushX (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 1, true);
    }
    static inline bool handlerRPop (ParamsType &params) {
        return handlerLPop(params);
    }
    static inline bool handlerLInsert (ParamsType &params) {
        INSERT_POSITION position;
        if (!checkHasParams(params.commandParams, params.resValue, 3)) return false;

        if (params.commandParams.params[0] == LIST_BEFORE) position = INSERT_POSITION::BEFORE;
        else if (params.commandParams.params[0] == LIST_AFTER) position = INSERT_POSITION::AFTER;
        else {
            params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::SYNTAX_ERROR);
            return false;
        }

        GET_ARG_VALUE(INSERT_POSITION) = position;
        return true;
    }
    static inline bool handlerLSet (ParamsType &params) {
        if (!checkHasParams(params.commandParams, params.resValue, 2)) return false;
        IntegerType index;
        if (!Utils::StringHelper::stringIsLL(params.commandParams.params[0], &index)) return false;

        GET_ARG_VALUE(IntegerType) = index;
        return true;
    }
    static inline bool handlerLRange (ParamsType &params) {
        if (!checkHasParams(params.commandParams, params.resValue, 2)) return false;
        int min, max;
        if (!Utils::StringHelper::stringIsI(params.commandParams.params[0], &min)
            || !Utils::StringHelper::stringIsI(params.commandParams.params[1], &max)) {
            params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::VALUE_NOT_INTEGER);
            return false;
        }

        GET_ARG_VALUE(IntegerRange) = {
            .min = min,
            .max = max
        };
        return true;
    }
    static inline bool handlerRPopLPush (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 1);
    }
    static inline bool handlerLRem (ParamsType &params) {
        IntegerType count;
        if (!checkHasParams(params.commandParams, params.resValue, 2)) return false;
        if (!Utils::StringHelper::stringIsLL(params.commandParams.params[0], &count)) return false;

        GET_ARG_VALUE(IntegerType) = count;
        return true;
    }
    static inline bool handlerLLen (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 0);
    }
    static inline bool handlerLTrim (ParamsType &params) {
        return handlerLRange(params);
    }
    static inline bool handlerBrPopLPush (ParamsType &params) {
        if (!checkHasParams(params.commandParams, params.resValue, 2)) return false;

        std::chrono::milliseconds ms;
        if (!checkTimeoutIsValid(
            params.commandParams.params[params.commandParams.params.size() - 1],
            params.commandParams,
            params.resValue,
            ms
        ))
            return false;

        GET_ARG_VALUE(std::chrono::milliseconds) = ms;
        return true;
    }
    static inline bool handlerBlPop (ParamsType &params) {
        if (!checkHasParams(params.commandParams, params.resValue, 1, true)) return false;

        std::chrono::milliseconds ms;
        if (!checkTimeoutIsValid(
            params.commandParams.params[params.commandParams.params.size() - 1],
            params.commandParams,
            params.resValue,
            ms
        ))
            return false;

        GET_ARG_VALUE(std::chrono::milliseconds) = ms;
        return true;
    }
    static inline bool handlerBrPop (ParamsType &params) {
        return handlerBlPop(params);
    }

private:
    static IntegerType fillPopCountParams (ParamsType &params) {
        IntegerType count;
        if (!params.commandParams.params.empty()) {
            if (!checkHasParams(params.commandParams, params.resValue, 1)) return -1;
            if (!Utils::StringHelper::stringIsLL(params.commandParams.params[0], &count) || count < 0) {
                params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::VALUE_MUST_BE_POSITIVE);
                return -1;
            }
            if (count == 0) {
                params.resValue.setEmptyVectorValue();
                return -1;
            }

            return count;
        }

        return 1;
    }

    static inline bool checkTimeoutIsValid (
        const StringType &s,
        const CommandParams &commandParams,
        ResValueType &resValue,
        std::chrono::milliseconds &ms
    ) {
        FloatType floatValue;
        if (!Utils::StringHelper::stringIsDouble(s, &floatValue)) {
            resValue.setErrorStr(commandParams, ResValueType::ErrorType::INVALID_TIMEOUT);
            return false;
        }

        if (floatValue < 0) {
            resValue.setErrorStr(commandParams, ResValueType::ErrorType::TIMEOUT_IS_NEGATIVE);
            return false;
        }

        auto integer = static_cast<IntegerType>(floatValue * 1000);
        ms = std::chrono::milliseconds { integer };
        return true;
    }

    static const StringType LIST_BEFORE;
    static const StringType LIST_AFTER;
};
struct SetParamsCheck : protected CommandCommon
{
    static inline bool handlerSAdd (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 1, true);
    }

    static inline bool handlerSMembers (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 0);
    }
    static inline bool handlerSCard (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 0);
    }
    static inline bool handlerSIsMember (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 1);
    }
    static inline bool handlerSRandMember (ParamsType &params) {
        if (!checkHasParams(params.commandParams, params.resValue, static_cast<size_t>(0), 1)) return false;
        long long count;

        if (params.commandParams.params.size() == 1)
            if (!checkValueIsLongLong(params.commandParams, params.resValue, &count))
                return false;
            else
                GET_ARG_VALUE(IntegerType) = count;
        else
            GET_ARG_VALUE(IntegerType) = 1;

        return true;
    }
    static inline bool handlerSMove (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 2);
    }
    static inline bool handlerSPop (ParamsType &params) {
        if (!checkHasParams(params.commandParams, params.resValue, static_cast<size_t>(0), 1)) return false;
        long long count = 0;

        if (params.commandParams.params.size() == 1)
            if (!checkValueIsLongLong(params.commandParams, params.resValue, &count))
                return false;
            else {
                if (count < 0) {
                    params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::VALUE_MUST_BE_POSITIVE);
                    return false;
                }
                GET_ARG_VALUE(size_t) = count;
            }
        else
            GET_ARG_VALUE(size_t) = 1;

        return true;
    }
    static inline bool handlerSRem (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 1, true);
    }
    static inline bool handlerSDiff (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 1, true);
    }
    static inline bool handlerSDiffStore (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 2, true);
    }
    static inline bool handlerSInter (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 1, true);
    }
    static inline bool handlerSInterStore (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 2, true);
    }
    static inline bool handlerSUnion (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 1, true);
    }
    static inline bool handlerSUnionStore (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 2, true);
    }

};
struct ZSetParamsCheck : protected CommandCommon
{
    /*
     * XX： 仅更新已存在的元素。不要添加新元素。 NX：仅添加新元素。不要更新已存在的元素。
     * LT：仅当新分数小于当前分数时才更新现有元素。此标志不会阻止添加新元素。 GT：仅当新分数大于当前分数时才更新现有元素。此标志不会阻止添加新元素。
     * CH：将返回值从添加的新元素数量修改为更改的元素总数（CH是changed的缩写）。更改的元素是添加的新元素和已经存在的元素，更新了它们的分数。
     * INCR：当指定此选项时，ZADD的行为类似于ZINCRBY。在此模式中只能指定一个分数元素对。
     */
    enum class ZAddFlag
    {
        DEFAULT = 0,
        XX = 1 << 0,
        NX = 1 << 1,
        LT = 1 << 2,
        GT = 1 << 3,
        CH = 1 << 4,
        INCR = 1 << 5
    };

    enum class ZRangeFlag
    {
        DEFAULT = 0,
        REV = 1 << 0,
        LIMIT = 1 << 1,
        WITH_SCORES = 1 << 2,
        BY_LEX = 1 << 3,
        BY_SCORE = 1 << 4
    };

    enum class AggregateMode
    {
        SUM,
        MIN,
        MAX
    };
    template <class Iterator>
    struct ZAddExtendArg
    {
        Utils::EnumHelper <ZAddFlag> flag { ZAddFlag::DEFAULT };
        Iterator begin;
    };
    struct ZSetLimitNumType
    {
        IntegerType limitOff {};
        IntegerType limitCount {};
    };

    struct AggregateExtendArg
    {
        IntegerType number {};
        AggregateMode aggregateMode = AggregateMode::SUM;
        bool withScores { false };
        ArrayType <FloatType> weights;
        ArrayType <StringType> keys;
    };
    // 开区间(a, b) 闭区间[0, 1]
    enum class IntervalModel
    {
        OPEN,
        CLOSED,
        MIN,
        MAX
    };
    struct ZSetByLexMinMaxType
    {
        struct ValueType
        {
            ValueType () = default;
            ValueType (const ValueType &r) = default;
            ValueType (ValueType &&r) noexcept = default;
            ValueType &operator= (const ValueType &r) = default;
            ValueType &operator= (ValueType &&r) noexcept = default;

            StringType val {};
            IntervalModel intervalModel {};
        };
        ValueType min;
        ValueType max;
    };
    struct ZSetByScoreMinMaxType
    {
        struct ValueType
        {
            FloatType val {};
            IntervalModel intervalModel {};
        };
        ValueType min {};
        ValueType max {};
    };
    using ZRangeFlagType = Utils::EnumHelper <ZRangeFlag>;
    struct ZRangeExtendArg
    {
        ZRangeFlagType flag { ZRangeFlag::DEFAULT };
        int start {};
        int stop {};
#if CHECK_VERSION_GREATER_THEN_OR_EQUAL_BUILD_SUPPORTED_REDIS_VERSION(6, 2, 0)
        ZSetLimitNumType limit {};
        ZSetByLexMinMaxType lexRange {};
        ZSetByScoreMinMaxType scoreRange {};
#endif
    };
    using ZAddExtendArgType = ZAddExtendArg <decltype(CommandParams::params)::iterator>;
    static inline bool handlerZAdd (ParamsType &params) {
        zAddExtendArg.flag.setFlag(ZAddFlag::DEFAULT);
        if (!checkHasParams(params.commandParams, params.resValue, 2, true)) return false;
        auto end = params.commandParams.params.end();
        zAddExtendArg.begin = params.commandParams.params.begin();
        // 检查可选项参数
        for (; zAddExtendArg.begin != end; ++zAddExtendArg.begin) {
            if (strcasecmp(zAddExtendArg.begin->c_str(), "nx") == 0) zAddExtendArg.flag.openFlag(ZAddFlag::NX);
            else if (strcasecmp(zAddExtendArg.begin->c_str(), "gt") == 0) zAddExtendArg.flag.openFlag(ZAddFlag::GT);
            else if (strcasecmp(zAddExtendArg.begin->c_str(), "lt") == 0) zAddExtendArg.flag.openFlag(ZAddFlag::LT);
            else if (strcasecmp(zAddExtendArg.begin->c_str(), "ch") == 0) zAddExtendArg.flag.openFlag(ZAddFlag::CH);
            else if (strcasecmp(zAddExtendArg.begin->c_str(), "incr") == 0) zAddExtendArg.flag.openFlag(ZAddFlag::INCR);
            else break;
        }

        auto distance = std::distance(zAddExtendArg.begin, end);
        // 如果zadd 实际参数列表 不为偶数，即缺失score 或 member
        // 或者不存在实际参数列表
        if ((distance & 1) != 0 || !distance) {
            params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::SYNTAX_ERROR);
            return false;
        }

        if (zAddExtendArg.flag.hasFlag(ZAddFlag::NX) && zAddExtendArg.flag.hasFlag(ZAddFlag::XX)) {
            params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::XX_NX_AT_SAME_TIME);
            return false;
        }
        // NX 标识 表示只添加新元素，与GT | LT 互斥
        if ((zAddExtendArg.flag.hasFlag(ZAddFlag::NX) && zAddExtendArg.flag.hasFlag(ZAddFlag::GT))
            || (zAddExtendArg.flag.hasFlag(ZAddFlag::NX) && zAddExtendArg.flag.hasFlag(ZAddFlag::LT))
            || (zAddExtendArg.flag.hasFlag(ZAddFlag::GT) && zAddExtendArg.flag.hasFlag(ZAddFlag::LT))) {
            params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::GT_LT_AT_SAME_TIME);
            return false;
        }

        // 如果使用incr可选项，则只能自增一个元素
        if (zAddExtendArg.flag.hasFlag(ZAddFlag::INCR) && distance > 2) {
            params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::INCR_OPTION_ERROR);
            return false;
        }

        GET_ARG_VALUE(ZAddExtendArgType *) = &zAddExtendArg;
        return true;
    }
    static inline bool handlerZCard (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 0);
    }
    static inline bool handlerZScore (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 1);
    }
    static inline bool handlerZCount (ParamsType &params) {
        if (!checkHasParams(params.commandParams, params.resValue, 2)) return false;
        if (!parseScoreRangeIndexRange(
            params.commandParams,
            params.resValue,
            params.commandParams.params[0],
            params.commandParams.params[1]
        ))
            return false;

        zRangeExtendArg.flag.setFlag(ZRangeFlag::DEFAULT);
        GET_ARG_VALUE(ZRangeExtendArg *) = &zRangeExtendArg;
        return true;
    }
    static inline bool handlerZIncrBy (ParamsType &params) {
        return checkHasParams(params.commandParams, params.resValue, 2);
    }
    static inline bool handlerZInterStore (ParamsType &params) {
    }
    static inline bool handlerZDiffStore (ParamsType &params) {
    }
    static inline bool handlerZUnionStore (ParamsType &params) {
    }
    static inline bool handlerZLexCount (ParamsType &params) {
    }
    static inline bool handlerZRange (ParamsType &params) {
        zRangeExtendArg.flag.setFlag(ZRangeFlag::DEFAULT);
        if (!checkHasParams(params.commandParams, params.resValue, 2, true)) return false;
#if CHECK_VERSION_GREATER_THEN_OR_EQUAL_BUILD_SUPPORTED_REDIS_VERSION(6, 2, 0)
        if (!handlerZRangeExtendArg(params)) return false;
#else
            if (!parseDefaultRangeIndex(
                params.commandParams,
                params.resValue,
                params.commandParams.params[0],
                params.commandParams.params[1]))
                return false;
#endif
        GET_ARG_VALUE(ZRangeExtendArg *) = &zRangeExtendArg;
        return true;
    }
    static inline bool handlerZRank (ParamsType &params) {
    }
    static inline bool handlerZRem (ParamsType &params) {
    }
    static inline bool handlerZRemRangeByLex (ParamsType &params) {
    }
    static inline bool handlerZRemRangeByRank (ParamsType &params) {
    }
    static inline bool handlerZRemRangeByScore (ParamsType &params) {
    }
    static inline bool handlerZRevRange (ParamsType &params) {
        zRangeExtendArg.flag.setFlag(ZRangeFlag::REV);
        if (!checkHasParams(params.commandParams, params.resValue, static_cast<size_t>(2), 3)
            || !parseDefaultRangeIndex(
                params.commandParams,
                params.resValue,
                params.commandParams.params[0],
                params.commandParams.params[1]
            ))
            return false;
        if (params.commandParams.params.size() == 3) {
            if (strcasecmp(params.commandParams.params[2].c_str(), "withscores") == 0)
                zRangeExtendArg.flag.openFlag(ZRangeFlag::WITH_SCORES);
            else {
                params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::SYNTAX_ERROR);
                return false;
            }
        }
        GET_ARG_VALUE(ZRangeExtendArg *) = &zRangeExtendArg;
        return true;
    }
    static inline bool handlerZRevRangeByLex (ParamsType &params) {
        if (!handlerZRangeByLex(params)) return false;
        zRangeExtendArg.flag.setFlag(ZRangeFlag::REV);
        return true;
    }
    static inline bool handlerZRevRangeByScore (ParamsType &params) {
    }
    static inline bool handlerZRevRanK (ParamsType &params) {
    }
    static inline bool handlerZPopMax (ParamsType &params) {
    }
    static inline bool handlerZPopMin (ParamsType &params) {
    }
    static inline bool handlerBZPopMax (ParamsType &params) {
    }
    static inline bool handlerBZPopMin (ParamsType &params) {
    }
    static inline bool handlerZRangeByLex (ParamsType &params) {
        zRangeExtendArg.flag.setFlag(ZRangeFlag::DEFAULT);
        if (!checkHasParams(params.commandParams, params.resValue, 2, true)) return false;
        // 检查limit额外选项
        auto it = params.commandParams.params.begin() + 2;
        const ZRangeFlagType checkFlags { ZRangeFlag::LIMIT };
        if (!parseZRangeExtendFlag(params, it, checkFlags)) return false;

        if (!parseLexRangeIndexRange(
            params.commandParams,
            params.resValue,
            params.commandParams.params[0],
            params.commandParams.params[1]
        ))
            return false;

        GET_ARG_VALUE(ZRangeExtendArg *) = &zRangeExtendArg;
        return true;
    }
    static inline bool handlerZRangeByScore (ParamsType &params) {
        zRangeExtendArg.flag.setFlag(ZRangeFlag::DEFAULT);
        if (!checkHasParams(params.commandParams, params.resValue, 2, true)) return false;
        auto it = params.commandParams.params.begin() + 2;
        const ZRangeFlagType checkFlags { ZRangeFlag::WITH_SCORES, ZRangeFlag::LIMIT };
        if (!parseZRangeExtendFlag(params, it, checkFlags)) return false;

        if (!parseScoreRangeIndexRange(
            params.commandParams,
            params.resValue,
            params.commandParams.params[0],
            params.commandParams.params[1]
        ))
            return false;

        GET_ARG_VALUE(ZRangeExtendArg *) = &zRangeExtendArg;
        return true;
    }
    static inline bool handlerZInter (ParamsType &params) {
        if (!checkValueIsLongLong(
            params.commandParams,
            params.commandParams.key,
            params.resValue, &aggregateExtendArg.number
        ))
            return false;
        if (aggregateExtendArg.number <= 0) {
            params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::ONE_INPUT_KEY_IS_NEEDED);
            return false;
        }
        if (!parseAggregateArg(
            params,
            params.commandParams.params.begin(),
            params.commandParams.params.end()))
            return false;
        GET_ARG_VALUE(AggregateExtendArg *) = &aggregateExtendArg;
        return true;
    }
    static inline bool handlerZDiff (ParamsType &params) {
    }
    static inline bool handlerZUnion (ParamsType &params) {
    }
    static inline bool handlerZMScore (ParamsType &params) {
    }
    static inline bool handlerZRandMember (ParamsType &params) {
    }
    static inline bool handlerZRangeStore (ParamsType &params) {
    }
    static inline bool handlerZInterCard (ParamsType &params) {
    }
    static inline bool handlerZMPop (ParamsType &params) {
    }
    static inline bool handlerBZMPop (ParamsType &params) {
    }

private:
    template <class Iterator>
    static inline bool parseZRangeExtendFlag (
        ParamsType &params,
        Iterator begin,
        const Utils::EnumHelper <ZRangeFlag> &checkFlag
    ) {
        for (; begin != params.commandParams.params.end(); ++begin) {
            if (!strcasecmp(begin->c_str(), "byscore")) zRangeExtendArg.flag.openFlag(ZRangeFlag::BY_SCORE);
            else if (!strcasecmp(begin->c_str(), "bylex")) zRangeExtendArg.flag.openFlag(ZRangeFlag::BY_LEX);
            else if (!strcasecmp(begin->c_str(), "withscores")) zRangeExtendArg.flag.openFlag(ZRangeFlag::WITH_SCORES);
            else if (!strcasecmp(begin->c_str(), "rev")) zRangeExtendArg.flag.openFlag(ZRangeFlag::REV);
            else if (!strcasecmp(begin->c_str(), "limit")) {
                if (!parseLimit(params.commandParams, params.resValue, begin)) return false;
                begin += 2;
            } else {
                params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::SYNTAX_ERROR);
                return false;
            }
        }
        // 检查是否为允许的flag
        ZRangeFlagType copyFlag = zRangeExtendArg.flag;
        copyFlag.closeFlag(checkFlag.getModel());
        if (copyFlag.getModel() != ZRangeFlag::DEFAULT) {
            params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::SYNTAX_ERROR);
            return false;
        }
        return true;
    }
    static inline bool handlerZRangeExtendArg (ParamsType &params) {
        auto begin = params.commandParams.params.begin() + 2;
        const ZRangeFlagType checkFlags
            { ZRangeFlag::BY_LEX, ZRangeFlag::BY_SCORE, ZRangeFlag::REV, ZRangeFlag::WITH_SCORES, ZRangeFlag::LIMIT };
        if (!parseZRangeExtendFlag(params, begin, checkFlags)) return false;

        if (zRangeExtendArg.flag.hasFlag(ZRangeFlag::BY_LEX) && zRangeExtendArg.flag.hasFlag(ZRangeFlag::BY_SCORE)) {
            params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::SYNTAX_ERROR);
            return false;
        }
        // limit 必须配合 byscore 或 bylex 使用
        if (zRangeExtendArg.flag.hasFlag(ZRangeFlag::LIMIT) && !zRangeExtendArg.flag.hasFlag(ZRangeFlag::BY_LEX)
            && !zRangeExtendArg.flag.hasFlag(ZRangeFlag::BY_SCORE)) {
            params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::LIMIT_OPTION_ERROR);
            return false;
        }
        // withscores 不能和 bylex 一起使用
        if (zRangeExtendArg.flag.hasFlag(ZRangeFlag::WITH_SCORES) && zRangeExtendArg.flag.hasFlag(ZRangeFlag::BY_LEX)) {
            params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::WITH_SCORE_NOT_BY_LEX);
            return false;
        }

        if (zRangeExtendArg.flag.hasFlag(ZRangeFlag::BY_LEX))
            return parseLexRangeIndexRange(
                params.commandParams,
                params.resValue,
                params.commandParams.params[0],
                params.commandParams.params[1]
            );
        else if (zRangeExtendArg.flag.hasFlag(ZRangeFlag::BY_SCORE))
            return parseScoreRangeIndexRange(
                params.commandParams,
                params.resValue,
                params.commandParams.params[0],
                params.commandParams.params[1]
            );
        else
            return parseDefaultRangeIndex(
                params.commandParams,
                params.resValue,
                params.commandParams.params[0],
                params.commandParams.params[1]
            );
    }

    template <class Iterator>
    static inline bool parseLimit (const CommandParams &commandParams, ResValueType &resValue, Iterator limitBegin) {
        zRangeExtendArg.flag.openFlag(ZRangeFlag::LIMIT);
        if (!checkValueIsLongLong(commandParams, *++limitBegin, resValue, &zRangeExtendArg.limit.limitOff)
            || !checkValueIsLongLong(commandParams, *++limitBegin, resValue, &zRangeExtendArg.limit.limitCount))
            return false;

        return true;
    }

    static inline bool parseDefaultRangeIndex (
        const CommandParams &commandParams,
        ResValueType &resValue,
        const StringType &min,
        const StringType &max
    ) {
        if (!checkValueIsLongLong(commandParams, min, resValue, reinterpret_cast<IntegerType *>(&zRangeExtendArg.start))
            || !checkValueIsLongLong(
                commandParams,
                max,
                resValue,
                reinterpret_cast<IntegerType *>(&zRangeExtendArg.stop)))
            return false;

        return true;
    }

    static inline bool parseScoreRangeIndexRange (
        const CommandParams &commandParams,
        ResValueType &resValue,
        const StringType &min,
        const StringType &max
    ) {
        if (!parseScoreRangeIndex(commandParams, resValue, min.c_str(), zRangeExtendArg.scoreRange.min) ||
            !parseScoreRangeIndex(commandParams, resValue, max.c_str(), zRangeExtendArg.scoreRange.max))
            return false;

        return true;
    }

    static inline bool parseLexRangeIndexRange (
        const CommandParams &commandParams,
        ResValueType &resValue,
        const StringType &min,
        const StringType &max
    ) {
        const char *minStr = min.c_str();
        const char *maxStr = max.c_str();

        if (!parseLexRangeIndex(commandParams, resValue, minStr, zRangeExtendArg.lexRange.min) ||
            !parseLexRangeIndex(commandParams, resValue, maxStr, zRangeExtendArg.lexRange.max))
            return false;

        return true;
    }

    static inline bool parseScoreRangeIndex (
        const CommandParams &commandParams,
        ResValueType &resValue,
        const char *index,
        ZSetByScoreMinMaxType::ValueType &val
    ) {
        if (*index == '(') {
            val.intervalModel = IntervalModel::OPEN;
            ++index;
        } else val.intervalModel = IntervalModel::CLOSED;

        if (!Utils::StringHelper::stringIsDouble(StringType(index), &val.val, false)) {
            resValue.setErrorStr(commandParams, ResValueType::ErrorType::SCORE_RANGE_ERROR);
            return false;
        }

        return true;
    }

    static inline bool parseLexRangeIndex (
        const CommandParams &commandParams,
        ResValueType &resValue,
        const char *index,
        ZSetByLexMinMaxType::ValueType &val
    ) {
        switch (*index) {
            case '+':
                if (*(index + 1) != '\0') goto ERR;
                val.intervalModel = IntervalModel::MAX;
                return true;
            case '-':
                if (*(index + 1) != '\0') goto ERR;
                val.intervalModel = IntervalModel::MIN;
                return true;
            case '(':
                val.intervalModel = IntervalModel::OPEN;
                val.val = StringType(index + 1);
                return true;
            case '[':
                val.intervalModel = IntervalModel::CLOSED;
                val.val = StringType(index + 1);
                return true;
        }

    ERR:
        resValue.setErrorStr(commandParams, ResValueType::ErrorType::LEX_RANGE_ERROR);
        return false;
    }

    template <class Iterator>
    static inline bool parseAggregateArg (ParamsType &params, Iterator begin, Iterator end) {
        auto distance = std::distance(begin, end);
        if (aggregateExtendArg.number > distance) {
            params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::SYNTAX_ERROR);
            return false;
        }
        for (IntegerType num = aggregateExtendArg.number; begin != end && num; ++begin, --num)
            aggregateExtendArg.keys.emplace_back(*begin);

        return parseAggregateExtendArg(params, begin, end);
    }
    template <class Iterator>
    static inline bool parseAggregateExtendArg (ParamsType &params, Iterator &it, const Iterator &end) {
        aggregateExtendArg.weights.resize(aggregateExtendArg.number);
        std::fill(aggregateExtendArg.weights.begin(), aggregateExtendArg.weights.end(), 1);
        aggregateExtendArg.withScores = false;
        auto distance = std::distance(it, end);
        while (distance) {
            // 只有元素个数 > number值时，检查weights
            if (distance > aggregateExtendArg.number && strcasecmp(it->c_str(), "weights") == 0) {
                FloatType weight;
                ++it;
                --distance;
                for (IntegerType i = 0; i < aggregateExtendArg.number; ++i, ++it, --distance) {
                    if (!Utils::StringHelper::stringIsDouble(*it, &weight, false)) goto WEIGHTS_VALUE_NOT_FLOAT;
                    aggregateExtendArg.weights[i] = weight;
                }
            } else if (distance >= 2 && strcasecmp(it->c_str(), "aggregate") == 0) {
                ++it;
                --distance;
                if (strcasecmp(it->c_str(), "sum") == 0) aggregateExtendArg.aggregateMode = AggregateMode::SUM;
                else if (strcasecmp(it->c_str(), "min") == 0) aggregateExtendArg.aggregateMode = AggregateMode::MIN;
                else if (strcasecmp(it->c_str(), "max") == 0) aggregateExtendArg.aggregateMode = AggregateMode::MAX;
                else goto SYNTAX_ERROR;
                ++it;
                --distance;
            } else if (strcasecmp(it->c_str(), "withscores") == 0) {
                aggregateExtendArg.withScores = true;
                ++it;
                --distance;
            } else goto SYNTAX_ERROR;
        }

        return true;

    WEIGHTS_VALUE_NOT_FLOAT:
        params.resValue
              .setErrorStr(params.commandParams, ResValueType::ErrorType::WEIGHTS_VALUE_NOT_FLOAT);
        return false;
    SYNTAX_ERROR:
        params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::SYNTAX_ERROR);
        return false;
    }

private:
    static ZAddExtendArgType zAddExtendArg;
    static ZRangeExtendArg zRangeExtendArg;
    static AggregateExtendArg aggregateExtendArg;
};

ZSetParamsCheck::ZAddExtendArgType ZSetParamsCheck::zAddExtendArg; //NOLINT
ZSetParamsCheck::ZRangeExtendArg ZSetParamsCheck::zRangeExtendArg; //NOLINT
ZSetParamsCheck::AggregateExtendArg ZSetParamsCheck::aggregateExtendArg; // NOLINT
const StringType ListParamsCheck::LIST_BEFORE = "before"; //NOLINT
const StringType ListParamsCheck::LIST_AFTER = "after"; //NOLINT
#endif //KV_STORE_KV_STORE_SERVER_COMMAND_STRUCTS_KV_CHECK_PARAMS_VALID_H_
