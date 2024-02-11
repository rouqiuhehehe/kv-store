//
// Created by 115282 on 2024/1/2.
//

#ifndef KV_STORE_KV_STORE_SERVER_CIRCLE_KV_SERVER_H_
#define KV_STORE_KV_STORE_SERVER_CIRCLE_KV_SERVER_H_

#include <algorithm>
#define BIND_COMMANDS(name, fn, checkParamsFn, emptyFn, structType, b) \
        allCommands.emplace(name, AllCommandsSecond(static_cast<void (CommandCommonWithServerPtr::*)(ParamsType &)>(fn),structType,checkParamsFn, emptyFn, b))
#define BIND_BASE_COMMANDS(name, fn, checkParamsFn) BIND_COMMANDS(name, fn, checkParamsFn, nullptr, StructType::BASE, false)
#define BIND_KEY_COMMANDS(name, fn, checkParamsFn) \
    BIND_COMMANDS(name, fn, checkParamsFn, nullptr, StructType::KEY, false)
#define BIND_STRING_COMMANDS(name, fn, checkParamsFn, emptyFn, createdOnNotExist) \
    BIND_COMMANDS(name, fn, checkParamsFn, emptyFn, StructType::STRING, createdOnNotExist)
#define BIND_HASH_COMMANDS(name, fn, checkParamsFn, emptyFn, createdOnNotExist) \
    BIND_COMMANDS(name, fn, checkParamsFn, emptyFn, StructType::HASH, createdOnNotExist)
#define BIND_LIST_COMMANDS(name, fn, checkParamsFn, emptyFn, createdOnNotExist) \
    BIND_COMMANDS(name, fn, checkParamsFn, emptyFn, StructType::LIST, createdOnNotExist)
#define BIND_SET_COMMANDS(name, fn, checkParamsFn, emptyFn, createdOnNotExist) \
    BIND_COMMANDS(name, fn, checkParamsFn, emptyFn, StructType::SET, createdOnNotExist)
#define BIND_ZSET_COMMANDS(name, fn, checkParamsFn, emptyFn, createdOnNotExist) \
    BIND_COMMANDS(name, fn, checkParamsFn, emptyFn, StructType::ZSET, createdOnNotExist)

#include "command-structs/kv-command-common.h"
#include "command-structs/kv-check-params-valid.h"
class KvDebug;
class KeyOfValue
{
    friend class KvServer;
public:
    explicit KeyOfValue (
        StructType structType,
        KvServer *server,
        std::chrono::milliseconds expire = std::chrono::milliseconds(NilExpire));
    explicit KeyOfValue (
        StructType structType,
        CommandCommonWithServerPtr *handler,
        std::chrono::milliseconds expire = std::chrono::milliseconds(NilExpire));
    KeyOfValue (KeyOfValue &&r) noexcept {
        operator=(std::move(r));
    }
    KeyOfValue &operator= (KeyOfValue &&r) noexcept {
        if (this == &r) return *this;
        structType = r.structType;
        handler = r.handler;
        expire = r.expire;

        r.handler = nullptr;
        r.expire = std::chrono::milliseconds { NilExpire };
        r.structType = StructType::NIL;
        return *this;
    }
    ~KeyOfValue () noexcept {
        delete handler;
    }

    StructType structType {};
    std::chrono::milliseconds expire { NilExpire };
    CommandCommonWithServerPtr *handler {};
};
class KvServer : CommandCommon
{
    friend class KvDebug;
    friend class ZSetCommandHandler;
    friend class SetCommandHandler;
    friend class ListCommandHandler;
    friend class CommandCommonWithServerPtr;
    friend class KeyCommandHandler;
    struct AllCommandsSecond
    {
        AllCommandsSecond (
            void (CommandCommonWithServerPtr::*handler) (ParamsType &),
            StructType structType,
            bool (*checkParamsValidHandler) (ParamsType &),
            void (*emptyHandler) (ParamsType &),
            bool createdOnNotExist
        )
            : handler(handler),
              checkParamsValidHandler(checkParamsValidHandler),
              emptyHandler(emptyHandler),
              structType(structType),
              createdOnNotExist(createdOnNotExist) {}
        void (CommandCommonWithServerPtr::*handler) (ParamsType &)  {};
        bool (*checkParamsValidHandler) (ParamsType &) {};
        void (*emptyHandler) (ParamsType &) {};
        StructType structType {};
        bool createdOnNotExist;
    };

    using KeyMapType = KvHashTable <KeyType, KeyOfValue>;

public:
    explicit KvServer (MainReactor *);
    ~KvServer () noexcept override;
    /*
     * 入口函数，处理命令
     * @param params 处理后的命令
     * @return ResValueType
     */
    void handlerCommand (ParamsType &params);

    void checkExpireKeys () {
        if (keyMap.empty())
            return;

        auto end = keyMap.end();
        int i = 0;
        auto now = getNow();

        while (i++ < onceCheckExpireKeyMaxNum && (getNow() - now) < onceCheckExpireKeyMaxTime) {
            auto it = keyMap.getRandomItem();
            if (it != end)
                checkExpireKey(it);
        }
    }

    static CommandParams splitCommandParams (ResValueType &recvValue) {
        CommandParams commandParams;
        commandParams.command = recvValue.elements[0].value;
        // 转成小写
        Utils::StringHelper::stringTolower(commandParams.command);
        ValueType key;
        if (recvValue.elements.size() > 1)
            commandParams.key = recvValue.elements[1].value;
        if (recvValue.elements.size() > 2) {
            commandParams.params.reserve(recvValue.elements.size() - 2);
            for (size_t i = 2; i < recvValue.elements.size(); ++i) {
                if (!recvValue.elements[i].value.empty())
                    commandParams.params.emplace_back(recvValue.elements[i].value);
            }
        }

        recvValue.elements.clear();
        return commandParams;
    }

    inline void clear () noexcept {
        keyMap.clear();
    }

private:
    static inline bool checkAuth (
        const CommandParams &commandParams, const bool &needAuth, const bool &auth, ResValueType &res
    ) {
        if (needAuth && !auth) {
            res.setErrorStr(commandParams, ResValueType::ErrorType::NO_AUTHENTICATION);
            return false;
        }

        return true;
    }

    // 检查当前key是否是当前操作的数据结构
    // ex: lpush 不能操作 set 的key
    inline bool checkKeyType (
        const CommandParams &commandParams,
        const KeyMapType::Iterator &it,
        StructType structType,
        ResValueType &res
    ) {
        if (it != keyMap.end() && it->second.structType != structType) {
            res.setErrorStr(commandParams, ResValueType::ErrorType::WRONGTYPE);
            return false;
        }
        return true;
    }

    void bindAllCommand ();

    KeyMapType::iterator checkExpireKey (KeyMapType::iterator &it) {
        if (it != keyMap.end()) {
            if (it->second.expire.count() >= 0 && getNow() > it->second.expire)
                return delKeyEvent(it);
        }

        return keyMap.end();
    }
    bool setExpire (const std::string &key, const std::chrono::milliseconds expire) {
        auto it = keyMap.find(key);
        if (it != keyMap.end()) {
            it->second.expire = expire + getNow();
            return true;
        }

        return false;
    }

    KeyMapType::iterator delKeyEvent (KeyMapType::iterator &it) {
        return keyMap.erase(it);
    }

    HashTable <KeyType, AllCommandsSecond> allCommands;
    KeyMapType keyMap {};
    KvDebug *debugHandler;
    MainReactor *mainReactor;

    static constexpr int onceCheckExpireKeyMaxNum = 20;
    static constexpr std::chrono::milliseconds onceCheckExpireKeyMaxTime { 30 };
};

constexpr int KvServer::onceCheckExpireKeyMaxNum;
constexpr std::chrono::milliseconds KvServer::onceCheckExpireKeyMaxTime;

#include "command-structs/kv-base-command.h"
#include "command-structs/kv-key-command.h"
#include "command-structs/kv-hash-command.h"
#include "command-structs/kv-string-command.h"
#include "command-structs/kv-list-command.h"
#include "command-structs/kv-set-command.h"
#include "command-structs/kv-zset-command.h"
#include "command-structs/kv-debug-command.h"

KeyOfValue::KeyOfValue (StructType structType, KvServer *server, std::chrono::milliseconds expire)
    : structType(structType),
      expire(expire.count() > 0 ? (expire + CommandCommon::getNow()) : expire) {
    switch (structType) {
        case StructType::STRING:
            handler = new StringCommandHandler(server);
            break;
        case StructType::LIST:
            handler = new ListCommandHandler(server);
            break;
        case StructType::HASH:
            handler = new HashCommandHandler(server);
            break;
        case StructType::SET:
            handler = new SetCommandHandler(server);
            break;
        case StructType::ZSET:
            handler = new ZSetCommandHandler(server);
            break;
        case StructType::NIL:
        case StructType::KEY:
        case StructType::BASE:
        case StructType::END:
            unreachable();
            KV_ASSERT(false);
    }
}
KeyOfValue::KeyOfValue (
    StructType structType,
    CommandCommonWithServerPtr *handler,
    std::chrono::milliseconds expire
)
    : structType(structType),
      expire(expire.count() > 0 ? (expire + CommandCommon::getNow()) : expire),
      handler(handler) {}

KvServer::KvServer (MainReactor *mainReactor)
    : debugHandler(new KvDebug(this)), mainReactor(mainReactor) {
    bindAllCommand();
}
KvServer::~KvServer () noexcept {
    delete debugHandler;
}
void KvServer::bindAllCommand () {
    // ------------------- server -------------------
    BIND_BASE_COMMANDS("flushall", &BaseCommandHandler::handlerFlushAll, &BaseParamsCheck::handlerFlushAll);
    BIND_BASE_COMMANDS("flushdb", &BaseCommandHandler::handlerFlushDb, &BaseParamsCheck::handlerFlushAll);
    BIND_BASE_COMMANDS("auth", &BaseCommandHandler::handlerAuth, &BaseParamsCheck::handlerAuth);

    // ------------------- key -------------------
    BIND_KEY_COMMANDS("del", &KeyCommandHandler::handlerDel, &KeyParamsCheck::handlerDel);
    BIND_KEY_COMMANDS("expire", &KeyCommandHandler::handlerExpire, &KeyParamsCheck::handlerExpire);
    BIND_KEY_COMMANDS("pexpire", &KeyCommandHandler::handlerPExpire, &KeyParamsCheck::handlerPExpire);
    BIND_KEY_COMMANDS("ttl", &KeyCommandHandler::handlerTTL, &KeyParamsCheck::handlerTTL);
    BIND_KEY_COMMANDS("pttl", &KeyCommandHandler::handlerPTTL, &KeyParamsCheck::handlerPTTL);
    BIND_KEY_COMMANDS("keys", &KeyCommandHandler::handlerKeys, &KeyParamsCheck::handlerKeys);
    BIND_KEY_COMMANDS("exists", &KeyCommandHandler::handlerExists, &KeyParamsCheck::handlerExists);
    BIND_KEY_COMMANDS("type", &KeyCommandHandler::handlerType, &KeyParamsCheck::handlerType);

    // ------------------- string -------------------
    BIND_STRING_COMMANDS("set", &StringCommandHandler::handlerSet, &StringParamsCheck::handlerSet, nullptr, true);
    BIND_STRING_COMMANDS("get",
        &StringCommandHandler::handlerGet,
        &StringParamsCheck::handlerGet,
        &EmptyHandler::setNilFlag,
        false);
    BIND_STRING_COMMANDS("incr", &StringCommandHandler::handlerIncr, &StringParamsCheck::handlerIncr, nullptr, true);
    BIND_STRING_COMMANDS("incrby",
        &StringCommandHandler::handlerIncrBy,
        &StringParamsCheck::handlerIncrBy,
        nullptr,
        true);
    BIND_STRING_COMMANDS("incrbyfloat",
        &StringCommandHandler::handlerIncrByFloat,
        &StringParamsCheck::handlerIncrByFloat,
        nullptr,
        true);
    BIND_STRING_COMMANDS("decr", &StringCommandHandler::handlerDecr, &StringParamsCheck::handlerDecr, nullptr, true);
    BIND_STRING_COMMANDS("decrby",
        &StringCommandHandler::handlerDecrBy,
        &StringParamsCheck::handlerDecrBy,
        nullptr,
        true);
    BIND_STRING_COMMANDS("append",
        &StringCommandHandler::handlerAppend,
        &StringParamsCheck::handlerAppend,
        nullptr,
        true);
    BIND_STRING_COMMANDS("mset", &StringCommandHandler::handlerMSet, &StringParamsCheck::handlerMSet, nullptr, true);

    // ------------------- hash -------------------
    BIND_HASH_COMMANDS("hset", &HashCommandHandler::handlerHSet, &HashParamsCheck::handlerHSet, nullptr, true);
    BIND_HASH_COMMANDS("hget",
        &HashCommandHandler::handlerHGet,
        &HashParamsCheck::handlerHGet,
        &EmptyHandler::setNilFlag,
        false);
    BIND_HASH_COMMANDS("hgetall",
        &HashCommandHandler::handlerHGetAll,
        &HashParamsCheck::handlerHGetAll,
        &EmptyHandler::setEmptyVector,
        false);
    BIND_HASH_COMMANDS("hexists",
        &HashCommandHandler::handlerHExists,
        &HashParamsCheck::handlerHExists,
        &EmptyHandler::setZeroInteger,
        false);
    BIND_HASH_COMMANDS("hincrby", &HashCommandHandler::handlerHIncrBy, &HashParamsCheck::handlerHIncrBy, nullptr, true);
    BIND_HASH_COMMANDS("hincrbyfloat",
        &HashCommandHandler::handlerHIncrByFloat,
        &HashParamsCheck::handlerHIncrByFloat,
        nullptr,
        true);
    BIND_HASH_COMMANDS("hlen",
        &HashCommandHandler::handlerHLen,
        &HashParamsCheck::handlerHLen,
        &EmptyHandler::setZeroInteger,
        false);
    BIND_HASH_COMMANDS("hvals",
        &HashCommandHandler::handlerHVals,
        &HashParamsCheck::handlerHVals,
        &EmptyHandler::setEmptyVector,
        false);
    BIND_HASH_COMMANDS("hkeys",
        &HashCommandHandler::handlerHKeys,
        &HashParamsCheck::handlerHKeys,
        &EmptyHandler::setEmptyVector,
        false);
    BIND_HASH_COMMANDS("hsetnx", &HashCommandHandler::handlerHSetNx, &HashParamsCheck::handlerHSetNx, nullptr, true);
    BIND_HASH_COMMANDS("hdel",
        &HashCommandHandler::handlerHDel,
        &HashParamsCheck::handlerHDel,
        &EmptyHandler::setZeroInteger,
        false);

    // ------------------- list -------------------
    BIND_LIST_COMMANDS("lindex",
        &ListCommandHandler::handlerLIndex,
        &ListParamsCheck::handlerLIndex,
        &EmptyHandler::setNilFlag,
        false);
    BIND_LIST_COMMANDS("lpush", &ListCommandHandler::handlerLPush, &ListParamsCheck::handlerLPush, nullptr, true);
    BIND_LIST_COMMANDS("lpushx",
        &ListCommandHandler::handlerLPushX,
        &ListParamsCheck::handlerLPushX,
        &EmptyHandler::setZeroInteger,
        false);
    BIND_LIST_COMMANDS("lpop",
        &ListCommandHandler::handlerLPop,
        &ListParamsCheck::handlerLPop,
        &EmptyHandler::setNilFlag,
        false);
    BIND_LIST_COMMANDS("rpush", &ListCommandHandler::handlerRPush, &ListParamsCheck::handlerRPush, nullptr, true);
    BIND_LIST_COMMANDS("rpushx",
        &ListCommandHandler::handlerRPushX,
        &ListParamsCheck::handlerRPushX,
        &EmptyHandler::setZeroInteger,
        false);
    BIND_LIST_COMMANDS("rpop",
        &ListCommandHandler::handlerRPop,
        &ListParamsCheck::handlerRPop,
        &EmptyHandler::setNilFlag,
        false);
    BIND_LIST_COMMANDS("blpop", &ListCommandHandler::handlerBlPop, &ListParamsCheck::handlerBlPop, nullptr, false);
    BIND_LIST_COMMANDS("brpop", &ListCommandHandler::handlerBrPop, &ListParamsCheck::handlerBrPop, nullptr, false);
    BIND_LIST_COMMANDS("brpoplpush",
        &ListCommandHandler::handlerBrPopLPush,
        &ListParamsCheck::handlerBrPopLPush,
        nullptr,
        false);
    BIND_LIST_COMMANDS("linsert",
        &ListCommandHandler::handlerLInsert,
        &ListParamsCheck::handlerLInsert,
        &EmptyHandler::setZeroInteger,
        false);
    BIND_LIST_COMMANDS("lset",
        &ListCommandHandler::handlerLSet,
        &ListParamsCheck::handlerLSet,
        &EmptyHandler::setNoSuchKeyError,
        false);
    BIND_LIST_COMMANDS("lrange",
        &ListCommandHandler::handlerLRange,
        &ListParamsCheck::handlerLRange,
        &EmptyHandler::setEmptyVector,
        false);
    BIND_LIST_COMMANDS("rpoplpush",
        &ListCommandHandler::handlerRPopLPush,
        &ListParamsCheck::handlerRPopLPush,
        &EmptyHandler::setNilFlag,
        false);
    BIND_LIST_COMMANDS("lrem",
        &ListCommandHandler::handlerLRem,
        &ListParamsCheck::handlerLRem,
        &EmptyHandler::setZeroInteger,
        false);
    BIND_LIST_COMMANDS("llen",
        &ListCommandHandler::handlerLLen,
        &ListParamsCheck::handlerLLen,
        &EmptyHandler::setZeroInteger,
        false);
    BIND_LIST_COMMANDS("ltrim",
        &ListCommandHandler::handlerLTrim,
        &ListParamsCheck::handlerLTrim,
        &EmptyHandler::setOKFlag,
        false);

    // ------------------- set -------------------
    BIND_SET_COMMANDS("sadd", &SetCommandHandler::handlerSAdd, &SetParamsCheck::handlerSAdd, nullptr, true);
    BIND_SET_COMMANDS("smembers",
        &::SetCommandHandler::handlerSMembers,
        &SetParamsCheck::handlerSMembers,
        &EmptyHandler::setEmptyVector,
        false);
    BIND_SET_COMMANDS("scard",
        &SetCommandHandler::handlerSCard,
        &SetParamsCheck::handlerSCard,
        &EmptyHandler::setZeroInteger,
        false);
    BIND_SET_COMMANDS("sismember",
        &SetCommandHandler::handlerSIsMember,
        &SetParamsCheck::handlerSIsMember,
        &EmptyHandler::setZeroInteger,
        false);
    BIND_SET_COMMANDS("srandmember",
        &SetCommandHandler::handlerSRandMember,
        &SetParamsCheck::handlerSRandMember,
        &EmptyHandler::nilOrVectorEmpty,
        false);
    BIND_SET_COMMANDS("smove",
        &SetCommandHandler::handlerSMove,
        &SetParamsCheck::handlerSMove,
        &EmptyHandler::setZeroInteger,
        false);
    BIND_SET_COMMANDS("spop",
        &SetCommandHandler::handlerSPop,
        &SetParamsCheck::handlerSPop,
        &EmptyHandler::nilOrVectorEmpty,
        false);
    BIND_SET_COMMANDS("srem",
        &SetCommandHandler::handlerSRem,
        &SetParamsCheck::handlerSRem,
        &EmptyHandler::setZeroInteger,
        false);
    BIND_SET_COMMANDS("sdiff",
        &SetCommandHandler::handlerSDiff,
        &SetParamsCheck::handlerSDiff,
        &EmptyHandler::setEmptyVector,
        false);
    BIND_SET_COMMANDS("sdiffstore",
        &SetCommandHandler::handlerSDiffStore,
        &SetParamsCheck::handlerSDiffStore,
        nullptr,
        false);
    BIND_SET_COMMANDS("sinter",
        &SetCommandHandler::handlerSInter,
        &SetParamsCheck::handlerSInter,
        &EmptyHandler::setEmptyVector,
        false);
    BIND_SET_COMMANDS("sinterstore",
        &SetCommandHandler::handlerSInterStore,
        &SetParamsCheck::handlerSInterStore,
        nullptr,
        false);
    BIND_SET_COMMANDS("sunion",
        &SetCommandHandler::handlerSUnion,
        &SetParamsCheck::handlerSUnion,
        nullptr,
        false);
    BIND_SET_COMMANDS("sunionstore",
        &SetCommandHandler::handlerSUnionStore,
        &SetParamsCheck::handlerSUnionStore,
        nullptr,
        false);

    // ------------------- zset -------------------
    BIND_ZSET_COMMANDS("zadd", &ZSetCommandHandler::handlerZAdd, &ZSetParamsCheck::handlerZAdd, nullptr, true);
    BIND_ZSET_COMMANDS("zcard",
        &ZSetCommandHandler::handlerZCard,
        &ZSetParamsCheck::handlerZCard,
        &EmptyHandler::setZeroInteger,
        false);
    BIND_ZSET_COMMANDS("zcount",
        &ZSetCommandHandler::handlerZCount,
        &ZSetParamsCheck::handlerZCount,
        &EmptyHandler::setZeroInteger,
        false);
    BIND_ZSET_COMMANDS("zscore",
        &ZSetCommandHandler::handlerZScore,
        &ZSetParamsCheck::handlerZScore,
        &EmptyHandler::setNilFlag,
        false);
    BIND_ZSET_COMMANDS("zincrby", &ZSetCommandHandler::handlerZIncrBy, &ZSetParamsCheck::handlerZIncrBy, nullptr, true);
    BIND_ZSET_COMMANDS("zinterstore",
        &ZSetCommandHandler::handlerZInterStore,
        &ZSetParamsCheck::handlerZInterStore,
        nullptr,
        false);
    BIND_ZSET_COMMANDS("zdiffstore",
        &ZSetCommandHandler::handlerZDiffStore,
        &ZSetParamsCheck::handlerZDiffStore,
        nullptr,
        false);
    BIND_ZSET_COMMANDS("zunionstore",
        &ZSetCommandHandler::handlerZUnionStore,
        &ZSetParamsCheck::handlerZUnionStore,
        nullptr,
        false);
    BIND_ZSET_COMMANDS("zlexcount",
        &ZSetCommandHandler::handlerZLexCount,
        &ZSetParamsCheck::handlerZLexCount,
        &EmptyHandler::setZeroInteger,
        false);
    BIND_ZSET_COMMANDS("zrange",
        &ZSetCommandHandler::handlerZRange,
        &ZSetParamsCheck::handlerZRange,
        &EmptyHandler::setEmptyVector,
        false);
    BIND_ZSET_COMMANDS("zrank",
        &ZSetCommandHandler::handlerZRank,
        &ZSetParamsCheck::handlerZRank,
        &EmptyHandler::setNilFlag,
        false);
    BIND_ZSET_COMMANDS("zrem",
        &ZSetCommandHandler::handlerZRem,
        &ZSetParamsCheck::handlerZRem,
        &EmptyHandler::setZeroInteger,
        false);
    BIND_ZSET_COMMANDS("zremrangebylex",
        &ZSetCommandHandler::handlerZRemRangeByLex,
        &ZSetParamsCheck::handlerZRemRangeByLex,
        &EmptyHandler::setZeroInteger,
        false);
    BIND_ZSET_COMMANDS("zremrangebyrank",
        &ZSetCommandHandler::handlerZRemRangeByRank,
        &ZSetParamsCheck::handlerZRemRangeByRank,
        &EmptyHandler::setZeroInteger,
        false);
    BIND_ZSET_COMMANDS("zremrangebyscore",
        &ZSetCommandHandler::handlerZRemRangeByScore,
        &ZSetParamsCheck::handlerZRemRangeByScore,
        &EmptyHandler::setZeroInteger,
        false);
    BIND_ZSET_COMMANDS("zrevrange",
        &ZSetCommandHandler::handlerZRevRange,
        &ZSetParamsCheck::handlerZRevRange,
        &EmptyHandler::setEmptyVector,
        false);
    BIND_ZSET_COMMANDS("zrevrangebylex",
        &ZSetCommandHandler::handlerZRevRangeByLex,
        &ZSetParamsCheck::handlerZRevRangeByLex,
        &EmptyHandler::setEmptyVector,
        false);
    BIND_ZSET_COMMANDS("zrevrangebyscore",
        &ZSetCommandHandler::handlerZRevRangeByScore,
        &ZSetParamsCheck::handlerZRevRangeByScore,
        &EmptyHandler::setEmptyVector,
        false);
    BIND_ZSET_COMMANDS("zrevrank",
        &ZSetCommandHandler::handlerZRevRanK,
        &ZSetParamsCheck::handlerZRevRanK,
        &EmptyHandler::setNilFlag,
        false);

    // 5.0.0 版本以后命令
#if CHECK_VERSION_GREATER_THEN_OR_EQUAL_BUILD_SUPPORTED_REDIS_VERSION(5, 0, 0)
    BIND_ZSET_COMMANDS("zpopmin",
        &ZSetCommandHandler::handlerZPopMin,
        &ZSetParamsCheck::handlerZPopMin,
        &EmptyHandler::setEmptyVector,
        false);
    BIND_ZSET_COMMANDS("zpopmax",
        &ZSetCommandHandler::handlerZPopMax,
        &ZSetParamsCheck::handlerZPopMax,
        &EmptyHandler::setEmptyVector,
        false);
    BIND_ZSET_COMMANDS("bzpopmin",
        &ZSetCommandHandler::handlerBZPopMin,
        &ZSetParamsCheck::handlerBZPopMin,
        nullptr,
        false);
    BIND_ZSET_COMMANDS("bzpopmax",
        &ZSetCommandHandler::handlerBZPopMax,
        &ZSetParamsCheck::handlerBZPopMax,
        nullptr,
        false);
#endif
    // 以下命令6.2版本弃用
    // 此处直接移除
#if CHECK_VERSION_LESS_THEN_BUILD_SUPPORTED_REDIS_VERSION(6, 2, 0) || 1
    BIND_ZSET_COMMANDS("zrangebylex",
        &ZSetCommandHandler::handlerZRangeByLex,
        &ZSetParamsCheck::handlerZRangeByLex,
        &EmptyHandler::setEmptyVector,
        false);
    BIND_ZSET_COMMANDS("zrangebyscore",
        &ZSetCommandHandler::handlerZRangeByScore,
        &ZSetParamsCheck::handlerZRangeByScore,
        &EmptyHandler::setEmptyVector,
        false);
#endif
    // 6.2.0 版本以后命令
#if CHECK_VERSION_GREATER_THEN_OR_EQUAL_BUILD_SUPPORTED_REDIS_VERSION(6, 2, 0)
    BIND_ZSET_COMMANDS("zinter",
        &ZSetCommandHandler::handlerZInter,
        &ZSetParamsCheck::handlerZInter,
        nullptr,
        false);
    BIND_ZSET_COMMANDS("zdiff",
        &ZSetCommandHandler::handlerZDiff,
        &ZSetParamsCheck::handlerZDiff,
        nullptr,
        false);
    BIND_ZSET_COMMANDS("zunion", &ZSetCommandHandler::handlerZUnion, &ZSetParamsCheck::handlerZUnion, nullptr, false);
    BIND_ZSET_COMMANDS("zmscore",
        &ZSetCommandHandler::handlerZMScore,
        &ZSetParamsCheck::handlerZMScore,
        &EmptyHandler::setZSetMScoreNoExists,
        false);
    BIND_ZSET_COMMANDS("zrandmember",
        &ZSetCommandHandler::handlerZRandMember,
        &ZSetParamsCheck::handlerZRandMember,
        &EmptyHandler::nilOrVectorEmpty,
        false);
    BIND_ZSET_COMMANDS("zrangestore",
        &ZSetCommandHandler::handlerZRangeStore,
        &ZSetParamsCheck::handlerZRangeStore,
        &EmptyHandler::setZeroInteger,
        false);
#endif
    // 7.0.0 版本以后命令
#if CHECK_VERSION_GREATER_THEN_OR_EQUAL_BUILD_SUPPORTED_REDIS_VERSION(7, 0, 0)
    BIND_ZSET_COMMANDS("zintercard",
        &ZSetCommandHandler::handlerZInterCard,
        &ZSetParamsCheck::handlerZInterCard,
        nullptr,
        false);
    BIND_ZSET_COMMANDS("zmpop",
        &ZSetCommandHandler::handlerZMPop,
        &ZSetParamsCheck::handlerZMPop,
        nullptr,
        false);
    BIND_ZSET_COMMANDS("bzmpop",
        &ZSetCommandHandler::handlerBZMPop,
        &ZSetParamsCheck::handlerBZMPop,
        nullptr,
        false);
#endif

}

size_t CommandCommonWithServerPtr::delKeyValue (const std::string &key) const {
    auto it = server->keyMap.find(key);
    if (it != server->keyMap.end()) {
        server->delKeyEvent(it);
        return 1;
    }
    return 0;
}
bool CommandCommonWithServerPtr::setExpire (const std::string &key, const std::chrono::milliseconds expire) const {
    return server->setExpire(key, expire);
}
KeyOfValue &CommandCommonWithServerPtr::setNewKey (const StringType &key, StructType structType) const {
    KeyOfValue keyVal(structType, server);

    auto it = server->keyMap.emplace(key, std::move(keyVal));
    return it.first->second;
}
void CommandCommonWithServerPtr::clear () const noexcept {
    server->clear();
}
#endif //KV_STORE_KV_STORE_SERVER_CIRCLE_KV_SERVER_H_
