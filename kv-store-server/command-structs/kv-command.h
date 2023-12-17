//
// Created by Yoshiki on 2023/8/13.
//

#ifndef LINUX_SERVER_LIB_KV_STORE_KV_COMMAND_H_
#define LINUX_SERVER_LIB_KV_STORE_KV_COMMAND_H_

#include <algorithm>
#include "util/string-helper.h"
#include "util/math-helper.h"
#include "kv-command-common.h"
#include "kv-string-command.h"
#include "kv-hash-command.h"
#include "kv-list-command.h"

#define BIND_COMMANDS(name, fn, emptyFn, structType, b) \
        allCommands.emplace(name, AllCommandsSecond(static_cast<void (CommandCommon::*)(ParamsType &)>(fn),structType, emptyFn, b))
#define BIND_BASE_COMMANDS(name, fn) BIND_COMMANDS(name, fn, nullptr, StructType::BASE, false)
#define BIND_STRING_COMMANDS(name, fn, emptyFn, createdOnNotExist) \
    BIND_COMMANDS(name, fn, emptyFn, StructType::STRING, createdOnNotExist)
#define BIND_HASH_COMMANDS(name, fn, emptyFn, createdOnNotExist) \
    BIND_COMMANDS(name, fn, emptyFn, StructType::HASH, createdOnNotExist)
class CommandHandler;
struct AllCommandsSecond
{
    AllCommandsSecond (
        void (CommandCommon::*handler) (ParamsType &),
        StructType structType,
        void (ResValueType::*emptyHandler) (),
        bool createdOnNotExist
    )
        : handler(handler),
          emptyHandler(emptyHandler),
          structType(structType),
          createdOnNotExist(createdOnNotExist) {}
    void (CommandCommon::*handler) (ParamsType &)  {};
    void (ResValueType::*emptyHandler) () {};
    StructType structType {};
    bool createdOnNotExist;
};
class BaseCommandHandler : public CommandCommon
{
protected:
    class KeyOfValue
    {
        friend class BaseCommandHandler;
        friend class CommandHandler;
    public:
        explicit KeyOfValue (StructType structType)
            : structType(structType)
        {
            switch (structType)
            {
                case StructType::STRING:
                    handler = new StringCommandHandler;
                    break;
                case StructType::LIST:
                    handler = new ListCommandHandler;
                    break;
                case StructType::HASH:
                    handler = new HashCommandHandler;
                    break;
                case StructType::SET:
                    break;
                case StructType::ZSET:
                    break;
                case StructType::NIL:
                case StructType::BASE:
                case StructType::END:
                    unreachable();
                    KV_ASSERT(false);
            }
        }
        ~KeyOfValue () noexcept
        {
            delete handler;
        }
    private:
        StructType structType {};
        std::chrono::milliseconds expire { -2 };
        CommandCommon *handler {};
    };
    using KeyMapType = KvHashTable <KeyType, KeyOfValue>;
    BaseCommandHandler ()
    {
        bindAllCommand();
    }

    virtual void handlerFlushAll (ParamsType &params) = 0;
    virtual void handlerDel (ParamsType &params) = 0;
    virtual void handlerExpire (ParamsType &params) = 0;
    virtual void handlerPExpire (ParamsType &params) = 0;
    virtual void handlerTTL (ParamsType &params) = 0;
    virtual void handlerPTTL (ParamsType &params) = 0;
    virtual void handlerKeys (ParamsType &params) = 0;
    virtual void handlerFlushDb (ParamsType &params) = 0;
    virtual void handlerExists (ParamsType &params) = 0;
    virtual void handlerType (ParamsType &params) = 0;
    virtual void handlerAuth (ParamsType &params) = 0;

    // 检查当前key是否是当前操作的数据结构
    // ex: lpush 不能操作 set 的key
    inline bool checkKeyType (
        const CommandParams &commandParams,
        const KeyMapType::Iterator &it,
        StructType structType,
        ResValueType &res
    )
    {
        if (it != keyMap.end() && it->second.structType != structType)
        {
            res.setErrorStr(commandParams, ResValueType::ErrorType::WRONGTYPE);
            return false;
        }
        return true;
    }

protected:
    void bindAllCommand ()
    {
        BIND_BASE_COMMANDS("flushall", &BaseCommandHandler::handlerFlushAll);
        BIND_BASE_COMMANDS("del", &BaseCommandHandler::handlerDel);
        BIND_BASE_COMMANDS("expire", &BaseCommandHandler::handlerExpire);
        BIND_BASE_COMMANDS("pexpire", &BaseCommandHandler::handlerPExpire);
        BIND_BASE_COMMANDS("ttl", &BaseCommandHandler::handlerTTL);
        BIND_BASE_COMMANDS("pttl", &BaseCommandHandler::handlerPTTL);
        BIND_BASE_COMMANDS("keys", &BaseCommandHandler::handlerKeys);
        BIND_BASE_COMMANDS("flushdb", &BaseCommandHandler::handlerFlushDb);
        BIND_BASE_COMMANDS("exists", &BaseCommandHandler::handlerExists);
        BIND_BASE_COMMANDS("type", &BaseCommandHandler::handlerType);
        BIND_BASE_COMMANDS("auth", &BaseCommandHandler::handlerAuth);

        BIND_STRING_COMMANDS("set", &StringCommandHandler::handlerSet, nullptr, true);
        BIND_STRING_COMMANDS("get",
                             &StringCommandHandler::handlerGet,
                             &ResValueType::setNilFlag,
                             false);
        BIND_STRING_COMMANDS("incr", &StringCommandHandler::handlerIncr, nullptr, true);
        BIND_STRING_COMMANDS("incrby", &StringCommandHandler::handlerIncrBy, nullptr, true);
        BIND_STRING_COMMANDS("incrbyfloat",
                             &StringCommandHandler::handlerIncrByFloat,
                             nullptr,
                             true);
        BIND_STRING_COMMANDS("decr", &StringCommandHandler::handlerDecr, nullptr, true);
        BIND_STRING_COMMANDS("decrby", &StringCommandHandler::handlerDecrBy, nullptr, true);
        BIND_STRING_COMMANDS("append", &StringCommandHandler::handlerAppend, nullptr, true);
        BIND_STRING_COMMANDS("mset", &StringCommandHandler::handlerMSet, nullptr, true);

        BIND_HASH_COMMANDS("hset", &HashCommandHandler::handlerHSet, nullptr, true);
        BIND_HASH_COMMANDS("hget",
                           &HashCommandHandler::handlerHGet,
                           &ResValueType::setNilFlag,
                           false);
        BIND_HASH_COMMANDS("hgetall", &HashCommandHandler::handlerHGetAll, nullptr, false);
        BIND_HASH_COMMANDS("hexists",
                           &HashCommandHandler::handlerHExists,
                           &ResValueType::setEmptyIntegerValue,
                           false);
        BIND_HASH_COMMANDS("hincrby", &HashCommandHandler::handlerHIncrBy, nullptr, true);
        BIND_HASH_COMMANDS("hincrbyfloat", &HashCommandHandler::handlerHIncrByFloat, nullptr, true);
        BIND_HASH_COMMANDS("hlen",
                           &HashCommandHandler::handlerHLen,
                           &ResValueType::setEmptyIntegerValue,
                           false);
        BIND_HASH_COMMANDS("hvals",
                           &HashCommandHandler::handlerHVals,
                           &ResValueType::setEmptyVectorValue,
                           false);
        BIND_HASH_COMMANDS("hkeys",
                           &HashCommandHandler::handlerHKeys,
                           &ResValueType::setEmptyVectorValue,
                           false);
        BIND_HASH_COMMANDS("hsetnx", &HashCommandHandler::handlerHSetNx, nullptr, true);
        BIND_HASH_COMMANDS("hdel",
                           &HashCommandHandler::handlerHDel,
                           &ResValueType::setEmptyIntegerValue,
                           false);
    }

    HashTable <KeyType, AllCommandsSecond> allCommands;
    KeyMapType keyMap {};
};
class CommandHandler final : public BaseCommandHandler
{
public:
    CommandHandler ()
    {
        registerEvents();
    }
    /*
     * 入口函数，处理命令
     * @param params 处理后的命令
     * @return ResValueType
     */
    void handlerCommand (ParamsType &params)
    {
        const CommandParams &commandParams = params.commandParams;
        ResValueType &res = params.resValue;

        auto pa = allCommands.find(commandParams.command);
        if (pa == allCommands.end())
        {
            res.setErrorStr(commandParams, ResValueType::ErrorType::UNKNOWN_COMMAND);
            return;
        }
        // 在设置了密码的情况下，没有使用auth命令登录，不让操作
        if (commandParams.command != "auth"
            && !checkAuth(commandParams, params.needAuth, params.auth, res))
            return;

        if (pa->second.structType != StructType::BASE)
        {
            // 如果不是base类型的命令操作，必须有key
            if (!checkKeyIsValid(params.commandParams, params.resValue))
                return;
            auto it = keyMap.find(params.commandParams.key);
            if (!checkKeyType(params.commandParams, it, pa->second.structType, res))
                return;

            checkExpireKey(it);
            // 处理前置钩子函数
            if (!it->second.handler->handlerBefore(params))
                return;
            (it->second.handler->*(pa->second.handler))(params);
            return;
        }
        this->handlerBefore(params);
        (this->*(pa->second.handler))(params);
    }
    static CommandParams splitCommandParams (ResValueType &recvValue)
    {
        CommandParams commandParams;
        commandParams.command = recvValue.elements[0].value;
        ValueType key;
        if (recvValue.elements.size() > 1)
            commandParams.key = recvValue.elements[1].value;
        if (recvValue.elements.size() > 2)
        {
            commandParams.params.reserve(recvValue.elements.size());
            for (size_t i = 2; i < recvValue.elements.size(); ++i)
                commandParams.params.emplace_back(recvValue.elements[i].value);
        }

        recvValue.elements.clear();
        return commandParams;
    }

    void checkExpireKeys ()
    {
        if (keyMap.empty())
            return;

        auto end = keyMap.end();
        int i = 0;
        auto now = getNow();

        while (i++ < onceCheckExpireKeyMaxNum && (getNow() - now) < onceCheckExpireKeyMaxTime)
        {
            auto it = keyMap.getRandomItem();
            if (it != end)
                checkExpireKey(it);
        }
    }
    bool handlerBefore (ParamsType &params) override
    {
        return true;
    }
protected:
    void handlerFlushAll (ParamsType &params) override
    {
        if (!params.commandParams.key.empty() || !params.commandParams.params.empty())
        {
            params.resValue
                  .setErrorStr(params.commandParams, ResValueType::ErrorType::SYNTAX_ERROR);
            return;
        }
        clear();
        params.resValue.setOKFlag();
    }
    void handlerDel (ParamsType &params) override
    {
        size_t num = keyMap.erase(params.commandParams.key);
        params.resValue.setIntegerValue(static_cast<IntegerType>(num));
    }
    // 秒
    void handlerExpire (ParamsType &params) override
    {
        IntegerType expire;
        StructType structType;
        if (!expireCommandCheck(params.commandParams, params.resValue, expire, structType))
            return;

        setExpire(params.commandParams.key, structType, std::chrono::milliseconds(expire * 1000));
    }
    // 毫秒
    void handlerPExpire (ParamsType &params) override
    {
        IntegerType expire;
        StructType structType;
        if (!expireCommandCheck(params.commandParams, params.resValue, expire, structType))
            return;

        setExpire(params.commandParams.key, structType, std::chrono::milliseconds(expire));
    }
    // 秒
    void handlerTTL (ParamsType &params) override
    {
        if (!checkKeyIsValid(params.commandParams, params.resValue))
            return;

        auto it = keyMap.find(params.commandParams.key);
        if (it == keyMap.end())
            params.resValue.setIntegerValue(nilExpire);
        else
            params.resValue.setIntegerValue((it->second.expire - getNow()).count() / 1000);
    }
    // 毫秒
    void handlerPTTL (ParamsType &params) override
    {
        if (!checkKeyIsValid(params.commandParams, params.resValue))
            return;

        auto it = keyMap.find(params.commandParams.key);
        if (it == keyMap.end())
            params.resValue.setIntegerValue(nilExpire);
        else
            params.resValue.setIntegerValue((it->second.expire - getNow()).count());
    }

    void handlerKeys (ParamsType &params) override
    {
        // 目前只做keys *
        if (!checkKeyIsValid(params.commandParams, params.resValue))
            return;

        if (params.commandParams.key == "*")
            for (auto &v : keyMap)
                params.resValue.setVectorValue(v.first);
    }
    void handlerFlushDb (ParamsType &params) override
    {
        // 目前不做多库
        handlerFlushAll(params);
    }
    void handlerExists (ParamsType &params) override
    {
        if (!checkKeyIsValid(params.commandParams, params.resValue))
            return;

        KeyType key = params.commandParams.key;
        size_t i = 0;
        IntegerType count = 0;
        KeyMapType::iterator it;
        auto end = keyMap.end();
        do
        {
            it = keyMap.find(key);
            if (it != end)
                count++;

            key = params.commandParams.params[i];
        } while (++i != params.commandParams.params.size());

        params.resValue.setIntegerValue(count);
    }
    void handlerType (ParamsType &params) override
    {
        if (!checkKeyIsValid(params.commandParams, params.resValue)
            || !checkHasParams(params.commandParams, params.resValue, 0))
            return;

        auto it = keyMap.find(params.commandParams.key);
        if (it != keyMap.end())
        {
            switch (it->second.structType)
            {
                case StructType::STRING:
                    params.resValue.setStringValue("string");
                    return;
                case StructType::LIST:
                    params.resValue.setStringValue("list");
                    return;
                case StructType::HASH:
                    params.resValue.setStringValue("hash");
                    return;
                case StructType::SET:
                    params.resValue.setStringValue("set");
                    return;
                case StructType::ZSET:
                    params.resValue.setStringValue("zset");
                    return;
                case StructType::BASE:
                case StructType::END:
                case StructType::NIL:
                    break;
            }
        }

        params.resValue.setStringValue("none");
    }

    void handlerAuth (ParamsType &params) override
    {
        if (!checkKeyIsValid(params.commandParams, params.resValue))
            return;

        // 如果不需要输入密码
        if (!params.needAuth)
        {
            params.resValue
                  .setErrorStr(params.commandParams, ResValueType::ErrorType::WITHOUT_PASSWORD);
            return;
        }

        auto &requirePass = KvConfig::getConfig().requirePass;
        if (params.commandParams.key == requirePass)
        {
            params.resValue.setOKFlag();
            params.auth = true;
        }
        else
            params.resValue
                  .setErrorStr(params.commandParams, ResValueType::ErrorType::INVALID_PASSWORD);
    }

private:
    inline void clear () noexcept
    {
        keyMap.clear();
    }

    inline bool expireCommandCheck (
        const CommandParams &commandParams,
        ResValueType &resValue,
        IntegerType &expire,
        StructType &structType
    )
    {
        auto it = keyMap.find(commandParams.key);
        if (it == keyMap.end())
        {
            resValue.setIntegerValue(0);
            return false;
        }
        structType = it->second.structType;
        return checkKeyIsValid(commandParams, resValue)
            && checkHasParams(commandParams, resValue, 1)
            && checkValueIsLongLong(commandParams, resValue, &expire);
    }

    void registerEvents ()
    {
        eventObserver.on(
            ENUM_TO_INT(EventType::RESET_EXPIRE),
            std::bind(&CommandHandler::resetExpire, this, std::placeholders::_1));
    }

    void resetExpire (void *arg)
    {
        auto eventAddObserverParams = static_cast<EventAddObserverParams *>(arg);

        setExpire(
            eventAddObserverParams->key,
            eventAddObserverParams->structType,
            eventAddObserverParams->expire
        );
    }

    void setExpire (
        const std::string &key,
        const StructType structType,
        const std::chrono::milliseconds expire
    )
    {
        auto it = keyMap.find(key);
        if (it != keyMap.end())
            it->second.expire = expire + getNow();
    }

    KeyMapType::iterator delKeyEvent (KeyMapType::iterator &it)
    {
        return keyMap.erase(it);;
    }

    KeyMapType::iterator checkExpireKey (KeyMapType::iterator &it)
    {
        if (it != keyMap.end())
        {
            if (it->second.expire.count() >= 0 && getNow() > it->second.expire)
                return delKeyEvent(it);
        }

        return keyMap.end();
    }

    static inline bool checkAuth (
        const CommandParams &commandParams,
        const bool &needAuth,
        const bool &auth,
        ResValueType &res
    )
    {
        if (needAuth && !auth)
        {
            res.setErrorStr(commandParams, ResValueType::ErrorType::NO_AUTHENTICATION);
            return false;
        }

        return true;
    }

private:
    static inline std::chrono::milliseconds getNow () noexcept
    {
        return std::chrono::duration_cast <std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch());
    }

    static constexpr int onceCheckExpireKeyMaxNum = 20;
    static constexpr int nilExpire = -2;
    static constexpr std::chrono::milliseconds onceCheckExpireKeyMaxTime { 30 };
};

constexpr std::chrono::milliseconds CommandHandler::onceCheckExpireKeyMaxTime;

#endif //LINUX_SERVER_LIB_KV_STORE_KV_COMMAND_H_
