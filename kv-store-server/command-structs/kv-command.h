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

#define BIND_COMMANDS(structType, name, fn, self) allCommands.emplace(name, std::make_pair(structType, std::bind(fn, self, std::placeholders::_1)))
#define BIND_BASE_COMMANDS(name, fn) BIND_COMMANDS(StructType::NIL, name, fn, this)
#define BIND_STRING_COMMANDS(name, fn) BIND_COMMANDS(StructType::STRING, name, fn, &stringCommandHandler)
#define BIND_HASH_COMMANDS(name, fn) BIND_COMMANDS(StructType::HASH, name, fn, &hashCommandHandler)

class BaseCommandHandler : public CommandCommon
{
protected:
    using ExpireMapType = KvHashTable <KeyType,
                                       std::pair <StructType, std::chrono::milliseconds>>;
    using AllKeyMapType = KvHashTable <KeyType, StructType>;
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
        StructType structType,
        ResValueType &res
    )
    {
        auto it = keyOfStructType.find(commandParams.key);
        if (it != keyOfStructType.end() && it->second != structType)
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

        BIND_STRING_COMMANDS("set",
            static_cast<void (StringCommandHandler::*) (ParamsType &)>(&StringCommandHandler::handlerSet));
        BIND_STRING_COMMANDS("get", &StringCommandHandler::handlerGet);
        BIND_STRING_COMMANDS("incr", &StringCommandHandler::handlerIncr);
        BIND_STRING_COMMANDS("incrby", &StringCommandHandler::handlerIncrBy);
        BIND_STRING_COMMANDS("incrbyfloat", &StringCommandHandler::handlerIncrByFloat);
        BIND_STRING_COMMANDS("decr", &StringCommandHandler::handlerDecr);
        BIND_STRING_COMMANDS("decrby", &StringCommandHandler::handlerDecrBy);
        BIND_STRING_COMMANDS("append", &StringCommandHandler::handlerAppend);
        BIND_STRING_COMMANDS("mset", &StringCommandHandler::handlerMSet);

        BIND_HASH_COMMANDS("hset", &HashCommandHandler::handlerHSet);
        BIND_HASH_COMMANDS("hget", &HashCommandHandler::handlerHGet);
        BIND_HASH_COMMANDS("hgetall", &HashCommandHandler::handlerHGetAll);
        BIND_HASH_COMMANDS("hexists", &HashCommandHandler::handlerHExists);
        BIND_HASH_COMMANDS("hincrby", &HashCommandHandler::handlerHIncrBy);
        BIND_HASH_COMMANDS("hincrbyfloat", &HashCommandHandler::handlerHIncrByFloat);
        BIND_HASH_COMMANDS("hlen", &HashCommandHandler::handlerHLen);
        BIND_HASH_COMMANDS("hvals", &HashCommandHandler::handlerHVals);
        BIND_HASH_COMMANDS("hkeys", &HashCommandHandler::handlerHKeys);
        BIND_HASH_COMMANDS("hsetnx", &HashCommandHandler::handlerHSetNx);
        BIND_HASH_COMMANDS("hdel", &HashCommandHandler::handlerHDel);
    }

    AllKeyMapType keyOfStructType;
    ExpireMapType expireKey;
    StringCommandHandler stringCommandHandler;
    HashCommandHandler hashCommandHandler;

    HashTable <KeyType,
               std::pair <StructType, std::function <void (ParamsType &)>>>
        allCommands;
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
        if (pa->second.first != StructType::NIL)
            checkExpireKey(commandParams.key);
        // key 类型不同不让操作
        if (!checkKeyType(commandParams, pa->second.first, res))
            return;
        // 在设置了密码的情况下，没有使用auth命令登录，不让操作
        if (commandParams.command != "auth"
            && !checkAuth(commandParams, params.needAuth, params.auth, res))
            return;

        pa->second.second(params);
    }
    static CommandParams splitCommandParams (const ResValueType &recvValue)
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

        return commandParams;
    }

    void checkExpireKeys ()
    {
        if (expireKey.empty())
            return;

        auto it = expireKey.begin();
        auto end = expireKey.end();
        ExpireMapType::iterator resIt;
        int i = 0;
        auto now = getNow();

        while (i++ < onceCheckExpireKeyMaxNum && (getNow() - now) < onceCheckExpireKeyMaxTime
            && it != end)
        {
            resIt = checkExpireKey(it);
            if (resIt != end)
                it = resIt;
            else
                ++it;
        }
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
        size_t num = delKey(params.commandParams.key);
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

        auto it = expireKey.find(params.commandParams.key);
        if (it == expireKey.end())
            params.resValue.setIntegerValue(nilExpire);
        else
            params.resValue.setIntegerValue((it->second.second - getNow()).count() / 1000);
    }
    // 毫秒
    void handlerPTTL (ParamsType &params) override
    {
        if (!checkKeyIsValid(params.commandParams, params.resValue))
            return;

        auto it = expireKey.find(params.commandParams.key);
        if (it == expireKey.end())
            params.resValue.setIntegerValue(nilExpire);
        else
            params.resValue.setIntegerValue((it->second.second - getNow()).count());
    }

    void handlerKeys (ParamsType &params) override
    {
        // 目前只做keys *
        if (!checkKeyIsValid(params.commandParams, params.resValue))
            return;

        if (params.commandParams.key == "*")
            for (auto &v : keyOfStructType)
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
        AllKeyMapType::iterator it;
        auto end = keyOfStructType.end();
        do
        {
            it = keyOfStructType.find(key);
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

        auto it = keyOfStructType.find(params.commandParams.key);
        if (it != keyOfStructType.end())
        {
            switch (it->second)
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
    inline void clear () noexcept override
    {
        expireKey.clear();
        keyOfStructType.clear();
        stringCommandHandler.clear();
    }
    inline size_t delKey (const std::string &key) noexcept override
    {
        return delKeyEvent(key);
    }

    inline bool expireCommandCheck (
        const CommandParams &commandParams,
        ResValueType &resValue,
        IntegerType &expire,
        StructType &structType
    )
    {
        auto it = keyOfStructType.find(commandParams.key);
        if (it == keyOfStructType.end())
        {
            resValue.setIntegerValue(0);
            return false;
        }
        structType = it->second;
        return checkKeyIsValid(commandParams, resValue)
            && checkHasParams(commandParams, resValue, 1)
            && checkValueIsLongLong(commandParams, resValue, &expire);
    }

    void registerEvents ()
    {
        eventObserver.on(
            ENUM_TO_INT(EventType::ADD_KEY),
            std::bind(&CommandHandler::addKeyEvent, this, std::placeholders::_1));
        eventObserver.on(
            ENUM_TO_INT(EventType::RESET_EXPIRE),
            std::bind(&CommandHandler::resetExpire, this, std::placeholders::_1));
        eventObserver.on(
            ENUM_TO_INT(EventType::DEL_KEY),
            std::bind(
                static_cast<void (CommandHandler::*) (void *)>(&CommandHandler::delKeyEvent),
                this,
                std::placeholders::_1
            ));
    }

    void addKeyEvent (void *arg)
    {
        auto eventAddObserverParams = static_cast<EventAddObserverParams *>(arg);
        keyOfStructType.emplace(eventAddObserverParams->key, eventAddObserverParams->structType);

        resetExpire(arg);
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

    void delKeyEvent (void *arg)
    {
        auto eventAddObserverParams = static_cast<EventAddObserverParams *>(arg);

        delKeyEvent(eventAddObserverParams->key);
    }

    void setExpire (
        const std::string &key,
        const StructType structType,
        const std::chrono::milliseconds expire
    )
    {
        if (expire > std::chrono::milliseconds(0))
        {
            auto it = expireKey.emplace(key, std::make_pair(structType, expire + getNow()));
            if (!it.second)
                it.first->second.second = expire + getNow();
        }
    }

    size_t delKeyEvent (const std::string &key)
    {
        auto it = expireKey.find(key);
        // 没有过期key
        if (it == expireKey.end())
        {
            auto keyIt = keyOfStructType.find(key);
            return delKeyEvent(keyIt);
        }
            // 有过期的key
        else
        {
            if (delKeyEvent(it) == expireKey.end())
                return 0;
        }

        return 1;
    }

    size_t delKeyEvent (AllKeyMapType::iterator &it)
    {
        if (it == keyOfStructType.end())
            return 0;

        keyOfStructType.erase(it);
        switchDelCommon(it->first, it->second);

        return 1;
    }

    ExpireMapType::iterator delKeyEvent (ExpireMapType::iterator &it)
    {
        size_t num = keyOfStructType.erase(it->first);
        ExpireMapType::iterator resIt;
        // 如果num为0则key不存在
        if (num)
        {
            const StructType structType = it->second.first;
            resIt = expireKey.erase(it);

            switchDelCommon(it->first, structType);
        }

        return resIt;
    }

    void switchDelCommon (const std::string &key, StructType structType)
    {
        switch (structType)
        {
            case StructType::STRING:
                stringCommandHandler.delKey(key);
                break;
            case StructType::LIST:
                break;
            case StructType::HASH:
                hashCommandHandler.delKey(key);
                break;
            case StructType::SET:
                break;
            case StructType::ZSET:
                break;
            case StructType::NIL:
                break;
            case StructType::END:
                break;
        }
    }

    StructType checkExpireKey (const std::string &key)
    {
        auto it = expireKey.find(key);
        if (checkExpireKey(it) != expireKey.end())
            return it->second.first;

        return StructType::NIL;
    }

    ExpireMapType::iterator checkExpireKey (ExpireMapType::iterator &it)
    {
        if (it != expireKey.end())
        {
            if (getNow() > it->second.second)
                return delKeyEvent(it);
        }

        return expireKey.end();
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
