//
// Created by Yoshiki on 2023/8/13.
//

#ifndef LINUX_SERVER_LIB_KV_STORE_KV_COMMAND_H_
#define LINUX_SERVER_LIB_KV_STORE_KV_COMMAND_H_

#include <algorithm>
#include "util/string-helper.h"
#include "util/math-helper.h"
#include "../data-structure/kv-value.h"
#include "kv-command-common.h"
#include "kv-string-command.h"
#include "kv-hash-command.h"

class BaseCommandHandler : public CommandCommon
{
protected:
    using ExpireMapType = KvHashTable <KeyType,
                                       std::pair <StructType, std::chrono::milliseconds>>;
    using AllKeyMapType = KvHashTable <KeyType, StructType>;
    enum class Commands
    {
        NIL = -1,
        FLUSHALL,
        DEL,
        EXPIRE,
        PEXPIRE,
        TTL,
        PTTL,
        KEYS,
        FLUSHDB,
        EXISTS,
        TYPE,
        END
    };

    virtual void handlerFlushAll (const CommandParams &commandParams, ResValueType &resValue) = 0;
    virtual void handlerDel (const CommandParams &commandParams, ResValueType &resValue) = 0;
    virtual void handlerExpire (const CommandParams &commandParams, ResValueType &resValue) = 0;
    virtual void handlerPExpire (const CommandParams &commandParams, ResValueType &resValue) = 0;
    virtual void handlerTTL (const CommandParams &commandParams, ResValueType &resValue) = 0;
    virtual void handlerPTTL (const CommandParams &commandParams, ResValueType &resValue) = 0;
    virtual void handlerKeys (const CommandParams &commandParams, ResValueType &resValue) = 0;
    virtual void handlerFlushDb (const CommandParams &commandParams, ResValueType &resValue) = 0;
    virtual void handlerExists (const CommandParams &commandParams, ResValueType &resValue) = 0;
    virtual void handlerType (const CommandParams &commandParams, ResValueType &resValue) = 0;

    inline ResValueType handlerCommand (const CommandParams &commandParams, Commands cmd)
    {
        ResValueType resValue;
        switch (cmd)
        {
            case Commands::FLUSHALL:
                handlerFlushAll(commandParams, resValue);
                break;
            case Commands::DEL:
                handlerDel(commandParams, resValue);
                break;
            case Commands::EXPIRE:
                handlerExpire(commandParams, resValue);
                break;
            case Commands::PEXPIRE:
                handlerPExpire(commandParams, resValue);
                break;
            case Commands::TTL:
                handlerTTL(commandParams, resValue);
                break;
            case Commands::PTTL:
                handlerPTTL(commandParams, resValue);
                break;
            case Commands::KEYS:
                handlerKeys(commandParams, resValue);
                break;
            case Commands::FLUSHDB:
                handlerFlushDb(commandParams, resValue);
                break;
            case Commands::EXISTS:
                handlerExists(commandParams, resValue);
                break;
            case Commands::TYPE:
                handlerType(commandParams, resValue);
                break;
            case Commands::NIL:
            case Commands::END:
                break;
        }

        return resValue;
    }

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

    FIND_COMMAND
protected:
    AllKeyMapType keyOfStructType;
    ExpireMapType expireKey;
    static constexpr const char *commands[] {
        "flushall",
        "del",
        "expire",
        "pexpire",
        "ttl",
        "pttl",
        "keys",
        "flushdb",
        "exists",
        "type"
    };
};

constexpr const char *BaseCommandHandler::commands[];
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
    ResValueType handlerCommand (const CommandParams &commandParams)
    {
        Commands command = findCommand(commandParams.command);
        ResValueType res;
        if (command != Commands::END)
        {
            return BaseCommandHandler::handlerCommand(commandParams, command);
        }

        StructType structType = checkExpireKey(commandParams.key);
        switch (structType)
        {
            case StructType::NIL:
                break;
            case StructType::STRING:
                findStringCommand(commandParams, res);
                return res;
            case StructType::LIST:
                break;
            case StructType::HASH:
                findHashCommand(commandParams, res);
                break;
            case StructType::SET:
                break;
            case StructType::ZSET:
                break;
            case StructType::END:
                break;
        }

        bool foundIt = findStringCommand(commandParams, res) || findHashCommand(commandParams, res);
        if (!foundIt)
            res.setErrorStr(commandParams, ResValueType::ErrorType::UNKNOWN_COMMAND);

        return res;
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
    void handlerFlushAll (const CommandParams &commandParams, ResValueType &resValue) override
    {
        if (!commandParams.key.empty() || !commandParams.params.empty())
        {
            resValue.setErrorStr(commandParams, ResValueType::ErrorType::SYNTAX_ERROR);
            return;
        }
        clear();
        resValue.setOKFlag();
    }
    void handlerDel (const CommandParams &commandParams, ResValueType &resValue) override
    {
        size_t num = delKey(commandParams.key);
        resValue.setIntegerValue(static_cast<IntegerType>(num));
    }
    // 秒
    void handlerExpire (const CommandParams &commandParams, ResValueType &resValue) override
    {
        IntegerType expire;
        StructType structType;
        if (!expireCommandCheck(commandParams, resValue, expire, structType))
            return;

        setExpire(commandParams.key, structType, std::chrono::milliseconds(expire * 1000));
    }
    // 毫秒
    void handlerPExpire (const CommandParams &commandParams, ResValueType &resValue) override
    {
        IntegerType expire;
        StructType structType;
        if (!expireCommandCheck(commandParams, resValue, expire, structType))
            return;

        setExpire(commandParams.key, structType, std::chrono::milliseconds(expire));
    }
    // 秒
    void handlerTTL (const CommandParams &commandParams, ResValueType &resValue) override
    {
        if (!checkKeyIsValid(commandParams, resValue))
            return;

        auto it = expireKey.find(commandParams.key);
        if (it == expireKey.end())
            resValue.setIntegerValue(nilExpire);
        else
            resValue.setIntegerValue((it->second.second - getNow()).count() / 1000);
    }
    // 毫秒
    void handlerPTTL (const CommandParams &commandParams, ResValueType &resValue) override
    {
        if (!checkKeyIsValid(commandParams, resValue))
            return;

        auto it = expireKey.find(commandParams.key);
        if (it == expireKey.end())
            resValue.setIntegerValue(nilExpire);
        else
            resValue.setIntegerValue((it->second.second - getNow()).count());
    }

    void handlerKeys (const CommandParams &commandParams, ResValueType &resValue) override
    {
        // 目前只做keys *
        if (!checkKeyIsValid(commandParams, resValue))
            return;

        if (commandParams.key == "*")
            for (auto &v : keyOfStructType)
                resValue.setVectorValue(v.first);
    }
    void handlerFlushDb (const CommandParams &commandParams, ResValueType &resValue) override
    {
        // 目前不做多库
        handlerFlushAll(commandParams, resValue);
    }
    void handlerExists (const CommandParams &commandParams, ResValueType &resValue) override
    {
        if (!checkKeyIsValid(commandParams, resValue))
            return;

        KeyType key = commandParams.key;
        size_t i = 0;
        IntegerType count = 0;
        AllKeyMapType::iterator it;
        auto end = keyOfStructType.end();
        do
        {
            it = keyOfStructType.find(key);
            if (it != end)
                count++;

            key = commandParams.params[i];
        } while (++i != commandParams.params.size());

        resValue.setIntegerValue(count);
    }
    void handlerType (const CommandParams &commandParams, ResValueType &resValue) override
    {
        if (!checkKeyIsValid(commandParams, resValue)
            || !checkHasParams(commandParams, resValue, 0))
            return;

        auto it = keyOfStructType.find(commandParams.key);
        if (it != keyOfStructType.end())
        {
            switch (it->second)
            {
                case StructType::STRING:
                    resValue.setStringValue("string");
                    return;
                case StructType::LIST:
                    resValue.setStringValue("list");
                    return;
                case StructType::HASH:
                    resValue.setStringValue("hash");
                    return;
                case StructType::SET:
                    resValue.setStringValue("set");
                    return;
                case StructType::ZSET:
                    resValue.setStringValue("zset");
                    return;
                case StructType::END:
                case StructType::NIL:
                    break;
            }
        }

        resValue.setStringValue("none");
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

    inline bool findStringCommand (const CommandParams &commandParams, ResValueType &res)
    {
        StringCommandHandler::Commands
            stringCommand = StringCommandHandler::findCommand(commandParams.command);

        if (stringCommand != StringCommandHandler::Commands::END)
        {
            commandHandlerResByString(commandParams, res, stringCommand);
            return true;
        }

        return false;
    }

    inline bool findHashCommand (const CommandParams &commandParams, ResValueType &res)
    {
        HashCommandHandler::Commands
            hashCommand = HashCommandHandler::findCommand(commandParams.command);

        if (hashCommand != HashCommandHandler::Commands::END)
        {
            commandHandlerResByHash(commandParams, res, hashCommand);
            return true;
        }

        return false;
    }

    inline void commandHandlerResByString (
        const CommandParams &commandParams,
        ResValueType &res,
        const StringCommandHandler::Commands &stringCommand
    )
    {
        if (!checkKeyType(commandParams, StructType::STRING, res))
            return;

        res = std::move(
            stringCommandHandler.handlerCommand(
                commandParams,
                stringCommand
            ));
    }

    inline void commandHandlerResByHash (
        const CommandParams &commandParams,
        ResValueType &res,
        const HashCommandHandler::Commands &hashCommand
    )
    {
        if (!checkKeyType(commandParams, StructType::HASH, res))
            return;

        res = std::move(
            hashCommandHandler.handlerCommand(
                commandParams,
                hashCommand
            ));
    }

private:
    static inline std::chrono::milliseconds getNow () noexcept
    {
        return std::chrono::duration_cast <std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch());
    }

    StringCommandHandler stringCommandHandler;
    HashCommandHandler hashCommandHandler;

    static constexpr int onceCheckExpireKeyMaxNum = 20;
    static constexpr int nilExpire = -2;
    static constexpr std::chrono::milliseconds onceCheckExpireKeyMaxTime { 30 };
};

constexpr std::chrono::milliseconds CommandHandler::onceCheckExpireKeyMaxTime;

#endif //LINUX_SERVER_LIB_KV_STORE_KV_COMMAND_H_
