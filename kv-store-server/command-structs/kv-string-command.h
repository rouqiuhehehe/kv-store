//
// Created by 115282 on 2023/8/15.
//

#ifndef LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_STRING_COMMAND_H_
#define LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_STRING_COMMAND_H_
#include "kv-command-common.h"

class StringCommandHandler : public CommandCommon
{
public:
    enum Commands
    {
        NIL = -1,
        SET,
        GET,
        INCR,
        INCRBY,
        INCRBYFLOAT,
        DECR,
        DECRBY,
        APPEND,
        MSET,
        END
    };
    FIND_COMMAND

    inline ResValueType handlerCommand (
        const CommandParams &commandParams,
        Commands cmd
    )
    {
        ResValueType resValue;
        // 检查是否有key
        bool success = checkKeyIsValid(commandParams, resValue);
        if (!success)
            return resValue;

        switch (cmd)
        {
            case Commands::SET:
                handlerSet(commandParams, resValue);
                break;
            case Commands::GET:
                handlerGet(commandParams, resValue);
                break;
            case Commands::INCR:
                handlerIncr(commandParams, resValue);
                break;
            case Commands::INCRBY:
                handlerIncrBy(commandParams, resValue);
                break;
            case Commands::DECR:
                handlerDecr(commandParams, resValue);
                break;
            case Commands::DECRBY:
                handlerDecrBy(commandParams, resValue);
                break;
            case Commands::APPEND:
                handlerAppend(commandParams, resValue);
                break;
            case Commands::MSET:
                handlerMSet(commandParams, resValue);
                break;
            case INCRBYFLOAT:
                handlerIncrByFloat(commandParams, resValue);
                break;
            case Commands::NIL:
            case Commands::END:
                break;
        }

        return resValue;
    }

    inline void clear () noexcept override
    {
        keyValues.clear();
    }
    inline size_t delKey (const std::string &key) noexcept override
    {
        return keyValues.erase(key);
    }
private:
    void handlerSet (
        const CommandParams &commandParams,
        ResValueType &resValue
    )
    {
        // 检查参数长度 是否缺少参数
        bool success = checkHasParams(commandParams, resValue, -1);
        if (!success)
            return;

        // 检查拓展参数 NX|XX EX|PX GET
        success = handlerExtraParams(commandParams, resValue);
        if (!success)
            return;

        // 填充value
        value.value = commandParams.params[0];
        resValue.setOKFlag();
        auto it = keyValues.find(commandParams.key);
        if (it == keyValues.end())
        {
            if (value.setModel == StringValueType::SetModel::NX)
            {
                resValue.setNilFlag();
                return;
            }
            if (value.isReturnOldValue)
                resValue.setNilFlag();

            setNewKeyValue(commandParams.key);
            return;
        }

        if (value.setModel == StringValueType::SetModel::XX)
        {
            resValue.setNilFlag();
            return;
        }
        if (value.isReturnOldValue)
            resValue.setStringValue(it->second);

        if (eventAddObserverParams.expire != std::chrono::milliseconds(0))
            EVENT_OBSERVER_EMIT(EventType::RESET_EXPIRE);

        it->second = value.value;
    }

    void handlerGet (const CommandParams &commandParams, ResValueType &resValue)
    {
        bool success = checkHasParams(commandParams, resValue, 0);
        if (!success)
            return;

        auto it = keyValues.find(commandParams.key);
        if (it == keyValues.end())
            resValue.setNilFlag();
        else
            resValue.setStringValue(it->second);
    }

    void handlerIncr (const CommandParams &commandParams, ResValueType &resValue)
    {
        bool success = checkHasParams(commandParams, resValue, 0);
        if (!success)
            return;

        handlerIncrCommon(commandParams, resValue, 1);
    }

    void handlerIncrBy (const CommandParams &commandParams, ResValueType &resValue)
    {
        bool success = checkHasParams(commandParams, resValue, 1);
        if (!success)
            return;

        IntegerType integer;
        if (!checkValueIsLongLong(commandParams, resValue, &integer))
            return;

        handlerIncrCommon(commandParams, resValue, integer);
    }

    void handlerDecr (const CommandParams &commandParams, ResValueType &resValue)
    {
        bool success = checkHasParams(commandParams, resValue, 0);
        if (!success)
            return;

        handlerIncrCommon(commandParams, resValue, -1);
    }

    void handlerDecrBy (const CommandParams &commandParams, ResValueType &resValue)
    {
        bool success = checkHasParams(commandParams, resValue, 1);
        if (!success)
            return;

        IntegerType integer;
        if (!checkValueIsLongLong(commandParams, resValue, &integer))
            return;

        handlerIncrCommon(commandParams, resValue, -integer);
    }
    void handlerAppend (const CommandParams &commandParams, ResValueType &resValue)
    {
        if (!checkKeyIsValid(commandParams, resValue))
            return;
        if (!checkHasParams(commandParams, resValue, 1))
            return;

        auto it = keyValues.find(commandParams.key);
        if (it == keyValues.end())
        {
            value.value = commandParams.params[0];
            setNewKeyValue(commandParams.key);

            resValue.setIntegerValue(value.value.size());
        }
        else
        {
            it->second += commandParams.params[0];
            resValue.setIntegerValue(it->second.size());
        }
    }
    void handlerMSet (const CommandParams &commandParams, ResValueType &resValue)
    {
        // 因为第一个key被截出来了，所以commandParams.params.size()必须是奇数才是正确的
        if ((commandParams.params.size() & 1) == 0)
        {
            resValue.setErrorStr(commandParams, ResValueType::ErrorType::WRONG_NUMBER);
            return;
        }
        CommandParams loopParams;
        loopParams.command = commands[ENUM_TO_INT(Commands::SET)];
        loopParams.key = commandParams.key;
        size_t i = 0;
        // 设置value
        loopParams.params.emplace_back(commandParams.params[i++]);
        for (;;)
        {
            handlerSet(loopParams, resValue);

            if (i == commandParams.params.size())
                break;
            // 设置key
            loopParams.key = commandParams.params[i++];
            loopParams.params[0] = commandParams.params[i++];
        }
    }

    void handlerIncrByFloat (const CommandParams &commandParams, ResValueType &resValue)
    {
        bool success = checkHasParams(commandParams, resValue, 1);
        if (!success)
            return;

        FloatType doubleValue;
        if (!Utils::StringHelper::stringIsDouble(commandParams.params[0], &doubleValue))
        {
            resValue.setErrorStr(commandParams, ResValueType::ErrorType::VALUE_NOT_FLOAT);
            return;
        }

        auto it = keyValues.find(commandParams.key);
        if (it == keyValues.end())
        {
            value.value = Utils::StringHelper::toString(doubleValue);
            setNewKeyValue(commandParams.key);

            resValue.setStringValue(value.value);
        }
        else
        {
            FloatType oldValue;
            if (!Utils::StringHelper::stringIsDouble(it->second, &oldValue))
                resValue.setErrorStr(commandParams, ResValueType::ErrorType::VALUE_NOT_FLOAT);
            else
            {
                if (Utils::MathHelper::doubleCalculateWhetherOverflow <std::plus <FloatType>>(
                    oldValue,
                    doubleValue
                ))
                {
                    resValue.setErrorStr(
                        commandParams,
                        ResValueType::ErrorType::INCR_OR_DECR_OVERFLOW
                    );
                    return;
                }

                it->second = std::move(Utils::StringHelper::toString(oldValue + doubleValue));
                resValue.setStringValue(it->second);
            }
        }
    }

private:
    // 设置key ，如果有过期时间需要提前设置
    void setNewKeyValue (const std::string &key)
    {
        CommandCommon::setNewKeyValue(key, StructType::STRING);

        keyValues.emplace(key, value.value);
    }

    static bool handlerExtraParams (
        const CommandParams &commandParams,
        ResValueType &resValue
    )
    {
        // 有额外选项 nx|xx get ex|px
        // set key 123 nx get ex 1000
        // 第一个值为value 不处理
        if (commandParams.params.size() > 1)
        {
            std::string command;
            IntegerType integer;
            auto it = commandParams.params.begin() + 1;
            for (; it < commandParams.params.end(); ++it)
            {
                command = *it;
                Utils::StringHelper::stringTolower(command);
                if (command == EXTRA_PARAMS_NX)
                {
                    if (value.setModel != StringValueType::SetModel::XX)
                        value.setModel = StringValueType::SetModel::NX;
                    else
                    {
                        resValue.setErrorStr(commandParams, ResValueType::ErrorType::SYNTAX_ERROR);
                        return false;
                    }
                }
                else if (command == EXTRA_PARAMS_XX)
                {
                    if (value.setModel != StringValueType::SetModel::NX)
                        value.setModel = StringValueType::SetModel::XX;
                    else
                    {
                        resValue.setErrorStr(commandParams, ResValueType::ErrorType::SYNTAX_ERROR);
                        return false;
                    }
                }
                else if (command == EXTRA_PARAMS_GET)
                    value.isReturnOldValue = true;
                else if (command == EXTRA_PARAMS_PX)
                {
                    if (value.timeModel != StringValueType::TimeModel::EX)
                    {
                        it++;
                        if (!checkValueIsLongLong(commandParams, *it, resValue, &integer))
                            return false;
                        if (!checkExpireIsValid(commandParams, integer, resValue))
                            return false;
                        value.timeModel = StringValueType::TimeModel::PX;
                        eventAddObserverParams.expire = std::chrono::milliseconds { integer };
                    }
                    else
                    {
                        resValue.setErrorStr(commandParams, ResValueType::ErrorType::SYNTAX_ERROR);
                        return false;
                    }
                }
                else if (command == EXTRA_PARAMS_EX)
                {
                    if (value.timeModel != StringValueType::TimeModel::PX)
                    {
                        it++;
                        if (!checkValueIsLongLong(commandParams, *it, resValue, &integer))
                            return false;
                        if (!checkExpireIsValid(commandParams, integer, resValue))
                            return false;
                        value.timeModel = StringValueType::TimeModel::EX;
                        eventAddObserverParams.expire =
                            std::chrono::milliseconds { integer * 1000 };
                    }
                    else
                    {
                        resValue.setErrorStr(commandParams, ResValueType::ErrorType::SYNTAX_ERROR);
                        return false;
                    }
                }
                else
                {
                    resValue.setErrorStr(commandParams, ResValueType::ErrorType::SYNTAX_ERROR);
                    return false;
                }
            }
        }

        return true;
    }

    void handlerIncrCommon (
        const CommandParams &commandParams,
        ResValueType &resValue,
        const IntegerType step
    )
    {
        auto it = keyValues.find(commandParams.key);
        if (it == keyValues.end())
        {
            value.value = std::to_string(step);
            setNewKeyValue(commandParams.key);

            resValue.setIntegerValue(step);
        }
        else
        {
            IntegerType integer;
            if (!Utils::StringHelper::stringIsLL(it->second, &integer))
                resValue.setErrorStr(commandParams, ResValueType::ErrorType::VALUE_NOT_INTEGER);
            else
            {
                if (Utils::MathHelper::integerPlusOverflow(integer, step))
                {
                    resValue.setErrorStr(
                        commandParams,
                        ResValueType::ErrorType::INCR_OR_DECR_OVERFLOW
                    );
                    return;
                }
                it->second = std::to_string(step + integer);
                resValue.setIntegerValue(step + integer);
            }
        }
    }

    KvHashTable <KeyType, ValueType> keyValues {};
    static constexpr const char *commands[]
        { "set", "get", "incr", "incrby", "incrbyfloat", "decr", "decrby", "append", "mset" };

    static StringValueType value;
};
StringValueType StringCommandHandler::value;
constexpr const char *StringCommandHandler::commands[];
#endif //LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_STRING_COMMAND_H_
