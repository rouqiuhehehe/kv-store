//
// Created by 115282 on 2023/8/15.
//

#ifndef LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_STRING_COMMAND_H_
#define LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_STRING_COMMAND_H_

class StringCommandHandler : public CommandCommon
{
    friend class BaseCommandHandler;
public:
    bool handlerBefore (ParamsType &params) override
    {
        return true;
    }
private:
    void handlerSet (ParamsType &params)
    {
        handlerSet(params.commandParams, params.resValue);
    }

    void handlerGet (ParamsType &params)
    {
        bool success = checkHasParams(params.commandParams, params.resValue, 0);
        if (!success)
            return;

        if (!this)
            params.resValue.setNilFlag();
        else
            params.resValue.setStringValue(keyValues);
    }

    void handlerIncr (ParamsType &params)
    {
        bool success = checkHasParams(params.commandParams, params.resValue, 0);
        if (!success)
            return;

        handlerIncrCommon(params.commandParams, params.resValue, 1);
    }

    void handlerIncrBy (ParamsType &params)
    {
        bool success = checkHasParams(params.commandParams, params.resValue, 1);
        if (!success)
            return;

        IntegerType integer;
        if (!checkValueIsLongLong(params.commandParams, params.resValue, &integer))
            return;

        handlerIncrCommon(params.commandParams, params.resValue, integer);
    }

    void handlerDecr (ParamsType &params)
    {
        bool success = checkHasParams(params.commandParams, params.resValue, 0);
        if (!success)
            return;

        handlerIncrCommon(params.commandParams, params.resValue, -1);
    }

    void handlerDecrBy (ParamsType &params)
    {
        bool success = checkHasParams(params.commandParams, params.resValue, 1);
        if (!success)
            return;

        IntegerType integer;
        if (!checkValueIsLongLong(params.commandParams, params.resValue, &integer))
            return;

        handlerIncrCommon(params.commandParams, params.resValue, -integer);
    }
    void handlerAppend (ParamsType &params)
    {
        if (!checkKeyIsValid(params.commandParams, params.resValue))
            return;
        if (!checkHasParams(params.commandParams, params.resValue, 1))
            return;

        auto it = keyValues.find(params.commandParams.key);
        if (it == keyValues.end())
        {
            value.value = params.commandParams.params[0];
            setNewKeyValue(params.commandParams.key);

            params.resValue.setIntegerValue(static_cast<IntegerType>(value.value.size()));
        }
        else
        {
            it->second += params.commandParams.params[0];
            params.resValue.setIntegerValue(static_cast<IntegerType>(it->second.size()));
        }
    }
    void handlerMSet (ParamsType &params)
    {
        // 因为第一个key被截出来了，所以commandParams.params.size()必须是奇数才是正确的
        if ((params.commandParams.params.size() & 1) == 0)
        {
            params.resValue
                  .setErrorStr(params.commandParams, ResValueType::ErrorType::WRONG_NUMBER);
            return;
        }
        CommandParams loopParams;
        loopParams.command = "set";
        loopParams.key = params.commandParams.key;
        size_t i = 0;
        // 设置value
        loopParams.params.emplace_back(params.commandParams.params[i++]);
        for (;;)
        {
            handlerSet(loopParams, params.resValue);

            if (i == params.commandParams.params.size())
                break;
            // 设置key
            loopParams.key = params.commandParams.params[i++];
            loopParams.params[0] = params.commandParams.params[i++];
        }
    }

    void handlerIncrByFloat (ParamsType &params)
    {
        bool success = checkHasParams(params.commandParams, params.resValue, 1);
        if (!success)
            return;

        FloatType doubleValue;
        if (!Utils::StringHelper::stringIsDouble(params.commandParams.params[0], &doubleValue))
        {
            params.resValue
                  .setErrorStr(params.commandParams, ResValueType::ErrorType::VALUE_NOT_FLOAT);
            return;
        }

        auto it = keyValues.find(params.commandParams.key);
        if (it == keyValues.end())
        {
            value.value = Utils::StringHelper::toString(doubleValue);
            setNewKeyValue(params.commandParams.key);

            params.resValue.setStringValue(value.value);
        }
        else
        {
            FloatType oldValue;
            if (!Utils::StringHelper::stringIsDouble(it->second, &oldValue))
                params.resValue
                      .setErrorStr(params.commandParams, ResValueType::ErrorType::VALUE_NOT_FLOAT);
            else
            {
                if (Utils::MathHelper::doubleCalculateWhetherOverflow <std::plus <FloatType>>(
                    oldValue,
                    doubleValue
                ))
                {
                    params.resValue.setErrorStr(
                        params.commandParams,
                        ResValueType::ErrorType::INCR_OR_DECR_OVERFLOW
                    );
                    return;
                }

                it->second = Utils::StringHelper::toString(oldValue + doubleValue);
                params.resValue.setStringValue(it->second);
            }
        }
    }

private:
    void handlerSet (const CommandParams &commandParams, ResValueType &resValue)
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

    ValueType keyValues {};

    static StringValueType value;
};
StringValueType StringCommandHandler::value;
#endif //LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_STRING_COMMAND_H_
