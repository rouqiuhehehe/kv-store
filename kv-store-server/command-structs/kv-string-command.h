//
// Created by 115282 on 2023/8/15.
//

#ifndef LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_STRING_COMMAND_H_
#define LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_STRING_COMMAND_H_

class StringCommandHandler : public CommandCommonWithServerPtr
{
    friend class BaseCommandHandler;
    friend class KvDebug;
public:
    using CommandCommonWithServerPtr::CommandCommonWithServerPtr;
    bool handlerBefore (ParamsType &params) override
    {
        if (params.commandParams.command == "set")
        {
            if (!handlerExtraParams(params.commandParams, params.resValue))
                return false;
            if (params.arg)
            {
                // 如果是新创建的，并且SetModel == XX(只在键已经存在时，才对键进行设置操作)，不让操作
                // 如果是已经存在的，并且SetModel == NX(只在键不存在时，才对键进行设置操作)，不让操作
                if ((params.arg->isNewKey && value.setModel == SetModel::XX)
                    || (!params.arg->isNewKey && value.setModel == SetModel::NX))
                {
                    params.resValue.setNilFlag();
                    return false;
                }
                params.arg = nullptr;
            }
        }
        return true;
    }
public:
    void handlerSet (ParamsType &params)
    {
        // 不需要处理拓展参数，在handlerBefore已经处理
        // 因为任务处理是单线程的，所以不需要考虑static数据安全性问题
        // 检查拓展参数 NX|XX EX|PX GET
        // success = handlerExtraParams(params.commandParams, params.resValue);
        // if (!success)
        //     return;

        if (value.isReturnOldValue)
        {
            if (keyValues.empty()) params.resValue.setNilFlag();
            else params.resValue.setStringValue(keyValues);
        }
        else params.resValue.setOKFlag();

        if (value.expire > std::chrono::milliseconds(0))
            setExpire(params.commandParams.key, value.expire);

        keyValues = params.commandParams.params[0];
    }

    void handlerGet (ParamsType &params)
    {
        params.resValue.setStringValue(keyValues);
    }

    void handlerIncr (ParamsType &params)
    {
        handlerIncrCommon(params.commandParams, params.resValue, 1);
    }

    void handlerIncrBy (ParamsType &params)
    {
        handlerIncrCommon(params.commandParams, params.resValue, GET_ARG_VALUE(IntegerType));
    }

    void handlerDecr (ParamsType &params)
    {
        handlerIncrCommon(params.commandParams, params.resValue, -1);
    }

    void handlerDecrBy (ParamsType &params)
    {
        handlerIncrCommon(params.commandParams, params.resValue, -GET_ARG_VALUE(IntegerType));
    }
    void handlerAppend (ParamsType &params)
    {
        keyValues += params.commandParams.params[0];
        params.resValue.setIntegerValue(static_cast<IntegerType>(keyValues.size()));
    }
    void handlerMSet (ParamsType &params)
    {
        keyValues = params.commandParams.params[0];

        // mset多值，需要额外创建key
        if (params.commandParams.params.size() > 1)
        {
            StringType *key = &params.commandParams.params[1];
            StringType *keyVal = &params.commandParams.params[2];
            size_t i = 3;
            for (;;)
            {
                auto &val = setNewKey(*key, StructType::STRING);
                static_cast<StringCommandHandler *>(val.handler)->keyValues = *keyVal;
                if (i == params.commandParams.params.size())
                    break;
                // 设置key
                key = &params.commandParams.params[i++];
                keyVal = &params.commandParams.params[i++];
            }
        }

        params.resValue.setOKFlag();
    }

    void handlerIncrByFloat (ParamsType &params)
    {
        FloatType doubleValue = GET_ARG_VALUE(FloatType);
        FloatType oldValue = 123456789;
        if (keyValues.empty()) oldValue = 0;
        if (oldValue != 0 && !Utils::StringHelper::stringIsDouble(keyValues, &oldValue))
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

            keyValues = Utils::StringHelper::toString(oldValue + doubleValue);
            params.resValue.setStringValue(keyValues);
        }
    }

private:
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
                    if (value.setModel != SetModel::XX)
                        value.setModel = SetModel::NX;
                    else
                    {
                        resValue.setErrorStr(commandParams, ResValueType::ErrorType::SYNTAX_ERROR);
                        return false;
                    }
                }
                else if (command == EXTRA_PARAMS_XX)
                {
                    if (value.setModel != SetModel::NX)
                        value.setModel = SetModel::XX;
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
                        value.expire = std::chrono::milliseconds { integer };
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
                        value.expire = std::chrono::milliseconds { integer * 1000 };
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
        IntegerType res;
        if (keyValues.empty())
        {
            keyValues = Utils::StringHelper::toString(step);
            res = step;
        }
        else
        {
            IntegerType integer;
            if (!Utils::StringHelper::stringIsLL(keyValues, &integer))
            {
                resValue.setErrorStr(commandParams, ResValueType::ErrorType::VALUE_NOT_INTEGER);
                return;
            }
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
                keyValues = Utils::StringHelper::toString(step + integer);
                res = step + integer;
            }
        }

        resValue.setIntegerValue(res);
    }

    ValueType keyValues {};

    static StringValueType value;
};
StringValueType StringCommandHandler::value;
#endif //LINUX_SERVER_LIB_KV_STORE_COMMAND_STRUCTS_KV_STRING_COMMAND_H_
