//
// Created by Yoshiki on 2023/8/13.
//

#ifndef LINUX_SERVER_LIB_KV_STORE_KV_COMMAND_H_
#define LINUX_SERVER_LIB_KV_STORE_KV_COMMAND_H_

class BaseCommandHandler final : public CommandCommonWithServerPtr
{

public:
    using CommandCommonWithServerPtr::CommandCommonWithServerPtr;

    bool handlerBefore (ParamsType &params) override
    {
        return true;
    }


    void handlerFlushAll (ParamsType &params)
    {
        clear();
        params.resValue.setOKFlag();
    }

    void handlerFlushDb (ParamsType &params)
    {
        // 目前不做多库
        handlerFlushAll(params);
    }

    void handlerAuth (ParamsType &params)
    {
        auto &requirePass = KvConfig::getConfig().requirePass;
        if (params.commandParams.key == requirePass)
        {
            params.resValue.setOKFlag();
            params.auth = true;
        }
        else
            params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::INVALID_PASSWORD);
    }
};

#endif //LINUX_SERVER_LIB_KV_STORE_KV_COMMAND_H_
