//
// Created by 115282 on 2024/1/2.
//

#ifndef KV_STORE_KV_STORE_SERVER_COMMAND_STRUCTS_KV_KEY_COMMAND_H_
#define KV_STORE_KV_STORE_SERVER_COMMAND_STRUCTS_KV_KEY_COMMAND_H_
class KeyCommandHandler : public CommandCommonWithServerPtr
{
public:
    using CommandCommonWithServerPtr::CommandCommonWithServerPtr;

    void handlerDel (ParamsType &params) {
        params.commandParams.params.emplace_back(params.commandParams.key);
        size_t num = 0;
        for (const auto &v : params.commandParams.params)
            num += delKeyValue(v);

        params.resValue.setIntegerValue(static_cast<IntegerType>(num));
    }
    // 秒
    void handlerExpire (ParamsType &params) {
        params.resValue.setIntegerValue(
            setExpire(
                params.commandParams.key,
                std::chrono::milliseconds(GET_ARG_VALUE(IntegerType) * 1000))
        );
    }
    // 毫秒
    void handlerPExpire (ParamsType &params) {
        params.resValue.setIntegerValue(
            setExpire(params.commandParams.key, std::chrono::milliseconds(GET_ARG_VALUE(IntegerType)))
        );
    }
    // 秒
    void handlerTTL (ParamsType &params) {
        auto it = server->keyMap.find(params.commandParams.key);
        if (it == server->keyMap.end())
            params.resValue.setIntegerValue(NilExpire);
        else
            params.resValue
                  .setIntegerValue(
                      it->second.expire.count() > 0
                      ? ((it->second.expire - getNow()).count() / 1000)
                      : it->second.expire.count());
    }
    // 毫秒
    void handlerPTTL (ParamsType &params) {
        auto it = server->keyMap.find(params.commandParams.key);
        if (it == server->keyMap.end())
            params.resValue.setIntegerValue(NilExpire);
        else
            params.resValue
                  .setIntegerValue(
                      it->second.expire.count() > 0
                      ? ((it->second.expire - getNow()).count())
                      : it->second.expire.count());
    }

    void handlerKeys (ParamsType &params) {
        if (params.commandParams.key == "*")
            for (auto &v : server->keyMap)
                params.resValue.setVectorValue(v.first);
    }

    void handlerExists (ParamsType &params) {
        KeyType key = params.commandParams.key;
        size_t i = 0;
        IntegerType count = 0;
        KvServer::KeyMapType::iterator it;
        auto end = server->keyMap.end();
        do {
            it = server->keyMap.find(key);
            if (it != end)
                count++;

            key = params.commandParams.params[i];
        } while (++i != params.commandParams.params.size());

        params.resValue.setIntegerValue(count);
    }
    void handlerType (ParamsType &params) {
        auto it = server->keyMap.find(params.commandParams.key);
        if (it != server->keyMap.end()) {
            switch (it->second.structType) {
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
                case StructType::KEY:
                    break;
            }
        }

        params.resValue.setStringValue("none");
    }
    bool handlerBefore (ParamsType &params) override {
        return true;
    }
};
#endif //KV_STORE_KV_STORE_SERVER_COMMAND_STRUCTS_KV_KEY_COMMAND_H_
