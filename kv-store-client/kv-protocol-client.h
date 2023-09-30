//
// Created by Yoshiki on 2023/9/29.
//

#ifndef LINUX_SERVER_LIB_KV_STORE_CLIENT_KV_PROTOCOL_CLIENT_H_
#define LINUX_SERVER_LIB_KV_STORE_CLIENT_KV_PROTOCOL_CLIENT_H_
#include "net/kv-protocol.h"

class KvProtocolClient : public KvProtocol
{
public:
    explicit KvProtocolClient (int sockfd)
        : KvProtocolBase(sockfd), KvProtocol(sockfd) {}

    using KvProtocol::KvProtocol;
    using KvProtocol::decodeRecv;

    int encodeSend (const std::string &msg)
    {
        ResValueType res;
        res.model = ResValueType::ReplyModel::REPLY_ARRAY;

        auto vec = Utils::StringHelper::stringSplit(msg, ' ');
        ResValueType vecRes;
        vecRes.model = ResValueType::ReplyModel::REPLY_STRING;
        for (auto &v : vec)
        {
            vecRes.setStringValue(v);
            res.elements.emplace_back(vecRes);
        }

        return KvProtocolSend::encodeSend(res);
    }
};
#endif //LINUX_SERVER_LIB_KV_STORE_CLIENT_KV_PROTOCOL_CLIENT_H_
