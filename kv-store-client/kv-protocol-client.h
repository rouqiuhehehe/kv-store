//
// Created by Yoshiki on 2023/9/29.
//

#ifndef LINUX_SERVER_LIB_KV_STORE_CLIENT_KV_PROTOCOL_CLIENT_H_
#define LINUX_SERVER_LIB_KV_STORE_CLIENT_KV_PROTOCOL_CLIENT_H_
#include "data-structure/kv-incrementally-hash.h"
#include <vector>
template <class _Key, class _Val>
using KvHashTable = IncrementallyHashTable <_Key, _Val>;

using StringType = std::string;
using KeyType = StringType;
using IntegerType = long long;
using FloatType = double;
using ValueType = StringType;
template <class T>
using ArrayType = std::vector <T>;
#include "data-structure/kv-value.h"
#include "net/kv-protocol.h"

class KvProtocolClient : public KvProtocol
{
public:
    explicit KvProtocolClient (int sockfd)
        : KvProtocolBase(sockfd), KvProtocol(sockfd) {}

    using KvProtocol::KvProtocol;
    using KvProtocol::decodeRecv;

    ssize_t encodeSend (const std::string &msg)
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
