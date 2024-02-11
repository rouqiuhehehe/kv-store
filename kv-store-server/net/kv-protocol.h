//
// Created by 115282 on 2023/9/19.
//

#ifndef LINUX_SERVER_LIB_KV_STORE_NET_KV_PROTOCOL_H_
#define LINUX_SERVER_LIB_KV_STORE_NET_KV_PROTOCOL_H_

#include <config/kv-value.h>
#define CHECK_PROTO \
        if (*(msg + offset) != '\r' || *(msg + offset + 1) != '\n' || offset > len) \
            return -EPROTO;                                                         \
        offset += 2

#define SET_HEAD_M_OFFSET \
        const char *head = msg + offset;    \
        int mOffset = setOffsetUntilEnd(head);  \
        if (mOffset < 0)    \
            return mOffset;                 \
        offset += 2

/*
 * set key 123
 * "*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\n123\r\n"
 *
 * 单行字符串（Simple Strings）： 响应的首字节是 "+"
错误（Errors）： 响应的首字节是 "-"
整型（Integers）： 响应的首字节是 ":"
多行字符串（Bulk Strings）： 响应的首字节是"$"  $0 空字符串
数组（Arrays）： 响应的首字节是 "*"
 **/
class KvProtocolBase
{
protected:
    size_t offset = 0;
    const char *msg {};
    size_t len {};
    std::string sendMsg {};
    int sockfd;
public:
    explicit KvProtocolBase (int sockfd)
        : sockfd(sockfd) {}
};
class KvProtocolRecv : virtual public KvProtocolBase
{
public:
    explicit KvProtocolRecv (int sockfd)
        : KvProtocolBase(sockfd) {}

protected:
    virtual ssize_t decodeRecv (ResValueType &res) {
        ssize_t ret;
        char message[MESSAGE_SIZE_MAX];

        ValueType originalMessage;
        do {
            ret = ::recv(sockfd, message, MESSAGE_SIZE_MAX, 0);
            if (ret == -1 && errno == EINTR)
                continue;

            if (ret > 0)
                originalMessage.append(message, ret);
        } while (ret > 0);

        if (ret < 0 && errno != EINTR && errno != EAGAIN)
            return -errno;
        if (ret == 0)
            return ret;

        if (message[originalMessage.size() - 1] == '\n'
            && message[originalMessage.size() - 2] == '\r') {
            msg = originalMessage.c_str();
            len = originalMessage.length();
            offset = 0;

            return decodeProto(res);
        }

        return -EPROTO;
    }

private:
    int decodeProto (ResValueType &res) {
        switch (*(msg + offset++)) {
            case '*':
                return decodeArrayItem(res);
            case '$':
                return decodeBulkStringsItem(res);
            case '-':
                return decodeErrorsItem(res);
            case '+':
                return decodeStringsItem(res);
            case ':':
                return decodeIntegerItem(res);
            default:
                return -EPROTO;
        }

    }

    int decodeArrayItem (ResValueType &res) {
        IntegerType paramsSize;
        if (!getParamsSize(paramsSize))
            return -EPROTO;

        res.model = ResValueType::ReplyModel::REPLY_ARRAY;
        res.elements.resize(paramsSize);

        int readSize = 0;
        int ret;
        while (readSize < paramsSize) {
            ret = decodeProto(res.elements[readSize++]);
            if (ret < 0)
                return ret;
        }

        return readSize;
    }

    int decodeBulkStringsItem (ResValueType &res) {
        IntegerType paramsSize;
        if (!getParamsSize(paramsSize))
            return -EPROTO;

        res.model = ResValueType::ReplyModel::REPLY_STRING;
        if (paramsSize == 0) {
            res.value.clear();
            return static_cast<int>(paramsSize);
        }

        const char *head = msg + offset;
        offset += paramsSize;
        res.value.append(head, msg + offset - head);

        CHECK_PROTO;

        return static_cast<int>(paramsSize);
    }

    int decodeErrorsItem (ResValueType &res) {
        return decodeItemCommon(res, ResValueType::ReplyModel::REPLY_ERROR);
    }

    int decodeStringsItem (ResValueType &res) {
        return decodeItemCommon(res, ResValueType::ReplyModel::REPLY_STATUS);
    }

    int decodeIntegerItem (ResValueType &res) {
        return decodeItemCommon(res, ResValueType::ReplyModel::REPLY_INTEGER);
    }

    int decodeItemCommon (ResValueType &res, const ResValueType::ReplyModel model) {
        res.model = model;
        res.value.clear();
        SET_HEAD_M_OFFSET;

        res.value.append(head, mOffset);
        CHECK_PROTO;
        return mOffset;
    }

    bool getParamsSize (IntegerType &paramsSize) {
        static std::string buf;
        SET_HEAD_M_OFFSET;

        buf.clear();
        buf.append(head, mOffset);

        bool success = Utils::StringHelper::stringIsLL(buf, &paramsSize);
        if (!success)
            PRINT_ERROR("this string is not number in array head , check %s", buf.c_str());

        return success;
    }

    inline int setOffsetUntilEnd (const char *head) {
        while (*(msg + offset) != '\r' && *(msg + offset + 1) != '\n' && offset < len) ++offset;
        if (offset >= len) {
            PRINT_ERROR("ending identifier error , check %s", head);
            return -EPROTO;
        }

        return static_cast<int>(msg + offset - head);
    }
};

class KvProtocolSend : virtual public KvProtocolBase
{
public:
    explicit KvProtocolSend (int sockfd)
        : KvProtocolBase(sockfd) {}

protected:
    virtual ssize_t encodeSend (const ResValueType &res) {
        size_t sizeCount = 0;
        getSizeCount(res, sizeCount);
        sendMsg.clear();
        sendMsg.reserve(sizeCount);

        int ret = encodeProto(res);
        if (ret < 0)
            return ret;

        return encodeToSend();
    }

private:
    int encodeProto (const ResValueType &res) {
        switch (res.model) {
            case ResValueType::ReplyModel::REPLY_UNKNOWN:
                errno = EINVAL;
                return -errno;
            case ResValueType::ReplyModel::REPLY_STATUS:
                return encodeStringItem(res);
            case ResValueType::ReplyModel::REPLY_ERROR:
                return encodeErrorItem(res);
            case ResValueType::ReplyModel::REPLY_INTEGER:
                return encodeIntegerItem(res);
            case ResValueType::ReplyModel::REPLY_STRING:
                return encodeBulkStringItem(res);
            case ResValueType::ReplyModel::REPLY_ARRAY:
                return encodeArrayItem(res);
        }

        return 0;
    }

    inline int encodeStringItem (const ResValueType &res) {
        sendMsg += '+';
        encodeValueItemCommon(res);
        return 0;
    }

    inline int encodeIntegerItem (const ResValueType &res) {
        sendMsg += ':';
        encodeValueItemCommon(res);
        return 0;
    }

    inline int encodeErrorItem (const ResValueType &res) {
        sendMsg += '-';
        encodeValueItemCommon(res);
        return 0;
    }

    inline int encodeBulkStringItem (const ResValueType &res) {
        sendMsg += '$';
        sendMsg += std::to_string(res.value.size());
        sendMsg += "\r\n";
        encodeValueItemCommon(res);
        return 0;
    }

    inline int encodeArrayItem (const ResValueType &res) {
        sendMsg += '*';
        sendMsg += std::to_string(res.elements.size());
        sendMsg += "\r\n";

        for (const auto &v : res.elements)
            encodeProto(v);

        return 0;
    }

    ssize_t encodeToSend () {
        ssize_t ret;
        do {
            ret = ::send(sockfd, sendMsg.c_str(), sendMsg.size(), 0);
            if (ret == -1 && errno == EINTR)
                continue;
            else
                break;
        } while (true);
        if (ret < 0 && errno != EINTR && errno != EAGAIN)
            return -errno;

        return ret;
    }

    inline void encodeValueItemCommon (const ResValueType &res) {
        sendMsg += res.value;
        sendMsg += "\r\n";
    }

    void getSizeCount (const ResValueType &res, size_t &sizeCount) const {
        if (res.model != ResValueType::ReplyModel::REPLY_ARRAY)
            sizeCount += res.value.size() + 3;
        else {
            for (auto &v : res.elements)
                getSizeCount(v, sizeCount);
        }
    }
};

class KvProtocol : public KvProtocolRecv, public KvProtocolSend
{
public:
    using KvProtocolRecv::decodeRecv;
    using KvProtocolSend::encodeSend;

    explicit KvProtocol (int sockfd)
        : KvProtocolBase(sockfd), KvProtocolRecv(sockfd), KvProtocolSend(sockfd) {}

    inline StringType getSendMsg () const noexcept {
        return sendMsg;
    }
};
#endif //LINUX_SERVER_LIB_KV_STORE_NET_KV_PROTOCOL_H_
