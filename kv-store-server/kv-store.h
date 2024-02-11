//
// Created by Yoshiki on 2023/8/3.
//

#ifndef LINUX_SERVER_LIB_INCLUDE_KV_STORE_KV_STORE_H_
#define LINUX_SERVER_LIB_INCLUDE_KV_STORE_KV_STORE_H_

#include <string>
#include <vector>

struct SockEpollPtr;
using StringType = std::string;
using KeyType = StringType;
using IntegerType = long long;
using FloatType = double;
using ValueType = StringType;
static constexpr int NilExpire = -2;
using ParamsType = SockEpollPtr;
template <class T>
using ArrayType = std::vector <T>;

#include "data-structure/kv-incrementally-hash.h"

template <class _Key, class _Val>
using HashTable = _HashTable <_Key, std::pair <const _Key, _Val>, std::__detail::_Select1st>;
template <class _Key>
using HashSet = _HashTable <_Key, _Key, std::__detail::_Identity>;
template <class _Key, class _Val>
using KvHashTable = IncrementallyHashTable <_Key, std::pair <const _Key, _Val>, std::__detail::_Select1st>;
template <class _Key>
using KvHashSet = IncrementallyHashTable <_Key, _Key, std::__detail::_Identity>;

#include "config/kv-config.h"
#if !defined(KV_STORE_CLIENT)
#define SET_COMMON_EPOLL_FLAG(event) ((event) | EPOLLHUP | EPOLLRDHUP)
#include "global-config.h"

#include "config/kv-env.h"
#include "util/global.h"

#include "util/string-helper.h"
#include "util/math-helper.h"

#include <numeric>

template <class T,
          class = typename std::enable_if <std::is_integral <typename std::remove_reference <T>::type>::value,
                                           void>::type>
union DataType // NOLINT
{
    struct // NOLINT
    {
        const char *s;
        size_t len;
    } str;
    T val;
};
enum class DataTypeEnum
{
    STRING,
    INTEGER,
    ERROR
};

template <class T = IntegerType>
struct DataUnion
{
    DataType <T> data {};
    DataTypeEnum mode = DataTypeEnum::ERROR;

    DataUnion () = default;

    ~DataUnion () noexcept {
        delete[] copyStr;
    }

    DataUnion (const DataUnion &r) {
        operator=(r);
    }
    DataUnion (DataUnion &&r) noexcept {
        operator=(std::move(r));
    }
    DataUnion &operator= (const DataUnion &r) {
        if (this == &r) return *this;

        mode = r.mode;
        switch (r.mode) {
            case DataTypeEnum::STRING:
                setCopyStringValue(r.data.str.s, r.data.str.len);
                break;
            case DataTypeEnum::INTEGER:
                setIntegerValue(r.data.val);
                break;
            case DataTypeEnum::ERROR:
                break;
        }
        return *this;
    }

    DataUnion &operator= (DataUnion &&r) noexcept {
        if (this == &r) return *this;

        mode = r.mode;
        switch (r.mode) {
            case DataTypeEnum::STRING:
                setStringValue(r.data.str.s, r.data.str.len);
                r.data.str.s = nullptr;
                r.data.str.len = 0;
                break;
            case DataTypeEnum::INTEGER:
                setIntegerValue(r.data.val);
                r.data.val = 0;
                break;
            case DataTypeEnum::ERROR:
                break;
        }

        r.mode = DataTypeEnum::ERROR;
        return *this;
    }

    inline operator StringType () const noexcept {
        return toString();
    }

    inline FloatType toDouble () const {
        FloatType f;
        switch (mode) {
            case DataTypeEnum::STRING:
                return Utils::StringHelper::stringIsDouble({ data.str.s, data.str.len }, &f, false);
            case DataTypeEnum::INTEGER:
                return static_cast<FloatType>(data.val);
            case DataTypeEnum::ERROR:
                KV_ASSERT(false);
        }
        unreachable();
    }

    inline StringType toString () const {
        switch (mode) {
            case DataTypeEnum::STRING:
                return { data.str.s, data.str.len };
            case DataTypeEnum::INTEGER:
                return Utils::StringHelper::toString(data.val);
            case DataTypeEnum::ERROR:
                KV_ASSERT(false);
        }
        unreachable();
    }

    inline size_t size () const noexcept {
        switch (mode) {
            case DataTypeEnum::STRING:
                return data.str.len;
            case DataTypeEnum::INTEGER:
                return Utils::MathHelper::getByteSize(data.val);
            case DataTypeEnum::ERROR:
                KV_ASSERT(false);
        }
        unreachable();
    }

    inline void setStringValue (const char *s, size_t len) {
        mode = DataTypeEnum::STRING;
        data.str.s = s;
        data.str.len = len;
    }

    inline void setStringValue (const StringType &s) {
        setStringValue(s.c_str(), s.size());
    }

    inline void setIntegerValue (T val) {
        mode = DataTypeEnum::INTEGER;
        data.val = val;
    }

    inline void setCopyStringValue (const char *s, size_t len) {
        copyStr = new char[len];
        memcpy(copyStr, s, len);

        mode = DataTypeEnum::STRING;
        data.str.s = copyStr;
        data.str.len = len;
    }

    // 用于拷贝string，避免参数string被释放后，结构体指针发生错误
    inline void setCopyStringValue (const StringType &s) {
        setCopyStringValue(s.c_str(), s.size());
    }

private:
    char *copyStr = nullptr;
};

class EventPoll;
#include "data-structure/kv-quick-list.h"
#include "data-structure/kv-intset.h"
#include "data-structure/kv-skiplist.h"

using KvListPack = KvListPack;
#endif
#include "config/kv-server-config.h"

#include "config/kv-value.h"

class KvServer;
struct ParamsArgs
{
    void *arg = nullptr; // 用于传参时拓展
    StructType structType {};
    bool isNewKey {};
    KvServer *server {};
};
struct SockEpollPtr
{
    enum QUEUE_STATUS
    {
        STATUS_LISTEN,
        STATUE_SLEEP,
        STATUS_RECV,
        STATUS_RECV_DOWN,
        STATUS_RECV_BAD,
        STATUS_SEND,
        STATUS_SEND_DOWN,
        STATUS_SEND_BAD,
        STATUS_CLOSE,
        STATUS_BLOCK
    };
    explicit SockEpollPtr (int fd)
        : fd(fd), isListenFd(true), sockaddr(sockaddr_in()), reactorParams(nullptr) {
        setAuth();
        updateTimeNow();
    }
    SockEpollPtr (int fd, sockaddr_in &sockaddrIn, ReactorParams *circle)
        : fd(fd), isListenFd(false), sockaddr(sockaddrIn), reactorParams(circle) {
        setAuth();
        updateTimeNow();
    }

    inline void updateTimeNow () noexcept {
        runtime = std::chrono::duration_cast <std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch());
    }

    inline bool isBlock () const noexcept { return status == STATUS_BLOCK; }
    inline void setBlock () noexcept { status = STATUS_BLOCK; }
    inline void unBlock () noexcept;

    int fd;
    bool isListenFd;
    std::chrono::milliseconds runtime {};
    struct sockaddr_in sockaddr;
    ResValueType resValue {};
    CommandParams commandParams {};
    bool auth {};
    bool needAuth {};
    StringType username = DEFAULT_USER_NAME;
    QUEUE_STATUS status = STATUS_LISTEN;
    ReactorParams *reactorParams;
    ParamsArgs *arg = nullptr;

private:
    void setAuth () {
        static bool auth = KvConfig::getConfig().requirePass != CONVERT_EMPTY_TO_NULL_VAL;
        needAuth = auth;
        this->auth = !needAuth;
    }
};

#if !defined(KV_STORE_CLIENT)
void EmptyHandler::setEmptyVector (ParamsType &params) {
    params.resValue.setEmptyVectorValue();
}
void EmptyHandler::setNilFlag (ParamsType &params) {
    params.resValue.setNilFlag();
}
void EmptyHandler::setZeroInteger (ParamsType &params) {
    params.resValue.setZeroIntegerValue();
}
void EmptyHandler::setNoSuchKeyError (ParamsType &params) {
    params.resValue.setErrorStr(params.commandParams, ResValueType::ErrorType::NO_SUCH_KEY);
}
void EmptyHandler::setOKFlag (ParamsType &params) {
    params.resValue.setOKFlag();
}
void EmptyHandler::nilOrVectorEmpty (ParamsType &params) {
    if (params.commandParams.params.empty())
        params.resValue.setNilFlag();
    else
        params.resValue.setEmptyVectorValue();
}
void EmptyHandler::setZSetMScoreNoExists (ParamsType &params) {
    params.resValue.setEmptyVectorValue();
    params.resValue.elements.resize(params.commandParams.params.size());
    for (auto &vec: params.resValue.elements)
        vec.setNilFlag();
}
#include "net/kv-tcp.h"
#endif
#endif //LINUX_SERVER_LIB_INCLUDE_KV_STORE_KV_STORE_H_
