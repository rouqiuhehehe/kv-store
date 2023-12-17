//
// Created by Yoshiki on 2023/8/3.
//

#ifndef LINUX_SERVER_LIB_INCLUDE_KV_STORE_KV_STORE_H_
#define LINUX_SERVER_LIB_INCLUDE_KV_STORE_KV_STORE_H_
#include "global-config.h"
#include "config/kv-env.h"
#include "util/global.h"

#include "data-structure/kv-incrementally-hash.h"
#include <vector>
#include <numeric>

template <class _Key, class _Val>
using KvHashTable = IncrementallyHashTable <_Key, _Val>;

using StringType = std::string;
using KeyType = StringType;
using IntegerType = long long;
using FloatType = double;
using ValueType = StringType;

template <class T>
using ArrayType = std::vector <T>;

#include "config/kv-config.h"
#include "data-structure/kv-quick-list.h"

using KvListPack = KvListPack;

#include "config/kv-config.h"
#include "config/kv-server-config.h"
#include "data-structure/kv-value.h"

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
        STATUS_CLOSE
    };
    explicit SockEpollPtr (int fd)
        : fd(fd), isListenFd(true), sockaddr(sockaddr_in())
    {
        setAuth();
        updateTimeNow();
    }
    SockEpollPtr (int fd, sockaddr_in &sockaddrIn)
        : fd(fd), isListenFd(false), sockaddr(sockaddrIn)
    {
        setAuth();
        updateTimeNow();
    }

    inline void updateTimeNow () noexcept
    {
        runtime = std::chrono::duration_cast <std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch());
    }

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

private:
    void setAuth ()
    {
        static bool auth = KvConfig::getConfig().requirePass != CONVERT_EMPTY_TO_NULL_VAL;
        needAuth = auth;
        this->auth = !needAuth;
    }
};
using ParamsType = SockEpollPtr;

#include "net/kv-tcp.h"

#endif //LINUX_SERVER_LIB_INCLUDE_KV_STORE_KV_STORE_H_
