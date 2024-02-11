//
// Created by Yoshiki on 2023/8/7.
//

#ifndef LINUX_SERVER_LIB_KV_STORE_EPOLL_H_
#define LINUX_SERVER_LIB_KV_STORE_EPOLL_H_

#include "circle/kv-server.h"
#include <unistd.h>
#include <functional>
#include "printf-color.h"
#include <thread>
#include <set>
#include <algorithm>
#include <mutex>
#include <atomic>
#include "circle/kv-circle.h"

#include "kv-protocol.h"
#include "util/net.h"

#define PERIOD_FREQUENCY (1000 / hz)
#define RUN_WITH_PERIOD(period) if((period) <= PERIOD_FREQUENCY || (cycleCount % ((period) / PERIOD_FREQUENCY)) == 0)
class Tcp;

struct CompareSockEpollPtr
{
    inline bool operator() (const ParamsType *a, const ParamsType *b) const noexcept {
        return a->runtime < b->runtime;
    }
};

class Reactor
{
protected:
    using Callback = std::function <void (ParamsType &)>;

public:
    template <class T>
    Reactor (T &&acceptCb, T &&recvCb, T &&sendCb)
        : acceptCb_(std::forward <T>(acceptCb)),
          recvCb_(std::forward <T>(recvCb)),
          sendCb_(std::forward <T>(sendCb)) {}

    virtual ~Reactor () noexcept = default;

    inline void accept (ParamsType &params) const {
        acceptCb_(params);
    }
    inline void recv (ParamsType &params) const {
        recvCb_(params);
    }
    inline void send (ParamsType &params) const {
        sendCb_(params);
    }
private:
    Callback acceptCb_;
    Callback recvCb_;
    Callback sendCb_;
};

class EventPoll
{
public:
    explicit EventPoll (int size = 1024)
        : epfd(epoll_create(size)) {
        // 用于退出epoll_wait 阻塞
        unixfd = socket(AF_UNIX, SOCK_DGRAM, 0);
        event.data.fd = unixfd;
        event.events = EPOLLIN;

        epoll_ctl(epfd, EPOLL_CTL_ADD, unixfd, &event);
    }

    virtual ~EventPoll () noexcept {
        close();
    }

    void epollAddEvent (SockEpollPtr &params) {
        event.data.ptr = &params;
        event.events = SET_COMMON_EPOLL_FLAG(EPOLLIN);

        epoll_ctl(epfd, EPOLL_CTL_ADD, params.fd, &event);

        if (!params.isListenFd)
            allEpollPtr.emplace(&params);
        else
            allListenPtr.emplace(&params);
    }
    void epollModEvent (SockEpollPtr &params, int events) {
        event.data.ptr = &params;
        event.events = SET_COMMON_EPOLL_FLAG(events);

        // EPOLLOUT 时视为一次交互完成 更新时间
        if (events & EPOLLOUT) {
            allEpollPtr.erase(&params);
            params.updateTimeNow();
            allEpollPtr.emplace(&params);
        }

        epoll_ctl(epfd, EPOLL_CTL_MOD, params.fd, &event);
    }
    void epollDelEvent (SockEpollPtr &params) {
        event.data.ptr = &params;
        event.events = 0;

        epoll_ctl(epfd, EPOLL_CTL_DEL, params.fd, &event);

        allEpollPtr.erase(&params);
    }
    int epollWait (ReactorParams &params, int expireTime) // NOLINT
    {
        // long expireTime = params.expireTime.count() == 0 ? -1 : params.expireTime.count();
        params.updateTimeNow();

        int ret = epoll_wait(
            epfd,
            params.sockEvents.epollEvents,
            MAX_EPOLL_ONCE_NUM,
            static_cast<int>(expireTime));

        if (ret == 0) {
            // 超时
        } else if (ret == -1) {
            if (errno == EINTR)
                return epollWait(params, 0);

            PRINT_ERROR("epoll_wait error : %s", std::strerror(errno));
            return -errno;
        } else
            params.sockEvents.sockfdLen = ret;

        return ret;
    }

    void close () {
        event.data.fd = unixfd;
        event.events = EPOLLOUT;
        epoll_ctl(epfd, EPOLL_CTL_MOD, unixfd, &event);

        for (const auto &item : allEpollPtr) {
            ::close(item->fd);
            delete item;
        }

        for (const auto &item : allListenPtr) {
            ::close(item->fd);
            delete item;
        }

        ::close(unixfd);
        ::close(epfd);
    }

private:
    int epfd;
    int unixfd;
    struct epoll_event event {};
    std::set <ParamsType *> allListenPtr;
protected:
    std::set <ParamsType *, CompareSockEpollPtr> allEpollPtr;
};

class IoReactor : protected Reactor
{
protected:
    using SockfdQueueType = std::list <SockEpollPtr *>;
public:
    IoReactor ()
        : Reactor(
        std::bind(&IoReactor::acceptCallback, this, std::placeholders::_1),
        std::bind(&IoReactor::recvCallback, this, std::placeholders::_1),
        std::bind(&IoReactor::sendCallback, this, std::placeholders::_1)) {}

    ~IoReactor () noexcept override = default;

    inline SockfdQueueType &sockfdQueue () noexcept {
        return sockfdQueue_;
    }
    inline void setStatus (SockEpollPtr::QUEUE_STATUS status) noexcept {
        status_ = status;
    }

    void ioHandler () {
        if (!sockfdQueue_.empty())
            switch (status_) {
                case SockEpollPtr::STATUS_RECV:
                    recvSocketHandler();
                    setStatus(SockEpollPtr::STATUS_RECV_DOWN);
                    break;
                case SockEpollPtr::STATUS_SEND:
                    sendSocketHandler();
                    setStatus(SockEpollPtr::STATUS_SEND_DOWN);
                    break;
                case SockEpollPtr::STATUS_RECV_DOWN:
                case SockEpollPtr::STATUS_SEND_DOWN:
                case SockEpollPtr::STATUE_SLEEP:
                case SockEpollPtr::STATUS_RECV_BAD:
                case SockEpollPtr::STATUS_LISTEN:
                case SockEpollPtr::STATUS_SEND_BAD:
                case SockEpollPtr::STATUS_CLOSE:
                case SockEpollPtr::STATUS_BLOCK:
                    break;
            }
    }

protected:
    template <class T>
    IoReactor (T &&acceptCb, T &&recvCb, T &&sendCb)
        : Reactor(
        std::forward <T>(acceptCb),
        std::forward <T>(recvCb),
        std::forward <T>(sendCb)) {}

public:
    void mainLoop (
        SockfdQueueType &queue,
        std::mutex &mutex,
        bool &terminate
    ) {
        PRINT_INFO("thread %lu is running...", pthread_self());
        while (!terminate) {
            for (int i = 0; i < defaultLoopNum; ++i) {
                if (!sockfdQueue().empty())
                    break;
            }

            if (sockfdQueue().empty()) {
                // 休眠
                mutex.lock();
                mutex.unlock();
                continue;
            }

            ioHandler();
            mutex.lock();
            // 合并任务
            queue.merge(std::move(sockfdQueue_));
            sockfdQueue_.clear();
            mutex.unlock();
        }
    }
private:
    inline void recvSocketHandler () {
        if (!sockfdQueue().empty())
            for (SockEpollPtr *sockParams : sockfdQueue())
                recv(*sockParams);
    }

    inline void sendSocketHandler () {
        if (!sockfdQueue().empty())
            for (SockEpollPtr *sockParams : sockfdQueue())
                send(*sockParams);
    }

    void acceptCallback (ParamsType &fnParams) {

    }
    void recvCallback (ParamsType &fnParams) // NOLINT
    {
        KvProtocol recvProtocol(fnParams.fd);

        ssize_t ret = recvProtocol.decodeRecv(fnParams.resValue);
        // 不做处理  留给mainReactor去处理
        if (ret < 0) {
            if (ret == -EPROTO) {
                PRINT_ERROR("protocol error : %s", std::strerror(-ret));
                fnParams.resValue
                        .setErrorStr(fnParams.commandParams, ResValueType::ErrorType::PROTO_ERROR);
            }

            fnParams.status = SockEpollPtr::STATUS_RECV_BAD;
        } else if (ret == 0) {
            fnParams.status = SockEpollPtr::STATUS_RECV_BAD;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                PRINT_INFO("get empty tcp body, we just reply nil flag, addr : %s, sockfd : %d",
                    Utils::getIpAndHost(fnParams.sockaddr).c_str(),
                    fnParams.fd);
                fnParams.resValue.setNilFlag();
                return;
            }
            PRINT_INFO("pipe close : %s, sockfd : %d",
                Utils::getIpAndHost(fnParams.sockaddr).c_str(),
                fnParams.fd);
        } else {
            // 命令解析
            fnParams.commandParams = KvServer::splitCommandParams(fnParams.resValue);
            fnParams.status = SockEpollPtr::STATUS_RECV_DOWN;

            PRINT_INFO("get client command : %s,  addr : %s, sockfd : %d, thread : %lu",
                fnParams.commandParams.toString().c_str(),
                Utils::getIpAndHost(fnParams.sockaddr).c_str(),
                fnParams.fd,
                pthread_self());
        }
    }

    void sendCallback (ParamsType &fnParams) // NOLINT
    {
        KvProtocol kvProtocolSend(fnParams.fd);
        ssize_t ret = kvProtocolSend.encodeSend(fnParams.resValue);
        if (ret < 0) {
            PRINT_ERROR("send error : %s , msg : %s", std::strerror(errno), kvProtocolSend.getSendMsg().c_str());
            fnParams.status = SockEpollPtr::STATUS_SEND_BAD;
        } else if (ret == 0) {
            PRINT_INFO("pipe close : %s, sockfd : %d",
                Utils::getIpAndHost(fnParams.sockaddr).c_str(),
                fnParams.fd);
            fnParams.status = SockEpollPtr::STATUS_SEND_BAD;
        } else {
            PRINT_INFO(
                "reply for command \"%s\", message : %s, addr : %s, sockfd : %d, thread : %lu",
                fnParams.commandParams.toString().c_str(),
                fnParams.resValue.toString().c_str(),
                Utils::getIpAndHost(fnParams.sockaddr).c_str(),
                fnParams.fd,
                pthread_self());

            fnParams.resValue.clear();
            fnParams.status = SockEpollPtr::STATUS_SEND_DOWN;
        }
    }

    static constexpr int defaultLoopNum = 1000000;

    SockfdQueueType sockfdQueue_;
    SockEpollPtr::QUEUE_STATUS status_ = SockEpollPtr::STATUS_RECV;
};

class MainReactor : IoReactor, public EventPoll
{
    friend class Tcp;
    friend class KvCirCle;
public:
    explicit MainReactor (
        int *listenfd
        // std::chrono::milliseconds expireTime = std::chrono::milliseconds { 0 }
    )
        : IoReactor(),
          EventPoll(), listenfd(listenfd) {
        auto config = KvConfig::getConfig();
        if (config.ioThreadsDoReads) {
            IoThreadNum = config.ioThreads;
            ioReactor = new IoReactor[IoThreadNum];
            ioThread = new std::thread[IoThreadNum];
        }

        params.mainReactor = this;
        ifAddr = Utils::Net::getIfconfig();
    }

    static inline ReactorParams &getReactorParams () noexcept {
        return params;
    }

    inline void setListenfdLen (size_t len) noexcept {
        listenfdLen = len;
    }

    ~MainReactor () noexcept override {
        terminate = true;
        mutex.unlock();

        for (int i = 0; i < IoThreadNum; ++i)
            ioThread[i].join();

        if (IoThreadNum > 0) {
            delete[] ioReactor;
            delete[] ioThread;
        }

        freeifaddrs(ifAddr);
    }

    void exitServer () noexcept {
        terminate = true;
    }

    void mainLoop () {
        ParamsType *epollPtr;
        for (size_t i = 0; i < listenfdLen; ++i) {
            epollPtr = new ParamsType(listenfd[i]);
            epollAddEvent(*epollPtr);
        }
        int ret;
        struct epoll_event *event;

        // 上锁让线程休眠
        // distributeSendSocket distributeRecvSocket 解锁让线程处理send recv 后，收集处理好的fd，继续上锁
        mutex.lock();
        setIoThread();
        KvCirCle::EventType cirCleEvent;
        while (!terminate) {
            auto expireTime = params.circleEvents.getOnceExpireTimeAndCheckTimeoutFd(cirCleEvent);
            ret = epollWait(params, expireTime);
            if (terminate)
                return;

            // 周期函数
            params.circleEvents.serverCycleEvent(cirCleEvent);
            cirCleEvent.reset();

            onceLoopRecvSum = 0;
            onceLoopSendSum = 0;
            for (int i = 0; i < ret; ++i) {
                event = &params.sockEvents.epollEvents[i];
                epollPtr = static_cast<SockEpollPtr *>(event->data.ptr);
                if (event->events & EPOLLRDHUP) {
                    PRINT_INFO("对端关闭， addr : %s , fd : %d",
                        Utils::getIpAndHost(epollPtr->sockaddr).c_str(),
                        epollPtr->fd);
                    closeSock(*epollPtr);
                } else if (event->events & EPOLLHUP) {
                    PRINT_WARNING("连接发生挂起 : %d", epollPtr->fd);
                    closeSock(*epollPtr);
                } else if (epollPtr->isListenFd)
                    acceptCallback(*epollPtr);
                else if (event->events & EPOLLIN)
                    recvCallback(*epollPtr);
                else if (event->events & EPOLLOUT)
                    sendCallback(*epollPtr);
            }

            if (onceLoopSendSum)
                distributeSendSocket();
            if (onceLoopRecvSum)
                distributeRecvSocket();
        }
    }
private:
    void checkTimeoutFd () {
        if (!allEpollPtr.empty()) {
            static auto end = allEpollPtr.end();
            bool needErase = false;
            auto it = allEpollPtr.begin();
            auto now = std::chrono::duration_cast <std::chrono::milliseconds>(Utils::getTimeNow());
            auto timeout = KvConfig::getConfig().timeout;
            do {
                // 未超时
                if (((*it)->runtime + std::chrono::milliseconds(timeout * 1000)) > now)
                    continue;

                PRINT_INFO("socket %d (%s) timeout",
                    (*it)->fd,
                    Utils::getIpAndHost((*it)->sockaddr).c_str());
                // 处于阻塞状态的io，需从阻塞队列中删除
                if ((*it)->isBlock())
                    params.circleEvents.delEvent(*it);

                ::close((*it)->fd);
                delete *it;
                needErase = true;
            } while (++it != end);

            if (needErase)
                allEpollPtr.erase(allEpollPtr.begin(), it);
        }
    }

    void setIoThread () {
        for (int i = 0; i < IoThreadNum; ++i) {
            ioThread[i] = std::thread(
                &IoReactor::mainLoop,
                &ioReactor[i],
                std::ref(sockfdQueue()),
                std::ref(mutex),
                std::ref(terminate)
            );
        }
    }
    void acceptCallback (ParamsType &fnParams) {
        static socklen_t len = sizeof(struct sockaddr);
        int fd = ::accept(fnParams.fd, (struct sockaddr *)&clientAddr, &len);
        if (fd == -1) {
            PRINT_ERROR("accept error : %s\n", std::strerror(errno));
            return;
        }

        // 检查配置protectedMode
        if (!checkProtectedMode()) {
            PRINT_ERROR("accept invalid in protected mode, invalid ip : %s",
                inet_ntoa(clientAddr.sin_addr));
            ::close(fd);
            return;
        }
        // int bufferSize = MESSAGE_SIZE_MAX;
        // socklen_t bufferSizeLen = sizeof(bufferSize);
        // setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &bufferSize, bufferSizeLen);
        Utils::Net::setSockReuseAddr(fd);
        Utils::Net::setSockNonblock(fd);
        auto *sockParams = new ParamsType(fd, clientAddr, &params);
        epollAddEvent(*sockParams);

        PRINT_INFO("accept by %s:%d , fd : %d",
            inet_ntoa(clientAddr.sin_addr),
            ntohs(clientAddr.sin_port),
            fd);
    }
    void recvCallback (ParamsType &fnParams) {
        fnParams.status = SockEpollPtr::STATUS_RECV;
        sockfdQueue().emplace_back(&fnParams);
        onceLoopRecvSum++;
    }
    void sendCallback (ParamsType &fnParams) {
        fnParams.status = SockEpollPtr::STATUS_SEND;
        sockfdQueue().emplace_back(&fnParams);
        onceLoopSendSum++;
    }

    void distributeRecvSocket () {
        setStatus(SockEpollPtr::STATUS_RECV);
        for (int i = 0; i < IoThreadNum; ++i)
            ioReactor[i].setStatus(SockEpollPtr::STATUS_RECV);

        distributeTask(onceLoopRecvSum);

        recvAfterHandler();
        // 123
    }

    void distributeSendSocket () {
        setStatus(SockEpollPtr::STATUS_SEND);
        for (int i = 0; i < IoThreadNum; ++i)
            ioReactor[i].setStatus(SockEpollPtr::STATUS_SEND);

        distributeTask(onceLoopSendSum);

        sendAfterHandler();
    }

    inline void distributeTask (size_t num) {
        static SockfdQueueType::iterator nextIt;
        // 分发任务
        for (auto it = sockfdQueue().begin(); it != sockfdQueue().end(); ++it) {
            IoReactor &reactor = getRandomReactor();
            if (&reactor != this) {
                nextIt = std::next(it);
                // 移动队列iterator至另一个队列
                reactor.sockfdQueue().splice(reactor.sockfdQueue().end(), sockfdQueue(), it);
                it = nextIt;
            }
        }
        // 解锁让线程去处理fd
        mutex.unlock();
        ioHandler();

        while (sockfdQueue().size() != num)
            std::this_thread::sleep_for(std::chrono::microseconds { 10 });

        // 让io线程休眠
        mutex.lock();
    }

    inline void recvAfterHandler () {
        for (SockEpollPtr *sockEpollPtr : sockfdQueue()) {
            if (sockEpollPtr->status == SockEpollPtr::QUEUE_STATUS::STATUS_RECV_DOWN)
                server.handlerCommand(*sockEpollPtr);

            if (sockEpollPtr->status != SockEpollPtr::QUEUE_STATUS::STATUS_CLOSE
                && sockEpollPtr->status != SockEpollPtr::QUEUE_STATUS::STATUS_BLOCK)
                epollModEvent(*sockEpollPtr, SET_COMMON_EPOLL_FLAG(EPOLLOUT));
        }

        sockfdQueue().clear();
    };

    inline void sendAfterHandler () {
        for (SockEpollPtr *sockEpollPtr : sockfdQueue())
            epollModEvent(*sockEpollPtr, SET_COMMON_EPOLL_FLAG(EPOLLIN));

        sockfdQueue().clear();
    }

    bool checkProtectedMode () {
        auto config = KvConfig::getConfig();
        if (config.protectedMode && config.requirePass == CONVERT_EMPTY_TO_NULL_VAL) {
            for (struct ifaddrs *ifa = ifAddr; ifa; ifa = ifa->ifa_next) {
                auto sockaddrIn = reinterpret_cast<struct sockaddr_in *>(ifa->ifa_addr);
                if (sockaddrIn->sin_family == clientAddr.sin_family
                    && sockaddrIn->sin_addr.s_addr == clientAddr.sin_addr.s_addr)
                    return true;
            }

            return false;
        }

        return true;
    }

    inline void closeSock (ParamsType &fnParams) noexcept {
        epollDelEvent(fnParams);
        if (fnParams.isBlock()) params.circleEvents.delEvent(&fnParams);
        ::close(fnParams.fd);
        delete &fnParams;
    }

    inline IoReactor &getRandomReactor () {
        static size_t index = 0;
        size_t randomReactor = index++ % (IoThreadNum + 1);
        if (randomReactor == IoThreadNum) {
            return *this;
        }
        return ioReactor[randomReactor];
    }

private:
    bool terminate = false;
    static ReactorParams params;
    struct sockaddr_in clientAddr {};
    std::mutex mutex;
    size_t onceLoopRecvSum = 0;
    size_t onceLoopSendSum = 0;
    KvServer server { this };
    int *listenfd;
    size_t listenfdLen = 0;

    uint16_t IoThreadNum = 0;
    IoReactor *ioReactor;
    std::thread *ioThread;
    struct ifaddrs *ifAddr;
};
ReactorParams MainReactor::params {};

void KvCirCle::KvCirCleEvent::handlerTimeout (ReactorParams &reactorParams) {
    auto needOut = timeoutCb(*params, *this);
    if (needOut && !isDelete && params)
        params->unBlock();

    if (expire == std::chrono::milliseconds::zero())
        reactorParams.circleEvents.delEvent(id);
}
void KvCirCle::KvCirCleEvent::handlerEvent (ReactorParams &reactorParams, const StringType &key) {
    auto needOut = handlerCb(*params, *this, key);
    if (needOut && !isDelete && params)
        params->unBlock();

    if (expire == std::chrono::milliseconds::zero())
        reactorParams.circleEvents.delEvent(id);
}

void SockEpollPtr::unBlock () noexcept {
    status = STATUS_SEND;

    reactorParams->mainReactor->epollModEvent(*this, EPOLLOUT);
}

void KvCirCle::serverCycleEvent (EventType &cirCleEvent) {
    cycleCount++;
    auto *server = reactorParams->mainReactor;
    // 处理过期事件
    // 如 blpop
    if (cirCleEvent && cirCleEvent->expireTime <= Utils::getTimeNow())
        cirCleEvent->handlerTimeout(*reactorParams);

    RUN_WITH_PERIOD(1000) {
        // 清除circleEvents中失效的event
        clearDeletedEvents();
    }

    // 每次epoll_wait返回后检查一次过期的key
    server->server.checkExpireKeys();
}
int KvCirCle::getOnceExpireTimeAndCheckTimeoutFd (KvCirCle::EventType &circleEvent) {
    long expireTime;
    if (empty()) expireTime = -1;
    else {
        circleEvent = getMinimalExpireEvent();
        if (circleEvent) {
            expireTime = (circleEvent->expireTime
                - std::chrono::duration_cast <std::chrono::milliseconds>(Utils::getTimeNow())).count();
            if (expireTime < 0) expireTime = -1;
        }
    }

    auto timeout = KvConfig::getConfig().timeout;
    if (timeout) {
        // 每次epoll_wait 之前检查是否有过期fd，通过config timeout 判断
        reactorParams->mainReactor->checkTimeoutFd();
        expireTime =
            expireTime < 0 ? timeout : std::min(static_cast<long>(timeout), expireTime);
    }

    return static_cast<int>(expireTime);
}

void KvServer::handlerCommand (ParamsType &params) {
    const CommandParams &commandParams = params.commandParams;
    ResValueType &res = params.resValue;
    ParamsArgs args;
    args.isNewKey = false;
    args.server = this;
    params.arg = &args;
    static KeyCommandHandler keyCommandHandler { this };
    static BaseCommandHandler baseCommandHandler { this };

#if defined(KV_STORE_DEBUG)
    // 用于测试内存泄漏，服务器关闭命令
    if (commandParams.command == "close") {
        return mainReactor->exitServer();
    }
#endif

    if (commandParams.command == "debug")
        return debugHandler->handlerDebugCommand(params);

    auto pa = allCommands.find(commandParams.command);
    if (pa == allCommands.end()) {
        res.setErrorStr(commandParams, ResValueType::ErrorType::UNKNOWN_COMMAND);
        return;
    }
    // 在设置了密码的情况下，没有使用auth命令登录，不让操作
    if (commandParams.command != "auth"
        && !checkAuth(commandParams, params.needAuth, params.auth, res))
        return;

    // base类型不检查是否存在，直接调用handler
    if (pa->second.structType == StructType::BASE) {
        // 如果参数有问题，直接返回
        if (!pa->second.checkParamsValidHandler(params))
            return;
        (baseCommandHandler.*pa->second.handler)(params);
        return;
    }

    // 如果不是base类型的命令操作，必须有key
    if (!CommandCommon::checkKeyIsValid(params.commandParams, params.resValue))
        return;
    // 如果参数有问题，直接返回
    if (!pa->second.checkParamsValidHandler(params))
        return;

    // key类型不检查是否存在，直接调用handler
    if (pa->second.structType == StructType::KEY) {
        // 如果参数有问题，直接返回
        if (!pa->second.checkParamsValidHandler(params))
            return;
        (keyCommandHandler.*pa->second.handler)(params);
        return;
    }

    args.structType = pa->second.structType;
    auto it = keyMap.find(params.commandParams.key);
    if (it != keyMap.end()) {
        if (!checkKeyType(params.commandParams, it, pa->second.structType, res))
            return;

        checkExpireKey(it);
        params.arg->isNewKey = false;
        // 处理前置钩子函数
        if (!it->second.handler->handlerBefore(params))
            return;
        (it->second.handler->*(pa->second.handler))(params);
    } else {
        params.arg->isNewKey = true;
        if (!pa->second.createdOnNotExist) {
            if (pa->second.emptyHandler)
                (pa->second.emptyHandler)(params);
            else {
                // 如果不需要创建，并且emptyHandler为空，则使用默认handler处理命令，只保证server指针有效
                static KeyOfValue newKey(StructType::STRING, this);
                (newKey.handler->*pa->second.handler)(params);
            }
        } else {
            KeyOfValue newKey(pa->second.structType, this);
            // 处理前置钩子函数
            if (!newKey.handler->handlerBefore(params))
                return;
            (newKey.handler->*pa->second.handler)(params);

            // 新增需要检测block key
            auto event = params.reactorParams->circleEvents.checkBlockKey(commandParams.key, pa->second.structType);
            keyMap.emplace(params.commandParams.key, std::move(newKey));

            if (event)
                event->handlerEvent(*params.reactorParams, commandParams.key);
        }
    }
}
#endif //LINUX_SERVER_LIB_KV_STORE_EPOLL_H_
