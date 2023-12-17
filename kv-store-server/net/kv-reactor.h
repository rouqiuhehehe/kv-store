//
// Created by Yoshiki on 2023/8/7.
//

#ifndef LINUX_SERVER_LIB_KV_STORE_EPOLL_H_
#define LINUX_SERVER_LIB_KV_STORE_EPOLL_H_

#include "command-structs/kv-command.h"
#include <unistd.h>
#include <functional>
#include <sys/epoll.h>
#include "printf-color.h"
#include <thread>
#include <set>
#include <algorithm>
#include <mutex>
#include <atomic>
#include "kv-protocol.h"
#include "util/net.h"

#define SET_COMMON_EPOLL_FLAG(event) ((event) | EPOLLHUP | EPOLLRDHUP)

class Tcp;

struct CompareSockEpollPtr
{
    inline bool operator() (const ParamsType *a, const ParamsType *b) const noexcept
    {
        return a->runtime < b->runtime;
    }
};
struct SockEvents
{
    int sockfdLen;
    struct epoll_event epollEvents[MAX_EPOLL_ONCE_NUM];
};
struct ReactorParams
{
    // int epfd;
    std::chrono::milliseconds expireTime;
    std::chrono::milliseconds runtime { 0 };
    void *arg = nullptr;
    SockEvents sockEvents {};

    explicit ReactorParams (std::chrono::milliseconds expireTime)
        : expireTime(expireTime) {}

    void updateTimeNow ()
    {
        runtime = std::chrono::duration_cast <std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch());
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

    inline void accept (ParamsType &params) const
    {
        acceptCb_(params);
    }
    inline void recv (ParamsType &params) const
    {
        recvCb_(params);
    }
    inline void send (ParamsType &params) const
    {
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
        : epfd(epoll_create(size))
    {
        // 用于退出epoll_wait 阻塞
        unixfd = socket(AF_UNIX, SOCK_DGRAM, 0);
        event.data.fd = unixfd;
        event.events = EPOLLIN;

        epoll_ctl(epfd, EPOLL_CTL_ADD, unixfd, &event);
    }

    virtual ~EventPoll () noexcept
    {
        close();
    }

    void epollAddEvent (SockEpollPtr &params)
    {
        event.data.ptr = &params;
        event.events = SET_COMMON_EPOLL_FLAG(EPOLLIN);

        epoll_ctl(epfd, EPOLL_CTL_ADD, params.fd, &event);

        if (!params.isListenFd)
            allEpollPtr.emplace(&params);
        else
            allListenPtr.emplace(&params);
    }
    void epollModEvent (SockEpollPtr &params, int events)
    {
        event.data.ptr = &params;
        event.events = SET_COMMON_EPOLL_FLAG(events);

        // EPOLLOUT 时视为一次交互完成 更新时间
        if (events & EPOLLOUT)
        {
            allEpollPtr.erase(&params);
            params.updateTimeNow();
            allEpollPtr.emplace(&params);
        }

        epoll_ctl(epfd, EPOLL_CTL_MOD, params.fd, &event);
    }
    void epollDelEvent (SockEpollPtr &params)
    {
        event.data.ptr = &params;
        event.events = 0;

        epoll_ctl(epfd, EPOLL_CTL_DEL, params.fd, &event);

        allEpollPtr.erase(&params);
    }
    int epollWait (ReactorParams &params) // NOLINT
    {
        long expireTime = params.expireTime.count() == 0 ? -1 : params.expireTime.count();
        params.updateTimeNow();

        // 每次epoll_wait 之前检查是否有过期fd，通过config timeout 判断
        checkTimeoutFd();
        int ret = epoll_wait(
            epfd,
            params.sockEvents.epollEvents,
            MAX_EPOLL_ONCE_NUM,
            static_cast<int>(expireTime));

        if (ret == 0)
        {
            // 超时
        }
        else if (ret == -1)
        {
            if (errno == EINTR)
                return epollWait(params);

            PRINT_ERROR("epoll_wait error : %s", std::strerror(errno));
            return -errno;
        }
        else
            params.sockEvents.sockfdLen = ret;

        return ret;
    }

    void close ()
    {
        event.data.fd = unixfd;
        event.events = EPOLLOUT;
        epoll_ctl(epfd, EPOLL_CTL_MOD, unixfd, &event);

        for (const auto &item : allEpollPtr)
        {
            ::close(item->fd);
            delete item;
        }

        for (const auto &item : allListenPtr)
        {
            ::close(item->fd);
            delete item;
        }

        ::close(unixfd);
        ::close(epfd);
    }

private:
    void checkTimeoutFd ()
    {
        auto timeout = KvConfig::getConfig().timeout;
        if (timeout > 0 && !allEpollPtr.empty())
        {
            static auto end = allEpollPtr.end();
            bool needErase = false;
            auto it = allEpollPtr.begin();
            auto now = std::chrono::duration_cast <std::chrono::milliseconds>(Utils::getTimeNow());
            do
            {
                // 未超时
                if (((*it)->runtime + std::chrono::milliseconds(timeout * 1000)) > now)
                    break;

                PRINT_INFO("socket %d (%s) timeout",
                    (*it)->fd,
                    Utils::getIpAndHost((*it)->sockaddr).c_str());
                ::close((*it)->fd);
                delete *it;
                needErase = true;
            } while (++it != end);

            if (needErase)
                allEpollPtr.erase(allEpollPtr.begin(), it);
        }
    }

private:
    int epfd;
    int unixfd;
    struct epoll_event event {};
    std::set <ParamsType *, CompareSockEpollPtr> allEpollPtr;
    std::set <ParamsType *> allListenPtr;
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

    inline SockfdQueueType &sockfdQueue () noexcept
    {
        return sockfdQueue_;
    }
    inline void setStatus (SockEpollPtr::QUEUE_STATUS status) noexcept
    {
        status_ = status;
    }

    void ioHandler ()
    {
        if (!sockfdQueue_.empty())
            switch (status_)
            {
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
    )
    {
        PRINT_INFO("thread %lu is running...", pthread_self());
        while (!terminate)
        {
            for (int i = 0; i < defaultLoopNum; ++i)
            {
                if (!sockfdQueue().empty())
                    break;
            }

            if (sockfdQueue().empty())
            {
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
    inline void recvSocketHandler ()
    {
        if (!sockfdQueue().empty())
            for (SockEpollPtr *sockParams : sockfdQueue())
                recv(*sockParams);
    }

    inline void sendSocketHandler ()
    {
        if (!sockfdQueue().empty())
            for (SockEpollPtr *sockParams : sockfdQueue())
                send(*sockParams);
    }

    void acceptCallback (ParamsType &fnParams)
    {

    }
    void recvCallback (ParamsType &fnParams) // NOLINT
    {
        KvProtocol recvProtocol(fnParams.fd);

        ssize_t ret = recvProtocol.decodeRecv(fnParams.resValue);
        // 不做处理  留给mainReactor去处理
        if (ret < 0)
        {
            if (ret == -EPROTO)
            {
                PRINT_ERROR("protocol error : %s", std::strerror(-ret));
                fnParams.resValue
                        .setErrorStr(fnParams.commandParams, ResValueType::ErrorType::PROTO_ERROR);
            }

            fnParams.status = SockEpollPtr::STATUS_RECV_BAD;
        }
        else if (ret == 0)
        {
            PRINT_INFO("pipe close : %s, sockfd : %d",
                Utils::getIpAndHost(fnParams.sockaddr).c_str(),
                fnParams.fd);
            fnParams.status = SockEpollPtr::STATUS_RECV_BAD;
        }
        else
        {
            // 命令解析
            fnParams.commandParams = CommandHandler::splitCommandParams(fnParams.resValue);
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
        if (ret < 0)
        {
            PRINT_ERROR("send error : %s", std::strerror(errno));
            fnParams.status = SockEpollPtr::STATUS_SEND_BAD;
        }
        else if (ret == 0)
        {
            PRINT_INFO("pipe close : %s, sockfd : %d",
                Utils::getIpAndHost(fnParams.sockaddr).c_str(),
                fnParams.fd);
            fnParams.status = SockEpollPtr::STATUS_SEND_BAD;
        }
        else
        {
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

class MainReactor : IoReactor, EventPoll
{
    friend class Tcp;
public:
    explicit MainReactor (
        int *listenfd,
        std::chrono::milliseconds expireTime = std::chrono::milliseconds { 0 }
    )
        : IoReactor(),
          EventPoll(), params(expireTime), listenfd(listenfd)
    {
        auto config = KvConfig::getConfig();
        if (config.ioThreadsDoReads)
        {
            IoThreadNum = config.ioThreads;
            ioReactor = new IoReactor[IoThreadNum];
            ioThread = new std::thread[IoThreadNum];
        }

        ifAddr = Utils::Net::getIfconfig();
    }

    inline void setListenfdLen (size_t len) noexcept
    {
        listenfdLen = len;
    }

    ~MainReactor () noexcept override
    {
        terminate = true;
        mutex.unlock();

        for (int i = 0; i < IoThreadNum; ++i)
            ioThread[i].join();

        if (IoThreadNum > 0)
        {
            delete[] ioReactor;
            delete[] ioThread;
        }

        freeifaddrs(ifAddr);
    }

    void mainLoop ()
    {
        ParamsType *epollPtr;
        for (size_t i = 0; i < listenfdLen; ++i)
        {
            epollPtr = new ParamsType(listenfd[i]);
            epollAddEvent(*epollPtr);
        }
        int ret;
        struct epoll_event *event;

        // 上锁让线程休眠
        // distributeSendSocket distributeRecvSocket 解锁让线程处理send recv 后，收集处理好的fd，继续上锁
        mutex.lock();
        setIoThread();
        while (!terminate)
        {
            ret = epollWait(params);
            if (terminate)
                return;

            // 每次epoll_wait返回后检查一次过期的key
            commandHandler.checkExpireKeys();

            onceLoopRecvSum = 0;
            onceLoopSendSum = 0;
            for (int i = 0; i < ret; ++i)
            {
                event = &params.sockEvents.epollEvents[i];
                epollPtr = static_cast<SockEpollPtr *>(event->data.ptr);
                if (event->events & EPOLLRDHUP)
                {
                    PRINT_INFO("对端关闭， addr : %s , fd : %d",
                        Utils::getIpAndHost(epollPtr->sockaddr).c_str(),
                        epollPtr->fd);
                    closeSock(*epollPtr);
                }
                else if (event->events & EPOLLHUP)
                {
                    PRINT_WARNING("连接发生挂起 : %d", epollPtr->fd);
                    closeSock(*epollPtr);
                }
                else if (epollPtr->isListenFd)
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
    void setIoThread ()
    {
        for (int i = 0; i < IoThreadNum; ++i)
        {
            ioThread[i] = std::thread(
                &IoReactor::mainLoop,
                &ioReactor[i],
                std::ref(sockfdQueue()),
                std::ref(mutex),
                std::ref(terminate)
            );
        }
    }
    void acceptCallback (ParamsType &fnParams)
    {
        static socklen_t len = sizeof(struct sockaddr);
        int fd = ::accept(fnParams.fd, (struct sockaddr *)&clientAddr, &len);
        if (fd == -1)
        {
            PRINT_ERROR("accept error : %s\n", std::strerror(errno));
            return;
        }

        // 检查配置protectedMode
        if (!checkProtectedMode())
        {
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
        auto *sockParams = new ParamsType(fd, clientAddr);
        epollAddEvent(*sockParams);

        PRINT_INFO("accept by %s:%d , fd : %d",
            inet_ntoa(clientAddr.sin_addr),
            ntohs(clientAddr.sin_port),
            fd);
    }
    void recvCallback (ParamsType &fnParams)
    {
        fnParams.status = SockEpollPtr::STATUS_RECV;
        sockfdQueue().emplace_back(&fnParams);
        onceLoopRecvSum++;
    }
    void sendCallback (ParamsType &fnParams)
    {
        fnParams.status = SockEpollPtr::STATUS_SEND;
        sockfdQueue().emplace_back(&fnParams);
        onceLoopSendSum++;
    }

    void distributeRecvSocket ()
    {
        setStatus(SockEpollPtr::STATUS_RECV);
        for (int i = 0; i < IoThreadNum; ++i)
            ioReactor[i].setStatus(SockEpollPtr::STATUS_RECV);

        distributeTask(onceLoopRecvSum);

        recvAfterHandler();
        // 123
    }

    void distributeSendSocket ()
    {
        setStatus(SockEpollPtr::STATUS_SEND);
        for (int i = 0; i < IoThreadNum; ++i)
            ioReactor[i].setStatus(SockEpollPtr::STATUS_SEND);

        distributeTask(onceLoopSendSum);

        sendAfterHandler();
    }

    inline void distributeTask (size_t num)
    {
        static SockfdQueueType::iterator nextIt;
        // 分发任务
        for (auto it = sockfdQueue().begin(); it != sockfdQueue().end(); ++it)
        {
            IoReactor &reactor = getRandomReactor();
            if (&reactor != this)
            {
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

    inline void recvAfterHandler ()
    {
        for (SockEpollPtr *sockEpollPtr : sockfdQueue())
        {
            if (sockEpollPtr->status == SockEpollPtr::QUEUE_STATUS::STATUS_RECV_DOWN)
                commandHandler.handlerCommand(*sockEpollPtr);

            if (sockEpollPtr->status != SockEpollPtr::QUEUE_STATUS::STATUS_CLOSE)
                epollModEvent(*sockEpollPtr, SET_COMMON_EPOLL_FLAG(EPOLLOUT));
        }

        sockfdQueue().clear();
    };

    inline void sendAfterHandler ()
    {
        for (SockEpollPtr *sockEpollPtr : sockfdQueue())
            epollModEvent(*sockEpollPtr, SET_COMMON_EPOLL_FLAG(EPOLLIN));

        sockfdQueue().clear();
    }

    bool checkProtectedMode ()
    {
        auto config = KvConfig::getConfig();
        if (config.protectedMode && config.requirePass == CONVERT_EMPTY_TO_NULL_VAL)
        {
            for (struct ifaddrs *ifa = ifAddr; ifa; ifa = ifa->ifa_next)
            {
                auto sockaddrIn = reinterpret_cast<struct sockaddr_in *>(ifa->ifa_addr);
                if (sockaddrIn->sin_family == clientAddr.sin_family
                    && sockaddrIn->sin_addr.s_addr == clientAddr.sin_addr.s_addr)
                    return true;
            }

            return false;
        }

        return true;
    }

    inline void closeSock (ParamsType &fnParams) noexcept
    {
        epollDelEvent(fnParams);
        ::close(fnParams.fd);
        delete &fnParams;
    }

    inline IoReactor &getRandomReactor ()
    {
        static size_t index = 0;
        size_t randomReactor = index++ % (IoThreadNum + 1);
        if (randomReactor == IoThreadNum)
        {
            return *this;
        }
        return ioReactor[randomReactor];
    }

private:
    bool terminate = false;
    ReactorParams params;
    struct sockaddr_in clientAddr {};
    std::mutex mutex;
    size_t onceLoopRecvSum = 0;
    size_t onceLoopSendSum = 0;
    CommandHandler commandHandler;
    int *listenfd;
    size_t listenfdLen = 0;

    uint16_t IoThreadNum = 0;
    IoReactor *ioReactor;
    std::thread *ioThread;
    struct ifaddrs *ifAddr;
};
#endif //LINUX_SERVER_LIB_KV_STORE_EPOLL_H_
