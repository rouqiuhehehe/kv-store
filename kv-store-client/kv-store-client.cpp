//
// Created by Yoshiki on 2023/9/29.
//
#include "kv-env.h"
#include "kv-tcp-client.h"
#include "kv-protocol-client.h"
#include <sys/epoll.h>

#define BUFFER_SIZE 65535
int main (int argc, char **argv)
{
    KvEnv env(argc, argv);

    KvTcpClient tcpClient(env.getIp().c_str(), env.getPort());
    KvProtocolClient protocol(tcpClient.getClientFd());

    ResValueType res;
    char buffer[BUFFER_SIZE];
    int epfd = epoll_create(1024);
    struct epoll_event event {}, events[1];
    event.events = EPOLLOUT;
    event.data.fd = tcpClient.getClientFd();

    epoll_ctl(epfd, EPOLL_CTL_ADD, tcpClient.getClientFd(), &event);
    while (1)
    {
        epoll_wait(epfd, events, 1, -1);
        if (events[0].events & EPOLLOUT)
        {
            std::cin.getline(buffer, BUFFER_SIZE);
            protocol.encodeSend(buffer);
            event.events = EPOLLIN;
        }
        else
        {
            res.clear();
            protocol.decodeRecv(res);
            std::cout << res.toString() << std::endl;
            event.events = EPOLLOUT;
        }
        epoll_ctl(epfd, EPOLL_CTL_MOD, tcpClient.getClientFd(), &event);
    }

    return 0;
}