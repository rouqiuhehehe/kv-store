//
// Created by Yoshiki on 2023/9/29.
//
#include "kv-store.h"
#include "./kv-env.h"
#include "kv-tcp-client.h"
#include "kv-protocol-client.h"

#define BUFFER_SIZE 65535
int main (int argc, char **argv) {
    KvEnv env(argc, argv);

    KvTcpClient tcpClient(env.getIp().c_str(), env.getPort());
    KvProtocolClient protocol(tcpClient.getClientFd());

    ResValueType res;
    StringType buffer;
    // char buffer[BUFFER_SIZE];
    int epfd = epoll_create(1024);
    struct epoll_event event {}, events[1];
    event.events = EPOLLOUT;
    event.data.fd = tcpClient.getClientFd();

    epoll_ctl(epfd, EPOLL_CTL_ADD, tcpClient.getClientFd(), &event);
    std::chrono::microseconds ms = Utils::getTimeNow();
    std::chrono::microseconds now;
    char c;
    size_t count = 0;
    while (1) {
        epoll_wait(epfd, events, 1, -1);
        if (events[0].events & EPOLLOUT) {
            while (1) {
                std::cout << env.getIp().c_str() << ":" << env.getPort() << "> ";
                // do {
                //     c = static_cast<char>(std::cin.get());
                // } while (c != '\n' || ++count < BUFFER_SIZE || std::cin.eof());
                std::getline(std::cin, buffer);
                buffer = Utils::StringHelper::stringTrim(buffer);
                if (buffer.size() != 0) break;
            }

            protocol.encodeSend(buffer);
            event.events = EPOLLIN;
            ms = Utils::getTimeNow();
        } else {
            res.clear();
            protocol.decodeRecv(res);
            now = Utils::getTimeNow();
            std::cout << res.toString()
                      << "\n(" << std::chrono::duration_cast <std::chrono::duration <double>>(now - ms).count()
                      << "s)" << std::endl;
            event.events = EPOLLOUT;
        }
        epoll_ctl(epfd, EPOLL_CTL_MOD, tcpClient.getClientFd(), &event);
    }

    return 0;
}