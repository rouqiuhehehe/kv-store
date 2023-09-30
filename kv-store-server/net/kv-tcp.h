//
// Created by Yoshiki on 2023/8/7.
//

#ifndef LINUX_SERVER_LIB_KV_STORE_KV_TCP_H_
#define LINUX_SERVER_LIB_KV_STORE_KV_TCP_H_

#include "kv-reactor.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include "util/global.h"

class Tcp
{
public:
    Tcp ()
        : reactor(reinterpret_cast<int *>(sockfds))
    {
        int bindCount = createTcpServer();
        reactor.setListenfdLen(bindCount);
    }

    void mainLoop ()
    {
        reactor.mainLoop();
    }

private:
    int createTcpServer ()
    {
        auto config = KvConfig::getConfig();
        int i = 0;
        for (; config.bind[i] != CONVERT_EMPTY_TO_NULL_VAL; ++i)
        {
            int sockfd = socket(AF_INET, SOCK_STREAM, 0);
            CHECK_RET(sockfd < 3, socket);
            Utils::Net::setSockReuseAddr(sockfd);

            auto bindIp = config.bind[i].c_str();
            in_addr_t ip = inet_addr(bindIp);
            if (ip == INADDR_NONE)
            {
                PRINT_ERROR("invalid ip in %s", bindIp);
                exit(EXIT_FAILURE);
            }
            struct sockaddr_in addr {
                .sin_family = AF_INET,
                .sin_port = htons(config.port),
                .sin_addr = {
                    .s_addr = ip
                }
            };
            CHECK_RET(bind(sockfd, (const struct sockaddr *)&addr, sizeof(struct sockaddr)) != 0,
                bind);
            CHECK_RET(listen(sockfd, 1024) != 0, listen);

            sockfds[i] = sockfd;
            PRINT_INFO("create server on listen : %s:%d", config.bind[i].c_str(), config.port);
        }
        return i;
    }

    int sockfds[BIND_MAX] {};
    MainReactor reactor;
};
#endif //LINUX_SERVER_LIB_KV_STORE_KV_TCP_H_
