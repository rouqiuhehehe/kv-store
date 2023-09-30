//
// Created by Yoshiki on 2023/9/29.
//

#ifndef LINUX_SERVER_LIB_KV_STORE_CLIENT_KV_TCP_CLIENT_H_
#define LINUX_SERVER_LIB_KV_STORE_CLIENT_KV_TCP_CLIENT_H_

#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>

class KvTcpClient
{
public:
    KvTcpClient (const char *host, uint16_t port)
    {
        createTcpClient(host, port);
    }
    inline int getClientFd () const
    {
        return clientFd;
    }

private:
    void createTcpClient (const char *host, uint16_t port)
    {
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);

        struct sockaddr_in sockAddr {
            .sin_family = AF_INET,
            .sin_port = htons(port),
            .sin_addr = {
                .s_addr = inet_addr(host)
            }
        };

        int ret = connect(sockfd, (const struct sockaddr *)&sockAddr, sizeof(struct sockaddr));
        if (ret != 0)
        {
            PRINT_ERROR("connect error : %s", strerror(errno));
            exit(EXIT_FAILURE);
        }

        fcntl(sockfd, F_SETFL, O_NONBLOCK);
        clientFd = sockfd;
    }

    int clientFd {};
};
#endif //LINUX_SERVER_LIB_KV_STORE_CLIENT_KV_TCP_CLIENT_H_
