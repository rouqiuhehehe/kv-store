//
// Created by 115282 on 2023/9/20.
//

#ifndef LINUX_SERVER_LIB_INCLUDE_UTIL_NET_H_
#define LINUX_SERVER_LIB_INCLUDE_UTIL_NET_H_

#include <fcntl.h>
#include <sys/socket.h>

namespace Utils
{
    namespace Net
    {
        static inline int setSockReuseAddr (int sockfd)
        {
            int opt = 1;
            return setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, sizeof(int));
        }

        static inline int setSockNonblock (int sockfd)
        {
            return fcntl(sockfd, F_SETFL, fcntl(sockfd, F_GETFL) | O_NONBLOCK);
        }

        static inline struct ifaddrs *getIfconfig () noexcept
        {
            struct ifaddrs *ifAddr;
            // 获取接口地址信息
            if (getifaddrs(&ifAddr) == -1)
            {
                PRINT_ERROR("Failed to get interface address.");
                return nullptr;
            }

            return ifAddr;
        }
    }
};
#endif //LINUX_SERVER_LIB_INCLUDE_UTIL_NET_H_
