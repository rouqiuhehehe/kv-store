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
    }
};
#endif //LINUX_SERVER_LIB_INCLUDE_UTIL_NET_H_
