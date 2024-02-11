//
// Created by Yoshiki on 2023/6/2.
//

#ifndef LINUX_SERVER_LIB_INCLUDE_UTIL_H_
#define LINUX_SERVER_LIB_INCLUDE_UTIL_H_

#ifdef __cplusplus
#include <cstdint>
#include <arpa/inet.h>
#include <execinfo.h>
#include "printf-color.h"
#include <dlfcn.h>
#include <cxxabi.h>
#include <chrono>

#define FORWARD_ITERATOR_TYPE(type) \
    using value_type = type;    \
    using difference_type = ptrdiff_t; \
    using pointer = const type *;   \
    using reference = const type &; \
    using iterator_category = std::forward_iterator_tag
// using difference_type = ptrdiff_t;

namespace Utils
{
    // 打印堆栈，需要编译参数-rdynamic
    inline void dumpTrackBack () {
        constexpr static int SIZE = 200;
        constexpr static size_t BUFFER_SIZE = 1024;
        static void *nBuffer[SIZE];
        static char buffer[BUFFER_SIZE];
        size_t len = BUFFER_SIZE;
        int status;

        int nPtr = backtrace(nBuffer, SIZE);
        if (nPtr) {
            Dl_info dlInfo;
            for (int i = 0; i < nPtr; ++i) {
                if (dladdr(nBuffer[i], &dlInfo)) {
                    abi::__cxa_demangle(dlInfo.dli_sname, buffer, &len, &status);
                    if (status == 0) {
                        buffer[len] = '\0';
                        PRINT_ERROR("at %p : %s[+0x%lx] (%s)",
                            nBuffer[i],
                            buffer,
                            (int *)nBuffer[i] - (int *)dlInfo.dli_saddr,
                            dlInfo.dli_fname);
                    }
                }
            }
        }
    }
    inline std::string getIpAndHost (const struct sockaddr_in &sockaddrIn) {
        char *ip = inet_ntoa(sockaddrIn.sin_addr);
        uint16_t port = ntohs(sockaddrIn.sin_port);

        return std::string(ip) + ":" + std::to_string(port);
    }

    inline std::string getDateNow (const std::string &format = "%Y-%m-%d %H:%M:%S") {
        char time[40];
        auto now = std::chrono::system_clock::now();
        auto currentTime = std::chrono::system_clock::to_time_t(now);

        std::strftime(time, 40, format.c_str(), std::localtime(&currentTime));

        return time;
    }

    inline std::chrono::microseconds getTimeNow () noexcept {
        return std::chrono::duration_cast <std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch());
    }

    template <class T>
    inline T *addressOf (const T &t) {
        return reinterpret_cast<T *>(&const_cast<char &>(reinterpret_cast<const volatile char &>(t)));
    }

    template <class T, uint32_t Size>
    inline uint32_t getArrLen (const T (&arr)[Size]) {
        return Size;
    }

    class AllDefaultCopy
    {
    public:
#if __cplusplus >= 201103L
        AllDefaultCopy () = default;
        virtual ~AllDefaultCopy () noexcept = default;
        AllDefaultCopy (const AllDefaultCopy &) = default;
        AllDefaultCopy &operator= (const AllDefaultCopy &) = default;
        AllDefaultCopy (AllDefaultCopy &&) noexcept = default;
        AllDefaultCopy &operator= (AllDefaultCopy &&) noexcept = default;
#else
        AllDefaultCopy () {}
        ~AllDefaultCopy () {}
private:
        AllDefaultCopy (const AllDefaultCopy &) {}
        AllDefaultCopy &operator= (const AllDefaultCopy &) {}
#endif
    };

    class NonAbleCopy
    {
    public:
#if __cplusplus >= 201103L
        NonAbleCopy () = default;
        ~NonAbleCopy () noexcept = default;
        NonAbleCopy (const NonAbleCopy &) = delete;
        NonAbleCopy &operator= (const NonAbleCopy &) = delete;
#else
        NonAbleCopy () {}
        ~NonAbleCopy () {}
private:
        NonAbleCopy (const NonAbleCopy &) {}
        NonAbleCopy &operator= (const NonAbleCopy &) {}
#endif
    };

#if __cplusplus >= 201103L
    class NonAbleMoveCopy
    {
    public:
        NonAbleMoveCopy () = default;
        ~NonAbleMoveCopy () noexcept = default;
        NonAbleMoveCopy (NonAbleMoveCopy &&) noexcept = delete;
        NonAbleMoveCopy &operator= (NonAbleMoveCopy &&) noexcept = delete;
    };

    class NonAbleAllCopy : NonAbleMoveCopy, NonAbleCopy
    {
    public:
        NonAbleAllCopy () = default;
        ~NonAbleAllCopy () noexcept = default;
    };
#endif

    template <class Model>
    class EnumHelper : Utils::AllDefaultCopy
    {
        using UnderlyingType = typename std::underlying_type <Model>::type;
        Model model;
    public:
        inline explicit EnumHelper (Model model)
            : model(model) {}
#if __cplusplus >= 201402L
        constexpr
#endif
        inline EnumHelper (const std::initializer_list <Model> &flags) {
            openFlag(flags);
        }
        inline Model getModel () const noexcept { return model; }
        inline void closeFlag (Model flag) noexcept {
            model = Model(static_cast<UnderlyingType>(model) & ~static_cast<UnderlyingType>(flag));
        }
        inline void openFlag (Model flag) noexcept {
            model = Model(static_cast<UnderlyingType>(model) | static_cast<UnderlyingType>(flag));
        }
        template <class ...Arg>
        inline void openFlag (Model flag, Arg ...arg) noexcept {
            openFlag(flag);
            openFlag(std::forward <Arg>(arg)...);
        }
        inline void openFlag (const std::initializer_list <Model> &flags) noexcept {
            for (const auto &v : flags) openFlag(v);
        }
        inline void toggleFlag (Model flag) noexcept {
            model = Model(static_cast<UnderlyingType>(model) ^ static_cast<UnderlyingType>(flag));
        }
        inline void setFlag (Model flag) noexcept {
            model = flag;
        }
        inline bool hasFlag (Model flag) const noexcept {
            return static_cast<UnderlyingType>(model) & static_cast<UnderlyingType>(flag);
        }
    };
}
#else
#include <string.h>
#include <stdio.h>
#include <errno.h>

inline static void getUuid (char *src)
{
    FILE *file = fopen(UUID_FILE_PATH, "r");
    if (!file)
    {
        fprintf(stderr, "can't open the file " UUID_FILE_PATH " error str : %s\n", strerror(errno));
        return;
    }

    fgets(src, UUID_STR_LEN, file);
    fclose(file);
}
#endif

#endif //LINUX_SERVER_LIB_INCLUDE_UTIL_H_
