//
// Created by 115282 on 2023/9/14.
//

#ifndef LINUX_SERVER_LIB_INCLUDE_UTIL_RANDOM_H_
#define LINUX_SERVER_LIB_INCLUDE_UTIL_RANDOM_H_

#define UUID_FILE_PATH "/proc/sys/kernel/random/uuid"
#define UUID_STR_LEN 37

#ifdef __cplusplus
#include <fstream>
#include <locale>
#include <codecvt>
#include <random>
#include <iostream>
#include <cstring>

namespace Utils
{
    namespace Random
    {
        inline int getRandomNum (int a, int b)
        {
            static std::random_device device;
            static std::default_random_engine engine(device());

            return std::uniform_int_distribution <int>(a, b)(engine);
        }

        inline std::string getRandomStr (size_t size)
        {
            std::string res;
            char c;

            do
            {
                c = static_cast<char>(getRandomNum(65, 122));
                if (c >= 91 && c <= 96)
                    continue;

                res += c;
            } while (res.size() != size);

            return res;
        }

        inline std::string getRandomStr (size_t minSize, size_t maxSize)
        {
            size_t size = getRandomNum(static_cast<int>(minSize), static_cast<int>(maxSize));

            return getRandomStr(size);
        }

        inline std::string getRandomChinese ()
        {
            int seek = getRandomNum(0, 0x9fa5 - 0x4e00 + 1);
            // 随机生成一个中文字符的 Unicode 值
            int unicode = 0x4e00 + seek % (0x9fa5 - 0x4e00 + 1);

            static std::wstring_convert <std::codecvt_utf8 <wchar_t>, wchar_t> utf8_converter;
            std::string uft8Str = utf8_converter.to_bytes(unicode);

            return uft8Str;
        }

        inline std::string getUuid ()
        {
            std::ifstream ifStream(UUID_FILE_PATH);
            if (ifStream.is_open())
            {
                char uuid[UUID_STR_LEN];
                ifStream.get(uuid, UUID_STR_LEN);

                if (ifStream.gcount() != UUID_STR_LEN - 1)
                {
                    std::cerr << "create uuid length error, need 36, real " << ifStream.gcount()
                              << std::endl;
                    ifStream.close();
                    return {};
                }

                ifStream.close();
                return uuid;
            }
            else
            {
                std::cerr << "can't open the file " UUID_FILE_PATH ", error str : "
                          << strerror(errno) << std::endl;
                return {};
            }
        }
    }
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
#endif //LINUX_SERVER_LIB_INCLUDE_UTIL_RANDOM_H_
