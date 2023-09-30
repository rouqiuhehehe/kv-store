//
// Created by 115282 on 2023/8/14.
//

#ifndef LINUX_SERVER_LIB_INCLUDE_STRING_IS_NUMBER_H_
#define LINUX_SERVER_LIB_INCLUDE_STRING_IS_NUMBER_H_

#include <cstring>
#include <algorithm>
#include <cerrno>
#include <sstream>
#include <vector>
#include <limits>
#include "printf-color.h"

namespace Utils
{
    namespace StringHelper
    {
        namespace __PRIVATE__
        {
            template <class T>
            inline bool stringIsInteger (
                const std::string &str,
                T (*stoInteger) (const std::string &, size_t *, int),
                T *value = nullptr
            )
            {
                if (str.empty())
                    return false;
                size_t endPtr;
                try
                {
                    auto res = stoInteger(str, &endPtr, 10);
                    if (value)
                        *value = res;

                    auto minmax = str[0] == '-'
                                  ? std::numeric_limits <T>::min()
                                  : std::numeric_limits <T>::max();

                    return (endPtr == str.size()) && res != minmax;
                }
                catch (const std::invalid_argument &e)
                {
                    PRINT_ERROR("%s", e.what());
                    return false;
                }
                catch (const std::out_of_range &e)
                {
                    PRINT_ERROR("%s", e.what());
                    return false;
                }
            }
        }

        inline std::string stringTrim (const std::string &str)
        {
            if (str.empty())
                return "";

            size_t start = str.find_first_not_of(" \t\r\n");
            size_t end = str.find_last_not_of(" \t\r\n");
            if (start == std::string::npos || end == std::string::npos)
                return "";
            return str.substr(start, end - start + 1);
        }
        inline void stringTolower (std::string &str)
        {
            std::transform(str.begin(), str.end(), str.begin(), ::tolower);
        }
        inline void stringToupper
            (std::string &str)
        {
            std::transform(str.begin(), str.end(), str.begin(), ::toupper);
        }
        /*
         * @param str 输入字符串
         * @param split 分隔符
         * @param isContinuously 分割时是否判断连续的分隔符
         * @return std::vector<std::string>
         */
        inline std::vector <std::string> stringSplit (
            const std::string &str,
            const char split,
            bool isContinuously = true
        )
        {
            if (str.empty())
                return {};

            std::vector <std::string> res;
            size_t count = str.find(split);
            size_t index = 0;
            if (count == std::string::npos)
                count = str.size();
            while ((index + count) <= str.size())
            {
                res.emplace_back(str.substr(index, count));

                index += count + 1;
                count = 0;
                if (isContinuously)
                    while (index < str.size() && str[index] == split) index++;

                while ((index + count) < str.size() && str[index + ++count] != split);
            }

            return res;
        }

        inline bool stringIsL (const std::string &str, long *value = nullptr)
        {
            return __PRIVATE__::stringIsInteger(str, &std::stol, value);
        }
        inline bool stringIsUL (const std::string &str, unsigned long *value = nullptr)
        {
            return __PRIVATE__::stringIsInteger(str, &std::stoul, value);
        }
        inline bool stringIsLL (const std::string &str, long long *value = nullptr)
        {
            return __PRIVATE__::stringIsInteger(str, &std::stoll, value);
        }
        inline bool stringIsULL (const std::string &str, unsigned long long *value = nullptr)
        {
            return __PRIVATE__::stringIsInteger(str, &std::stoull, value);
        }

        inline bool stringIsI (const std::string &str, int *value = nullptr)
        {
            return __PRIVATE__::stringIsInteger(str, &std::stoi, value);
        }
        inline bool stringIsUI (const std::string &str, unsigned *value = nullptr)
        {
            unsigned long v;
            bool res = stringIsUL(str, &v);
            if (res && value)
                *value = v;

            return res;
        }
        // inline bool stringIsULL (const std::string &str, unsigned long long *value = nullptr)
        // {
        //     return __PRIVATE__::stringIsInteger(str, &std::strtoull, value);
        // }

        inline bool stringIsDouble (const std::string &str, double *value = nullptr)
        {
            if (str.empty())
                return false;
            errno = 0;
            char *endPtr;
            auto res = std::strtod(str.c_str(), &endPtr);
            if (value)
                *value = res;

            double minmax = str[0] == '-'
                            ? -std::numeric_limits <double>::infinity()
                            : std::numeric_limits <double>::infinity();
            bool success =
                (endPtr == (str.rbegin().operator->() + 1)) && errno != ERANGE && res != minmax;

            errno = 0;
            return success;
        }

        template <class T>
        inline std::string toString (T val)
        {
            static std::ostringstream stream;
            stream.str("");
            stream << val;

            return stream.str();
        }
    }
}
#endif //LINUX_SERVER_LIB_INCLUDE_STRING_IS_NUMBER_H_
