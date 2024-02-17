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
#include <type_traits>

#include "util/type-helper.h"

namespace Utils
{
    namespace Random
    {
        // 生成 [a,b] 之间的整数
        template <class T>
        inline typename std::enable_if <
            std::is_integral <typename std::remove_reference <T>::type>::value, typename std::remove_reference <T>::type
        >::type getRandomNum (T a, T b) {
            static std::random_device device;
            static std::default_random_engine engine(device());

            return std::uniform_int_distribution <typename std::remove_reference <T>::type>(a, b)(engine);
        }

        // 生成 [a,b] 之间的浮点数
        template <class T>
        inline typename std::enable_if <
            std::is_floating_point <typename std::remove_reference <T>::type>::value,
            typename std::remove_reference <T>::type
        >::type getRandomNum (T a, T b) {
            static std::random_device device;
            static std::default_random_engine engine(device());

            return std::uniform_real_distribution <typename std::remove_reference <T>::type>(a, b)(engine);
        }

        inline std::string getRandomStr (size_t size) {
            std::string res;
            char c;

            do {
                c = static_cast<char>(getRandomNum(65, 122));
                if (c >= 91 && c <= 96)
                    continue;

                res += c;
            } while (res.size() != size);

            return res;
        }

        inline std::string getRandomStr (size_t minSize, size_t maxSize) {
            size_t size = getRandomNum(static_cast<int>(minSize), static_cast<int>(maxSize));

            return getRandomStr(size);
        }

        inline std::string getRandomChinese () {
            int seek = getRandomNum(0, 0x9fa5 - 0x4e00 + 1);
            // 随机生成一个中文字符的 Unicode 值
            int unicode = 0x4e00 + seek % (0x9fa5 - 0x4e00 + 1);

            static std::wstring_convert <std::codecvt_utf8 <wchar_t>, wchar_t> utf8_converter;
            std::string uft8Str = utf8_converter.to_bytes(unicode);

            return uft8Str;
        }

        inline std::string getUuid () {
            std::ifstream ifStream(UUID_FILE_PATH);
            if (ifStream.is_open()) {
                char uuid[UUID_STR_LEN];
                ifStream.get(uuid, UUID_STR_LEN);

                if (ifStream.gcount() != UUID_STR_LEN - 1) {
                    PRINT_ERROR("create uuid length error, need 36, real %d", ifStream.gcount());
                    ifStream.close();
                    return {};
                }

                ifStream.close();
                return uuid;
            }
            else {
                PRINT_ERROR("can't open the file " UUID_FILE_PATH ", error str : %s", strerror(errno));
                return {};
            }
        }

        // 获取一个区间范围内不重复的随机值
        template <class T, class Engine = std::default_random_engine>
        class Sample
        {
            using _UniformTemplate = std::uniform_int_distribution <size_t>;
            using _ParamsType = typename _UniformTemplate::param_type;

        public:
            template <class U = T>
            typename std::enable_if
                <std::is_integral <typename std::remove_reference <U>::type>::value, ArrayType <U>>::type
            operator() (T a, T b, size_t n) const {
                using _CommonType = typename std::common_type <T, size_t>::type;
                if (b < a)
                    std::swap(a, b);

                std::random_device device;
                Engine engine(device());
                _UniformTemplate uniformTemplate {};
                _CommonType sampleRange = b - a + 1;
                // 如果需要的个数比范围大，将个数设为range
                n = std::min(n, sampleRange);
                ArrayType <T> ret(n);
                _CommonType cursor = a;
                auto it = ret.begin();
                _CommonType tempRange = engine.max() - engine.min();
                // 如果给定的模板范围，大于
                if (tempRange / sampleRange >= sampleRange) {
                    while (n != 0 && sampleRange >= 2) {
                        auto p = genTwoUniformInts(sampleRange, sampleRange - 1, engine);

                        --sampleRange;
                        if (p.first < static_cast<_CommonType>(n)) {
                            *it++ = cursor;
                            --n;
                        }

                        ++cursor;
                        if (n == 0) break;

                        --sampleRange;
                        if (p.second < static_cast<_CommonType>(n)) {
                            *it++ = cursor;
                            --n;
                        }
                        ++cursor;
                    }
                }

                for (; n != 0; ++cursor) {
                    if (uniformTemplate(engine, _ParamsType { std::numeric_limits <size_t>::min(), --sampleRange })
                        < n) {
                        *it++ = cursor;
                        --n;
                    }
                }

                return ret;
            }

            template <class Iterator, class OutIterator, class Diff, class Operator>
            void operator() (
                Iterator begin,
                Iterator end,
                OutIterator out,
                Diff diff,
                size_t n,
                Operator &&op
            ) {
                using _CommonType = typename std::common_type <decltype(diff), size_t>::type;
                if (diff == 0 || begin == end) return;

                std::random_device device;
                Engine engine(device());
                _UniformTemplate uniformTemplate {};
                _CommonType sampleRange = diff;
                // 如果需要的个数比范围大，将个数设为range
                n = std::min(n, sampleRange);
                _CommonType tempRange = engine.max() - engine.min();
                // 如果给定的模板范围，大于
                if (tempRange / sampleRange >= sampleRange) {
                    while (n != 0 && sampleRange >= 2) {
                        auto p = genTwoUniformInts(sampleRange, sampleRange - 1, engine);

                        --sampleRange;
                        if (p.first < static_cast<_CommonType>(n)) {
                            assign <OutIterator, Iterator, Operator>(out, *begin, std::forward <Operator>(op));
                            out++;
                            // op(out++, *begin);
                            --n;
                        }

                        ++begin;
                        if (n == 0) break;

                        --sampleRange;
                        if (p.second < static_cast<_CommonType>(n)) {
                            assign <OutIterator, Iterator, Operator>(out, *begin, std::forward <Operator>(op));
                            out++;
                            // op(out++, *begin);
                            --n;
                        }
                        ++begin;
                    }
                }

                for (; n != 0; ++begin) {
                    if (uniformTemplate(engine, _ParamsType { std::numeric_limits <size_t>::min(), --sampleRange })
                        < n) {
                        assign <OutIterator, Iterator, Operator>(out, *begin, std::forward <Operator>(op));
                        out++;
                        // op(out++, *begin);
                        --n;
                    }
                }
            }

            template <class Iterator, class Diff, class Operator>
            ArrayType <T> operator() (
                Iterator begin,
                Iterator end,
                Diff diff,
                size_t n,
                Operator &&op
            ) {
                ArrayType <T> ret(n);

                operator()(begin, end, ret.begin(), diff, n, std::forward <Operator>(op));
                return ret;
            }

            template <class Iterator, class Diff>
            ArrayType <T> operator() (
                Iterator begin,
                Iterator end,
                Diff diff,
                size_t n
            ) {
                return operator()(
                    begin,
                    end,
                    diff,
                    n,
                    std::function <decltype(defaultAssign < Iterator > )> { defaultAssign < Iterator > }
                );
            }

            template <class Iterator>
            ArrayType <T> operator() (
                Iterator begin,
                Iterator end,
                size_t n
            ) {
                auto diff = std::distance(begin, end);
                return operator()(begin, end, diff, n);
            }

        private:
            template <class U, class _UniformRandomBitGenerator>
            static inline std::pair <U, U> genTwoUniformInts (U a, U b, _UniformRandomBitGenerator &&g) {
                U x = std::uniform_int_distribution <U> { 0, a * b - 1 }(g);
                return { x / b, x % b };
            }

            template <class OutIterator,
                class Iterator,
                class Operator>
            static inline typename std::enable_if <Utils::TypeHelper::lambdaTraits <Operator>::arity == 1,
                                                   void>::type assign (
                OutIterator it,
                typename std::iterator_traits <Iterator>::reference v,
                Operator &&op
            ) {
                *it = op(v);
            }

            template <class OutIterator,
                class Iterator,
                class Operator>
            static inline typename std::enable_if <Utils::TypeHelper::lambdaTraits <Operator>::arity == 2,
                                                   void>::type assign (
                OutIterator it,
                typename std::iterator_traits <Iterator>::reference v,
                Operator &&op
            ) {
                static_assert(
                    std::is_same <typename Utils::TypeHelper::lambdaTraits <Operator>::returnType,
                                  void>::value, "return type must be void, please assign value in lambda"
                );
                op(it, v);
            }

            template <class Iterator>
            static inline typename std::iterator_traits <Iterator>::value_type defaultAssign (
                typename std::iterator_traits <Iterator>::reference v
            ) {
                return v;
            }
        };
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
