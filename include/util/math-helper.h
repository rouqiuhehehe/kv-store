//
// Created by 115282 on 2023/8/17.
//

#ifndef LINUX_SERVER_LIB_INCLUDE_UTIL_MATH_COMPUTE_H_
#define LINUX_SERVER_LIB_INCLUDE_UTIL_MATH_COMPUTE_H_

#include <cmath>
namespace Utils
{
    namespace MathHelper
    {
        template <class Compare>
        inline bool doubleCalculateWhetherOverflow (double count, double b) {
            return std::isinf(Compare()(count, b));
        }
        template <class Compare, class ...Arg>
        inline bool doubleCalculateWhetherOverflow (double a, double b, Arg ...nums) {
            return doubleCalculateWhetherOverflow <Compare>(Compare()(a, b), nums...);
        }

        template <class T>
        inline bool integerPlusOverflow (T a, T b) {
            T count = a + b;
            if ((a > 0 && b > 0 && count < 0) || (a < 0 && b < 0 && count > 0))
                return true;

            return false;
        }

        template <class T, class ...Arg>
        inline bool integerPlusOverflow (T a, T b, Arg ...nums) {
            T count = a + b;
            if ((a > 0 && b > 0 && count < 0) || (a < 0 && b < 0 && count > 0))
                return true;

            return integerPlusOverflow(count, nums...);
        }

        template <class T>
        inline bool integerMultipliesOverflow (T a, T b) {
            T count = a * b;
            if (a != 0 && (count / a != b))
                return true;

            return false;
        }

        template <class T, class ...Arg>
        inline bool integerMultipliesOverflow (T a, T b, Arg ...nums) {
            T count = a * b;
            if (a != 0 && (count / a != b))
                return true;

            return integerMultipliesOverflow(count, nums...);
        }

        template <class T>
        inline typename std::enable_if <
            Utils::TypeHelper::CheckTypeIsInteger <T>::value && std::is_unsigned <T>::value,
            size_t>::type getByteSize (T num) {
            if (num < std::numeric_limits <uint8_t>::max()) return sizeof(uint8_t);
            else if (num < std::numeric_limits <uint16_t>::max()) return sizeof(uint16_t);
            else if (num < std::numeric_limits <uint32_t>::max()) return sizeof(uint32_t);
            else if (num < std::numeric_limits <uint64_t>::max()) return sizeof(uint64_t);
            else return 0;
        }

        template <class T>
        inline typename std::enable_if <
            Utils::TypeHelper::CheckTypeIsInteger <T>::value && std::is_signed <T>::value,
            size_t>::type getByteSize (T num) {
            if (num < std::numeric_limits <int8_t>::max() && num > std::numeric_limits <int8_t>::min())
                return sizeof(int8_t);
            else if (num < std::numeric_limits <int16_t>::max() && num > std::numeric_limits <int16_t>::min())
                return sizeof(int16_t);
            else if (num < std::numeric_limits <int32_t>::max() && num > std::numeric_limits <int32_t>::min())
                return sizeof(int32_t);
            else if (num < std::numeric_limits <int64_t>::max() && num > std::numeric_limits <int64_t>::min())
                return sizeof(int64_t);
            else return 0;
        }
    }
}
#endif //LINUX_SERVER_LIB_INCLUDE_UTIL_MATH_COMPUTE_H_
