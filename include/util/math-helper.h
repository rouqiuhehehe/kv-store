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
        inline bool doubleCalculateWhetherOverflow (double count, double b)
        {
            return std::isinf(Compare()(count, b));
        }
        template <class Compare, class ...Arg>
        inline bool doubleCalculateWhetherOverflow (double a, double b, Arg ...nums)
        {
            return doubleCalculateWhetherOverflow <Compare>(Compare()(a, b), nums...);
        }

        template <class T>
        inline bool integerPlusOverflow (T a, T b)
        {
            T count = a + b;
            if ((a > 0 && b > 0 && count < 0) || (a < 0 && b < 0 && count > 0))
                return true;

            return false;
        }

        template <class T, class ...Arg>
        inline bool integerPlusOverflow (T a, T b, Arg ...nums)
        {
            T count = a + b;
            if ((a > 0 && b > 0 && count < 0) || (a < 0 && b < 0 && count > 0))
                return true;

            return integerPlusOverflow(count, nums...);
        }

        template <class T>
        inline bool integerMultipliesOverflow (T a, T b)
        {
            T count = a * b;
            if (a != 0 && (count / a != b))
                return true;

            return false;
        }

        template <class T, class ...Arg>
        inline bool integerMultipliesOverflow (T a, T b, Arg ...nums)
        {
            T count = a * b;
            if (a != 0 && (count / a != b))
                return true;

            return integerMultipliesOverflow(count, nums...);
        }
    }
}
#endif //LINUX_SERVER_LIB_INCLUDE_UTIL_MATH_COMPUTE_H_
