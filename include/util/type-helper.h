//
// Created by 115282 on 2024/1/18.
//

#ifndef KV_STORE_INCLUDE_UTIL_TYPE_HELPER_H_
#define KV_STORE_INCLUDE_UTIL_TYPE_HELPER_H_

#include <functional>
#ifndef __cpp_lib_void_t
template <typename...> using void_t = void;
#else
using std::void_t<>;
#endif
namespace Utils
{
    namespace TypeHelper
    {
        template <class T>
        using CheckTypeIsInteger = typename std::is_integral <typename std::remove_reference <T>::type>;
        template <typename Lambda, typename = void>
        struct lambdaTraits
            : public lambdaTraits <decltype(&std::function <Lambda>::operator())>
        {
        };

        template <typename Lambda>
        struct lambdaTraits <Lambda, void_t < decltype(&Lambda::operator())>>
        : public lambdaTraits <decltype(&Lambda::operator())>
    {
    };

    template <typename ClassType, typename ReturnType, typename... Args>
    struct lambdaTraits <ReturnType(ClassType::*) (Args...) const>
    {
        using classType = ClassType;
        using returnType = ReturnType;
        static constexpr size_t arity = sizeof...(Args);
    };
}
}

#endif //KV_STORE_INCLUDE_UTIL_TYPE_HELPER_H_
