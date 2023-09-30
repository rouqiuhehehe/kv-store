#pragma clang diagnostic push
#pragma ide diagnostic ignored "bugprone-macro-parentheses"
//
// Created by Yoshiki on 2023/6/2.
//

#ifndef LINUX_SERVER_LIB_INCLUDE_GLOBAL_H_
#define LINUX_SERVER_LIB_INCLUDE_GLOBAL_H_

#if defined __GNUC__
#define likely(x) __builtin_expect ((x), 1)
#define unlikely(x) __builtin_expect ((x), 0)
#else
#define likely(x) (x)
#define unlikely(x) (x)
#endif

#ifdef __cplusplus
#include <cerrno>
#else
#include <errno.h>
#endif
#define CHECK_RET(expr, fnName)                                                 \
            if (expr)                                                           \
            {                                                                   \
                fprintf(stderr,"%s:%s:%d:  "#fnName" err %s\n",                 \
                         __FILE__, __FUNCTION__, __LINE__, strerror(errno));    \
                exit(EXIT_FAILURE);                                             \
            }

#ifdef __cplusplus

#include <memory>

template <class T>
inline T *GetPtrHelper (T *d) { return d; }
template <class T>
inline auto GetPtrHelper (const T &d) -> decltype(d.operator->()) { return d.operator->(); }

#define DECLARE_PTR_Q(Class) Class *q_ptr
#define DECLARE_PTR_D(Class) std::unique_ptr<Class##Private> d_ptr
#define DECLARE_TEMPLATE_PTR_Q(Class, T) Class<T> *q_ptr
#define DECLARE_TEMPLATE_PTR_D(Class, T) std::unique_ptr<Class##Private<T>> d_ptr
#define DECLARE_TEMPLATE_2_PTR_Q(Class, T, U) Class<T, U> *q_ptr
#define DECLARE_TEMPLATE_2_PTR_D(Class, T, U) std::unique_ptr<Class##Private<T, U>> d_ptr
#define DECLARE_TEMPLATE_3_PTR_Q(Class, T, U, V) Class<T, U, V> *q_ptr
#define DECLARE_TEMPLATE_3_PTR_D(Class, T, U, V) std::unique_ptr<Class##Private<T, U, V>> d_ptr
#define DECLARE_TEMPLATE_4_PTR_Q(Class, T, U, V, W) Class<T, U, V, W> *q_ptr
#define DECLARE_TEMPLATE_4_PTR_D(Class, T, U, V, W) std::unique_ptr<Class##Private<T, U, V, W>> d_ptr

#define DECLARE_PRIVATE_D(Class) \
    inline Class##Private *d_fun() { return reinterpret_cast<Class##Private *>(GetPtrHelper(d_ptr)); } \
    inline const Class##Private *d_fun() const { return reinterpret_cast<const Class##Private *>(GetPtrHelper(d_ptr)); } \
    friend class Class##Private

#define DECLARE_TEMPLATE_PRIVATE_D(Class, T) \
    inline Class##Private<T> *d_fun() { return reinterpret_cast<Class##Private<T> *>(GetPtrHelper(this->d_ptr)); } \
    inline const Class##Private<T> *d_fun() const { return reinterpret_cast<const Class##Private<T> *>(GetPtrHelper(this->d_ptr)); } \
    friend class Class##Private<T>
#define DECLARE_TEMPLATE_2_PRIVATE_D(Class, T, U) \
    inline Class##Private<T,U> *d_fun() { return reinterpret_cast<Class##Private<T,U> *>(GetPtrHelper(this->d_ptr)); } \
    inline const Class##Private<T,U> *d_fun() const { return reinterpret_cast<const Class##Private<T,U> *>(GetPtrHelper(this->d_ptr)); } \
    friend class Class##Private<T,U>
#define DECLARE_TEMPLATE_3_PRIVATE_D(Class, T, U, V) \
    inline Class##Private<T,U,V> *d_fun() { return reinterpret_cast<Class##Private<T,U,V> *>(GetPtrHelper(this->d_ptr)); } \
    inline const Class##Private<T,U,V> *d_fun() const { return reinterpret_cast<const Class##Private<T,U,V> *>(GetPtrHelper(this->d_ptr)); } \
    friend class Class##Private<T,U,V>
#define DECLARE_TEMPLATE_4_PRIVATE_D(Class, T, U, V, W) \
    inline Class##Private<T,U,V,W> *d_fun() { return reinterpret_cast<Class##Private<T,U,V,W> *>(GetPtrHelper(this->d_ptr)); } \
    inline const Class##Private<T,U,V,W> *d_fun() const { return reinterpret_cast<const Class##Private<T,U,V,W> *>(GetPtrHelper(this->d_ptr)); } \
    friend class Class##Private<T,U,V,W>

#define D_PTR(Class) Class##Private *const d = d_fun()
#define D_TEMPLATE_PTR(Class, T) Class##Private<T> *const d = d_fun()
#define D_TEMPLATE_2_PTR(Class, T, U) Class##Private<T, U> *const d = d_fun()
#define D_TEMPLATE_3_PTR(Class, T, U, V) Class##Private<T, U, V> *const d = d_fun()
#define D_TEMPLATE_4_PTR(Class, T, U, V, W) Class##Private<T, U, V, W> *const d = d_fun()

#define DECLARE_PUBLIC_Q(Class) \
    inline Class *q_fun() { return reinterpret_cast<Class *>(GetPtrHelper(q_ptr)); } \
    inline const Class *q_fun() const { return reinterpret_cast<Class *>(GetPtrHelper(q_ptr)); } \
    friend class Class
#define DECLARE_TEMPLATE_PUBLIC_Q(Class, T) \
    inline Class<T> *q_fun() { return reinterpret_cast<Class<T> *>(GetPtrHelper(this->q_ptr)); } \
    inline const Class<T> *q_fun() const { return reinterpret_cast<Class<T> *>(GetPtrHelper(this->q_ptr)); } \
    friend class Class<T>
#define DECLARE_TEMPLATE_2_PUBLIC_Q(Class, T, U) \
    inline Class<T, U> *q_fun() { return reinterpret_cast<Class<T, U> *>(GetPtrHelper(this->q_ptr)); } \
    inline const Class<T, U> *q_fun() const { return reinterpret_cast<Class<T, U> *>(GetPtrHelper(this->q_ptr)); } \
    friend class Class<T, U>
#define DECLARE_TEMPLATE_3_PUBLIC_Q(Class, T, U, V) \
    inline Class<T, U, V> *q_fun() { return reinterpret_cast<Class<T, U, V> *>(GetPtrHelper(this->q_ptr)); } \
    inline const Class<T, U, V> *q_fun() const { return reinterpret_cast<Class<T, U, V> *>(GetPtrHelper(this->q_ptr)); } \
    friend class Class<T, U, V>
#define DECLARE_TEMPLATE_4_PUBLIC_Q(Class, T, U, V, W) \
    inline Class<T, U, V, W> *q_fun() { return reinterpret_cast<Class<T, U, V, W> *>(GetPtrHelper(this->q_ptr)); } \
    inline const Class<T, U, V, W> *q_fun() const { return reinterpret_cast<Class<T, U, V, W> *>(GetPtrHelper(this->q_ptr)); } \
    friend class Class<T, U, V, W>

#define Q_PTR(Class) Class *const q = q_fun()
#define Q_TEMPLATE_PTR(Class, T) Class<T> *const q = q_fun()
#define Q_TEMPLATE_2_PTR(Class, T, U) Class<T, U> *const q = q_fun()
#define Q_TEMPLATE_3_PTR(Class, T, U, V) Class<T, U, V> *const q = q_fun()
#define Q_TEMPLATE_4_PTR(Class, T, U, V, W) Class<T, U, V, W> *const q = q_fun()

#define CLASS_IS_VALID(Class, exp) inline bool isValid () const noexcept override { \
                                    const D_PTR(Class);                        \
                                    return ((typeid(*d) == typeid(Class##Private)) && d->isValid && exp); \
                              }

#define CLASS_TEMPLATE_IS_VALID(Class, exp, T) inline bool isValid () const noexcept override { \
                                    const D_TEMPLATE_PTR(Class, T);                        \
                                    return ((typeid(*d) == typeid(Class##Private<T>)) && d->isValid && exp); \
                              }
#define CLASS_TEMPLATE_2_IS_VALID(Class, exp, T, U) inline bool isValid () const noexcept override { \
                                    const D_TEMPLATE_2_PTR(Class, T, U);                        \
                                    return ((typeid(*d) == typeid(Class##Private<T, U>)) && d->isValid && exp); \
                              }
#define CLASS_TEMPLATE_3_IS_VALID(Class, exp, T, U, V) inline bool isValid () const noexcept override { \
                                    const D_TEMPLATE_3_PTR(Class, T, U, V);                        \
                                    return ((typeid(*d) == typeid(Class##Private<T, U, V>)) && d->isValid && exp); \
                              }
#define CLASS_TEMPLATE_4_IS_VALID(Class, exp, T, U, V, W) inline bool isValid () const noexcept override { \
                                    const D_TEMPLATE_4_PTR(Class, T, U, V, W);                        \
                                    return ((typeid(*d) == typeid(Class##Private<T, U, V, W>)) && d->isValid && exp); \
                              }

#define CLASS_DEFAULT_COPY_CONSTRUCTOR(Class, Parent) \
    Class(const Class &rhs) : Parent(nullptr) { this->operator=(rhs); } \
    Class &operator=(const Class &rhs) { if (this == &rhs) return *this;    \
                                        this->d_ptr = std::unique_ptr <Class##Private>(new Class##Private(*rhs.d_fun())); \
                                        return *this; }                 \

#define CLASS_DEFAULT_MOVE_COPY_CONSTRUCTOR(Class, Parent) \
    Class(Class &&rhs) noexcept : Parent(nullptr) { this->operator=(std::move(rhs)); } \
    Class &operator=(Class &&rhs) noexcept { if (this == &rhs) return *this; \
                                            this->d_ptr = std::move(rhs.d_ptr); \
                                            return *this; }

#define CLASS_DEFAULT_ALL_COPY_CONSTRUCTOR(Class, Parent) \
    CLASS_DEFAULT_COPY_CONSTRUCTOR(Class, Parent)         \
    CLASS_DEFAULT_MOVE_COPY_CONSTRUCTOR(Class, Parent)

#define CLASS_COPY_CONSTRUCTOR_DISABLED(Class) \
    Class(const Class &) = delete;             \
    Class &operator=(const Class &) = delete;

#include "base-class.h"
#endif
#endif //LINUX_SERVER_LIB_INCLUDE_GLOBAL_H_

#pragma clang diagnostic pop