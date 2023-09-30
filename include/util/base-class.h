//
// Created by 115282 on 2023/7/28.
//

#ifndef LINUX_SERVER_LIB_INCLUDE_BASE_CLASS_H_
#define LINUX_SERVER_LIB_INCLUDE_BASE_CLASS_H_
#include "global.h"
class Base;
class BasePrivate
{
public:
    BasePrivate ()
        : q_ptr(nullptr) {};
    virtual ~BasePrivate () noexcept = default;

protected:
    explicit BasePrivate (Base *parent)
        : q_ptr(parent) {}

    DECLARE_PUBLIC_Q(Base);
    DECLARE_PTR_Q(Base);

    bool isValid = true;
};
class Base
{
public:
    Base ()
        : d_ptr(nullptr) {};

    virtual ~Base () noexcept = default;

    virtual inline bool isValid () const noexcept = 0;

protected:
    explicit Base (BasePrivate *d)
        : d_ptr(d) {}
    DECLARE_PRIVATE_D(Base);
    DECLARE_PTR_D(Base);
};

template <class T>
class BaseTemplate;
template <class T>
class BaseTemplatePrivate
{
public:
    BaseTemplatePrivate ()
        : q_ptr(nullptr) {};
    virtual ~BaseTemplatePrivate () noexcept = default;

protected:
    explicit BaseTemplatePrivate (BaseTemplate <T> *parent)
        : q_ptr(parent) {}

    DECLARE_TEMPLATE_PUBLIC_Q(BaseTemplate, T
    );
    DECLARE_TEMPLATE_PTR_Q(BaseTemplate, T
    );

    bool isValid = true;
};
template <class T>
class BaseTemplate
{
public:
    BaseTemplate ()
        : d_ptr(nullptr) {};

    virtual ~BaseTemplate () noexcept = default;

    virtual inline bool isValid () const noexcept = 0;

protected:
    explicit BaseTemplate (BaseTemplatePrivate <T> *d)
        : d_ptr(d) {}
    DECLARE_TEMPLATE_PRIVATE_D(BaseTemplate, T);
    DECLARE_TEMPLATE_PTR_D(BaseTemplate, T);
};

template <class T, class U>
class BaseTemplate2;
template <class T, class U>
class BaseTemplate2Private
{
public:
    BaseTemplate2Private ()
        : q_ptr(nullptr) {};
    virtual ~BaseTemplate2Private () noexcept = default;

protected:
    explicit BaseTemplate2Private (BaseTemplate2 <T, U> *parent)
        : q_ptr(parent) {}

    DECLARE_TEMPLATE_2_PUBLIC_Q(BaseTemplate2, T, U);
    DECLARE_TEMPLATE_2_PTR_Q(BaseTemplate2, T, U);

    bool isValid = true;
};
template <class T, class U>
class BaseTemplate2
{
public:
    BaseTemplate2 ()
        : d_ptr(nullptr) {};

    virtual ~BaseTemplate2 () noexcept = default;

    virtual inline bool isValid () const noexcept = 0;

protected:
    explicit BaseTemplate2 (BaseTemplate2Private <T, U> *d)
        : d_ptr(d) {}
    DECLARE_TEMPLATE_2_PRIVATE_D(BaseTemplate2, T, U);
    DECLARE_TEMPLATE_2_PTR_D(BaseTemplate2, T, U);
};

template <class T, class U, class V>
class BaseTemplate3;
template <class T, class U, class V>
class BaseTemplate3Private
{
public:
    BaseTemplate3Private ()
        : q_ptr(nullptr) {};
    virtual ~BaseTemplate3Private () noexcept = default;

protected:
    explicit BaseTemplate3Private (BaseTemplate3 <T, U, V> *parent)
        : q_ptr(parent) {}

    DECLARE_TEMPLATE_3_PUBLIC_Q(BaseTemplate3, T, U, V);
    DECLARE_TEMPLATE_3_PTR_Q(BaseTemplate3, T, U, V);

    bool isValid = true;
};
template <class T, class U, class V>
class BaseTemplate3
{
public:
    BaseTemplate3 ()
        : d_ptr(nullptr) {};

    virtual ~BaseTemplate3 () noexcept = default;

    virtual inline bool isValid () const noexcept = 0;

protected:
    explicit BaseTemplate3 (BaseTemplate3Private <T, U, V> *d)
        : d_ptr(d) {}
    DECLARE_TEMPLATE_3_PRIVATE_D(BaseTemplate3, T, U, V);
    DECLARE_TEMPLATE_3_PTR_D(BaseTemplate3, T, U, V);
};

template <class T, class U, class V, class W>
class BaseTemplate4;
template <class T, class U, class V, class W>
class BaseTemplate4Private
{
public:
    BaseTemplate4Private ()
        : q_ptr(nullptr) {};
    virtual ~BaseTemplate4Private () noexcept = default;

protected:
    explicit BaseTemplate4Private (BaseTemplate4 <T, U, V, W> *parent)
        : q_ptr(parent) {}

    DECLARE_TEMPLATE_4_PUBLIC_Q(BaseTemplate4, T, U, V, W);
    DECLARE_TEMPLATE_4_PTR_Q(BaseTemplate4, T, U, V, W);

    bool isValid = true;
};
template <class T, class U, class V, class W>
class BaseTemplate4
{
public:
    BaseTemplate4 ()
        : d_ptr(nullptr) {};

    virtual ~BaseTemplate4 () noexcept = default;

    virtual inline bool isValid () const noexcept = 0;

protected:
    explicit BaseTemplate4 (BaseTemplate4Private <T, U, V, W> *d)
        : d_ptr(d) {}
    DECLARE_TEMPLATE_4_PRIVATE_D(BaseTemplate4, T, U, V, W);
    DECLARE_TEMPLATE_4_PTR_D(BaseTemplate4, T, U, V, W);
};
#endif //LINUX_SERVER_LIB_INCLUDE_BASE_CLASS_H_
