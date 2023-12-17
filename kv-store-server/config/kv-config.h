//
// Created by 115282 on 2023/9/25.
//

#ifndef LINUX_SERVER_LIB_KV_STORE_CONFIG_KV_CONFIG_H_
#define LINUX_SERVER_LIB_KV_STORE_CONFIG_KV_CONFIG_H_
#include <fstream>
#include "printf-color.h"
#include "util/string-helper.h"
#include "data-structure/kv-hash-table.h"

#define LINE_BUFFER_SIZE 128
#define BIND_MAX 16
#define BOOL_YES "yes"
#define BOOL_NO "no"

#define ENUM_SET_NIL (-1)

#define GET_ENUM_UINT64(v) static_cast<uint64_t>(v)
#define GET_ENUM_INT(v) static_cast<int>(v)
#define CREATE_CONFIG_ADD_MAP_COMMON(name) \
        configParseMap[#name] = &(name);  \
        (name).getHandler().init()

#define CREATE_NUMERIC_CONFIG_CUSTOM(Class, name, flags, numericType, defaultValue, configAddr, min, max) \
        static Class name(numericType,defaultValue, configAddr,GET_ENUM_UINT64(flags),#name,min,max);    \
        CREATE_CONFIG_ADD_MAP_COMMON(name)

#define CREATE_NUMERIC_CONFIG(name, flags, numericType, defaultValue, configAddr, min, max) \
        CREATE_NUMERIC_CONFIG_CUSTOM(__KV_PRIVATE__::NumericConfigData, name, flags, numericType, defaultValue, configAddr, min, max)

#define CREATE_STRING_CONFIG_CUSTOM(Class, name, flags, defaultValue, configAddr, convertEmptyToNull) \
        static Class name(defaultValue,configAddr,convertEmptyToNull,GET_ENUM_UINT64(flags),#name);  \
        CREATE_CONFIG_ADD_MAP_COMMON(name)

#define CREATE_STRING_CONFIG(name, flags, defaultValue, configAddr, convertEmptyToNull) \
        CREATE_STRING_CONFIG_CUSTOM(__KV_PRIVATE__::StringConfigData, name, flags, defaultValue, configAddr, convertEmptyToNull)

#define CREATE_BOOL_CONFIG_CUSTOM(Class, name, flags, defaultValue, configAddr) \
        static Class name(defaultValue,configAddr,GET_ENUM_UINT64(flags),#name);  \
        CREATE_CONFIG_ADD_MAP_COMMON(name)

#define CREATE_BOOL_CONFIG(name, flags, defaultValue, configAddr) \
        CREATE_BOOL_CONFIG_CUSTOM(__KV_PRIVATE__::BoolConfigData, name, flags, defaultValue, configAddr)

#define CREATE_BOOL_CONFIG_CUSTOM(Class, name, flags, defaultValue, configAddr) \
        static Class name(defaultValue,configAddr,GET_ENUM_UINT64(flags),#name);  \
        CREATE_CONFIG_ADD_MAP_COMMON(name)

#define CREATE_ENUM_CONFIG(name, flags, enumSet, defaultValue, configAddr) \
        CREATE_ENUM_CONFIG_CUSTOM(__KV_PRIVATE__::EnumConfigData, name, flags, enumSet, defaultValue, configAddr)

#define CREATE_ENUM_CONFIG_CUSTOM(Class, name, flags, enumSet, defaultValue, configAddr) \
        static Class name(enumSet,defaultValue,configAddr,GET_ENUM_UINT64(flags),#name);  \
        CREATE_CONFIG_ADD_MAP_COMMON(name)

enum class ListMaxListPackSizeType
{
    TYPE_NUM,  // 最大数量
    TYPE_SIZE  // 最大字节数
};
struct ListMaxListPackSize
{
    ListMaxListPackSizeType type;
    uint32_t number;
};
// 注释详见kv-store.conf配置
struct KvConfigFields // NOLINT
{
    StringType bind[BIND_MAX + 1]; // 绑定ip
    uint32_t port; // 端口
    bool ioThreadsDoReads; // 是否开启多线程处理网络io
    uint32_t ioThreads; // 开启的线程数
    bool daemonize; // 是否为守护进程模式启动
    StringType pidFile; // pid文件路径
    StringType logFile; // 日志路径
    int logLevel; // 日志等级
    uint32_t databases; // 数据库数量，目前只支持了一个
    uint32_t timeout; // tcp连接闲置N秒后关闭，0为不开启
    bool protectedMode; // 是否开启保护模式
    StringType requirePass; // 密码

    ListMaxListPackSize listMaxListPackSize; // list命令 ziplist超出当前阈值，创建quicklist（暂未支持）
    uint32_t listCompressDepth;

    uint64_t hashMaxListPackEntries; // hash命令 允许的最大条目数
    uint64_t hashMaxListPackValue; // hash命令 允许单个条目数的最大占用字节数
};

namespace __KV_PRIVATE__
{
#define CONVERT_EMPTY_TO_NULL_VAL "(NULL)"
#define NUMERIC_ASSIGN_VALUE(val, field) *base->data.numeric.configValue.field = val
#define NUMERIC_SET_VALUE(val) \
    switch (base->data.numeric.numericType) \
    {   \
        case NumericType::NUMERIC_TYPE_INT: \
            NUMERIC_ASSIGN_VALUE(val, i);    \
            break;  \
        case NumericType::NUMERIC_TYPE_UINT:    \
            NUMERIC_ASSIGN_VALUE(val, ui);   \
            break;  \
        case NumericType::NUMERIC_TYPE_LONG:    \
            NUMERIC_ASSIGN_VALUE(val, l);    \
            break;  \
        case NumericType::NUMERIC_TYPE_ULONG:   \
            NUMERIC_ASSIGN_VALUE(val, ul);   \
            break;  \
        case NumericType::NUMERIC_TYPE_LONG_LONG:   \
            NUMERIC_ASSIGN_VALUE(val, ll);   \
            break;  \
        case NumericType::NUMERIC_TYPE_ULONG_LONG:  \
            NUMERIC_ASSIGN_VALUE(val, ull);  \
            break;  \
        case NumericType::NUMERIC_TYPE_SIZE_T:  \
            NUMERIC_ASSIGN_VALUE(val, st);   \
            break;  \
        case NumericType::NUMERIC_TYPE_SSIZE_T: \
            NUMERIC_ASSIGN_VALUE(val, sst);  \
            break;  \
        case NumericType::NUMERIC_TYPE_OFF_T:   \
            NUMERIC_ASSIGN_VALUE(val, ot);   \
            break;  \
        case NumericType::NUMERIC_TYPE_TIME_T:  \
            NUMERIC_ASSIGN_VALUE(val, tt);   \
            break;  \
    }
#define NUMERIC_ASSIGN_DEFAULT_VALUE NUMERIC_SET_VALUE(base->data.numeric.defaultValue)
#define NUMERIC_GET_VALUE(val) \
    switch (base->data.numeric.numericType) \
    {   \
        case NumericType::NUMERIC_TYPE_INT: \
            *(val) = *base->data.numeric.configValue.i;    \
            break;  \
        case NumericType::NUMERIC_TYPE_UINT:    \
            *(val) = *base->data.numeric.configValue.ui;   \
            break;  \
        case NumericType::NUMERIC_TYPE_LONG:    \
            *(val) = *base->data.numeric.configValue.l;    \
            break;  \
        case NumericType::NUMERIC_TYPE_ULONG:   \
            *(val) = *base->data.numeric.configValue.ul;   \
            break;  \
        case NumericType::NUMERIC_TYPE_LONG_LONG:   \
            *(val) = *base->data.numeric.configValue.ll;   \
            break;  \
        case NumericType::NUMERIC_TYPE_ULONG_LONG:  \
            *(val) = *base->data.numeric.configValue.ull;  \
            break;  \
        case NumericType::NUMERIC_TYPE_SIZE_T:  \
            *(val) = *base->data.numeric.configValue.st;   \
            break;  \
        case NumericType::NUMERIC_TYPE_SSIZE_T: \
            *(val) = *base->data.numeric.configValue.sst;  \
            break;  \
        case NumericType::NUMERIC_TYPE_OFF_T:   \
            *(val) = *base->data.numeric.configValue.ot;   \
            break;  \
        case NumericType::NUMERIC_TYPE_TIME_T:  \
            *(val) = *base->data.numeric.configValue.tt;   \
            break;  \
    }

    enum class ConfigFlag : uint64_t
    {
        MODIFIABLE_CONFIG = 0, //默认的配置属性，表示该配置项可以在运行时被修改。
        IMMUTABLE_CONFIG = 1 << 0, //配置属性，表示该配置项只能在启动时设置，一旦设置后将无法再修改。
        SENSITIVE_CONFIG = 1 << 1, //配置属性，表示该配置项包含敏感信息，需要特别保护。
        DEBUG_CONFIG = 1 << 2, //配置属性，表示该配置项对于调试和排查问题很有用。
        MULTI_ARG_CONFIG = 1 << 3, //配置属性，表示该配置项可以接受多个参数
        // HIDDEN_CONFIG: 配置属性，表示该配置项在 config get <pattern> 命令中被隐藏起来，通常用于测试和调试目的。
        // PROTECTED_CONFIG: 配置属性，当启用了 enable-protected-configs 选项后，该配置项将变为不可修改。
        // DENY_LOADING_CONFIG: 配置属性，表示该配置项在加载过程中被禁止使用。
        // ALIAS_CONFIG: 配置属性，表示该配置项是一个别名，用于标识具有多个名称的配置项。
        // MODULE_CONFIG: 配置属性，表示该配置项是一个模块配置项。
        // VOLATILE_CONFIG: 配置属性，表示该配置项是易变的，可能会在运行时频繁变化。
    };
    enum class ConfigType
    {
        BOOL_CONFIG,
        NUMERIC_CONFIG,
        STRING_CONFIG,
        ENUM_CONFIG
    };
    enum class NumericType
    {
        NUMERIC_TYPE_INT,
        NUMERIC_TYPE_UINT,
        NUMERIC_TYPE_LONG,
        NUMERIC_TYPE_ULONG,
        NUMERIC_TYPE_LONG_LONG,
        NUMERIC_TYPE_ULONG_LONG,
        NUMERIC_TYPE_SIZE_T,
        NUMERIC_TYPE_SSIZE_T,
        NUMERIC_TYPE_OFF_T,
        NUMERIC_TYPE_TIME_T,
    };

    enum class ReturnNum
    {
        FAILURE,
        SUCCESS,
        NO_CHANGES
    };

    struct EnumSet
    {
        template <class R>
        EnumSet (R &&name, int val) // NOLINT
            : name(std::forward <R>(name)), val(val) {}

        EnumSet () = default;

        inline bool operator== (int v) const noexcept { return val == v; }
        inline bool operator!= (int v) const noexcept { return !(val == v); } // NOLINT
        inline bool operator== (const StringType &n) const noexcept { return name == n; }
        inline bool operator!= (const StringType &n) const noexcept
        {
            return !(name == n);
        } // NOLINT
        StringType name{};
        int val = ENUM_SET_NIL;
    };

    EnumSet logLevelSet[] {
        { "debug", GET_ENUM_INT(Logger::Level::DEBUG) },
        { "notice", GET_ENUM_INT(Logger::Level::NOTICE) },
        { "warning", GET_ENUM_INT(Logger::Level::WARNING) },
        { "error", GET_ENUM_INT(Logger::Level::ERROR) },
        {}
    };

    struct StandardConfig // NOLINT
    {
        StandardConfig () = default;
        ~StandardConfig () = default;
        template <class R>
        StandardConfig (uint64_t flags, R &&name)
            : flags(flags), name(std::forward <R>(name)) {}
        uint64_t flags;
        StringType name {};
    };

    struct BoolConfigDataBase // NOLINT
    {
        BoolConfigDataBase () = default;
        ~BoolConfigDataBase () = default;
        BoolConfigDataBase (bool defaultValue, bool *value)
            : defaultValue(defaultValue), configValue(value) {}
        bool defaultValue;
        bool *configValue;
    };
    struct StringConfigDataBase // NOLINT
    {
        StringConfigDataBase () = default;
        ~StringConfigDataBase () = default;
        template <class R1>
        StringConfigDataBase ( // NOLINT
            R1 &&defaultValue,
            StringType *value,
            bool convertEmptyToNull
        )
            : defaultValue(std::forward <R1>(defaultValue)),
              configValue(value),
              convertEmptyToNull(convertEmptyToNull) {}
        StringType defaultValue;
        StringType *configValue;
        bool convertEmptyToNull;
    };
    struct EnumConfigDataBase // NOLINT
    {
        EnumConfigDataBase () = default;
        ~EnumConfigDataBase () = default;
        EnumConfigDataBase (
            EnumSet *enumSet,
            int defaultValue,
            int *value
        )
            : enumSet(enumSet),
              defaultValue(defaultValue),
              configValue(value) {}
        EnumSet *enumSet;
        int defaultValue;
        int *configValue;
    };
    struct NumericConfigDataBase // NOLINT
    {
        NumericConfigDataBase () = default;
        ~NumericConfigDataBase () = default;
        template <class T>
        NumericConfigDataBase (
            NumericType numericType,
            long long defaultValue,
            T *value
        )
            : numericType(numericType),
              defaultValue(defaultValue)
        {
            memcpy(&configValue, &value, sizeof(T *));
        }
        union
        {
            int *i;
            unsigned int *ui;
            long *l;
            unsigned long *ul;
            long long *ll;
            unsigned long long *ull;
            size_t *st;
            ssize_t *sst;
            off_t *ot;
            time_t *tt;
        } configValue {};
        NumericType numericType;
        long long defaultValue;
    };
    union TypeData
    {
        BoolConfigDataBase yesno;
        StringConfigDataBase string;
        EnumConfigDataBase enumd;
        NumericConfigDataBase numeric;

        TypeData (bool defaultValue, bool *value)
            : yesno(defaultValue, value) {}

        template <class R1>
        TypeData ( // NOLINT
            R1 &&defaultValue,
            StringType *value,
            bool convertEmptyToNull
        )
            : string(
            std::forward <R1>(defaultValue),
            value,
            convertEmptyToNull
        ) {}

        TypeData (EnumSet *enumSet, int defaultValue, int *value)
            : enumd(enumSet, defaultValue, value) {}

        template <class T>
        TypeData (
            NumericType numericType,
            long long defaultValue,
            T *value
        )
            : numeric(numericType, defaultValue, value) {}

        ~TypeData () noexcept {}; // NOLINT
    };

    struct ConfigDataBase;
    struct ConfigDataHandler
    {
        explicit ConfigDataHandler (ConfigDataBase *base)
            : base(base) {}
        virtual inline void init () const noexcept {}
        /* Called on server startup and CONFIG SET, returns 1 on success,
         * 2 meaning no actual change done, 0 on error and can set a verbose err
         * string */
        virtual inline ReturnNum set (const StringType &it) const noexcept { return ReturnNum::SUCCESS; }
        virtual inline ReturnNum set (const ArrayType <StringType> &it) const noexcept { return ReturnNum::SUCCESS; }
        virtual inline StringType get () const noexcept { return ""; }
        inline void *getBase () const noexcept;

    protected:
        ConfigDataBase *base;
    };

    struct ConfigDataBase : StandardConfig
    {
        template <class R>
        ConfigDataBase (bool defaultValue, bool *value, uint64_t flags, R &&name)
            : StandardConfig(flags, std::forward <R>(name)),
              data(defaultValue, value),
              type(ConfigType::BOOL_CONFIG),
              handler(this) {}

        template <class R1, class R2>
        ConfigDataBase ( // NOLINT
            R1 &&defaultValue,
            StringType *value,
            bool convertEmptyToNull,
            uint64_t flags,
            R2 &&name
        )
            : StandardConfig(flags, std::forward <R2>(name)),
              data(
                  std::forward <R1>(defaultValue),
                  value,
                  convertEmptyToNull
              ), type(ConfigType::STRING_CONFIG), handler(this) {}

        template <class R>
        ConfigDataBase (EnumSet *enumSet, int defaultValue, int *value, uint64_t flags, R &&name)
            : StandardConfig(flags, std::forward <R>(name)),
              data(enumSet, defaultValue, value),
              type(ConfigType::ENUM_CONFIG),
              handler(this) {}

        template <class T, class R>
        ConfigDataBase (
            NumericType numericType,
            long long defaultValue,
            T *value,
            uint64_t flags,
            R &&name
        )
            : StandardConfig(flags, std::forward <R>(name)),
              data(numericType, defaultValue, value),
              type(ConfigType::NUMERIC_CONFIG),
              handler(this) {}

        virtual const ConfigDataHandler &getHandler () const noexcept { return handler; }

        ~ConfigDataBase () noexcept {}; // NOLINT
        TypeData data;
        ConfigType type;

    private:
        ConfigDataHandler handler;
    };

    inline void *ConfigDataHandler::getBase () const noexcept
    {
        switch (base->type)
        {
            case ConfigType::BOOL_CONFIG:
                return base->data.yesno.configValue;
            case ConfigType::NUMERIC_CONFIG:
                return base->data.numeric.configValue.ull;
            case ConfigType::STRING_CONFIG:
                return base->data.string.configValue;
            case ConfigType::ENUM_CONFIG:
                return base->data.enumd.configValue;
        }
    }

    struct NumericConfigDataHandler : ConfigDataHandler
    {
        NumericConfigDataHandler (
            ConfigDataBase *base,
            unsigned long long min,
            unsigned long long max
        )
            : ConfigDataHandler(base), min(min), max(max) {}
        void init () const noexcept override
        {
            // NUMERIC_ASSIGN_DEFAULT_VALUE;
            switch (base->data.numeric.numericType)
            {
                case NumericType::NUMERIC_TYPE_INT:
                    *base->data.numeric.configValue.i =
                        static_cast<int>(base->data.numeric.defaultValue);
                    break;
                case NumericType::NUMERIC_TYPE_UINT:
                    *base->data.numeric.configValue.ui =
                        static_cast<unsigned>(base->data.numeric.defaultValue);
                    break;
                case NumericType::NUMERIC_TYPE_LONG:
                    *base->data.numeric.configValue.l =
                        static_cast<long>(base->data.numeric.defaultValue);
                    break;
                case NumericType::NUMERIC_TYPE_ULONG:
                    *base->data.numeric.configValue.ul =
                        static_cast<unsigned long>(base->data.numeric.defaultValue);
                    break;
                case NumericType::NUMERIC_TYPE_LONG_LONG:
                    *base->data.numeric.configValue.ll = base->data.numeric.defaultValue;
                    break;
                case NumericType::NUMERIC_TYPE_ULONG_LONG:
                    *base->data.numeric.configValue.ull =
                        static_cast<unsigned long long>(base->data.numeric.defaultValue);
                    break;
                case NumericType::NUMERIC_TYPE_SIZE_T:
                    *base->data.numeric.configValue.st =
                        static_cast<size_t>(base->data.numeric.defaultValue);
                    break;
                case NumericType::NUMERIC_TYPE_SSIZE_T:
                    *base->data.numeric.configValue.sst =
                        static_cast<ssize_t>(base->data.numeric.defaultValue);
                    break;
                case NumericType::NUMERIC_TYPE_OFF_T:
                    *base->data.numeric.configValue.ot =
                        static_cast<off_t>(base->data.numeric.defaultValue);
                    break;
                case NumericType::NUMERIC_TYPE_TIME_T:
                    *base->data.numeric.configValue.tt =
                        static_cast<time_t>(base->data.numeric.defaultValue);
                    break;
            }
        }
        ReturnNum set (const StringType &it) const noexcept override
        {
            long long data, oldValue;
            if (!isValid(it, &data))
                return ReturnNum::FAILURE;

            NUMERIC_GET_VALUE(&oldValue);
            if (oldValue == data)
                return ReturnNum::NO_CHANGES;

            NUMERIC_SET_VALUE(data);
            return ReturnNum::SUCCESS;
        }
        StringType get () const noexcept override
        {
            unsigned long long oldValue;
            NUMERIC_GET_VALUE(&oldValue);
            return std::to_string(oldValue);
        }

    protected:
        virtual bool isValid (const StringType &it, long long *data) const noexcept
        {
            bool success = Utils::StringHelper::stringIsLL(it, data);
            if (!success)
            {
                PRINT_ERROR("check the value in %s : %s, must be number",
                    base->name.c_str(),
                    it.c_str());
                return false;
            }

            if (base->data.numeric.numericType == NumericType::NUMERIC_TYPE_UINT ||
                base->data.numeric.numericType == NumericType::NUMERIC_TYPE_SIZE_T ||
                base->data.numeric.numericType == NumericType::NUMERIC_TYPE_TIME_T ||
                base->data.numeric.numericType == NumericType::NUMERIC_TYPE_ULONG ||
                base->data.numeric.numericType == NumericType::NUMERIC_TYPE_ULONG_LONG)
                success = static_cast<unsigned long long>(*data) < min
                    || static_cast<unsigned long long>(*data) > max;
            else
                success =
                    *data < static_cast<long long>(min) || *data > static_cast<long long>(max);
            if (success)
            {
                PRINT_ERROR("check the value in %s : %s , must in min(%llu)-max(%llu)",
                    base->name.c_str(),
                    it.c_str(),
                    min,
                    max);
                return false;
            }

            return true;
        }

    private:
        unsigned long long min;
        unsigned long long max;
    };

    struct NumericConfigDataHandlerListMaxListPackSize : NumericConfigDataHandler
    {
        using NumericConfigDataHandler::NumericConfigDataHandler;
        void init () const noexcept override
        {
            setListMaxListPackSizeConfig(static_cast<int>(base->data.numeric.defaultValue));
        }
        ReturnNum set (const StringType &it) const noexcept override
        {
            long long data;
            if (!isValid(it, &data))
                return ReturnNum::FAILURE;

            return setListMaxListPackSizeConfig(static_cast<int>(data));
        }
        StringType get () const noexcept override
        {
            auto configPtr = getListMaxListPackSizePtr();
            StringType str;
            switch (configPtr->type)
            {
                case ListMaxListPackSizeType::TYPE_NUM:
                    str = std::to_string(configPtr->number);
                    break;
                case ListMaxListPackSizeType::TYPE_SIZE:
                    str = std::to_string(configPtr->number / 1024) + "kb";
                    break;
            }

            return str;
        }

    private:
        inline ListMaxListPackSize *getListMaxListPackSizePtr () const noexcept
        {
            return reinterpret_cast<ListMaxListPackSize *>(
                reinterpret_cast<char *>(base->data.numeric.configValue.i)
                    - sizeof(ListMaxListPackSizeType));
        }

        ReturnNum setListMaxListPackSizeConfig (int t) const
        {
            auto *configPtr = getListMaxListPackSizePtr();
            uint16_t max;
            ListMaxListPackSizeType type;
            if (t < 0)
            {
                type = ListMaxListPackSizeType::TYPE_SIZE;
                if (t < -5)
                {
                    PRINT_ERROR("config listMaxListPackSize error : %d is not allowed [-1 ~ -5]",
                        t);
                    return ReturnNum::FAILURE;
                }
                max = (1 << (-t + 1)) * 1024;
            }
            else
            {
                type = ListMaxListPackSizeType::TYPE_NUM;
                max = static_cast<uint16_t>(t);
            }

            if (configPtr->type == type && configPtr->number == max)
                return ReturnNum::NO_CHANGES;

            configPtr->type = type;
            configPtr->number = max;
            return ReturnNum::SUCCESS;
        }
        bool isValid (const StringType &it, long long int *data) const noexcept override
        {
            bool ret = NumericConfigDataHandler::isValid(it, data);
            if (ret && *data == 0)
            {
                PRINT_ERROR("check the value in %s : %s, 0 is not allow",
                    base->name.c_str(),
                    it.c_str());
                return false;
            }

            return ret;
        }
    };

    struct BoolConfigDataHandler : ConfigDataHandler
    {
        using ConfigDataHandler::ConfigDataHandler;
        void init () const noexcept override
        {
            *base->data.yesno.configValue = base->data.yesno.defaultValue;
        }
        ReturnNum set (const StringType &it) const noexcept override
        {
            bool value;
            if (isEqualIgnoreCase(it, BOOL_YES))
                value = true;
            else if (isEqualIgnoreCase(it, BOOL_NO))
                value = false;
            else
            {
                PRINT_ERROR("bool param value must be yes or no, check %s", it.c_str());
                return ReturnNum::FAILURE;
            }

            *base->data.yesno.configValue = value;
            return ReturnNum::SUCCESS;
        }
        StringType get () const noexcept override
        {
            if (*base->data.yesno.configValue)
                return BOOL_YES;
            return BOOL_NO;
        }

    private:
        static bool isEqualIgnoreCase (const StringType &s1, const StringType &s2)
        {
            if (s1.size() != s2.size())
                return false;

            return std::equal(
                s1.begin(), s1.end(), s2.begin(), [] (const char &c1, const char &c2) {
                    return std::tolower(c1) == std::tolower(c2);
                }
            );
        }
    };

    struct StringConfigDataHandler : ConfigDataHandler
    {
        using ConfigDataHandler::ConfigDataHandler;
        void init () const noexcept override
        {
            base->data.string.configValue->assign(base->data.string.defaultValue);
        }
        ReturnNum set (const StringType &it) const noexcept override
        {
            if (it.empty())
            {
                if (base->data.string.convertEmptyToNull)
                {
                    if (*base->data.string.configValue == CONVERT_EMPTY_TO_NULL_VAL)
                        return ReturnNum::NO_CHANGES;
                    base->data.string.configValue->assign(CONVERT_EMPTY_TO_NULL_VAL);
                    return ReturnNum::SUCCESS;
                }
                else
                {
                    base->data.string.configValue->clear();
                    // PRINT_ERROR("this param not be null , check %s", it.c_str());
                    // return ReturnNum::FAILURE;
                    return ReturnNum::SUCCESS;
                }
            }
            if (*base->data.string.configValue == it)
                return ReturnNum::NO_CHANGES;

            base->data.string.configValue->assign(it);
            return ReturnNum::SUCCESS;
        }
        StringType get () const noexcept override
        {
            return *base->data.string.configValue;
        }
        ReturnNum set (const ArrayType <StringType> &it) const noexcept override
        {
            return ConfigDataHandler::set(it);
        }
    };

    struct StringConfigDataHandlerBind : StringConfigDataHandler
    {
        using StringConfigDataHandler::StringConfigDataHandler;
        ReturnNum set (const ArrayType <StringType> &it) const noexcept override
        {
            if (it.size() > BIND_MAX)
            {
                PRINT_ERROR("bind params out of the max length, max : %d, params len : %zu",
                    BIND_MAX,
                    it.size());
                return ReturnNum::FAILURE;
            }

            size_t i = 0;
            for (; i < it.size(); ++i)
                base->data.string.configValue[i] = it[i];

            base->data.string.configValue[i] = CONVERT_EMPTY_TO_NULL_VAL;
            return ReturnNum::SUCCESS;
        }
        void init () const noexcept override
        {
            base->data.string.configValue[0] = base->data.string.defaultValue;
        }
        StringType get () const noexcept override
        {
            StringType params;
            for (int i = 0; base->data.string.configValue[i] != CONVERT_EMPTY_TO_NULL_VAL; ++i)
            {
                params += base->data.string.configValue[i];
                params += ' ';
            }
            params.pop_back();
            return params;
        }
    };

    struct EnumConfigDataHandler : ConfigDataHandler
    {
        using ConfigDataHandler::ConfigDataHandler;
        void init () const noexcept override
        {
            *base->data.enumd.configValue = base->data.enumd.defaultValue;
        }
        ReturnNum set (const StringType &it) const noexcept override
        {
            for (int i = 0; base->data.enumd.enumSet[i] != ENUM_SET_NIL; ++i)
            {
                if (base->data.enumd.enumSet[i] == it)
                {
                    if (base->data.enumd.enumSet[i] == *base->data.enumd.configValue)
                        return ReturnNum::NO_CHANGES;

                    *base->data.enumd.configValue = base->data.enumd.enumSet[i].val;
                    return ReturnNum::SUCCESS;
                }
            }

            return ReturnNum::FAILURE;
        }
        StringType get () const noexcept override
        {
            for (int i = 0; base->data.enumd.enumSet[i] != ENUM_SET_NIL; ++i)
            {
                if (base->data.enumd.enumSet[i] == *base->data.enumd.configValue)
                    return base->data.enumd.enumSet[i].name;
            }
            return "";
        }
    };

    struct NumericConfigData : ConfigDataBase
    {
        template <class T, class R>
        NumericConfigData (
            NumericType numericType,
            long long defaultValue,
            T *value,
            uint64_t flags,
            R &&name,
            unsigned long long min,
            unsigned long long max
        )
            : ConfigDataBase(numericType, defaultValue, value, flags, name),
              handler(this, min, max) {}

        const NumericConfigDataHandler &getHandler () const noexcept override { return handler; }

    private:
        NumericConfigDataHandler handler;
    };

    struct NumericConfigDataListMaxListPackSize : ConfigDataBase
    {
        template <class T, class R>
        NumericConfigDataListMaxListPackSize (
            NumericType numericType,
            long long defaultValue,
            T *value,
            uint64_t flags,
            R &&name,
            unsigned long long min,
            unsigned long long max
        )
            : ConfigDataBase(numericType, defaultValue, value, flags, name),
              handler(this, min, max) {}

        const NumericConfigDataHandlerListMaxListPackSize &getHandler () const noexcept override
        {
            return handler;
        }

    private:
        NumericConfigDataHandlerListMaxListPackSize handler;
    };

    struct BoolConfigData : ConfigDataBase
    {
        // using ConfigDataBase::ConfigDataBase;
        template <class ...Arg>
        explicit BoolConfigData (Arg &&...arg)
            : ConfigDataBase(std::forward <Arg>(arg)...) {}
        const BoolConfigDataHandler &getHandler () const noexcept override { return handler; }
    private:
        BoolConfigDataHandler handler { this };
    };

    struct StringConfigData : ConfigDataBase
    {
        // using ConfigDataBase::ConfigDataBase;
        template <class ...Arg>
        explicit StringConfigData (Arg &&...arg)
            : ConfigDataBase(std::forward <Arg>(arg)...) {}
        const StringConfigDataHandler &getHandler () const noexcept override { return handler; }

    private:
        StringConfigDataHandler handler { this };
    };

    struct StringConfigDataBind : ConfigDataBase
    {
        // using StringConfigData::StringConfigData;
        template <class ...Arg>
        explicit StringConfigDataBind (Arg &&...arg)
            : ConfigDataBase(std::forward <Arg>(arg)...) {}
        const StringConfigDataHandlerBind &getHandler () const noexcept override { return handler; }

    private:
        StringConfigDataHandlerBind handler { this };
    };

    struct EnumConfigData : ConfigDataBase
    {
        template <class ...Arg>
        explicit EnumConfigData (Arg &&...arg)
            : ConfigDataBase(std::forward <Arg>(arg)...) {}

        const ConfigDataHandler &getHandler () const noexcept override { return handler; }

    private:
        EnumConfigDataHandler handler { this };
    };
};

class KvConfig
{
public:
    static int init (const std::string &path)
    {
        static bool isInit = false;
        if (!isInit)
        {
            isInit = true;
            loadAllConfig();

            return readConfigFile(path);
        }

        return 0;
    }
    static inline KvConfigFields &getConfig ()
    {
        return config;
    }
private:
    static int readConfigFile (const std::string &path)
    {
        std::ifstream inBuffer(path, std::ios_base::in);
        char lineBuffer[LINE_BUFFER_SIZE];

        if (!inBuffer.is_open())
        {
            PRINT_ERROR("open config file error : %s", std::strerror(errno));
            return -errno;
        }

        while (inBuffer.getline(lineBuffer, LINE_BUFFER_SIZE))
            parseConfigLine(lineBuffer);

        if (inBuffer.eof())
            PRINT_INFO("read config file success ...");
        else if (inBuffer.fail())
        {
            PRINT_ERROR("read config file error : %s", std::strerror(errno));
            inBuffer.close();
            return -errno;
        }

        return 0;
    }

    template <int Size>
    static bool parseConfigLine (char (&lineBuffer)[Size])
    {
        std::string str = lineBuffer;
        str = Utils::StringHelper::stringTrim(str);
        if (str.empty() || str[0] == '#')
            return false;

        auto strArr = Utils::StringHelper::stringSplit(str, ' ');
        if (strArr.size() < 2)
        {
            PRINT_ERROR("config parameter missing, check %s", str.c_str());
            return false;
        }
        auto it = configParseMap.find(strArr[0]);
        if (!it)
        {
            PRINT_ERROR("config is no definition, check %s", str.c_str());
            return false;
        }
        __KV_PRIVATE__::ReturnNum res;
        if (it->second->flags & GET_ENUM_UINT64(__KV_PRIVATE__::ConfigFlag::MULTI_ARG_CONFIG))
        {
            strArr.erase(strArr.begin());
            res = it->second->getHandler().set(strArr);
        }
        else
        {
            if (strArr.size() > 2)
            {
                PRINT_ERROR(
                    "config parameter error, this configuration does not require multiple parameters , check %s",
                    str.c_str());
                return false;
            }
            res = it->second->getHandler().set(strArr[1]);
        }

        if (res == __KV_PRIVATE__::ReturnNum::FAILURE)
            return false;
        return true;
    }

    static void loadAllConfig ()
    {
        CREATE_NUMERIC_CONFIG(port,
            __KV_PRIVATE__::ConfigFlag::MODIFIABLE_CONFIG,
            __KV_PRIVATE__::NumericType::NUMERIC_TYPE_UINT,
            3000,
            &config.port,
            0,
            65535);

        CREATE_STRING_CONFIG_CUSTOM(__KV_PRIVATE__::StringConfigDataBind, bind,
            GET_ENUM_UINT64(__KV_PRIVATE__::ConfigFlag::MODIFIABLE_CONFIG)
                | GET_ENUM_UINT64(__KV_PRIVATE__::ConfigFlag::MULTI_ARG_CONFIG),
            "0.0.0.0",
            config.bind,
            false);

        CREATE_BOOL_CONFIG(ioThreadsDoReads,
            GET_ENUM_UINT64(__KV_PRIVATE__::ConfigFlag::IMMUTABLE_CONFIG)
                | GET_ENUM_UINT64(__KV_PRIVATE__::ConfigFlag::DEBUG_CONFIG),
            true,
            &config.ioThreadsDoReads);

        CREATE_NUMERIC_CONFIG(ioThreads,
            GET_ENUM_UINT64(__KV_PRIVATE__::ConfigFlag::IMMUTABLE_CONFIG)
                | GET_ENUM_UINT64(__KV_PRIVATE__::ConfigFlag::DEBUG_CONFIG),
            __KV_PRIVATE__::NumericType::NUMERIC_TYPE_UINT,
            4,
            &config.ioThreads,
            1,
            128);

        CREATE_BOOL_CONFIG(daemonize,
            __KV_PRIVATE__::ConfigFlag::IMMUTABLE_CONFIG,
            true,
            &config.daemonize);

        CREATE_STRING_CONFIG(pidFile,
            __KV_PRIVATE__::ConfigFlag::IMMUTABLE_CONFIG,
            CONVERT_EMPTY_TO_NULL_VAL,
            &config.pidFile,
            true);

        CREATE_STRING_CONFIG(logFile,
            __KV_PRIVATE__::ConfigFlag::IMMUTABLE_CONFIG,
            "",
            &config.logFile,
            false);

        CREATE_ENUM_CONFIG(logLevel,
            __KV_PRIVATE__::ConfigFlag::MODIFIABLE_CONFIG,
            __KV_PRIVATE__::logLevelSet,
            GET_ENUM_INT(Logger::Level::NOTICE),
            &config.logLevel);

        CREATE_NUMERIC_CONFIG(databases,
            __KV_PRIVATE__::ConfigFlag::IMMUTABLE_CONFIG,
            __KV_PRIVATE__::NumericType::NUMERIC_TYPE_UINT,
            1,
            &config.databases,
            1,
            16);

        CREATE_NUMERIC_CONFIG(timeout,
            __KV_PRIVATE__::ConfigFlag::MODIFIABLE_CONFIG,
            __KV_PRIVATE__::NumericType::NUMERIC_TYPE_UINT,
            0,
            &config.timeout,
            0,
            std::numeric_limits <uint32_t>::max());

        CREATE_BOOL_CONFIG(protectedMode,
            __KV_PRIVATE__::ConfigFlag::MODIFIABLE_CONFIG,
            true,
            &config.protectedMode);

        CREATE_STRING_CONFIG(requirePass,
            GET_ENUM_UINT64(__KV_PRIVATE__::ConfigFlag::MODIFIABLE_CONFIG)
                | GET_ENUM_UINT64(__KV_PRIVATE__::ConfigFlag::SENSITIVE_CONFIG),
            CONVERT_EMPTY_TO_NULL_VAL,
            &config.requirePass,
            true);

        CREATE_NUMERIC_CONFIG_CUSTOM(__KV_PRIVATE__::NumericConfigDataListMaxListPackSize,
            listMaxListPackSize,
            __KV_PRIVATE__::ConfigFlag::MODIFIABLE_CONFIG,
            __KV_PRIVATE__::NumericType::NUMERIC_TYPE_INT,
            -2,
            &config.listMaxListPackSize.number,
            -5,
            std::numeric_limits <uint16_t>::max());

        CREATE_NUMERIC_CONFIG(listCompressDepth,
            __KV_PRIVATE__::ConfigFlag::MODIFIABLE_CONFIG,
            __KV_PRIVATE__::NumericType::NUMERIC_TYPE_UINT,
            0,
            &config.listCompressDepth,
            0,
            std::numeric_limits <uint32_t>::max());

        CREATE_NUMERIC_CONFIG(hashMaxListPackEntries,
            __KV_PRIVATE__::ConfigFlag::MODIFIABLE_CONFIG,
            __KV_PRIVATE__::NumericType::NUMERIC_TYPE_ULONG,
            512,
            &config.hashMaxListPackEntries,
            0,
            std::numeric_limits <uint64_t>::max());

        CREATE_NUMERIC_CONFIG(hashMaxListPackValue,
            __KV_PRIVATE__::ConfigFlag::MODIFIABLE_CONFIG,
            __KV_PRIVATE__::NumericType::NUMERIC_TYPE_UINT,
            64,
            &config.hashMaxListPackValue,
            0,
            std::numeric_limits <uint16_t>::max());
    }

    static KvConfigFields config;
    static HashTable <StringType, __KV_PRIVATE__::ConfigDataBase *, true>
        configParseMap;
};
KvConfigFields KvConfig::config;
HashTable <StringType, __KV_PRIVATE__::ConfigDataBase *, true>
    KvConfig::configParseMap;
#endif //LINUX_SERVER_LIB_KV_STORE_CONFIG_KV_CONFIG_H_
