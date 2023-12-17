//
// Created by Yoshiki on 2023/8/13.
//

#ifndef LINUX_SERVER_LIB_KV_STORE_KV_VALUE_H_
#define LINUX_SERVER_LIB_KV_STORE_KV_VALUE_H_
#include "kv-incrementally-hash.h"
#include <string>
#include <chrono>
#include <sstream>
#include "util/events-observer.h"
#include "util/util.h"

#define MAX_EPOLL_ONCE_NUM 1024
#define MESSAGE_SIZE_MAX 65535

#define EXTRA_PARAMS_NX "nx"
#define EXTRA_PARAMS_XX "xx"
#define EXTRA_PARAMS_EX "ex"
#define EXTRA_PARAMS_PX "px"
#define EXTRA_PARAMS_GET "get"

#define MESSAGE_NIL "(nil)"
#define MESSAGE_OK "OK"
#define ERROR_MESSAGE_HELPER(str) str

#define DEFAULT_USER_NAME "default"

enum class StructType { NIL = -1, BASE, STRING, LIST, HASH, SET, ZSET, END };
enum EventType { ADD_KEY, RESET_EXPIRE, DEL_KEY };
using EventsObserverType = EventsObserver <int>;

enum class DataStructureType
{
    HASH_TABLE,
    SKIP_LIST,
    LIST_PACK,
    QUICK_LIST,
    STREAM
};

struct CommandParams
{
    KeyType command;
    KeyType key;
    ArrayType <ValueType> params;

    inline ValueType toString () const noexcept
    {
        ValueType str;

        str += command;
        str += ' ';
        str += key;
        str += ' ';
        if (!params.empty())
        {
            for (auto &s : params)
            {
                str += s;
                str += ' ';
            }
        }
        str.pop_back();

        return str;
    }
};
struct StringValueType
{
    enum class SetModel
    {
        DEFAULT,
        NX,
        XX
    };
    enum class TimeModel
    {
        NIL,
        // seconds
        EX,
        // milliseconds
        PX
    };
    ValueType value;
    //NX ：只在键不存在时，才对键进行设置操作。 SET key value NX 效果等同于 SETNX key value 。
    // XX ：只在键已经存在时，才对键进行设置操作。
    SetModel setModel = SetModel::DEFAULT;
    TimeModel timeModel = TimeModel::NIL;

    // 是否返回之前储存的值 get 可选参数
    bool isReturnOldValue = false;
};

struct ResValueType : Utils::AllDefaultCopy
{
    enum class ReplyModel
    {
        REPLY_UNKNOWN,
        REPLY_STATUS,
        REPLY_ERROR,
        REPLY_INTEGER,
        REPLY_NIL,
        REPLY_STRING,
        REPLY_ARRAY
    };

    enum class ErrorType
    {
        // 协议错误,
        PROTO_ERROR,
        // 设置一个类型不相同的key报错   比如 lset key  后  get key
        WRONGTYPE,
        // 未定义的 命令报错
        UNKNOWN_COMMAND,
        // set key 为number 时报错
        WRONG_NUMBER,
        // incr decr incrBy decrBy value不为数字时 报错
        VALUE_NOT_INTEGER,
        // incrByFloat params不为数字或value 不为数字时报错
        VALUE_NOT_FLOAT,
        // ex : set 同时设置NX XX 时会报错
        SYNTAX_ERROR,
        // incr或decr 后溢出报错
        INCR_OR_DECR_OVERFLOW,
        // 时间为 复数或0时报错
        INVALID_EXPIRE_TIME,
        // hincrby 储存value不为整数时报错
        HASH_VALUE_NOT_INTEGER,
        // hincrbyfloat 储存value不为double时报错
        HASH_VALUE_NOT_FLOAT,
        // 设置requirePass后，未使用auth 登录，操作后报错
        NO_AUTHENTICATION,
        // 设置requirePass后，使用auth登录，密码错误后报错
        INVALID_PASSWORD,
        // 未设置requirePass时，使用auth命令
        WITHOUT_PASSWORD
    };

    ResValueType () = default;
    explicit ResValueType (const ValueType &rhs) noexcept
    {
        operator=(rhs);
    }
    explicit ResValueType (ValueType &&rhs) noexcept
    {
        operator=(std::move(rhs));
    }

    ResValueType (const ValueType &rhs, ReplyModel replyModel) noexcept
    {
        value = rhs;
        model = replyModel;
    }

    ResValueType (ValueType &&rhs, ReplyModel replyModel) noexcept
    {
        value = std::move(rhs);
        model = replyModel;
    }

    ResValueType &operator= (const ValueType &rhs) noexcept
    {
        value = rhs;
        if (Utils::StringHelper::stringIsLL(value))
            model = ReplyModel::REPLY_INTEGER;
        else
            model = ReplyModel::REPLY_STRING;

        return *this;
    }
    ResValueType &operator= (ValueType &&rhs) noexcept
    {
        value = std::move(rhs);
        if (Utils::StringHelper::stringIsLL(value))
            model = ReplyModel::REPLY_INTEGER;
        else
            model = ReplyModel::REPLY_STRING;

        return *this;
    }

    void setErrorStr (const CommandParams &commandParams, ErrorType type)
    {
        static char msg[MESSAGE_SIZE_MAX];
        static std::stringstream paramsStream;
        model = ReplyModel::REPLY_ERROR;

#ifndef NDEBUG
        Utils::dumpTrackBack();
#endif
        switch (type)
        {
            case ErrorType::WRONGTYPE:
                value = ERROR_MESSAGE_HELPER(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
                break;
            case ErrorType::UNKNOWN_COMMAND:
                paramsStream.str("");
                paramsStream << '\'' << commandParams.key << "\' ";
                for (auto &v : commandParams.params)
                    paramsStream << '\'' << v << "\' ";

                sprintf(
                    msg,
                    ERROR_MESSAGE_HELPER("ERR unknown command '%s', with args beginning with: %s%c"),
                    commandParams.command.c_str(), paramsStream.str().c_str(), '\0'
                );
                paramsStream.clear();
                value = msg;
                break;
            case ErrorType::WRONG_NUMBER:
                sprintf(
                    msg,
                    ERROR_MESSAGE_HELPER("ERR wrong number of arguments for '%s' command%c"),
                    commandParams.command.c_str(), '\0'
                );
                value = msg;
                break;
            case ErrorType::VALUE_NOT_INTEGER:
                value = ERROR_MESSAGE_HELPER("ERR value is not an integer or out of range");
                break;
            case ErrorType::VALUE_NOT_FLOAT:
                value = ERROR_MESSAGE_HELPER("ERR value is not a valid float");
                break;
            case ErrorType::SYNTAX_ERROR:
                value = ERROR_MESSAGE_HELPER("ERR syntax error");
                break;
            case ErrorType::INCR_OR_DECR_OVERFLOW:
                value = ERROR_MESSAGE_HELPER("ERR increment or decrement would overflow");
                break;
            case ErrorType::INVALID_EXPIRE_TIME:
                sprintf(
                    msg,
                    ERROR_MESSAGE_HELPER("ERR invalid expire time in '%s' command%c"),
                    commandParams.command.c_str(), '\0'
                );
                value = msg;
                break;
            case ErrorType::HASH_VALUE_NOT_INTEGER:
                value = "ERR hash value is not an integer";
                break;
            case ErrorType::HASH_VALUE_NOT_FLOAT:
                value = "ERR hash value is not a float";
                break;
            case ErrorType::NO_AUTHENTICATION:
                value = "NOAUTH Authentication required.";
                break;
            case ErrorType::INVALID_PASSWORD:
                value = "WRONGPASS invalid username-password pair or user is disabled.";
                break;
            case ErrorType::WITHOUT_PASSWORD:
                value =
                    "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?";
                break;
            case ErrorType::PROTO_ERROR:
                value = "ERR PROTO.";
                break;
        }
    }

    inline void setNilFlag ()
    {
        model = ReplyModel::REPLY_NIL;
        value = MESSAGE_NIL;
    }
    inline void setOKFlag ()
    {
        model = ReplyModel::REPLY_STATUS;
        value = MESSAGE_OK;
    }

    template <class T>
    inline void setIntegerValue (const T num)
    {
        model = ReplyModel::REPLY_INTEGER;
        value = std::to_string(static_cast<IntegerType>(num));
    }

    inline void setStringValue (const ValueType &v)
    {
        model = ReplyModel::REPLY_STRING;
        value = v;
    }

    inline void setStringValue (ValueType &&v)
    {
        model = ReplyModel::REPLY_STRING;
        value = std::move(v);
    }

    template <class ...Arg>
    inline void setVectorValue (const ResValueType &res, Arg &&...arg)
    {
        elements.emplace_back(res);
        setVectorValue(std::forward <Arg>(arg)...);
    }

    template <class ...Arg>
    inline void setVectorValue (ResValueType &&res, Arg &&...arg)
    {
        elements.emplace_back(std::move(res));
        setVectorValue(std::forward <Arg>(arg)...);
    }

    template <class ...Arg>
    inline void setVectorValue (const ValueType &res, Arg &&...arg)
    {
        elements.emplace_back(res, ReplyModel::REPLY_STRING);
        setVectorValue(std::forward <Arg>(arg)...);
    }

    template <class ...Arg>
    inline void setVectorValue (ValueType &&res, Arg &&...arg)
    {
        elements.emplace_back(std::move(res), ReplyModel::REPLY_STRING);
        setVectorValue(std::forward <Arg>(arg)...);
    }

    inline void setVectorValue (const ResValueType &res)
    {
        elements.emplace_back(res);
        model = ReplyModel::REPLY_ARRAY;
    }

    inline void setVectorValue (ResValueType &&res)
    {
        elements.emplace_back(std::move(res));
        model = ReplyModel::REPLY_ARRAY;
    }

    inline void setVectorValue (const ValueType &res)
    {
        elements.emplace_back(res, ReplyModel::REPLY_STRING);
        model = ReplyModel::REPLY_ARRAY;
    }

    inline void setVectorValue (ValueType &&res)
    {
        elements.emplace_back(std::move(res), ReplyModel::REPLY_STRING);
        model = ReplyModel::REPLY_ARRAY;
    }

    inline void setEmptyVectorValue ()
    {
        model = ReplyModel::REPLY_ARRAY;
    }

    inline void setEmptyIntegerValue ()
    {
        setIntegerValue(0);
    }

    inline void clear () noexcept
    {
        value.clear();
        elements.clear();
        model = ReplyModel::REPLY_UNKNOWN;
    }

    std::string toString ()
    {
        static std::stringstream stringFormatter;

        if (model == ResValueType::ReplyModel::REPLY_ARRAY)
            stringFormatter.str("");
        return formatResValueRecursive(stringFormatter);
    }

    ValueType value;
    ReplyModel model = ReplyModel::REPLY_UNKNOWN;
    std::vector <ResValueType> elements {};

private:
    std::string formatResValueRecursive (std::stringstream &stringFormatter) //NOLINT
    {
        std::string resMessage;
        int idx = 0;
        switch (model)
        {
            case ResValueType::ReplyModel::REPLY_UNKNOWN:
                break;
            case ResValueType::ReplyModel::REPLY_INTEGER:
                resMessage = "(integer) ";
                resMessage += value;
                break;
            case ResValueType::ReplyModel::REPLY_ERROR:
                resMessage = "(error) ";
                resMessage += value;
                break;
            case ResValueType::ReplyModel::REPLY_STATUS:
            case ResValueType::ReplyModel::REPLY_NIL:
            case ResValueType::ReplyModel::REPLY_STRING:
                resMessage = "\"" + value + "\"";
                break;
            case ResValueType::ReplyModel::REPLY_ARRAY:
                if (elements.empty())
                {
                    resMessage = "(empty array)";
                    break;
                }
                for (auto &v : elements)
                    stringFormatter << ++idx << ") \""
                                    << v.formatResValueRecursive(stringFormatter)
                                    << "\"" << std::endl;
                resMessage = stringFormatter.str();
                break;
        }

        return resMessage;
    }
};

struct EventObserverParams
{
    EventObserverParams () noexcept = default;
    std::string key;
    StructType structType = StructType::NIL;
};
struct EventAddObserverParams : public EventObserverParams
{
    EventAddObserverParams () noexcept = default;
    std::chrono::milliseconds expire { 0 };
};

#endif //LINUX_SERVER_LIB_KV_STORE_KV_VALUE_H_
