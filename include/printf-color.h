//
// Created by Yoshiki on 2023/6/11.
//

#ifndef LINUX_SERVER_LIB_INCLUDE_PRINTF_COLOR_H_
#define LINUX_SERVER_LIB_INCLUDE_PRINTF_COLOR_H_

#include <cstdarg>
#include <cstring>
#define NONE                 "\e[0m"
#define BLACK                "\e[0;30m"
#define L_BLACK              "\e[1;30m"
#define RED                  "\e[0;31m"
#define L_RED                "\e[1;31m"
#define GREEN                "\e[0;32m"
#define L_GREEN              "\e[1;32m"
#define BROWN                "\e[0;33m"
#define YELLOW               "\e[1;33m"
#define BLUE                 "\e[0;34m"
#define L_BLUE               "\e[1;34m"
#define PURPLE               "\e[0;35m"
#define L_PURPLE             "\e[1;35m"
#define CYAN                 "\e[0;36m"
#define L_CYAN               "\e[1;36m"
#define GRAY                 "\e[0;37m"
#define WHITE                "\e[1;37m"

#define BOLD                 "\e[1m"
#define UNDERLINE            "\e[4m"
#define BLINK                "\e[5m"
#define REVERSE              "\e[7m"
#define HIDE                 "\e[8m"
#define CLEAR                "\e[2J"
#define CLRLINE              "\r\e[K" //or "\e[1K\r"

#ifdef __cplusplus
#define STD std::
#else
#define STD
#endif

#define KV_ASSERT(expr) \
    do {                \
        if (unlikely(!(expr))) { \
            PRINT_ERROR("=== ASSERTION FAILED ==="); \
            PRINT_ERROR(#expr " is not true"); \
        }\
    } while(0)

#if defined(KV_STORE_TEST)
#define PRINT_DEBUG(str, ...) Logger::printInfo(Logger::Level::DEBUG, UNDERLINE "%s [%s:%d]" NONE "\t" str "\n", \
                                __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__)
#else
#define PRINT_DEBUG(str, ...)
#endif

#define PRINT_INFO(str, ...) Logger::printInfo(Logger::Level::NOTICE, UNDERLINE "%s [%s:%d]" NONE "\t" str "\n", \
                                __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__)

#define PRINT_ERROR(str, ...) Logger::printErr(UNDERLINE "%s [%s:%d]" NONE "\t" str "\n", \
                                __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__)

#define PRINT_WARNING(str, ...) Logger::printInfo(Logger::Level::WARNING, BROWN UNDERLINE"%s [%s:%d]" NONE "\t" YELLOW str "\n" NONE, \
                                    __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__)

namespace Utils
{
    std::string getDateNow (const std::string &);
}
class Logger
{
public:
    enum class Level
    {
        DEBUG,
        NOTICE,
        WARNING,
        ERROR
    };
    ~Logger () noexcept {
        fclose(in);
        fclose(out);
        fclose(err);
    }
    static bool initLogger (
        const char *inPath,
        const char *outPath,
        const char *errPath,
        Level level
    ) {
        logLevel = level;
        return setStandardStreams(inPath, outPath, errPath);
    }

    static void printInfo (Level level, const char *str, ...) {
        if (level >= logLevel) {
            va_list list;
            va_start(list, str);
            fprintf(out, "[%s] ", Utils::getDateNow("%Y-%m-%d %H:%M:%S").c_str());
            vfprintf(out, str, list);
            va_end(list);
        }
    }

    static void printErr (const char *str, ...) {
        va_list list;
        va_start(list, str);
        fprintf(err, "[%s] ", Utils::getDateNow("%Y-%m-%d %H:%M:%S").c_str());
        vfprintf(err, str, list);
        va_end(list);
    }

private:
    static bool setStandardStreams (const char *inPath, const char *outPath, const char *errPath) {
        out = fopen(outPath, "a");
        if (out == nullptr) {
            PRINT_ERROR("fopen %s error : %s", outPath, strerror(errno));
            return false;
        }

        in = fopen(inPath, "r");
        if (in == nullptr) {
            PRINT_ERROR("fopen %s error : %s", inPath, strerror(errno));
            return false;
        }

        err = fopen(errPath, "a");
        if (err == nullptr) {
            PRINT_ERROR("fopen %s error : %s", errPath, strerror(errno));
            return false;
        }

        setbuf(out, nullptr);
        setbuf(in, nullptr);
        setbuf(err, nullptr);

        return true;
    }

private:
    static FILE *in;
    static FILE *out;
    static FILE *err;

    static Level logLevel;
};
FILE *Logger::in = stdin;
FILE *Logger::out = stdout;
FILE *Logger::err = stderr;
Logger::Level Logger::logLevel = Logger::Level::DEBUG;
#endif //LINUX_SERVER_LIB_INCLUDE_PRINTF_COLOR_H_
