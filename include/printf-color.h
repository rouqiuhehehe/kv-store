//
// Created by Yoshiki on 2023/6/11.
//

#ifndef LINUX_SERVER_LIB_INCLUDE_PRINTF_COLOR_H_
#define LINUX_SERVER_LIB_INCLUDE_PRINTF_COLOR_H_
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

#ifdef NDEBUG
#define PRINT_INFO(str, ...)
#define PRINT_ERROR(str, ...)
#define PRINT_WARNING(str, ...)
#else

#define PRINT_INFO(str, ...) STD printf(UNDERLINE "%s [%s:%d]" NONE "\t" str "\n", \
                                __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__)

#define PRINT_ERROR(str, ...) STD fprintf(stderr, UNDERLINE "%s [%s:%d]" NONE "\t" str "\n", \
                                __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__)

#define PRINT_WARNING(str, ...) STD printf(BROWN UNDERLINE"%s [%s:%d]" NONE "\t" YELLOW str "\n" NONE, \
                                    __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__)

#endif
#endif //LINUX_SERVER_LIB_INCLUDE_PRINTF_COLOR_H_
