//
// Created by 115282 on 2023/9/28.
//

#ifndef LINUX_SERVER_LIB_KV_STORE_CONFIG_KV_ENV_H_
#define LINUX_SERVER_LIB_KV_STORE_CONFIG_KV_ENV_H_

#include <iomanip>
#include <cstring>
#include <iostream>

#define DEFAULT_FILE_PATH "kv-store.conf"
class KvEnv
{
public:
    KvEnv (int argc, char **argv) {
        checkParams(argc, argv);
    }

    inline std::string getFilePath () const noexcept {
        return filePath;
    }

private:
    void checkParams (int argc, char **argv) {
        if (argc == 1) {
            return;
        }
        else if (argc == 2) {
            char *paramsKey = argv[1];

            if (paramsKey[0] == '-')
                paramsHandler(paramsKey);
            else
                // 开头不是-的默认为是config路径
                filePath = paramsKey;
        }
    }

    static void paramsHandler (char *param) noexcept {
        if (strcmp(param, "-v") == 0 || strcmp(param, "--version") == 0)
            versionHandler();
        else if (strcmp(param, "-h") == 0 || strcmp(param, "--help") == 0)
            helpHandler();
    }

    static void versionHandler () noexcept {
        std::cout << PROJECT_NAME << " v=" << PROJECT_VER << std::endl;
        exit(EXIT_SUCCESS);
    }

    static void helpHandler () noexcept {
        std::cout << "Usage: \n       "
                  << "./kv-store-server [/path/to/kv-store.conf]\n       "
                  << "./kv-store-server -v or --version\n       "
                  << "./kv-store-server -h or --help\n\n" << "Examples: \n       "
                  << "./kv-store-server /etc/kv-store.conf" << std::endl;
        exit(EXIT_SUCCESS);
    }

    std::string filePath = DEFAULT_FILE_PATH;
};
#endif //LINUX_SERVER_LIB_KV_STORE_CONFIG_KV_ENV_H_
