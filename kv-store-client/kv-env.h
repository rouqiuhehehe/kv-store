//
// Created by 115282 on 2023/9/28.
//

#ifndef KV_STORE_CLIENT_KV_ENV_H_
#define KV_STORE_CLIENT_KV_ENV_H_

#include <iomanip>
#include <cstring>
#include <iostream>
#include "util/string-helper.h"

#define DEFAULT_IP "127.0.0.1"
#define DEFAULT_PORT 3000

class KvEnv
{
public:
    KvEnv (int argc, char **argv) {
        checkParams(argc, argv);
    }
    inline uint16_t getPort () const {
        return port;
    }
    inline const std::string &getIp () const {
        return ip;
    }

private:
    void checkParams (int argc, char **argv) {
        if (argc == 1) {
            return;
        }
        else {
            for (int i = 0; i < argc; ++i) {
                char *paramsKey = argv[i];
                char *paramsVal;

                if (paramsKey[0] == '-') {
                    if (strcmp(paramsKey, "-p") == 0 || strcmp(paramsKey, "--port") == 0) {
                        if (i + 1 == argc) {
                            std::cerr << "Unrecognized option or bad number of args for: '-p'"
                                      << std::endl;
                            exit(EXIT_FAILURE);
                        }
                        i++;
                        paramsVal = paramsKey + 1;
                        portHandler(paramsVal);
                    }
                    else if (strcmp(paramsKey, "-h") == 0) {
                        if (i + 1 == argc || (paramsKey + 1)[0] == '-')
                            helpHandler();
                        else {
                            i++;
                            paramsVal = paramsKey + 1;
                            hostHandler(paramsVal);
                        }
                    }
                    else if (strcmp(paramsKey, "--help") == 0)
                        helpHandler();
                }
            }
        }
    }

    void portHandler (const char *paramsVal) noexcept {
        unsigned p;
        if (!Utils::StringHelper::stringIsUI(paramsVal, &p)) {
            std::cerr << "port must be a unsigned int" << std::endl;
            exit(EXIT_FAILURE);
        }

        port = p;
    }
    void hostHandler (const char *paramsVal) noexcept {
        ip = paramsVal;
    }
    static void helpHandler () noexcept {
        std::cout << "Usage: redis-cli [OPTIONS] [cmd [arg [arg ...]]]\n"
                  << "  -h <hostname>      Server hostname (default: " << DEFAULT_IP << ")\n"
                  << "  -p <port>          Server port (default: " << DEFAULT_PORT << ").\n"
                  << "./kv-store -h or --help\n\n" << "Examples: \n       "
                  << "./kv-store /etc/kv-store.conf" << std::endl;
        exit(EXIT_SUCCESS);
    }

    uint16_t port = DEFAULT_PORT;
    std::string ip = DEFAULT_IP;
};
#endif //KV_STORE_CLIENT_KV_ENV_H_
