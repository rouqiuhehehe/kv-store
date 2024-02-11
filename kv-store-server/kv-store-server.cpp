//
// Created by Yoshiki on 2023/8/3.
//

#undef KV_STORE_TEST
#include "kv-store.h"

int main (int argc, char **argv) {
    setbuf(stdout, nullptr);

    KvEnv env(argc, argv);
    if (KvConfig::init(env.getFilePath()) != 0 || !KvServerConfig::initServerConfig())
        exit(EXIT_FAILURE);

    Tcp tcpServer;
    tcpServer.mainLoop();
    return 0;
}