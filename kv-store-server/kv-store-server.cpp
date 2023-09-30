//
// Created by Yoshiki on 2023/8/3.
//
#include "kv-store.h"

int main (int argc, char **argv)
{
    // c[1] = 2;
    // tt.getHandler().init();

    // std::vector <int> a { 1, 2, 3, 4, 5 };
    // std::vector <int> b;
    //
    // std::string ad = "123";
    // ad.clear();
    //
    // std::move(a.begin(), a.end(), std::back_inserter(b));
    setbuf(stdout, 0);

    KvEnv env(argc, argv);
    KvConfig::init(env.getFilePath());
    Tcp tcpServer;
    tcpServer.mainLoop();

    return 0;
}