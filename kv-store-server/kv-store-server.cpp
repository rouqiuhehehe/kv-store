//
// Created by Yoshiki on 2023/8/3.
//
#include <netdb.h>
#include <ifaddrs.h>
#include "kv-store.h"
#include "data-structure/kv-ziplist.h"

class A
{
public:
    void a ()
    {
        static int c = 10;
        c++;
        std::cout << c << std::endl;
    }
};
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
    A a, b;
    a.a();
    b.a();
    setbuf(stdout, nullptr);

    KvEnv env(argc, argv);
    if (KvConfig::init(env.getFilePath()) != 0)
        exit(EXIT_FAILURE);
    if (!KvServerConfig::initServerConfig())
        exit(EXIT_FAILURE);

    Tcp tcpServer;
    tcpServer.mainLoop();

    return 0;
}