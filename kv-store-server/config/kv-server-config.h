//
// Created by 115282 on 2023/10/7.
//

#ifndef LINUX_SERVER_KV_STORE_SERVER_CONFIG_KV_SERVER_CONFIG_H_
#define LINUX_SERVER_KV_STORE_SERVER_CONFIG_KV_SERVER_CONFIG_H_

#include <unistd.h>
#include <fcntl.h>
class KvServerConfig
{
public:
    static bool initServerConfig ()
    {
        if (!initServerDaemonize())
            return false;

        if (!initServerLoggerStream())
            return false;

        if (!initServerPidFile())
            return false;

        return true;
    }

private:
    static bool initServerLoggerStream ()
    {
        auto config = KvConfig::getConfig();
        auto logFile = config.logFile;

        if (!logFile.empty())
            return Logger::initLogger(
                logFile.c_str(),
                logFile.c_str(),
                logFile.c_str(),
                Logger::Level(config.logLevel));

        return true;
    }
    static bool initServerDaemonize ()
    {
        auto config = KvConfig::getConfig();
        bool daemonize = config.daemonize;

        if (daemonize)
            return KvServerConfig::daemonize();

        return true;
    }

    static bool initServerPidFile ()
    {
        auto config = KvConfig::getConfig();
        auto &pidFile = config.pidFile;

        if (config.daemonize && pidFile != CONVERT_EMPTY_TO_NULL_VAL)
            return KvServerConfig::pidFile(pidFile);

        return true;
    }

    static bool daemonize ()
    {
        // 父进程
        if (fork() != 0)
            exit(EXIT_SUCCESS);
        setsid();

        int fd = open("/dev/null", O_RDONLY);
        if (fd == -1)
        {
            PRINT_ERROR("open /dev/null error : %s", strerror(errno));
            return false;
        }

        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO)
            close(fd);

        return true;
    }

    static bool pidFile (const StringType &filePath)
    {
        std::ofstream file(filePath, std::ios_base::out | std::ios_base::trunc);

        if (file.is_open())
        {
            file << getpid() << std::endl;
            file.close();

            return true;
        }

        PRINT_ERROR("%s open error : %s", filePath.c_str(), strerror(errno));
        return false;
    }
};
#endif //LINUX_SERVER_KV_STORE_SERVER_CONFIG_KV_SERVER_CONFIG_H_
