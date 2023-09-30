//
// Created by 115282 on 2023/8/18.
//

#ifndef LINUX_SERVER_LIB_INCLUDE_UTIL_EVENTS_OBSERVER_H_
#define LINUX_SERVER_LIB_INCLUDE_UTIL_EVENTS_OBSERVER_H_

#include <string>
#include <list>
#include <unordered_map>
#include <functional>

template <class EventsKeyType = std::string>
class EventsObserver
{
public:
    using FunctionalType = std::pair <std::function <void (void *)>, intptr_t *>;
    using ContainerType = std::unordered_map <EventsKeyType, std::list <FunctionalType>>;

    void emit (const EventsKeyType &key, void *arg)
    {
        auto it = observer.find(key);
        if (it != observer.end())
        {
            for (auto &v : it->second)
                v.first(arg);
        }
    }

    template <class T>
    void on (const EventsKeyType &key, T &&fun)
    {
        auto it = observer.find(key);
        if (it == observer.end())
            it = observer.emplace(key, std::list <FunctionalType>()).first;

        it->second
          .emplace_back(std::forward <T>(fun), reinterpret_cast<intptr_t *>(&fun));
    }

    template <class T>
    void once (const EventsKeyType &key, T &&fun)
    {
        auto cb = [key, fun, this] (void *arg) {
            fun(arg);

            off(key, fun);
        };

        on(key, cb);
    }

    void off () noexcept
    {
        observer.clear();
    }

    void off (const EventsKeyType &key) noexcept
    {
        observer.erase(key);
    }

    template <class T>
    void off (const EventsKeyType &key, T &&fun) noexcept
    {
        auto list = observer.find(key);
        if (list != observer.end())
        {
            for (auto it = list->second.begin(); it != list->second.end();)
            {
                if (it->second == reinterpret_cast<intptr_t *>(&fun))
                    it = list->second.erase(it);
                else
                    ++it;
            }

            if (list->second.empty())
                observer.erase(list);
        }
    }
private:
    ContainerType observer;
};
#endif //LINUX_SERVER_LIB_INCLUDE_UTIL_EVENTS_OBSERVER_H_
