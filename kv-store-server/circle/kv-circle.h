//
// Created by 115282 on 2023/12/21.
//

#ifndef KV_STORE_KV_STORE_SERVER_CIRCLE_KV_CIRCLE_H_
#define KV_STORE_KV_STORE_SERVER_CIRCLE_KV_CIRCLE_H_

#include <list>
#include <memory>
struct ReactorParams;
class KvCirCle
{
public:
    class KvCirCleEvent;
    using EventType = std::shared_ptr <KvCirCleEvent>;
    using EventList = std::list <EventType>;
    using KeyMapType = HashTable <StringType, EventType>;
    using HandlerCallback = std::function <bool (ParamsType &, KvCirCleEvent &, const StringType &)>;
    using TimeoutCallback = std::function <bool (ParamsType &, KvCirCleEvent &)>;

    class KvCirCleEvent
    {
    public:
        KvCirCleEvent () = default;
        KvCirCleEvent (
            ParamsType *params,
            std::chrono::milliseconds expire,
            StructType structType,
            HandlerCallback &&handlerCb,
            TimeoutCallback &&timeoutCb
        )
            : params(params),
              structType(structType),
              expire(expire),
              expireTime(
                  expire + std::chrono::duration_cast <std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch())),
              handlerCb(std::move(handlerCb)),
              timeoutCb(std::move(timeoutCb)),
              id(eventId++) {}

        inline void handlerTimeout (ReactorParams &);
        inline void handlerEvent (ReactorParams &, const StringType &);
        inline void updateExpireTime () noexcept {
            expireTime = expire + std::chrono::duration_cast <std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch());
        };
        inline void clearExpire () noexcept { expire = std::chrono::milliseconds::zero(); };
        ParamsType *params {};
        StructType structType {};
        std::chrono::milliseconds expire {};
        std::chrono::milliseconds expireTime {};
        HandlerCallback handlerCb;
        TimeoutCallback timeoutCb;
        size_t id {};
        // 做软删除，不清除KeyMap里的key，只清除EventList里的Event，每次需要操作key时检查是否删除
        // 避免每次删除去遍历KeyMap，用shared_ptr做引用计数
        bool isDelete = false;
        static size_t eventId;
    };

    explicit KvCirCle (ReactorParams *reactorParams)
        : reactorParams(reactorParams) {};
    EventType &addEvent (
        const ArrayType <StringType> &keys,
        ParamsType *params,
        std::chrono::milliseconds expire,
        StructType structType,
        HandlerCallback &&handlerCb,
        TimeoutCallback &&timeoutCb
    ) {
        list.emplace_back(
            std::make_shared <KvCirCleEvent>(
                params,
                expire,
                structType,
                std::move(handlerCb),
                std::move(timeoutCb)));

        auto &event = list.back();
        for (const auto &v : keys)
            blockKeyMap.emplace(v, event);

        return event;
    }

    EventType &addEvent (
        const StringType &key,
        ParamsType *params,
        std::chrono::milliseconds expire,
        StructType structType,
        HandlerCallback &&handlerCb,
        TimeoutCallback &&timeoutCb
    ) {
        list.emplace_back(
            std::make_shared <KvCirCleEvent>(
                params,
                expire,
                structType,
                std::move(handlerCb),
                std::move(timeoutCb)));

        auto &event = list.back();
        blockKeyMap.emplace(key, event);

        return event;
    }

    bool delEvent (size_t id) {
        auto it = std::find_if(
            list.begin(), list.end(), [&id] (const EventType &event) {
                return event->id == id;
            }
        );
        if (it != list.end()) {
            (*it)->isDelete = true;
            list.erase(it);
            return true;
        }

        return false;
    }

    bool delEvent (ParamsType *params) {
        auto it = std::find_if(
            list.begin(), list.end(), [&params] (const EventType &event) {
                return event->params == params;
            }
        );

        if (it != list.end()) {
            (*it)->isDelete = true;
            list.erase(it);
            return true;
        }

        return false;
    }

    EventType checkBlockKey (const StringType &key, StructType structType) const noexcept {
        auto it = blockKeyMap.find(key);
        if (it != blockKeyMap.end() && it->second->structType == structType) {
            return it->second;
        }

        return { nullptr };
    }

    inline size_t size () const noexcept {
        return list.size();
    }

    inline bool empty () const noexcept {
        return size() == 0;
    }

    int getOnceExpireTimeAndCheckTimeoutFd (KvCirCle::EventType &circleEvent);

    void serverCycleEvent (EventType &cirCleEvent);

private:
    EventType getMinimalExpireEvent () noexcept {
        auto it = std::min_element(
            list.begin(), list.end(), [] (const EventType &event, const EventType &min) {
                return !event->isDelete && event->expire.count() >= 0 && event->expire < min->expire;
            }
        );
        if (it == list.end()) return { nullptr };
        return *it;
    };

    size_t clearDeletedEvents () {
        size_t count = 0;
        auto it = blockKeyMap.begin();
        auto end = blockKeyMap.end();
        while (it != end) {
            it = std::find_if(
                it, end, [] (const KeyMapType::HashNodeType &event) {
                    return event.second->isDelete;
                }
            );
            if (it == end) break;

            auto res = blockKeyMap.erase(it);
            it = res.second;
            count++;
        }

        return count;
    }
private:
    EventList list;
    KeyMapType blockKeyMap;
    ReactorParams *reactorParams;
    const int &hz = KvConfig::getConfig().hz;
    int cycleCount = 0;
};

using CirCleEventType = KvCirCle::KvCirCleEvent;
size_t KvCirCle::KvCirCleEvent::eventId = 1;
#endif //KV_STORE_KV_STORE_SERVER_CIRCLE_KV_CIRCLE_H_
