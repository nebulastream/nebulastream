//
// Created by ls on 8/15/25.
//

#pragma once
#include <string>
#include <thread>

#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <Util/ThreadNaming.hpp>
#include <spdlog/mdc.h>
#include <spdlog/spdlog.h>

namespace NES
{
class Thread
{
    std::jthread thread;
public:
    static thread_local WorkerId WorkerNodeId;
    static thread_local std::string ThreadName;
    Thread() = default;
    template <typename FN, typename... Args>
    Thread(std::string name, WorkerId worker_id, FN&& fn, Args&&... args)
    {
        thread = std::jthread(
            []<typename FN2, typename... Args2>(std::stop_token token, WorkerId worker_id, std::string name, FN2&& fn, Args2&&... args)
            {
                WorkerNodeId = worker_id;
                ThreadName = std::move(name);
                setThreadName(ThreadName);
                if constexpr (std::is_invocable_v<FN2, std::stop_token, Args2...>)
                {
                    std::invoke(fn, token, std::forward<Args2>(args)...);
                }
                else if constexpr (std::is_invocable_v<FN2, Args2...>)
                {
                    std::invoke(fn, std::forward<Args2>(args)...);
                }
                else
                {
                    static_assert(
                        std::is_invocable_v<FN2, std::stop_token, Args2...> || std::is_invocable_v<FN, Args2...>,
                        "Function must be callable with arguments");
                }
            },
            worker_id,
            std::move(name),
            std::forward<FN>(fn),
            std::forward<Args>(args)...);
    }


    template <typename FN, typename... Args>
    Thread(std::string name, FN fn, Args&&... args) : Thread(std::move(name), Thread::WorkerNodeId, fn, std::forward<Args>(args)...)
    {
    }


    static const std::string& getThisThreadName() { return ThreadName; }
    static WorkerId getThisWorkerNodeId() { return WorkerNodeId; }

    bool isCurrentThread() const { return std::this_thread::get_id() == thread.get_id(); }

    bool requestStop() { return thread.request_stop(); }
};
}