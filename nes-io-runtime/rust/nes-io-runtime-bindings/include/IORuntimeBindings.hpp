#pragma once
#include <atomic>
#include <cstddef>
#include <memory>
#include <ranges>

#include <Identifiers/Identifiers.hpp>
#include <Thread.hpp>

class ThreadInitializationContext
{
    explicit ThreadInitializationContext(NES::Host host, std::vector<std::function<void()>> threadInitHooks)
        : host(std::move(host)), threadInitHooks(std::move(threadInitHooks))
    {
    }

    friend void init_thread(std::shared_ptr<ThreadInitializationContext> context);
    NES::Host host;
    std::vector<std::function<void()>> threadInitHooks;
    std::atomic<size_t> counter = 0;

public:
    static std::shared_ptr<ThreadInitializationContext> fromCurrentThreadsContext()
    {
        return std::shared_ptr<ThreadInitializationContext>(new ThreadInitializationContext(
            NES::Thread::WorkerNodeId, NES::detail::threadInitHooks | std::views::values | std::ranges::to<std::vector>()));
    }
};

void init_thread(std::shared_ptr<ThreadInitializationContext> context);
