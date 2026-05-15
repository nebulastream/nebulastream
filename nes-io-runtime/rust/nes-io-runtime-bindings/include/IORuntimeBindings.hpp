#pragma once
#include <atomic>
#include <cstddef>
#include <memory>
#include <ranges>
#include <Identifiers/Identifiers.hpp>
#include <Thread.hpp>

class ThreadInitializationContext
{
    explicit ThreadInitializationContext(std::vector<std::function<void()>> threadInitHooks) : threadInitHooks(std::move(threadInitHooks))
    {
    }

    friend void init_thread(std::shared_ptr<ThreadInitializationContext> context);
    std::vector<std::function<void()>> threadInitHooks;

public:
    static std::shared_ptr<ThreadInitializationContext> fromCurrentThreadsContext()
    {
        return std::shared_ptr<ThreadInitializationContext>(
            new ThreadInitializationContext(NES::detail::threadInitHooks | std::views::values | std::ranges::to<std::vector>()));
    }
};

void init_thread(std::shared_ptr<ThreadInitializationContext> context);
