/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

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
