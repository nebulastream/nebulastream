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
#include <functional>
#include <string>
#include <thread>
#include <typeindex>
#include <unordered_map>

#include <utility>
#include <Identifiers/Identifiers.hpp>

namespace NES
{

/// Set the current thread's name (for logging and debugging).
/// Use this for main threads that aren't created via NES::Thread.
void setCurrentThreadName(std::string name);

namespace detail
{
/// Thread-local hook map shared with WorkerLocalSingleton.
/// Captured by NES::Thread at construction and replayed on child threads.
/// Keyed by type_index so each WorkerLocalSingleton<T> gets exactly one slot.
/// Non-singleton users can also write to this map with a custom key.
extern thread_local std::unordered_map<std::type_index, std::function<void()>> threadInitHooks;

/// Set to true by NES::Thread on child threads.
extern thread_local bool isNESThread;

/// Internal — apply a captured hook snapshot on a new thread.
void applyThreadInitHooks(std::unordered_map<std::type_index, std::function<void()>> hooks);

/// Internal — set the pthread name (truncated to 15 chars).
void setPthreadName(std::string_view name);
}

/// `NES::Thread` is a wrapper around `std::jthread`. It provides:
/// 1. ThreadName — set per thread, used during debugging and attached to logs.
/// 2. Thread init hooks — a type-indexed map of callbacks that propagate to child threads.
///    WorkerLocalSingleton<T> uses this to propagate worker-scoped instances.
class Thread
{
    std::jthread thread;

public:
    static thread_local Host WorkerNodeId;
    static thread_local std::string ThreadName;
    Thread() = default;

    template <typename FN, typename... Args>
    Thread(std::string name, FN&& fn, Args&&... args)
    {
        auto parentHooks = detail::threadInitHooks;

        thread = std::jthread(
            []<typename FN2, typename... Args2>(
                std::stop_token token,
                std::string name,
                std::unordered_map<std::type_index, std::function<void()>> parentHooks,
                FN2&& fn,
                Args2&&... args)
            {
                detail::applyThreadInitHooks(std::move(parentHooks));
                detail::isNESThread = true;
                ThreadName = name;
                detail::setPthreadName(name);
                if constexpr (std::is_member_function_pointer_v<FN2>)
                {
                    [&token]<typename MemFN, typename ThisArg, typename... Args3>(MemFN memberFunction, ThisArg&& thisArg, Args3&&... args)
                    {
                        if constexpr (std::is_invocable_v<MemFN, ThisArg, std::stop_token, Args3...>)
                        {
                            std::invoke(memberFunction, std::forward<ThisArg>(thisArg), token, std::forward<Args3>(args)...);
                        }
                        else if constexpr (std::is_invocable_v<MemFN, ThisArg, Args3...>)
                        {
                            std::invoke(memberFunction, std::forward<ThisArg>(thisArg), std::forward<Args3>(args)...);
                        }
                        else
                        {
                            static_assert(
                                std::is_invocable_v<MemFN, ThisArg, std::stop_token, Args3...>
                                    || std::is_invocable_v<MemFN, ThisArg, Args3...>,
                                "Member Function must be callable with arguments");
                        }
                    }(std::mem_fn(fn), std::forward<Args2>(args)...);
                }
                else if constexpr (std::is_invocable_v<FN2, std::stop_token, Args2...>)
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
                        std::is_invocable_v<FN2, std::stop_token, Args2...> || std::is_invocable_v<FN2, Args2...>,
                        "Function must be callable with arguments");
                }
            },
            std::move(name),
            std::move(parentHooks),
            std::forward<FN>(fn),
            std::forward<Args>(args)...);
    }

    static const std::string& getThisThreadName() { return ThreadName; }

    static const Host& getThisWorkerNodeId() { return WorkerNodeId; }

    [[nodiscard]] bool isCurrentThread() const { return std::this_thread::get_id() == thread.get_id(); }

    [[nodiscard]] bool joinable() const { return thread.joinable(); }

    bool requestStop() { return thread.request_stop(); }
};
}
