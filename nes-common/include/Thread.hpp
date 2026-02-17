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
#include <string>
#include <thread>
#include <utility>
#include <ittnotify.h>
#include <Identifiers/Identifiers.hpp>
#include <spdlog/spdlog.h>
#include <ErrorHandling.hpp>

namespace NES
{
namespace detail
{
///POSIX limits the thread name to 16 bytes which includes null termination:
///https://man7.org/linux/man-pages/man3/pthread_setname_np.3.html
constexpr size_t PTHREAD_NAME_LENGTH = 15;
}

/// `NES::Thread` is a wrapper around `std::jthread`. The main purpose is that it is a standardized way
/// to add additional information to a thread. Currently, it contains:
/// 1. ThreadName which is used during debugging and is attached to logs.
/// 2. It holds the WorkerId. Which can be automatically inherited to threads created within this thread.
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
        /// The complexity of this function is caused by the combination of different ways a std::jthread can be created.
        /// 1. It can accept an addtional std::stop_token
        /// 2. The first parameter has to be the this ptr if the function is a member function
        thread = std::jthread(
            []<typename FN2, typename... Args2>(std::stop_token token, WorkerId worker_id, std::string name, FN2&& fn, Args2&&... args)
            {
                initializeThread(std::move(worker_id), std::move(name));
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

    static const WorkerId& getThisWorkerNodeId() { return WorkerNodeId; }

    [[nodiscard]] bool isCurrentThread() const { return std::this_thread::get_id() == thread.get_id(); }

    [[nodiscard]] bool joinable() const { return thread.joinable(); }

    bool requestStop() { return thread.request_stop(); }

    static void initializeThread(WorkerId worker_id, std::string name)
    {
        WorkerNodeId = std::move(worker_id);
        ThreadName = std::move(name);
        if (WorkerNodeId.getRawValue() != WorkerId::INVALID)
        {
            setThreadName(ThreadName, WorkerNodeId.getRawValue());
        }
    }

    ///Sets the currents thread's name.
    ///threadName has to be non-empty and will be truncated to PTHREAD_NAME_LENGTH character
    static void setThreadName(const std::string_view threadName, const std::string_view worker = "")
    {
        PRECONDITION(!threadName.empty(), "Thread name cannot be empty");
        std::array<char, detail::PTHREAD_NAME_LENGTH + 1> truncatedStringName{};
        std::copy_n(threadName.begin(), std::min(detail::PTHREAD_NAME_LENGTH, threadName.size()), truncatedStringName.begin());
        pthread_setname_np(pthread_self(), truncatedStringName.data());
        if (!worker.empty())
        {
            __itt_thread_set_name(fmt::format("{}@{}", threadName, worker).c_str());
        }
        else
        {
            __itt_thread_set_name(std::string(threadName).c_str());
        }
    }
};
}
