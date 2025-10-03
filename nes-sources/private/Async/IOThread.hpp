/*
 *
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

#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include <atomic>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include "Util/Logger/Logger.hpp"

namespace NES
{
namespace asio = boost::asio;

/// A singleton primitive that has a few special properties:
/// - It is lazily initialized (i.e., it does not have static storage duration but is initialized on first request)
/// - It has thread-safe initialization (mutex) and destruction (atomic reference counting)
/// - It is shared among all users of the singleton by giving out a shared_ptr
/// - It is destroyed when the last shared_ptr goes out of scope, BUT can be re-initialized by calling getOrCreate() again at any time
template <typename SingletonType>
class LazySingleton
{
public:
    /// Retrieve a strong reference to the underlying object by either creating it or returning the existing one.
    template <typename... Args>
    static std::shared_ptr<SingletonType> getOrCreate(Args&&... args)
    {
        /// calling .lock() on the weak_ptr is not sufficient to prevent races during initialization/destruction
        std::lock_guard lock(instantiationMutex);
        auto strongRef = weakRef.lock();
        if (!strongRef)
        {
            strongRef = std::make_shared<SingletonType>(std::forward<Args>(args)...);
            weakRef = strongRef;
        }
        return strongRef;
    }

private:
    static std::weak_ptr<SingletonType> weakRef;
    static std::mutex instantiationMutex;
};

/// Declare the static members of the DeferredSharedSingleton template
/// This is required such that the singleton class can be used indepently of concrete instances of the class
template <typename SingletonType>
std::weak_ptr<SingletonType> LazySingleton<SingletonType>::weakRef;

template <typename SingletonType>
std::mutex LazySingleton<SingletonType>::instantiationMutex;

/// IOThread implementation with multiple io_contexts for better thread-cache locality
/// Each thread runs its own io_context, and tasks are distributed round-robin
class IOThread
{
public:
    explicit IOThread(const size_t poolSize = 8)
        : ioContexts(poolSize), nextContext(0)
    {
        NES_DEBUG("IOThread: starting with {} threads.", poolSize);

        // Create one io_context per thread
        for (size_t i = 0; i < poolSize; ++i)
        {
            // Create work guard to keep io_context running
            workGuards.emplace_back(asio::make_work_guard(ioContexts.at(i)));

            // Create thread that runs this specific io_context
            threads.emplace_back([ioc = &ioContexts.at(i)]() {
                ioc->run();
            });
        }
    }

    ~IOThread()
    {
        NES_DEBUG("IOThread: stopping all contexts.");

        // Reset all work guards to allow io_contexts to exit
        workGuards.clear();

        // Stop all io_contexts
        for (auto& ioc : ioContexts)
        {
            ioc.stop();
        }

        // Threads will join automatically due to jthread
        NES_DEBUG("IOThread: stopped.");
    }

    IOThread(const IOThread&) = delete;
    IOThread& operator=(const IOThread&) = delete;

    IOThread(IOThread&&) = delete;
    IOThread& operator=(IOThread&&) = delete;

    asio::io_context& ioContext()
    {
        // Use atomic fetch_add for thread-safe round-robin selection
        const size_t index = nextContext.fetch_add(1, std::memory_order_relaxed) % ioContexts.size();
        return ioContexts[index];
    }

    asio::io_context& ioContext(size_t index)
    {
        if (index >= ioContexts.size())
        {
            throw std::out_of_range("IOThread: io_context index out of range");
        }
        return ioContexts[index];
    }

    /// Get the number of io_contexts in the pool
    size_t size() const noexcept
    {
        return ioContexts.size();
    }

private:
    /// Pool of io_contexts, one per thread
    std::vector<asio::io_context> ioContexts;

    /// Work guards to keep io_contexts running
    std::vector<asio::executor_work_guard<asio::io_context::executor_type>> workGuards;

    /// Threads running the io_contexts
    std::vector<std::jthread> threads;

    /// Atomic counter for round-robin selection
    std::atomic<size_t> nextContext;
};
}