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

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

namespace NES::Sources
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

/// Wraps an asio::io_context and an associated thread that runs the io_context.
/// This ensures that the lifetime of the io_context is tied to the lifetime of the thread and both can be managed by the owner of the IOThread.
/// The thread is started upon construction and calls io_context::run() until the destructor is called.
/// Without the workGuard, the io_context::run() function would return as soon as there are no more handlers to execute, causing the thread to exit.
/// This way, the io_context is usable as long as this object is alive and can be controlled by us.
class IOThread
{
public:
    IOThread();
    ~IOThread();

    IOThread(const IOThread&) = delete;
    IOThread& operator=(const IOThread&) = delete;

    IOThread(IOThread&&) = delete;
    IOThread& operator=(IOThread&&) = delete;

    /// This needs to be exposed for operations that require the io_context, e.g., for creating sockets from an accepted TCP connection.
    asio::io_context& ioContext() { return ioc; }

private:
    asio::io_context ioc;
    asio::executor_work_guard<decltype(ioc.get_executor())> workGuard;
    std::jthread ioThread;
};

}
