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
#include <boost/asio/post.hpp>

namespace NES::Sources
{

namespace asio = boost::asio;

template <typename SingletonT>
class DeferredSharedSingleton
{
public:
    static std::shared_ptr<SingletonT> getOrCreate()
    {
        std::lock_guard lock(instantiationMutex);
        auto strongRef =  weakRef.lock();
        if (!strongRef)
        {
            strongRef = std::make_shared<SingletonT>();
            weakRef = strongRef;
        }
        return strongRef;
    }

private:
    static std::weak_ptr<SingletonT> weakRef;
    static std::mutex instantiationMutex;
};

template <typename SingletonT>
std::weak_ptr<SingletonT> DeferredSharedSingleton<SingletonT>::weakRef;

template <typename SingletonT>
std::mutex DeferredSharedSingleton<SingletonT>::instantiationMutex;

class IOThread
{
public:
    IOThread();
    ~IOThread();

    IOThread(const IOThread&) = delete;
    IOThread(IOThread&&) = delete;

    IOThread& operator=(const IOThread&) = delete;
    IOThread& operator=(IOThread&&) = delete;

    asio::io_context& ioContext() { return ioc; }

private:
    asio::io_context ioc;
    asio::executor_work_guard<decltype(ioc.get_executor())> workGuard;
    std::jthread thread;
};

}
