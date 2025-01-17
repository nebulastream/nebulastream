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

class AsyncSourceExecutor
{
public:
    friend class std::shared_ptr<AsyncSourceExecutor>;

    AsyncSourceExecutor(const AsyncSourceExecutor&) = delete;
    AsyncSourceExecutor& operator=(const AsyncSourceExecutor&) = delete;
    AsyncSourceExecutor(AsyncSourceExecutor&&) = delete;
    AsyncSourceExecutor& operator=(AsyncSourceExecutor&&) = delete;

    static std::shared_ptr<AsyncSourceExecutor> getOrCreate()
    {
        const std::lock_guard lock(instanceMutex);
        if (auto shared = instance.lock())
        {
            return shared;
        }
        auto sharedInstance = createInstance();
        instance = sharedInstance;
        return sharedInstance;
    }

    template <typename Callable>
    void execute(Callable&& task);

    asio::io_context& ioContext() { return ioc; }

private:
    AsyncSourceExecutor();
    ~AsyncSourceExecutor();

    static std::shared_ptr<AsyncSourceExecutor> createInstance()
    {
        return std::shared_ptr<AsyncSourceExecutor>(new AsyncSourceExecutor(), [](AsyncSourceExecutor* p) { delete p; });
    }

    static std::weak_ptr<AsyncSourceExecutor> instance;
    static std::mutex instanceMutex;

    asio::io_context ioc;
    asio::executor_work_guard<decltype(ioc.get_executor())> workGuard;
    std::jthread thread;
};

std::weak_ptr<AsyncSourceExecutor> AsyncSourceExecutor::instance;
std::mutex AsyncSourceExecutor::instanceMutex;

}
