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
#include <thread>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>

#include <Util/Logger/Logger.hpp>

namespace NES::Sources
{

namespace asio = boost::asio;

class AsyncTaskExecutor
{
public:
    AsyncTaskExecutor(const AsyncTaskExecutor&) = delete;
    AsyncTaskExecutor& operator=(const AsyncTaskExecutor&) = delete;
    AsyncTaskExecutor(AsyncTaskExecutor&&) = delete;
    AsyncTaskExecutor& operator=(AsyncTaskExecutor&&) = delete;

    static AsyncTaskExecutor& getOrCreate()
    {
        static AsyncTaskExecutor executor;
        return executor;
    }

    template <typename Callable>
    void dispatch(Callable&& task)
    {
        asio::post(ioc, std::forward<Callable>(task));
    }

    asio::io_context& ioContext() { return ioc; }

private:
    AsyncTaskExecutor();
    ~AsyncTaskExecutor();

    asio::io_context ioc;
    asio::executor_work_guard<decltype(ioc.get_executor())> workGuard;
    std::jthread thread;
};

}
