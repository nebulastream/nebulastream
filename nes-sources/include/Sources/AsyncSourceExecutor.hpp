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

#include <thread>
#include <vector>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

namespace NES::Sources
{

namespace asio = boost::asio;

class AsyncSourceExecutor
{
public:
    explicit AsyncSourceExecutor(size_t numThreads);
    AsyncSourceExecutor() = delete;
    ~AsyncSourceExecutor();

    AsyncSourceExecutor(const AsyncSourceExecutor&) = delete;
    AsyncSourceExecutor& operator=(const AsyncSourceExecutor&) = delete;
    AsyncSourceExecutor(AsyncSourceExecutor&&) = delete;
    AsyncSourceExecutor& operator=(AsyncSourceExecutor&&) = delete;


    void dispatch(const std::function<asio::awaitable<void>()>& coroutine);

    asio::io_context& ioContext() { return ioc; }

private:
    asio::io_context ioc;
    asio::executor_work_guard<decltype(ioc.get_executor())> workGuard;
    std::vector<std::jthread> threadPool;
};

}
