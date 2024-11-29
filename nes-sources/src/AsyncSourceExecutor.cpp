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

#include "AsyncSourceExecutor.hpp"

#include "boost/asio/awaitable.hpp"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "boost/asio/io_context.hpp"


namespace NES::Sources
{

AsyncSourceExecutor::AsyncSourceExecutor(size_t numThreads) : workGuard(asio::make_work_guard(ioc))
{
    threadPool.reserve(numThreads);
    for (size_t i = 0; i < numThreads; ++i)
    {
        threadPool.emplace_back([this] { ioc.run(); });
    }
}

AsyncSourceExecutor::~AsyncSourceExecutor()
{
    workGuard.reset();
    ioc.stop();
}

void AsyncSourceExecutor::dispatch(const std::function<asio::awaitable<void>()>& coroutine)
{
    asio::co_spawn(ioc, coroutine(), asio::detached);
}

}
