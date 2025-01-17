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

#include <Async/AsyncSourceExecutor.hpp>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/post.hpp>

namespace NES::Sources
{

AsyncSourceExecutor::AsyncSourceExecutor() : workGuard(asio::make_work_guard(ioc)), thread([this] { ioc.run(); })
{
}

AsyncSourceExecutor::~AsyncSourceExecutor()
{
    workGuard.reset();
    ioc.stop();
}

template<typename Callable>
void AsyncSourceExecutor::execute(Callable&& task)
{
    asio::post(ioc, std::forward<Callable>(task));
}

}
