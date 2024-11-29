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

#include <Sources/SourceReturnType.hpp>
#include <boost/asio/io_context.hpp>
#include <AsyncSourceExecutor.hpp>
#include <SourceRunner.hpp>

namespace NES::Sources
{

namespace asio = boost::asio;

class AsyncSourceRunner : public SourceRunner
{
public:
    AsyncSourceRunner(
        OriginId originId,
        std::shared_ptr<Memory::AbstractPoolProvider> poolProvider,
        SourceReturnType::EmitFunction&& emitFn,
        size_t numSourceLocalBuffers,
        std::unique_ptr<Source> sourceImpl,
        std::shared_ptr<AsyncSourceExecutor> executor);

    void start() override;

    void stop() override;

    void close() override;

    SourceType run() override;


private:
    asio::io_context ioc;
    std::shared_ptr<AsyncSourceExecutor> executor;

    asio::awaitable<SourceReturnType::SourceReturnType> rootCoroutine();
};

}
