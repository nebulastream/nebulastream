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

#include <Sources/Source.hpp>
#include <Sources/SourceReturnType.hpp>
#include <boost/asio/awaitable.hpp>
#include <AsyncSourceExecutor.hpp>
#include <AsyncSourceRunner.hpp>
#include <ErrorHandling.hpp>
#include <SourceRunner.hpp>

namespace NES::Sources
{

namespace asio = boost::asio;

AsyncSourceRunner::AsyncSourceRunner(
    OriginId originId,
    std::shared_ptr<Memory::AbstractPoolProvider> poolProvider,
    SourceReturnType::EmitFunction&& emitFn,
    size_t numSourceLocalBuffers,
    std::unique_ptr<Source> sourceImpl,
    std::shared_ptr<AsyncSourceExecutor> executor)
    : SourceRunner(originId, std::move(poolProvider), std::move(emitFn), numSourceLocalBuffers, std::move(sourceImpl))
    , executor(std::move(executor))
{
}

void AsyncSourceRunner::start()
{
}

void AsyncSourceRunner::stop()
{
}

void AsyncSourceRunner::close()
{
}

asio::awaitable<SourceReturnType::SourceReturnType> AsyncSourceRunner::rootCoroutine()
{
    co_await sourceImpl->open(ioc);
    while (true)
    {
        auto buffer = bufferProvider->getBufferBlocking();

        auto result = co_await sourceImpl->fillBuffer(buffer);
        if (std::holds_alternative<Source::EoS>(result))
        {
            co_return SourceReturnType::EoS{};
        }
        else if (std::holds_alternative<Source::Error>(result))
        {
            boost::system::system_error error = std::get<Source::Error>(result).error;
            co_return SourceReturnType::Error{Exception{std::string{error.what()}, 0}};
        }
        else
        {
            emitFn(originId, SourceReturnType::Data{std::move(buffer)});
        }
    }
    co_await sourceImpl->close(ioc);
}

}
