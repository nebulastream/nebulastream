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

#include <Sources/SourceExecutionContext.hpp>

#include <memory>
#include <mutex>

#include <Identifiers/Identifiers.hpp>
#include <Sources/AsyncSource.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <AsyncSourceExecutor.hpp>

namespace NES::Sources {

AsyncSourceExecutionContext::AsyncSourceExecutionContext(
    OriginId originId,
    std::unique_ptr<AsyncSource> sourceImpl,
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider)
    : originId(originId)
    , sourceImpl(std::move(sourceImpl))
    , bufferProvider(bufferProvider)
    , maxSequenceNumber(0)
    , executor(getExecutor())
{
}


std::shared_ptr<AsyncSourceExecutor> AsyncSourceExecutionContext::getExecutor()
    {
        static std::weak_ptr<AsyncSourceExecutor> weakExecutor;
        static std::mutex mutex;

        std::lock_guard const lock(mutex);

        std::shared_ptr<AsyncSourceExecutor> executor = weakExecutor.lock();
        if (!executor)
        {
            executor = std::make_shared<AsyncSourceExecutor>();
            weakExecutor = executor;
        }
        return executor;
    }
}
