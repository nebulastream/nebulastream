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

#include <cstdint>
#include <memory>
#include <mutex>

#include <Identifiers/Identifiers.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <AsyncSourceExecutor.hpp>

namespace NES::Sources {

struct CoroutineExecutionContext
{
    static std::shared_ptr<AsyncSourceExecutor> getExecutor()
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

    OriginId originId;
    std::unique_ptr<Source> sourceImpl;
    SourceReturnType::EmitFunction emitFn;
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    uint64_t maxSequenceNumber{0};
    std::shared_ptr<AsyncSourceExecutor> executor{getExecutor()};
};

}
