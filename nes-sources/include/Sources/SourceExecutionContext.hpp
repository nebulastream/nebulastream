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

#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/AsyncSource.hpp>

namespace NES::Sources
{

class AsyncSourceExecutor;

struct AsyncSourceExecutionContext
{
    AsyncSourceExecutionContext() = delete;
    AsyncSourceExecutionContext(
        OriginId originId,
        std::unique_ptr<AsyncSource> sourceImpl,
        std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider);

    void setEmitFunction(SourceReturnType::EmitFunction emit)
    {
        emitFn = emit;
    }

    OriginId originId;
    std::unique_ptr<AsyncSource> sourceImpl;
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    uint64_t maxSequenceNumber;
    std::shared_ptr<AsyncSourceExecutor> executor;
    SourceReturnType::EmitFunction emitFn;

private:
    /// If the shared_ptr is nullptr (does not manage an underlying pointer to an executor, create one in a thread-safe way and return it
    /// This makes sure that the I/O thread(s) within the executor are only running when at least one source is active
    static std::shared_ptr<AsyncSourceExecutor> getExecutor();
};

}
