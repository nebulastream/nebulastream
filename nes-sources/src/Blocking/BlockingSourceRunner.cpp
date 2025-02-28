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

#include <Blocking/BlockingSourceRunner.hpp>

#include <future>
#include <memory>
#include <stop_token>
#include <utility>
#include <exception>

#include <Sources/BlockingSource.hpp>
#include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceUtility.hpp>
#include "Identifiers/Identifiers.hpp"

namespace NES::Sources
{

using IOBuffer = Memory::TupleBuffer;

BlockingSourceRunner::BlockingSourceRunner(
    std::promise<void> terminationPromise, EmitFunction&& emitFn, std::unique_ptr<SourceExecutionContext<BlockingSource>> executionContext)
    : terminationPromise(std::move(terminationPromise)), executionContext(std::move(executionContext)), emitFn(std::move(emitFn))
{
}

void BlockingSourceRunner::runningRoutine(const std::stop_token& stopToken) const
{
    auto sequenceNumber = SequenceNumber::INITIAL;
    const EmitFn dataEmit = [&](IOBuffer&& buffer) -> void
    {
        addBufferMetadata(executionContext->originId, buffer, sequenceNumber++);
        emitFn(executionContext->originId, Data{std::move(buffer)});
    };

    const BlockingSourceWrapper handle{*executionContext->sourceImpl};
    while (!stopToken.stop_requested())
    {
        auto buffer = executionContext->bufferProvider->getBufferBlocking();

        const auto numReadBytes = handle.source.fillBuffer(buffer, stopToken);

        /// 1. Happy Path: Source produces a tuple buffer and emit is called. The loop continues.
        if (numReadBytes != 0)
        {
            buffer.setNumberOfTuples(numReadBytes);
            dataEmit(std::move(buffer));
        }

        /// 2. Stop was requested by the owner of the data source. Stop is propagated to the source implementation.
        /// The source returns control as soon as possible.
        if (stopToken.stop_requested())
        {
            return;
        }

        /// 3. EndOfStream was signaled by the source implementation. It returned that it read nothing, but the Stop Token was not triggered.
        ///    The thread emits EoS to the query engine.
        if (numReadBytes == 0)
        {
            return;
        }
    }
}

void BlockingSourceRunner::operator()(const std::stop_token& stopToken)
{
    try
    {
        runningRoutine(stopToken);
        /// Emitting EoS ONLY if a stop was not requested until now
        if (!stopToken.stop_requested())
        {
            emitFn(executionContext->originId, EoS{});
        }
        terminationPromise.set_value();
    }
    catch (const std::exception& exception)
    {
        /// TODO(yschroeder97): this converts e.g., CannotOpenSource() exceptions into DataIngestionFailure, losing information
        const auto ingestionException = DataIngestionFailure(exception.what());
        emitFn(executionContext->originId, Error{ingestionException});
        terminationPromise.set_exception(std::make_exception_ptr(ingestionException));
    }
}

}
