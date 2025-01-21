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
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <stop_token>
#include <utility>
#include <variant>

#include <Sources/BlockingSource.hpp>
#include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceUtility.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <magic_enum.hpp>

namespace NES::Sources
{

/// RAII-Wrapper around source open and close
struct SourceHandle
{
    explicit SourceHandle(BlockingSource& source) : source(source)
    {
        source.open();
    }
    SourceHandle(const SourceHandle& other) = delete;
    SourceHandle(SourceHandle&& other) noexcept = delete;
    SourceHandle& operator=(const SourceHandle& other) = delete;
    SourceHandle& operator=(SourceHandle&& other) noexcept = delete;

    ~SourceHandle()
    {
        /// Throwing in a destructor would terminate the application
        try
        {
            source.close();
        }
        catch (...)
        {
            tryLogCurrentException();
        }
    }
    BlockingSource& source; ///NOLINT Source handle should never outlive the source
};

class BlockingSourceThread
{
public:
    using EmitFn = std::function<void(IOBuffer)>;

    BlockingSourceThread(
        std::promise<void> terminationPromise,
        EmitFunction&& emitFn,
        std::unique_ptr<SourceExecutionContext> executionContext)
        : terminationPromise(std::move(terminationPromise)), executionContext(std::move(executionContext)), emitFn(std::move(emitFn))
    {
    }

    BlockingSourceThread() = delete;
    ~BlockingSourceThread() = default;

    BlockingSourceThread(BlockingSourceThread&&) = default;
    BlockingSourceThread& operator=(BlockingSourceThread&&) = default;

    BlockingSourceThread(const BlockingSourceThread&) = delete;
    BlockingSourceThread& operator=(const BlockingSourceThread&) = delete;

    void runningRoutine(const std::stop_token& stopToken) const
    {
        PRECONDITION(
            std::holds_alternative<std::unique_ptr<BlockingSource>>(executionContext->sourceImpl),
            "Internal source running in BlockingSourceThread must be of type BlockingSource.");

        uint64_t sequenceNumber = SequenceNumber::INITIAL;
        const EmitFn dataEmit
            = [&](IOBuffer&& buffer) -> void
            {
                addBufferMetadata(executionContext->originId, buffer, sequenceNumber++);
                emitFn(executionContext->originId, Data{std::move(buffer)});
            };

        const SourceHandle handle{*std::get<std::unique_ptr<BlockingSource>>(executionContext->sourceImpl)};
        /// 4 Things that could happen:
        /// 1. Happy Path: Source produces a tuple buffer and emit is called. The loop continues.
        /// 2. Stop was requested by the owner of the data source. Stop is propagated to the source implementation.
        /// The source returns control as soon as possible.
        /// 3. EndOfStream was signaled by the source implementation. It returned that it read nothing, but the Stop Token was not triggered.
        ///    The thread emits EoS to the query engine.
        /// 4. Failure. If anything internal to the source throws, the exception is caught by the initiating operator() and an Error event is emitted.
        ///    The promise is set to the exception.
        while (!stopToken.stop_requested())
        {
            auto buffer = executionContext->bufferProvider->getBufferBlocking();

            const auto numReadBytes = handle.source.fillBuffer(buffer, stopToken);

            if (numReadBytes != 0)
            {
                executionContext->inputFormatter->parseTupleBufferRaw(buffer, *executionContext->bufferProvider, numReadBytes, dataEmit);
            }

            if (stopToken.stop_requested())
            {
                return;
            }

            if (numReadBytes == 0)
            {
                return;
            }
        }
    }

    void operator()(const std::stop_token& stopToken)
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

private:
    std::promise<void> terminationPromise;
    std::unique_ptr<SourceExecutionContext> executionContext;
    EmitFunction emitFn;
};

}