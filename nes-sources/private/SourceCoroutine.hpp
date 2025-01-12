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

#include <chrono>
#include <memory>
#include <string>
#include <type_traits>
#include <variant>

#include <boost/asio/awaitable.hpp>
#include <boost/system/system_error.hpp>

#include <Identifiers/Identifiers.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/Logger.hpp>
#include <boost/shared_ptr.hpp>
#include <CoroutineExecutionContext.hpp>
#include <ErrorHandling.hpp>

namespace NES::Sources
{

class SourceCoroutine : std::enable_shared_from_this<SourceCoroutine>
{
public:
    explicit SourceCoroutine(CoroutineExecutionContext cec) : cec(std::move(cec)) { }
    ~SourceCoroutine() = default;

    SourceCoroutine(const SourceCoroutine&) = delete;
    SourceCoroutine(SourceCoroutine&&) = delete;
    SourceCoroutine& operator=(const SourceCoroutine&) = delete;
    SourceCoroutine& operator=(SourceCoroutine&&) = delete;

    asio::awaitable<void> operator()()
    {
        /// Try to open the source. On failure, propagate the error to the query engine via the emitFn.
        try
        {
            co_await cec.sourceImpl->open(cec.executor->ioContext());
        }
        catch (const Exception& e)
        {
            cec.emitFn(cec.originId, SourceReturnType::Error{.ex = e});
        }

        bool shouldContinue = true;
        /// Run forever, or until either:
        /// a) Cancellation is requested from the query engine (query stop)
        /// b) An error occurs within the source
        while (shouldContinue)
        {
            /// TODO(yschroeder97): write `getBufferAsync()` machinery, this will slow down all sources when the system is running at capacity
            /// 1. Acquire buffer
            auto buffer = cec.bufferProvider->getBufferBlocking();
            /// 2. Add buffer metadata
            addBufferMetadata(buffer, SequenceNumber{cec.maxSequenceNumber++});
            /// 3. Let source ingest raw bytes into buffer
            const auto result = co_await cec.sourceImpl->fillBuffer(buffer);
            /// 4. Handle the result and communicate it to the query engine via the emitFn
            shouldContinue = handleSourceResult(buffer, result);
        }

        cec.sourceImpl->close();
    }

    void stop() const { cec.sourceImpl->cancel(); }


private:
    CoroutineExecutionContext cec;

    bool handleSourceResult(IOBuffer& buffer, Source::InternalSourceResult result)
    {
        return std::visit(
            [this, &buffer](auto&& result) -> bool
            {
                using T = std::decay_t<decltype(result)>;
                if constexpr (std::is_same_v<T, Source::Continue>)
                {
                    /// Usual case, the source is ready to continue to ingest more data
                    cec.emitFn(cec.originId, SourceReturnType::Data(std::move(buffer)));
                    return true;
                }
                else if constexpr (std::is_same_v<T, Source::EndOfStream>)
                {
                    /// The source signalled the end of the stream, i.e., because the external system closed the connection.
                    /// Check for a partially filled buffer before signalling EoS and exiting
                    if (result.dataAvailable)
                    {
                        cec.emitFn(cec.originId, SourceReturnType::Data(std::move(buffer)));
                    }
                    cec.emitFn(cec.originId, SourceReturnType::EoS{});
                    return false;
                }
                else if constexpr (std::is_same_v<T, Source::Cancelled>)
                {
                    /// The source received a cancellation request, and
                    // asio reported successful cancellation via the operation_aborted signal
                    /// Therefore, it is now safe to exit the loop and release the buffer and close the source
                    cec.emitFn(cec.originId, SourceReturnType::EoS{});
                    return false;
                }
                else if constexpr (std::is_same_v<T, Source::Error>)
                {
                    const boost::system::system_error error = result.error;
                    cec.emitFn(cec.originId, SourceReturnType::Error{Exception{std::string(error.what()), 0}});
                    return false;
                }
            },
            result);
    }

    void addBufferMetadata(IOBuffer& buffer, const SequenceNumber sequenceNumber) const
    {
        buffer.setOriginId(cec.originId);
        buffer.setCreationTimestampInMS(
            Runtime::Timestamp(
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch())
                    .count()));
        buffer.setSequenceNumber(sequenceNumber);
        buffer.setChunkNumber(ChunkNumber{1});
        buffer.setLastChunk(true);

        NES_TRACE(
            "Setting buffer metadata with originId={} sequenceNumber={} chunkNumber={} lastChunk={}",
            buffer.getOriginId(),
            buffer.getSequenceNumber(),
            buffer.getChunkNumber(),
            buffer.isLastChunk());
    }
};

}