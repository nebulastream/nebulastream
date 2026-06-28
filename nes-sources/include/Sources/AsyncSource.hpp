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


#include <cstddef>
#include <memory>
#include <optional>
#include <variant>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <fmt/ostream.h>

#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class AbstractBufferProvider;

using Executor = boost::asio::io_context::executor_type;

namespace asio = boost::asio;

/// AsyncSource is the interface for all sources that read data into buffers asynchronously.
class AsyncSource
{
public:
    struct EndOfStream
    {
        size_t bytesRead;
    };

    struct Continue
    {
        size_t bytesRead;
    };

    struct Error
    {
        Exception exception;
    };

    struct Cancelled
    {
    };

    using InternalSourceResult = std::variant<Continue, Cancelled, EndOfStream, Error>;

    AsyncSource() = default;
    virtual ~AsyncSource() = default;

    AsyncSource(const AsyncSource&) = delete;
    AsyncSource& operator=(const AsyncSource&) = delete;

    AsyncSource(AsyncSource&&) = delete;
    AsyncSource& operator=(AsyncSource&&) = delete;

    /// If applicable, opens a connection, e.g., a socket connection to get ready for data consumption.
    /// The source's own (inflight-sized) buffer pool is handed in here -- mirroring BlockingSource::open --
    /// so a source can acquire its own buffers (e.g. a deep-QD io_uring ring keeping many reads in flight).
    /// Sources that only fill the single buffer the runner hands to fillBuffer() may ignore it.
    virtual asio::awaitable<void, Executor> open(std::shared_ptr<AbstractBufferProvider> bufferProvider) = 0;
    /// Read data from a source into a buffer, until it is full or an EoS/Error occurs.
    virtual asio::awaitable<InternalSourceResult, Executor> fillBuffer(TupleBuffer& buffer) = 0;
    /// If applicable, closes a connection, e.g., a socket connection.
    virtual void close() = 0;

    /// Prefill model (mirrors BlockingSource::preFillsBuffers): when true, the runner does NOT pre-acquire a
    /// buffer and call fillBuffer(); instead it drains takePreFilledBuffer(), which hands back buffers the
    /// source filled itself (from the pool it received in open()). This is what lets a source keep many reads
    /// in flight (deep QD) -- impossible with the one-buffer-per-fillBuffer interface.
    [[nodiscard]] virtual bool preFillsBuffers() const { return false; }

    /// Reap and return the next ready buffer in source order (nullopt = end-of-stream). Only called when
    /// preFillsBuffers() is true. Suspends (without blocking the io_context thread) until a buffer is ready.
    virtual asio::awaitable<std::optional<TupleBuffer>, Executor> takePreFilledBuffer();

    friend std::ostream& operator<<(std::ostream& out, const AsyncSource& source);

protected:
    /// Implemented by children of Source. Called by '<<'. Allows to use '<<' on abstract Source.
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;
};

}

FMT_OSTREAM(NES::AsyncSource);
