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
#include <variant>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <fmt/ostream.h>

#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ErrorHandling.hpp>

namespace NES::Sources
{

using IOBuffer = Memory::TupleBuffer;

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
    virtual asio::awaitable<void> open() = 0;
    /// Read data from a source into a buffer, until it is full or an EoS/Error occurs.
    virtual asio::awaitable<InternalSourceResult> fillBuffer(IOBuffer& buffer) = 0;
    /// If applicable, closes a connection, e.g., a socket connection.
    virtual void close() = 0;

    /// Cancel outstanding asynchronous operations
    /// Handlers for completed operations will reveive asio::error::operation_aborted signal
    /// Sources will typically implement this by calling cancel() on their associated I/O object
    virtual void cancel() = 0;

    friend std::ostream& operator<<(std::ostream& out, const AsyncSource& source);

protected:
    /// Implemented by children of Source. Called by '<<'. Allows to use '<<' on abstract Source.
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;
};

}

FMT_OSTREAM(NES::Sources::AsyncSource);
