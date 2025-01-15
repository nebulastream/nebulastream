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
#include <stop_token>

#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES::Sources
{

using IOBuffer = Memory::TupleBuffer;

/// BlockingSource is the interface for all blocking sources that read data into buffers in a blocking fashion.
/// 'BlockingSourceRunner' creates TupleBuffers and uses 'BlockingSource' to fill.
/// When 'fillBuffer()' returns successfully, 'BlockingSourceThread' creates a new Task using the filled TupleBuffer.
class BlockingSource
{
public:
    BlockingSource() = default;
    virtual ~BlockingSource() = default;

    BlockingSource(const BlockingSource&) = delete;
    BlockingSource& operator=(const BlockingSource&) = delete;
    BlockingSource(BlockingSource&&) = delete;
    BlockingSource& operator=(BlockingSource&&) = delete;

    /// Read data from a source into a buffer, until it is full (or a timeout is reached).
    /// @return the number of bytes read
    virtual size_t fillBuffer(IOBuffer& buffer, const std::stop_token& stopToken) = 0;

    /// If applicable, opens a connection, e.g., a socket connection to get ready for data consumption.
    virtual void open() = 0;
    /// If applicable, closes a connection, e.g., a socket connection.
    virtual void close() = 0;

    friend std::ostream& operator<<(std::ostream& out, const BlockingSource& source);

protected:
    /// Implemented by children of BlockingSource. Called by '<<'. Allows to use '<<' on abstract BlockingSource.
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;
};

}

namespace fmt
{
template <>
struct formatter<NES::Sources::BlockingSource> : ostream_formatter
{
};
}