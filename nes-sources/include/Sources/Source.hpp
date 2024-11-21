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
#include <string>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES::Sources
{

/// Source is the interface for all sources that read data into TupleBuffers.
/// 'SourceThread' creates TupleBuffers and uses 'Source' to fill.
/// When 'fillTupleBuffer()' returns successfully, 'SourceThread' creates a new Task using the filled TupleBuffer.
class Source
{
public:
    Source() = default;
    virtual ~Source() = default;

    Source(const Source&) = delete;
    Source& operator=(const Source&) = delete;
    Source(Source&&) = delete;
    Source& operator=(Source&&) = delete;

    /// Read data from a source into a TupleBuffer, until the TupleBuffer is full (or a timeout is reached).
    /// @return the number of bytes read
    virtual size_t fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer) = 0;

    /// If applicable, opens a connection, e.g., a socket connection to get ready for data consumption.
    virtual void open() = 0;
    /// If applicable, closes a connection, e.g., a socket connection.
    virtual void close() = 0;

    friend std::ostream& operator<<(std::ostream& out, const Source& source);

protected:
    /// Implemented by children of Source. Called by '<<'. Allows to use '<<' on abstract Source.
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;
};

}

namespace fmt
{
template <>
struct formatter<NES::Sources::Source> : ostream_formatter
{
};
}
