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

#include <optional>
#include <string>
#include <Util/TestTupleBuffer.hpp>

namespace NES
{

/// Source is the interface for all sources that read data into TupleBuffers.
/// 'DataSource' creates TupleBuffers and uses 'Source' to fill.
/// When 'fillTupleBuffer()' returns successfully, 'DataSource' creates a new Task using the filled TupleBuffer.
class Source
{
public:
    Source() = default;
    virtual ~Source() = default;

    /// Read data from a source into a TupleBuffer, until the TupleBuffer is full (or a timeout is reached).
    virtual bool fillTupleBuffer(Runtime::MemoryLayouts::TestTupleBuffer& tupleBuffer) = 0;

    virtual void open() = 0;
    virtual void close() = 0;

    [[nodiscard]] virtual SourceType getType() const = 0;

    [[nodiscard]] virtual std::string toString() const = 0;
};

} /// namespace NES
