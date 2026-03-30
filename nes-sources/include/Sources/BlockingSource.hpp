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
#include <ostream>
#include <stop_token>

#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <variant>
#include <Runtime/AbstractBufferProvider.hpp>

namespace NES
{

/// BlockingSource is the interface for all blocking sources that read data into buffers in a blocking fashion.
/// 'BlockingSourceRunner' creates IOBuffers and uses 'BlockingSource' to fill.
/// When 'fillBuffer()' returns successfully, 'BlockingSourceThread' creates a new Task using the filled TupleBuffer.
class BlockingSource
{
public:
    class FillTupleBufferResult
    {
        explicit FillTupleBufferResult(size_t sizeInBytes) : result(Data{sizeInBytes}) { };
        FillTupleBufferResult() = default;

        struct EoS
        {
        };

        struct Data
        {
            size_t sizeInBytes;
        };

        std::variant<EoS, Data> result = EoS{};

    public:
        static FillTupleBufferResult eos() { return {}; }

        static FillTupleBufferResult withBytes(size_t sizeInBytes) { return FillTupleBufferResult{sizeInBytes}; }

        [[nodiscard]] bool isEoS() const { return std::holds_alternative<EoS>(result); }

        [[nodiscard]] size_t getNumberOfBytes() const { return std::get<Data>(result).sizeInBytes; }
    };

    BlockingSource() = default;
    virtual ~BlockingSource() = default;

    BlockingSource(BlockingSource&&) = delete;
    BlockingSource& operator=(BlockingSource&&) = delete;

    /// Read data from a source into a buffer, until it is full (or a timeout is reached).
    /// @return the number of bytes read
    virtual FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) = 0;

    /// If applicable, opens a connection, e.g., a socket connection to get ready for data consumption.
    virtual void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) = 0;
    /// If applicable, closes a connection, e.g., a socket connection.
    virtual void close() = 0;

    friend std::ostream& operator<<(std::ostream& out, const BlockingSource& source);

    [[nodiscard]] virtual bool addsMetadata() const { return false; }

    /// Implemented by children of BlockingSource. Called by '<<'. Allows to use '<<' on abstract BlockingSource.
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;
};

}

FMT_OSTREAM(NES::BlockingSource);
