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
#include <variant>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Buffer.hpp>
#include <Util/Logger/Formatter.hpp>

namespace NES
{

/// Source is the interface for all sources that read data into Buffers.
/// 'SourceThread' creates Buffers and uses 'Source' to fill.
/// When 'fillBuffer()' returns successfully, 'SourceThread' creates a new Task using the filled Buffer.
class Source
{
public:
    class FillBufferResult
    {
        explicit FillBufferResult(size_t sizeInBytes) : result(Data{sizeInBytes}) { };
        FillBufferResult() = default;

        struct EoS
        {
        };

        struct Data
        {
            size_t sizeInBytes;
        };

        std::variant<EoS, Data> result = EoS{};

    public:
        static FillBufferResult eos() { return {}; }

        static FillBufferResult withBytes(size_t sizeInBytes) { return FillBufferResult{sizeInBytes}; }

        [[nodiscard]] bool isEoS() const { return std::holds_alternative<EoS>(result); }

        [[nodiscard]] size_t getNumberOfBytes() const { return std::get<Data>(result).sizeInBytes; }
    };

    Source() = default;
    virtual ~Source() = default;

    /// Read data from a source into a Buffer, until the Buffer is full (or a timeout is reached).
    /// @return the number of bytes read
    virtual FillBufferResult fillBuffer(Buffer& tupleBuffer, const std::stop_token& stopToken) = 0;

    /// If applicable, opens a connection, e.g., a socket connection to get ready for data consumption.
    virtual void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) = 0;
    /// If applicable, closes a connection, e.g., a socket connection.
    virtual void close() = 0;

    friend std::ostream& operator<<(std::ostream& out, const Source& source);

    [[nodiscard]] virtual bool addsMetadata() const { return false; }

protected:
    /// Implemented by children of Source. Called by '<<'. Allows to use '<<' on abstract Source.
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;
};

}

FMT_OSTREAM(NES::Source);
