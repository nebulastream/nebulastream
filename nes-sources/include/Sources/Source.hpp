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
#include <string>
#include <utility>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>

namespace NES
{

/// Source is the interface for all sources that read data into TupleBuffers.
/// 'SourceThread' creates TupleBuffers and uses 'Source' to fill.
/// When 'fillTupleBuffer()' returns successfully, 'SourceThread' creates a new Task using the filled TupleBuffer.
class Source
{
public:
    class FillTupleBufferResult
    {
        struct EoS
        {
        };

        struct Data
        {
            size_t sizeInBytes;
        };

        struct Marker
        {
            /// The serialized reconfiguration marker.
            std::string marker;
        };

        explicit FillTupleBufferResult(size_t sizeInBytes) : result(Data{sizeInBytes}) { };
        explicit FillTupleBufferResult(Marker marker) : result(std::move(marker)) { };
        FillTupleBufferResult() = default;

        std::variant<EoS, Data, Marker> result = EoS{};

    public:
        static FillTupleBufferResult eos() { return {}; }

        static FillTupleBufferResult withBytes(size_t sizeInBytes) { return FillTupleBufferResult{sizeInBytes}; }

        /// The source received a reconfiguration marker (ISQD). Like EoS, the marker ends the stream, but the source thread reports that the
        /// source was drained by the marker.
        static FillTupleBufferResult marker(std::string marker) { return FillTupleBufferResult{Marker{std::move(marker)}}; }

        [[nodiscard]] bool isEoS() const { return std::holds_alternative<EoS>(result); }

        [[nodiscard]] bool isMarker() const { return std::holds_alternative<Marker>(result); }

        [[nodiscard]] const std::string& getMarker() const { return std::get<Marker>(result).marker; }

        [[nodiscard]] size_t getNumberOfBytes() const { return std::get<Data>(result).sizeInBytes; }
    };

    Source() = default;
    virtual ~Source() = default;

    /// Read data from a source into a TupleBuffer, until the TupleBuffer is full (or a timeout is reached).
    /// @return the number of bytes read
    virtual FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) = 0;

    /// If applicable, opens a connection, e.g., a socket connection to get ready for data consumption.
    virtual void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) = 0;
    /// If applicable, closes a connection, e.g., a socket connection.
    virtual void close() = 0;

    /// Called from another thread when the query of the source is drained by a reconfiguration marker, possibly before 'open()' or after
    /// 'close()'.
    /// Returns true if the source ends its stream by itself: 'fillTupleBuffer()' keeps delivering the data that belongs to the drained
    /// query and returns EoS afterwards.
    /// Returns false if the source thread has to interrupt the source like a stop. Most sources do not hold any data which
    /// belongs to the query but has not been read yet, so this is the default.
    [[nodiscard]] virtual bool drain() { return false; }

    friend std::ostream& operator<<(std::ostream& out, const Source& source);

    [[nodiscard]] virtual bool addsMetadata() const { return false; }

protected:
    /// Implemented by children of Source. Called by '<<'. Allows to use '<<' on abstract Source.
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;
};

}

FMT_OSTREAM(NES::Source);
