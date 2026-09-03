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
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Interface/Record.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// Layout of the serialized reservoir sample statistic blob:
/// | tupleCount: uint64 @ 0 | dataSizeBytes: uint64 @ 8 | tuple data @ 16 |
/// Tuples are stored back-to-back in the field order of the build's input schema, without null bytes
/// (nullable input fields are rejected during lowering). Fixed-size fields take getSizeInBytesWithoutNull()
/// bytes; VARSIZED fields are stored as a uint32 length prefix followed by the content.
struct ReservoirSampleBlob
{
    static constexpr uint64_t TUPLE_COUNT_OFFSET = 0;
    static constexpr uint64_t DATA_SIZE_OFFSET = 8;
    static constexpr uint64_t HEADER_SIZE = 16;
    static constexpr uint64_t VARSIZED_LENGTH_PREFIX_SIZE = sizeof(uint32_t);
};

/// A field of the sample tuples as declared by the probe (or derived from the build's input schema).
struct ReservoirSampleField
{
    Record::RecordFieldIdentifier name;
    DataType type;
};

/// Nautilus accessors for the blob header
void writeReservoirSampleBlobHeader(
    const nautilus::val<int8_t*>& blobMemArea, const nautilus::val<uint64_t>& tupleCount, const nautilus::val<uint64_t>& dataSizeBytes);
[[nodiscard]] nautilus::val<uint64_t> readReservoirSampleBlobTupleCount(const nautilus::val<int8_t*>& blobMemArea);
[[nodiscard]] nautilus::val<int8_t*> getReservoirSampleBlobDataArea(const nautilus::val<int8_t*>& blobMemArea);

/// Iterates the sample tuples of a reservoir sample blob. Not a full C++ iterator: the caller drives the
/// loop with the tuple count from the header.
class ReservoirSampleBlobRowReader
{
public:
    ReservoirSampleBlobRowReader(const nautilus::val<int8_t*>& blobMemArea, const std::vector<ReservoirSampleField>& fields);

    /// Reads the tuple at the current position into a Record and advances the cursor past it.
    [[nodiscard]] Record readNextTuple();

private:
    nautilus::val<int8_t*> cursor;
    const std::vector<ReservoirSampleField>& fields;
};

}
