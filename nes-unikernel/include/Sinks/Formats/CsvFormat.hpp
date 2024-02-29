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

#ifndef NES_CSVFORMAT_HPP
#define NES_CSVFORMAT_HPP
#include <Runtime/TupleBuffer.hpp>
#include <SchemaBuffer.hpp>
#include <ranges>
#include <scn/scan.h>
#include <sstream>
#include <tuple>

namespace NES::Unikernel {

template<typename OUT, typename TupleType>
void write_csv(OUT& out, const TupleType& tuple) {
    std::apply(
        [&out](auto&&... value) {
            size_t index = 0;
            ((out << value << ((++index < sizeof...(value)) ? ',' : '\n')), ...);
        },
        tuple);
}

template<typename Schema, size_t BufferSize>
std::string printTupleBufferAsCSV(const NES::Runtime::TupleBuffer& tbuffer) {
    std::stringstream ss;
    const auto schemaBuffer = SchemaBuffer<Schema, BufferSize>::of(tbuffer);
    for (size_t i = 0; i < tbuffer.getNumberOfTuples(); ++i) {
        auto tuple = schemaBuffer.readTuple(i);
        write_csv(ss, tuple);
    }

    return ss.str();
}

template<typename SchemaBufferType, typename... T>
static std::string_view scanRecursively(std::string_view sv, SchemaBufferType& buffer, size_t tuplesLeft) {
    if (tuplesLeft == 0)
        return sv;

    if (auto result = scn::scan<T...>(sv, SchemaBufferType::SchemaType::csvFormat)) {
        buffer.writeTuple(result->values());
        auto new_sv = std::string_view{result->range().begin(), result->range().end()};
        [[clang::musttail]] return scanRecursively<SchemaBufferType, T...>(new_sv, buffer, tuplesLeft - 1);
    }
    return sv;
}

template<typename SchemaBufferType>
std::string_view copyIntoBuffer(const std::span<const char> data, SchemaBufferType& buffer) {
    uint64_t sizeOfBuffer = 0;
    if (sizeof(sizeOfBuffer) > data.size()) {
        return {data.data(), data.size()};
    }

    sizeOfBuffer = *std::bit_cast<uint64_t*>(data.data());
    NES_ASSERT(sizeOfBuffer % SchemaBufferType::SchemaType::TupleSize == 0, "Schema Missmatch");
    auto numberOfTuples = sizeOfBuffer / SchemaBufferType::SchemaType::TupleSize;

    NES_TRACE("Number of tuples: {}", numberOfTuples);
    if (sizeOfBuffer + sizeof(sizeOfBuffer) > data.size()) {
        NES_DEBUG("Could not extract full Data Segment Size: {} Required: {}", data.size(), sizeOfBuffer);
        return {data.data(), data.size()};
    }

    auto tupleBufferData = data.subspan(sizeof(sizeOfBuffer), sizeOfBuffer);
    std::memcpy(buffer.getBuffer().getBuffer(), tupleBufferData.data(), tupleBufferData.size());
    buffer.getBuffer().setNumberOfTuples(numberOfTuples);

    return {tupleBufferData.end(), data.end()};
}

template<typename SchemaBufferType>
std::string_view parseCSVIntoBuffer(std::span<const char> data, SchemaBufferType& buffer) {
    size_t tuplesLeft = buffer.getCapacity() - buffer.getSize();
    // size_t bytesConsumed = 0;
    typename SchemaBufferType::TupleType t;
    return std::apply(
        [data, &buffer, tuplesLeft]<typename... T>(T...) {
            return scanRecursively<SchemaBufferType, T...>(std::string_view{data.begin(), data.end()}, buffer, tuplesLeft);
        },
        t);
}
}// namespace NES::Unikernel
#endif//NES_CSVFORMAT_HPP
