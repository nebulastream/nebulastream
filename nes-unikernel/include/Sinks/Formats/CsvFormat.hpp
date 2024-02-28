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
#include <Util/DisableWarningsPragma.hpp>
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
std::string_view copyIntoBuffer(std::span<const char> data, SchemaBufferType& buffer) {
    uint64_t numberOfTuplesInBuffer = 0;
    if (data.size() < sizeof(numberOfTuplesInBuffer)) {
        return {data.data(), data.size()};
    }

    numberOfTuplesInBuffer = *std::bit_cast<uint64_t*>(data.data());
    auto tupleBufferData = data.subspan(sizeof(numberOfTuplesInBuffer));
    if (numberOfTuplesInBuffer * SchemaBufferType::SchemaType::TupleSize < tupleBufferData.size()) {
        return {data.data(), data.size()};
    }

    std::memcpy(buffer.getBuffer().getBuffer(),
                tupleBufferData.data(),
                tupleBufferData.data() + numberOfTuplesInBuffer * SchemaBufferType::SchemaType::TupleSize);
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

    // scn::scan(data, SchemaBufferType::SchemaType::csvFormat).and_then([&buffer](auto& result) {
    //     buffer.writeTuple(result.values());
    // });
    // for (const auto& line :
    //      std::ranges::views::split(std::string_view(data.begin(), data.end()), "\n") | std::ranges::views::take(tuplesLeft)) {
    //     typename SchemaBufferType::TupleType tuple;
    //     NES_INFO("Line: {}", std::string_view(line.begin(), line.end()));
    //     auto result = std::apply(
    //         [&line]<typename... T>(const T&...) {
    //             return scn::scan<T...>(line, SchemaBufferType::SchemaType::csvFormat);
    //         },
    //         tuple);
    //     if (!result) {
    //         NES_ERROR("Failed to parse");
    //         return std::span{data.begin() + bytesConsumed, data.end()};
    //     }
    //     bytesConsumed += line.size() + 1;
    //     buffer.writeTuple(result->values());
    // }
    //
    // if (bytesConsumed >= data.size()) {
    //     return std::span{data.end(), data.end()};
    // }
}
}// namespace NES::Unikernel
#endif//NES_CSVFORMAT_HPP
