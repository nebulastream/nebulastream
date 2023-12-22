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
#include <sstream>
namespace NES::Unikernel {

template<typename IN, typename TupleType>
TupleType read_csv(IN& in) {
    TupleType tuple;
    std::apply(
        [&in](auto&&... value) {
            char comma;
            ((in >> value >> comma), ...);
        },
        tuple);

    return tuple;
}

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

template<typename SchemaBufferType, typename IS>
size_t parseCSVIntoBuffer(IS& istream, SchemaBufferType& buffer) {
    size_t tuples_written = 0;
    while (buffer.getSize() < buffer.getCapacity() && !istream.eof()) {
        auto tuple = read_csv<IS, typename SchemaBufferType::TupleType>(istream);
        buffer.writeTuple(tuple);
        tuples_written++;
    }
    return tuples_written;
}
}// namespace NES::Unikernel
#endif//NES_CSVFORMAT_HPP
