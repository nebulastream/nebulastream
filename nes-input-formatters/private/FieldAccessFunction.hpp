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
#include <string_view>
#include <vector>

#include <InputFormatters/InputFormatter.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp> /// FieldOffsetType
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <RawInputDataParser.hpp>

namespace NES::InputFormatters
{

/// CRTP Interface that enables InputFormatters to define specialized functions to access fields that the InputFormatterTask can call directly
/// (templated) without the overhead of a virtual function call (or a lambda function/std::function)
/// Different possible kinds of FieldAccessFunctions(FieldOffsets(index raw data), Internal, ZStdCompressed, Arrow, ...)
/// A FieldAccessFunction may also combine different kinds of field access variants for the different fields of one schema
template <typename Derived>
class FieldAccessFunction
{
    /// Expose the FieldAccessFunction interface functions only to the InputFormatterTask
    template <typename FormatterType, typename FieldAccessFunctionType, bool HasSpanningTuple>
    friend class InputFormatterTask;

    /// Allows the free function 'processTuple' to access the protected 'readFieldAt' function
    template <typename FieldAccessFunctionType>
    friend void processTuple(
        std::string_view bufferView,
        const FieldAccessFunction<FieldAccessFunctionType>& fieldAccessFunction,
        size_t numTuplesReadFromRawBuffer,
        Memory::TupleBuffer& formattedBuffer,
        const TupleMetaData& tupleMetaData,
        const std::vector<RawInputDataParser::ParseFunctionSignature>& parseFunctions,
        Memory::AbstractBufferProvider& bufferProvider);

protected:
    FieldAccessFunction()
    {
        /// Cannot use Concepts / requires because of the cyclic nature of the CRTP pattern.
        /// The InputFormatterTask (IFT) guarantees that the reference to AbstractBufferProvider (ABP) outlives the FieldAccessFunction
        static_assert(std::is_constructible_v<Derived, Memory::AbstractBufferProvider&>, "Derived class must have a default constructor");
    };
    ~FieldAccessFunction() = default;

    [[nodiscard]] FieldOffsetsType getOffsetOfFirstTupleDelimiter() const
    {
        return static_cast<const Derived*>(this)->applyGetOffsetOfFirstTupleDelimiter();
    }

    [[nodiscard]] FieldOffsetsType getOffsetOfLastTupleDelimiter() const
    {
        return static_cast<const Derived*>(this)->applyGetOffsetOfLastTupleDelimiter();
    }

    [[nodiscard]] size_t getTotalNumberOfTuples() const { return static_cast<const Derived*>(this)->applyGetTotalNumberOfTuples(); }

    [[nodiscard]] std::string_view readFieldAt(const std::string_view bufferView, size_t tupleIdx, size_t fieldIdx) const
    {
        return static_cast<const Derived*>(this)->applyReadFieldAt(bufferView, tupleIdx, fieldIdx);
    }
};
}
