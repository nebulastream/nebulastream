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
#include <cstdint>
#include <ranges>
#include <string_view>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Concepts.hpp>
#include <UncompiledRawValueParser.hpp>

namespace NES
{

using UncompiledFieldIndex = uint32_t;

class UncompiledSchemaInfo
{
public:
    explicit UncompiledSchemaInfo(const Schema& schema) : sizeOfTupleInBytes(schema.getSizeOfSchemaInBytes())
    {
        this->fieldSizesInBytes = schema.getFields()
            | std::views::transform([](const auto& field) { return static_cast<size_t>(field.dataType.getSizeInBytesWithoutNull()); })
            | std::ranges::to<std::vector>();
    }

    [[nodiscard]] size_t getSizeOfTupleInBytes() const { return sizeOfTupleInBytes; }

    [[nodiscard]] const std::vector<size_t>& getFieldSizesInBytes() const { return fieldSizesInBytes; }

private:
    size_t sizeOfTupleInBytes;
    std::vector<size_t> fieldSizesInBytes;
};

/// CRTP Interface that enables InputFormatters to define specialized functions to access fields that the UncompiledInputFormatterTask can call directly
/// (templated) without the overhead of a virtual function call (or a lambda function/std::function)
/// Different possible kinds of FieldIndexFunctions(UncompiledFieldOffsets(index raw data), Internal, ZStdCompressed, Arrow, ...)
/// A UncompiledFieldIndexFunction may also combine different kinds of field access variants for the different fields of one schema
template <typename Derived>
class UncompiledFieldIndexFunction
{
public:
    /// Expose the UncompiledFieldIndexFunction interface functions only to the UncompiledInputFormatterTask
    template <UncompiledInputFormatIndexerType FormatterType>
    friend class UncompiledInputFormatterTask;

    /// Allows the free function 'processUncompiledTuple' to access the protected 'readFieldAt' function
    template <typename UncompiledFieldIndexFunctionType>
    friend void processUncompiledTuple(
        std::string_view tupleView,
        UncompiledFieldIndexFunction<UncompiledFieldIndexFunctionType>& fieldIndexFunction,
        size_t numTuplesReadFromRawBuffer,
        TupleBuffer& formattedBuffer,
        const UncompiledSchemaInfo& schemaInfo,
        const std::vector<UncompiledParseFunctionSignature>& parseFunctions,
        AbstractBufferProvider& bufferProvider);

    friend Derived;

private:
    UncompiledFieldIndexFunction()
    {
        /// Cannot use Concepts / requires because of the cyclic nature of the CRTP pattern.
        /// The UncompiledInputFormatterTask (IFT) guarantees that the reference to AbstractBufferProvider (ABP) outlives the UncompiledFieldIndexFunction
        static_assert(std::is_constructible_v<Derived>, "Derived class must have a default constructor");
    };

public:
    ~UncompiledFieldIndexFunction() = default;

    [[nodiscard]] UncompiledFieldIndex getOffsetOfFirstTupleDelimiter() const
    {
        return static_cast<const Derived*>(this)->applyGetByteOffsetOfFirstTuple();
    }

    [[nodiscard]] UncompiledFieldIndex getOffsetOfLastTupleDelimiter() const
    {
        return static_cast<const Derived*>(this)->applyGetByteOffsetOfLastTuple();
    }

    [[nodiscard]] size_t getTotalNumberOfTuples() const { return static_cast<const Derived*>(this)->applyGetTotalNumberOfTuples(); }

    [[nodiscard]] bool hasNext(size_t tupleIdx) { return static_cast<Derived*>(this)->applyHasNext(tupleIdx); }

    [[nodiscard]] std::string_view readFieldAt(const std::string_view bufferView, size_t tupleIdx, size_t fieldIdx)
    {
        return static_cast<Derived*>(this)->applyReadFieldAt(bufferView, tupleIdx, fieldIdx);
    }
};

struct UncompiledNoopFieldIndexFunction
{
    [[nodiscard]] static UncompiledFieldIndex getOffsetOfFirstTupleDelimiter() { return 0; }

    [[nodiscard]] static UncompiledFieldIndex getOffsetOfLastTupleDelimiter() { return 0; }

    [[nodiscard]] static size_t getTotalNumberOfTuples() { return 0; }

    [[nodiscard]] static bool hasNext(size_t) { return false; }

    [[nodiscard]] static std::string_view readFieldAt(const std::string_view bufferView, size_t, size_t) { return bufferView; }
};
}
