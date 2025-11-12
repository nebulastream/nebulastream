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
#include <RawValueParser.hpp>

namespace NES
{

class SchemaInfo
{
public:
    explicit SchemaInfo(const Schema& schema) : sizeOfTupleInBytes(schema.getSizeOfSchemaInBytes())
    {
        this->fieldSizesInBytes = schema.getFields()
            | std::views::transform([](const auto& field) { return static_cast<size_t>(field.dataType.getSizeInBytes()); })
            | std::ranges::to<std::vector>();
    }

    [[nodiscard]] size_t getSizeOfTupleInBytes() const { return sizeOfTupleInBytes; }

    [[nodiscard]] const std::vector<size_t>& getFieldSizesInBytes() const { return fieldSizesInBytes; }

private:
    size_t sizeOfTupleInBytes;
    std::vector<size_t> fieldSizesInBytes;
};

/// CRTP Interface that enables InputFormatters to define specialized functions to access fields that the InputFormatterTask can call directly
/// (templated) without the overhead of a virtual function call (or a lambda function/std::function)
/// Different possible kinds of FieldIndexFunctions(FieldOffsets(index raw data), Internal, ZStdCompressed, Arrow, ...)
/// A FieldIndexFunction may also combine different kinds of field access variants for the different fields of one schema
template <typename Derived>
class FieldIndexFunction
{
public:
    /// Expose the FieldIndexFunction interface functions only to the InputFormatterTask
    template <InputFormatIndexerType FormatterType>
    friend class InputFormatterTask;

    /// Allows the free function 'processTuple' to access the protected 'readFieldAt' function
    template <typename FieldIndexFunctionType>
    friend void processTuple(
        std::string_view tupleView,
        const FieldIndexFunction<FieldIndexFunctionType>& fieldIndexFunction,
        size_t numTuplesReadFromRawBuffer,
        TupleBuffer& formattedBuffer,
        const SchemaInfo& schemaInfo,
        const std::vector<ParseFunctionSignature>& parseFunctions,
        AbstractBufferProvider& bufferProvider);

    friend Derived;

private:
    FieldIndexFunction()
    {
        /// Cannot use Concepts / requires because of the cyclic nature of the CRTP pattern.
        /// The InputFormatterTask (IFT) guarantees that the reference to AbstractBufferProvider (ABP) outlives the FieldIndexFunction
        static_assert(std::is_constructible_v<Derived, AbstractBufferProvider&>, "Derived class must have a default constructor");
    };

public:
    ~FieldIndexFunction() = default;

    [[nodiscard]] FieldIndex getOffsetOfFirstTupleDelimiter() const
    {
        return static_cast<const Derived*>(this)->applyGetOffsetOfFirstTupleDelimiter();
    }

    [[nodiscard]] FieldIndex getOffsetOfLastTupleDelimiter() const
    {
        return static_cast<const Derived*>(this)->applyGetOffsetOfLastTupleDelimiter();
    }

    [[nodiscard]] size_t getTotalNumberOfTuples() const { return static_cast<const Derived*>(this)->applyGetTotalNumberOfTuples(); }

    [[nodiscard]] std::string_view readFieldAt(const std::string_view bufferView, size_t tupleIdx, size_t fieldIdx) const
    {
        return static_cast<const Derived*>(this)->applyReadFieldAt(bufferView, tupleIdx, fieldIdx);
    }
};

struct NoopFieldIndexFunction
{
    [[nodiscard]] static FieldIndex getOffsetOfFirstTupleDelimiter() { return 0; }

    [[nodiscard]] static FieldIndex getOffsetOfLastTupleDelimiter() { return 0; }

    [[nodiscard]] static size_t getTotalNumberOfTuples() { return 0; }

    [[nodiscard]] static std::string_view readFieldAt(const std::string_view bufferView, size_t, size_t) { return bufferView; }
};
}
