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

#include <InputFormatters/InputFormatIndexer.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp> /// FieldOffsetType
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <RawValueParser.hpp>

namespace NES::InputFormatters
{

class SchemaInfo
{
public:
    explicit SchemaInfo(const Schema& schema)
    {
        this->sizeOfTupleInBytes = schema.getSizeOfSchemaInBytes();
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
    /// Expose the FieldIndexFunction interface functions only to the InputFormatterTask
    template <typename FormatterType, typename FieldIndexFunctionType, IndexerMetaDataType IndexerMetaData, bool HasSpanningTuple>
    requires(HasSpanningTuple or not FormatterType::IsFormattingRequired)
    friend class InputFormatterTask;

    /// Allows the free function 'processTuple' to access the protected 'readFieldAt' function
    template <typename FieldIndexFunctionType>
    friend void processTuple(
        const std::string_view tupleView,
        const FieldIndexFunction<FieldIndexFunctionType>& fieldIndexFunction,
        const size_t numTuplesReadFromRawBuffer,
        Memory::TupleBuffer& formattedBuffer,
        const SchemaInfo& schemaInfo,
        const std::vector<RawValueParser::ParseFunctionSignature>& parseFunctions,
        Memory::AbstractBufferProvider& bufferProvider);

protected:
    FieldIndexFunction() = default;
    ~FieldIndexFunction() = default;

private:
    [[nodiscard]] FieldOffsetsType getOffsetOfFirstTupleDelimiter() const
    {
        return static_cast<const Derived*>(this)->applyGetOffsetOfFirstTupleDelimiter();
    }

    [[nodiscard]] FieldOffsetsType getOffsetOfLastTupleDelimiter() const
    {
        return static_cast<const Derived*>(this)->applyGetOffsetOfLastTupleDelimiter();
    }

    [[nodiscard]] size_t getTotalNumberOfTuples() const { return static_cast<const Derived*>(this)->applyGetTotalNumberOfTuples(); }

    template <typename IndexerMetaData>
    [[nodiscard]] Record readNextRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const RecordBuffer& recordBuffer,
        nautilus::val<uint64_t>& recordIndex,
        const IndexerMetaData& metaData,
        const size_t configuredBufferSize,
        const std::vector<RawValueParser::ParseFunctionSignature>& parseFunctions) const
    {
        return static_cast<const Derived*>(this)->template applyReadNextRecord<IndexerMetaData>(projections, recordBuffer, recordIndex, metaData, configuredBufferSize, parseFunctions);
    }

    template <typename IndexerMetaData>
    [[nodiscard]] Record readSpanningRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const nautilus::val<int8_t*>& recordBufferPtr,
        nautilus::val<uint64_t>& recordIndex,
        const IndexerMetaData& metaData,
        const std::vector<RawValueParser::ParseFunctionSignature>& parseFunctions) const
    {
        return static_cast<const Derived*>(this)->template applyReadSpanningRecord<IndexerMetaData>(projections, recordBufferPtr, recordIndex, metaData, parseFunctions);
    }
};
}
