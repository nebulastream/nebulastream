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
#include <ostream>
#include <string_view>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Nautilus/Interface/BufferRef/RowTupleBufferRef.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatIndexer.hpp>
#include <NativeFIF.hpp>
#include <RawTupleBuffer.hpp>
#include <RawValueParser.hpp>
#include <static.hpp>

namespace NES
{

struct ConfigParametersNative
{
    static inline const DescriptorConfig::ConfigParameter<std::string> STRING_LENGTH{
        "string_length",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        {
            const auto stringLengthOpt = DescriptorConfig::tryGet(STRING_LENGTH, config);
            if (not stringLengthOpt)
            {
                NES_ERROR("String length not set for NativeInputFormatIndexer");
                return std::nullopt;
            }
            // if (stringLengthOpt.value() > 255)
            // {
            //     NES_ERROR("String length {} is too large. Max is 255", stringLengthOpt.value());
            //     return std::nullopt;
            // }
            return stringLengthOpt;
        }};

    static inline const std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(InputFormatterDescriptor::parameterMap, STRING_LENGTH);
};

/// Metadata for the native input format. Captures the row layout of the schema (per-field offsets and the fixed
/// per-tuple stride) and exposes the IndexerMetaDataType interface required by the InputFormatter.
struct NativeMetaData
{
    /// Can't store tupleBufferRef as member, since it's a ref to an object of an abstract class
    // Todo: handle varsized
    // - get varsized field sizes from input formatter descriptor (maybe just one per entire schema?
    // - change tupleSize and capacity
    // - adapt fields
    // - adapt readFromRecordWithOffset
    NativeMetaData(const InputFormatterDescriptor& config, const TupleBufferRef& tupleBufferRef)
    {
        const std::string stringLengths = config.getFromConfig(ConfigParametersNative::STRING_LENGTH);
        this->maxStringLengths = splitWithStringDelimiter<std::string>(stringLengths, ",")
            | std::views::transform([](const std::string_view value) { return from_chars_with_exception<uint32_t>(value); })
            | std::ranges::to<std::vector>();

        size_t runningOffset = 0;
        size_t varsizedCounter = 0;
        std::vector<RowTupleBufferRef::Field> fields;
        for (const auto& [fieldName, fieldDataType] : std::views::zip(tupleBufferRef.getAllFieldNames(), tupleBufferRef.getAllDataTypes()))
        {
            if (fieldDataType.type == DataType::Type::VARSIZED)
            {
                fields.emplace_back(fieldName, fieldDataType, runningOffset);
                INVARIANT(
                    maxStringLengths.size() > varsizedCounter,
                    "counted more varsized values {} than provided string lengths {}",
                    varsizedCounter,
                    maxStringLengths.size());
                runningOffset += maxStringLengths.at(varsizedCounter);
                ++varsizedCounter;
            }
            else
            {
                fields.emplace_back(fieldName, fieldDataType, runningOffset);
                runningOffset += fieldDataType.getSizeInBytesWithNull();
            }
        }
        INVARIANT(runningOffset < tupleBufferRef.getBufferSize(), "Tuple must not be larger than buffer, but was {} bytes", runningOffset);
        this->bufferRef.tupleSize = runningOffset;
        this->bufferRef.bufferSize = tupleBufferRef.getBufferSize();
        this->bufferRef.capacity = tupleBufferRef.getBufferSize() / runningOffset;
        this->bufferRef.fields = std::move(fields);
    }

    [[nodiscard]] Record readRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const nautilus::val<int8_t*>& bufferAddress,
        const nautilus::val<uint64_t>& recordIndex,
        const nautilus::val<FieldIndex>& offset) const
    {
        return bufferRef.readRecordWithOffset(projections, bufferAddress, recordIndex, offset, maxStringLengths);
    }

    [[nodiscard]] static std::string_view getTupleDelimitingBytes() { return {}; }

    [[nodiscard]] static QuotationType getQuotationType() { return QuotationType::NONE; }

    [[nodiscard]] const Record::RecordFieldIdentifier& getFieldNameAt(const nautilus::static_val<uint64_t>& i) const
    {
        return fieldNames[i];
    }

    [[nodiscard]] const DataType& getFieldDataTypeAt(const nautilus::static_val<uint64_t>& i) const { return bufferRef.fields.at(i).type; }

    [[nodiscard]] uint64_t getNumberOfFields() const { return fieldNames.size(); }

    [[nodiscard]] static const std::vector<std::string>& getNullValues()
    {
        INVARIANT(false, "getNullValues is not used by the native input format (fields are read directly from binary).");
        std::unreachable();
    }

    [[nodiscard]] uint64_t getTupleSize() const { return bufferRef.tupleSize; }

    [[nodiscard]] uint64_t getBufferSize() const { return bufferRef.bufferSize; }

private:
    std::vector<uint32_t> maxStringLengths{};
    std::vector<Record::RecordFieldIdentifier> fieldNames;
    RowTupleBufferRef bufferRef{};
};

/// Indexer for the native input format. Buffers contain data already laid out in the internal binary tuple layout, so
/// no per-field parsing is required. Tuples may span buffer boundaries; the indexer derives leading/trailing partial
/// boundaries from the global byte offset of the buffer (sequence number * fixed buffer size) and the fixed tuple size,
/// and reports them via offsetOfFirstTuple/offsetOfLastTuple so the SequenceShredder can stitch spanning tuples.
class NativeInputFormatIndexer final : public InputFormatIndexer<NativeInputFormatIndexer>
{
public:
    static constexpr std::string_view NAME = "SystemNative";
    static constexpr bool IsSequential = false;

    using IndexerMetaData = NativeMetaData;
    using FieldIndexFunctionType = NativeFIF;

    NativeInputFormatIndexer() = default;
    ~NativeInputFormatIndexer() = default;

    void indexRawBuffer(NativeFIF& fieldIndexFunction, const RawTupleBuffer& rawBuffer, const NativeMetaData& metaData) const;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& os, const NativeInputFormatIndexer&);
};

}
