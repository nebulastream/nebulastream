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

#include <SIMDJSONRawBufferIndex.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <simdjson.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatIndexer.hpp>
#include <RawBufferIndex.hpp>
#include <RawTupleBuffer.hpp>
#include <SIMDJSONParsingUtil.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>
#include <common/FunctionAttributes.hpp>

namespace NES
{
SIMDJSONRawBufferIndex::SIMDJSONRawBufferIndex()
{
    INVARIANT(
        static_cast<void*>(this) == static_cast<void*>(static_cast<RawBufferIndex*>(this)),
        "RawBufferIndex base subobject must lay out at offset 0 in SIMDJSONRawBufferIndex");
}

[[nodiscard]] nautilus::val<bool>
SIMDJSONRawBufferIndex::hasNext(const nautilus::val<uint64_t>&, const nautilus::val<RawBufferIndex*>& rawBufferIndex) const
{
    const nautilus::val<bool> lastTuple = readValueFromMemRef<bool>(getMemberRef(rawBufferIndex, &SIMDJSONRawBufferIndex::isAtLastTuple));
    return not lastTuple;
}

void writeValueToRecord(
    CompilationContext& compilationContext,
    const DataType dataType,
    Record& record,
    const QualifiedIdentifier& fieldName,
    const nautilus::val<FieldIndex>& fieldIndex,
    const nautilus::val<RawBufferIndex*>& rawBufferIndex,
    const nautilus::val<const InputFormatIndexer*>& indexer)
{
    /// Type dispatch: the lambda binds the shared arguments once, so each case below only selects the C++ type.
    /// Without it, every case would repeat the full parseJsonFixedSizeIntoVarVal call with all its arguments.
    const auto parseFixedSizeField = [&]<typename T>
    {
        record.write(
            fieldName, parseJsonFixedSizeIntoVarVal<T>(compilationContext, dataType.nullable, fieldIndex, rawBufferIndex, indexer));
    };
    switch (dataType.type)
    {
        case DataType::Type::INT8:
            parseFixedSizeField.operator()<int8_t>();
            return;
        case DataType::Type::INT16:
            parseFixedSizeField.operator()<int16_t>();
            return;
        case DataType::Type::INT32:
            parseFixedSizeField.operator()<int32_t>();
            return;
        case DataType::Type::INT64:
            parseFixedSizeField.operator()<int64_t>();
            return;
        case DataType::Type::UINT8:
            parseFixedSizeField.operator()<uint8_t>();
            return;
        case DataType::Type::UINT16:
            parseFixedSizeField.operator()<uint16_t>();
            return;
        case DataType::Type::UINT32:
            parseFixedSizeField.operator()<uint32_t>();
            return;
        case DataType::Type::UINT64:
            parseFixedSizeField.operator()<uint64_t>();
            return;
        case DataType::Type::FLOAT32:
            parseFixedSizeField.operator()<float>();
            return;
        case DataType::Type::FLOAT64:
            parseFixedSizeField.operator()<double>();
            return;
        case DataType::Type::CHAR:
            parseFixedSizeField.operator()<char>();
            return;
        case DataType::Type::BOOLEAN:
            parseFixedSizeField.operator()<bool>();
            return;
        case DataType::Type::VARSIZED: {
            record.write(fieldName, parseJsonVarSized(fieldIndex, rawBufferIndex, indexer, dataType.nullable));
            return;
        }
        case DataType::Type::UNDEFINED:
            throw NotImplemented("Cannot parse undefined type.");
    }
    std::unreachable();
}

Record SIMDJSONRawBufferIndex::readSpanningRecord(
    CompilationContext& compilationContext,
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const nautilus::val<int8_t*>&,
    const nautilus::val<uint64_t>&,
    const InputFormatIndexer& indexer,
    nautilus::val<RawBufferIndex*> rawBufferIndex,
    const TupleBufferRef& bufferRef) const
{
    Record record;
    /// Both getters return by value, so hoist them out of the loop: on a wide schema they would otherwise rebuild
    /// the whole vector once per field while tracing.
    const auto& allFieldNames = bufferRef.getAllFieldNames();
    const auto& allFieldDataTypes = bufferRef.getAllDataTypes();
    const auto numberOfFields = allFieldDataTypes.size();
    for (nautilus::static_val<uint64_t> i = 0; i < numberOfFields; ++i)
    {
        const auto fieldName = allFieldNames.at(i);

        if (std::ranges::find(projections, fieldName) == projections.end())
        {
            continue;
        }

        auto fieldIndex = static_cast<nautilus::val<FieldIndex>>(i);
        const auto fieldDataType = allFieldDataTypes.at(i);
        writeValueToRecord(
            compilationContext,
            fieldDataType,
            record,
            fieldName,
            fieldIndex,
            rawBufferIndex,
            nautilus::val<const InputFormatIndexer*>(&indexer));
    }
    /// Increment iterator and return record
    nautilus::invoke(
        +[](RawBufferIndex* rawBufferIndexPtr)
        {
            PRECONDITION(
                dynamic_cast<SIMDJSONRawBufferIndex*>(rawBufferIndexPtr) != nullptr, "rawBufferIndex must be a SIMDJSONRawBufferIndex");
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast): type verified by PRECONDITION above.
            auto* simdJsonBufferIndex = static_cast<SIMDJSONRawBufferIndex*>(rawBufferIndexPtr);
            ++simdJsonBufferIndex->docStreamIterator;
            simdJsonBufferIndex->isAtLastTuple = simdJsonBufferIndex->docStreamIterator.at_end();
        },
        rawBufferIndex);
    return record;
}

/// Marks the buffer as containing no tuple delimiters by setting both offsets to `max()`.
void SIMDJSONRawBufferIndex::markNoTupleDelimiters()
{
    this->offsetOfFirstTuple = std::numeric_limits<FieldIndex>::max();
    this->offsetOfLastTuple = std::numeric_limits<FieldIndex>::max();
}

void SIMDJSONRawBufferIndex::markWithTupleDelimiters(const FieldIndex offsetToFirstTuple, const std::optional<FieldIndex> offsetToLastTuple)
{
    this->offsetOfFirstTuple = offsetToFirstTuple;
    this->offsetOfLastTuple = offsetToLastTuple.value_or(std::numeric_limits<FieldIndex>::max());
}

std::pair<bool, FieldIndex> SIMDJSONRawBufferIndex::indexJSON(const std::string_view jsonSV)
{
    return indexJSON(jsonSV, simdjson::ondemand::DEFAULT_BATCH_SIZE);
}

std::pair<bool, FieldIndex> SIMDJSONRawBufferIndex::indexJSON(const std::string_view jsonSV, size_t batchSize)
{
    const simdjson::padded_string_view paddedJSONSV{jsonSV.data(), jsonSV.size(), jsonSV.size() + simdjson::SIMDJSON_PADDING};
    this->varSizedValues.clear();
    this->parser = std::make_shared<simdjson::ondemand::parser>();
    this->parser->threaded = false;
    if (jsonSV.size() > batchSize)
    {
        throw CannotFormatSourceData("Size of raw buffer: {} exceeds SIMDJSONs configured batch_size: {}", jsonSV.size(), batchSize);
    }
    docStream = std::make_shared<simdjson::ondemand::document_stream>(parser->iterate_many(paddedJSONSV, batchSize));
    docStreamIterator = docStream->begin();
    isAtLastTuple = docStreamIterator == docStream->end();
    return {docStreamIterator.at_end(), docStream->truncated_bytes()};
}

}
