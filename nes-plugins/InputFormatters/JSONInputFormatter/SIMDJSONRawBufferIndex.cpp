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
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
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
    const DataType dataType,
    Record& record,
    const std::string& fieldName,
    const nautilus::val<FieldIndex>& fieldIndex,
    const nautilus::val<RawBufferIndex*>& rawBufferIndex,
    const nautilus::val<const InputFormatIndexer*>& indexer)
{
    switch (dataType.type)
    {
        case DataType::Type::INT8: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<int8_t>(dataType.nullable, fieldIndex, rawBufferIndex, indexer));
            return;
        }
        case DataType::Type::INT16: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<int16_t>(dataType.nullable, fieldIndex, rawBufferIndex, indexer));
            return;
        }
        case DataType::Type::INT32: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<int32_t>(dataType.nullable, fieldIndex, rawBufferIndex, indexer));
            return;
        }
        case DataType::Type::INT64: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<int64_t>(dataType.nullable, fieldIndex, rawBufferIndex, indexer));
            return;
        }
        case DataType::Type::UINT8: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<uint8_t>(dataType.nullable, fieldIndex, rawBufferIndex, indexer));
            return;
        }
        case DataType::Type::UINT16: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<uint16_t>(dataType.nullable, fieldIndex, rawBufferIndex, indexer));
            return;
        }
        case DataType::Type::UINT32: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<uint32_t>(dataType.nullable, fieldIndex, rawBufferIndex, indexer));
            return;
        }
        case DataType::Type::UINT64: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<uint64_t>(dataType.nullable, fieldIndex, rawBufferIndex, indexer));
            return;
        }
        case DataType::Type::FLOAT32: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<float>(dataType.nullable, fieldIndex, rawBufferIndex, indexer));
            return;
        }
        case DataType::Type::FLOAT64: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<double>(dataType.nullable, fieldIndex, rawBufferIndex, indexer));
            return;
        }
        case DataType::Type::CHAR: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<char>(dataType.nullable, fieldIndex, rawBufferIndex, indexer));
            return;
        }
        case DataType::Type::BOOLEAN: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<bool>(dataType.nullable, fieldIndex, rawBufferIndex, indexer));
            return;
        }
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
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const nautilus::val<int8_t*>&,
    const nautilus::val<uint64_t>&,
    const InputFormatIndexer& indexer,
    nautilus::val<RawBufferIndex*> rawBufferIndex,
    const TupleBufferRef& bufferRef) const
{
    Record record;
    const auto numberOfFields = nautilus::static_val{bufferRef.getAllDataTypes().size()};
    for (nautilus::static_val<uint64_t> i = 0; i < numberOfFields; ++i)
    {
        const auto fieldName = bufferRef.getAllFieldNames().at(i);

        if (std::ranges::find(projections, fieldName) == projections.end())
        {
            continue;
        }

        auto fieldIndex = static_cast<nautilus::val<FieldIndex>>(i);
        const auto fieldDataType = bufferRef.getAllDataTypes().at(i);
        writeValueToRecord(
            fieldDataType, record, fieldName, fieldIndex, rawBufferIndex, nautilus::val<const InputFormatIndexer*>(&indexer));
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
