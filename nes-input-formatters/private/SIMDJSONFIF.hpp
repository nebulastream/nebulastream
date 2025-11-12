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
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include <simdjson.h>
#include <Nautilus/Interface/Record.hpp>
#include <Concepts.hpp>
#include <ErrorHandling.hpp>
#include <FieldIndexFunction.hpp>
#include <InputFormatter.hpp>

namespace NES
{


struct SIMDJSONMetaData
{
    explicit SIMDJSONMetaData(const ParserConfig& config, const MemoryLayout& memoryLayout)
        : schema(memoryLayout.getSchema()), tupleDelimiter(config.tupleDelimiter)
    {
        for (const auto& field : schema)
        {
            indexToFieldName.emplace_back(field.name);
        }
    };

    const Schema& getSchema() const { return this->schema; }

    std::string_view getTupleDelimitingBytes() const { return this->tupleDelimiter; }

    static QuotationType getQuotationType() { return QuotationType::DOUBLE_QUOTE; }

    const std::vector<std::string>& getIndexToFieldName() const { return this->indexToFieldName; }

private:
    Schema schema;
    std::string tupleDelimiter;
    std::vector<std::string> indexToFieldName;
};

class SIMDJSONFIF final : public FieldIndexFunction<SIMDJSONFIF>
{
    friend FieldIndexFunction<SIMDJSONFIF>;

    /// FieldIndexFunction (CRTP) interface functions
    [[nodiscard]] FieldIndex applyGetByteOffsetOfFirstTuple() const { return this->offsetOfFirstTuple; }

    [[nodiscard]] FieldIndex applyGetByteOffsetOfLastTuple() const { return this->offsetOfLastTuple; }

    /// SIMDJSON can only determine the correct number of tuples after parsing the entire buffer
    [[nodiscard]] size_t applyGetTotalNumberOfTuples() const { return 0; }

    [[nodiscard]] nautilus::val<bool> applyHasNext(const nautilus::val<uint64_t>&, nautilus::val<SIMDJSONFIF*> fieldIndexFunction) const
    {
        nautilus::val<bool> hasNext = *Nautilus::Util::getMemberWithOffset<bool>(fieldIndexFunction, offsetof(SIMDJSONFIF, isAtLastTuple));
        return hasNext;
    }

    template <typename T>
    nautilus::val<T> parseNonStringValueIntoNautilusRecord(
        nautilus::val<FieldIndex> fieldIdx,
        nautilus::val<SIMDJSONFIF*> fieldIndexFunction,
        nautilus::val<const SIMDJSONMetaData*> metaData) const
    {
        return nautilus::invoke(
            +[](FieldIndex fieldIndex, SIMDJSONFIF* fieldIndexFunction, const SIMDJSONMetaData* metaData)
            {
                INVARIANT(
                    fieldIndex < metaData->getIndexToFieldName().size(),
                    "fieldIndex {} is out or bounds for schema keys of size: {}",
                    fieldIndex,
                    metaData->getIndexToFieldName().size());
                const auto& fieldName = metaData->getIndexToFieldName()[fieldIndex];
                auto currentDoc = *fieldIndexFunction->docStreamIterator;
                /// Order is important, since signed_integral<char> is true and unsigned_integral<bool> is true
                if constexpr (std::same_as<T, bool>)
                {
                    const T value = currentDoc[fieldName].get_bool();
                    return value;
                }
                else if constexpr (std::same_as<T, char>)
                {
                    std::string_view valueSV = currentDoc[fieldName];
                    PRECONDITION(valueSV.size() == 1, "Cannot take {} as character, because size is not 1", valueSV);
                    const T value = currentDoc[fieldName].get_string()->front();
                    return value;
                }
                else if constexpr (std::signed_integral<T>)
                {
                    const auto value = static_cast<T>(currentDoc[fieldName].get_int64());
                    return value;
                }
                else if constexpr (std::unsigned_integral<T>)
                {
                    const auto value = static_cast<T>(currentDoc[fieldName].get_uint64());
                    return value;
                }
                else if constexpr (std::is_same_v<T, double>)
                {
                    const auto value = static_cast<T>(currentDoc[fieldName].get_double());
                    return value;
                }
                else if constexpr (std::is_same_v<T, float>)
                {
                    const auto value = static_cast<T>(currentDoc[fieldName].get_double());
                    return value;
                }
            },
            fieldIdx,
            fieldIndexFunction,
            metaData);
    }

    VariableSizedData parseStringIntoNautilusRecord(
        nautilus::val<FieldIndex> fieldIdx,
        nautilus::val<SIMDJSONFIF*> fieldIndexFunction,
        nautilus::val<SIMDJSONMetaData*> metaData,
        ArenaRef arenaRef) const
    {
        nautilus::val<int8_t*> varSizedPointer = nautilus::invoke(
            +[](FieldIndex fieldIndex, SIMDJSONFIF* fieldIndexFunction, SIMDJSONMetaData* metaData, Arena* arena)
            {
                INVARIANT(
                    fieldIndex < metaData->getIndexToFieldName().size(),
                    "fieldIndex {} is out or bounds for schema keys of size: {}",
                    fieldIndex,
                    metaData->getIndexToFieldName().size());
                auto currentDoc = *fieldIndexFunction->docStreamIterator;
                const auto& fieldName = metaData->getIndexToFieldName()[fieldIndex];

                /// Get the value from the document and convert it to a span of bytes
                std::string_view value = currentDoc[fieldName];
                const uint32_t sizeOfValue = static_cast<uint32_t>(value.size());
                auto valueBytes = std::as_bytes(std::span(value));

                /// Get memory from arena that holds length of the string and the bytes of the string
                auto arenaPointer = arena->allocateMemory(sizeOfValue + sizeof(uint32_t));
                std::memcpy(arenaPointer.data(), &sizeOfValue, sizeof(uint32_t));
                std::ranges::copy(valueBytes, arenaPointer.subspan(sizeof(uint32_t)).begin());
                return arenaPointer.data();
            },
            fieldIdx,
            fieldIndexFunction,
            metaData,
            arenaRef.getArena());
        VariableSizedData varSizedString{varSizedPointer};
        return varSizedString;
    }

    void writeValueToRecord(
        const DataType::Type physicalType,
        Record& record,
        const std::string& fieldName,
        nautilus::val<FieldIndex> fieldIdx,
        nautilus::val<SIMDJSONFIF*> fieldIndexFunction,
        nautilus::val<const SIMDJSONMetaData*> metaData,
        ArenaRef& arenaRef) const
    {
        switch (physicalType)
        {
            case DataType::Type::INT8: {
                record.write(fieldName, parseNonStringValueIntoNautilusRecord<int8_t>(fieldIdx, fieldIndexFunction, metaData));
                return;
            }
            case DataType::Type::INT16: {
                record.write(fieldName, parseNonStringValueIntoNautilusRecord<int16_t>(fieldIdx, fieldIndexFunction, metaData));
                return;
            }
            case DataType::Type::INT32: {
                record.write(fieldName, parseNonStringValueIntoNautilusRecord<int32_t>(fieldIdx, fieldIndexFunction, metaData));
                return;
            }
            case DataType::Type::INT64: {
                record.write(fieldName, parseNonStringValueIntoNautilusRecord<int64_t>(fieldIdx, fieldIndexFunction, metaData));
                return;
            }
            case DataType::Type::UINT8: {
                record.write(fieldName, parseNonStringValueIntoNautilusRecord<uint8_t>(fieldIdx, fieldIndexFunction, metaData));
                return;
            }
            case DataType::Type::UINT16: {
                record.write(fieldName, parseNonStringValueIntoNautilusRecord<uint16_t>(fieldIdx, fieldIndexFunction, metaData));
                return;
            }
            case DataType::Type::UINT32: {
                record.write(fieldName, parseNonStringValueIntoNautilusRecord<uint32_t>(fieldIdx, fieldIndexFunction, metaData));
                return;
            }
            case DataType::Type::UINT64: {
                record.write(fieldName, parseNonStringValueIntoNautilusRecord<uint64_t>(fieldIdx, fieldIndexFunction, metaData));
                return;
            }
            case DataType::Type::FLOAT32: {
                record.write(fieldName, parseNonStringValueIntoNautilusRecord<float>(fieldIdx, fieldIndexFunction, metaData));
                return;
            }
            case DataType::Type::FLOAT64: {
                record.write(fieldName, parseNonStringValueIntoNautilusRecord<double>(fieldIdx, fieldIndexFunction, metaData));
                return;
            }
            case DataType::Type::CHAR: {
                record.write(fieldName, parseNonStringValueIntoNautilusRecord<char>(fieldIdx, fieldIndexFunction, metaData));
                return;
            }
            case DataType::Type::BOOLEAN: {
                record.write(fieldName, parseNonStringValueIntoNautilusRecord<bool>(fieldIdx, fieldIndexFunction, metaData));
                return;
            }
            case DataType::Type::VARSIZED: {
                record.write(fieldName, parseStringIntoNautilusRecord(fieldIdx, fieldIndexFunction, metaData, arenaRef));
                return;
            }
            case DataType::Type::VARSIZED_POINTER_REP:
                throw NotImplemented("Cannot parse varsized pointer rep type.");
            case DataType::Type::UNDEFINED:
                throw NotImplemented("Cannot parse undefined type.");
        }
        std::unreachable();
    }

    template <typename IndexerMetaData>
    [[nodiscard]] Record applyReadSpanningRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const nautilus::val<int8_t*>&,
        const nautilus::val<uint64_t>&,
        const IndexerMetaData& metaData,
        nautilus::val<SIMDJSONFIF*> fieldIndexFunction,
        ArenaRef& arenaRef) const
    {
        Nautilus::Record record;
        for (nautilus::static_val<FieldIndex> i = 0; i < static_cast<FieldIndex>(metaData.getSchema().getNumberOfFields()); ++i)
        {
            const auto& field = metaData.getSchema().getFieldAt(i);
            if (std::ranges::find(projections, field.name) == projections.end())
            {
                continue;
            }

            auto fieldIdx = static_cast<nautilus::val<FieldIndex>>(i);

            writeValueToRecord(
                field.dataType.type,
                record,
                field.name,
                fieldIdx,
                fieldIndexFunction,
                nautilus::val<const IndexerMetaData*>(&metaData),
                arenaRef);
        }
        /// Increment iterator and return record
        nautilus::invoke(
            +[](SIMDJSONFIF* simdJSONFIF)
            {
                ++simdJSONFIF->docStreamIterator;
                simdJSONFIF->isAtLastTuple = simdJSONFIF->docStreamIterator.at_end();
            },
            fieldIndexFunction);
        return record;
    }


public:
    SIMDJSONFIF() = default;
    ~SIMDJSONFIF() = default;

    /// Resets the indexes and pointers, calculates and sets the number of tuples in the current buffer, returns the total number of tuples.
    void markNoTupleDelimiters()
    {
        this->offsetOfFirstTuple = std::numeric_limits<FieldIndex>::max();
        this->offsetOfLastTuple = std::numeric_limits<FieldIndex>::max();
    }

    void markWithTupleDelimiters(const FieldIndex offsetToFirstTuple, const std::optional<FieldIndex> offsetToLastTuple)
    {
        this->offsetOfFirstTuple = offsetToFirstTuple;
        this->offsetOfLastTuple = offsetToLastTuple.value_or(std::numeric_limits<FieldIndex>::max());
    }

    std::pair<bool, FieldIndex> indexJSON(const std::string_view jsonSV)
    {
        const simdjson::padded_string_view paddedJSONSV{jsonSV.data(), jsonSV.size(), jsonSV.size() + simdjson::SIMDJSON_PADDING};
        this->parser = std::make_shared<simdjson::ondemand::parser>();
        docStream = std::make_shared<simdjson::ondemand::document_stream>(parser->iterate_many(paddedJSONSV));
        docStreamIterator = docStream->begin();
        return {docStreamIterator.at_end(), docStream->truncated_bytes()};
    }

private:
    bool isAtLastTuple{};
    size_t numberOfFieldsInSchema{};
    FieldIndex offsetOfFirstTuple{};
    FieldIndex offsetOfLastTuple{};
    std::shared_ptr<simdjson::ondemand::parser> parser{};
    std::shared_ptr<simdjson::ondemand::document_stream> docStream{};
    simdjson::ondemand::document_stream::iterator docStreamIterator{};
};

static_assert(std::is_standard_layout_v<SIMDJSONFIF>, "SIMDJSONFIF must have a standard layout");

}
