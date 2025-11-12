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

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <simdjson.h>
#include <DataTypes/DataType.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Ranges.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <FieldIndexFunction.hpp>
#include <InputFormatter.hpp>
#include <RawValueParser.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{


struct SIMDJSONMetaData
{
    explicit SIMDJSONMetaData(const ParserConfig& config, const MemoryLayout& memoryLayout)
        : schema(memoryLayout.getSchema()), tupleDelimiter(config.tupleDelimiter)
    {
        for (const auto& [fieldIdx, field] : schema | NES::views::enumerate)
        {
            if (const auto& qualifierPosition = field.name.find(Schema::ATTRIBUTE_NAME_SEPARATOR); qualifierPosition != std::string::npos)
            {
                indexToFieldName.emplace_back(field.name.substr(qualifierPosition + 1));
            }
            else
            {
                indexToFieldName.emplace_back(field.name);
            }
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

    /// Fieldstatic IndexFunction (CRTP) interface functions
    [[nodiscard]] FieldIndex applyGetByteOffsetOfFirstTuple() const { return this->offsetOfFirstTuple; }

    [[nodiscard]] FieldIndex applyGetByteOffsetOfLastTuple() const { return this->offsetOfLastTuple; }

    /// SIMDJSON can only determine the correct number of tuples after parsing the entire buffer
    [[nodiscard]] static size_t applyGetTotalNumberOfTuples() { return 0; }

    [[nodiscard]] static nautilus::val<bool>
    applyHasNext(const nautilus::val<uint64_t>&, const nautilus::val<SIMDJSONFIF*>& fieldIndexFunction);

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
                    const std::string_view valueSV = currentDoc[fieldName];
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

    static VariableSizedData parseStringIntoNautilusRecord(
        const nautilus::val<FieldIndex>& fieldIdx,
        const nautilus::val<SIMDJSONFIF*>& fieldIndexFunction,
        const nautilus::val<SIMDJSONMetaData*>& metaData,
        const ArenaRef& arenaRef);

    void writeValueToRecord(
        DataType::Type physicalType,
        Record& record,
        const std::string& fieldName,
        const nautilus::val<FieldIndex>& fieldIdx,
        const nautilus::val<SIMDJSONFIF*>& fieldIndexFunction,
        const nautilus::val<const SIMDJSONMetaData*>& metaData,
        ArenaRef& arenaRef) const;

    template <typename IndexerMetaData>
    [[nodiscard]] Record applyReadSpanningRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const nautilus::val<int8_t*>&,
        const nautilus::val<uint64_t>&,
        const IndexerMetaData& metaData,
        nautilus::val<SIMDJSONFIF*> fieldIndexFunction,
        ArenaRef& arenaRef) const
    {
        Record record;
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
    void markNoTupleDelimiters();

    void markWithTupleDelimiters(FieldIndex offsetToFirstTuple, std::optional<FieldIndex> offsetToLastTuple);

    std::pair<bool, FieldIndex> indexJSON(std::string_view jsonSV);

    std::pair<bool, FieldIndex> indexJSON(std::string_view jsonSV, size_t batchSize);

private:
    bool isAtLastTuple{false};
    size_t numberOfFieldsInSchema{};
    FieldIndex offsetOfFirstTuple{};
    FieldIndex offsetOfLastTuple{};
    std::shared_ptr<simdjson::ondemand::parser> parser;
    std::shared_ptr<simdjson::ondemand::document_stream> docStream;
    simdjson::ondemand::document_stream::iterator docStreamIterator;
};

static_assert(std::is_standard_layout_v<SIMDJSONFIF>, "SIMDJSONFIF must have a standard layout");

}
