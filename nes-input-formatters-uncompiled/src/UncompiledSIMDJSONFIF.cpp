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

#include <UncompiledSIMDJSONFIF.hpp>

#include <cstddef>
#include <limits>
#include <string_view>
#include <utility>

#include <simdjson.h>
#include <ErrorHandling.hpp>

namespace NES
{

simdjson::simdjson_result<simdjson::ondemand::value> accessSIMDJsonFieldOrThrow(
    simdjson::simdjson_result<simdjson::ondemand::document_reference>& simdJsonReference, const std::string_view fieldName)
{
    const auto simdJsonResult = simdJsonReference[fieldName];
    if (not simdJsonResult.has_value())
    {
        throw FieldNotFound(
            "SimdJson has not found the fieldName {} with error: {}", fieldName, magic_enum::enum_name(simdJsonResult.error()));
    }
    return simdJsonResult;
}

bool UncompiledSIMDJSONFIF::applyHasNext(const size_t tupleIdx)
{
    /// Advance the document stream iterator to the requested tuple position
    while (lastProcessedTupleIdx < tupleIdx)
    {
        ++docStreamIterator;
        isAtLastTuple = docStreamIterator.at_end();
        ++lastProcessedTupleIdx;
    }
    return !isAtLastTuple;
}

std::string_view UncompiledSIMDJSONFIF::applyReadFieldAt(const std::string_view, const size_t, const size_t fieldIdx)
{
    const auto& fieldName = metaDataPtr->getFieldNameInJsonAt(fieldIdx);
    auto currentDoc = *this->docStreamIterator;
    auto simdJsonResult = accessSIMDJsonFieldOrThrow(currentDoc, fieldName);
    return simdJsonResult.raw_json().value();
}

void UncompiledSIMDJSONFIF::markNoTupleDelimiters()
{
    this->offsetOfFirstTuple = std::numeric_limits<UncompiledFieldIndex>::max();
    this->offsetOfLastTuple = std::numeric_limits<UncompiledFieldIndex>::max();
}

void UncompiledSIMDJSONFIF::markWithTupleDelimiters(
    const UncompiledFieldIndex offsetToFirstTuple, const UncompiledFieldIndex offsetToLastTuple)
{
    this->offsetOfFirstTuple = offsetToFirstTuple;
    this->offsetOfLastTuple = offsetToLastTuple;
}

std::pair<bool, UncompiledFieldIndex> UncompiledSIMDJSONFIF::indexJSON(
    const std::string_view jsonSV, const UncompiledSIMDJSONMetaData& metaData)
{
    return indexJSON(jsonSV, metaData, simdjson::ondemand::DEFAULT_BATCH_SIZE);
}

std::pair<bool, UncompiledFieldIndex> UncompiledSIMDJSONFIF::indexJSON(
    const std::string_view jsonSV, const UncompiledSIMDJSONMetaData& metaData, const size_t batchSize)
{
    this->metaDataPtr = &metaData;
    this->lastProcessedTupleIdx = 0;

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
