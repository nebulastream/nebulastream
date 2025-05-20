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

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <Aggregation/Function/AggregationFunction.hpp>
#include <Aggregation/WindowAggregation.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

WindowAggregation::WindowAggregation(
    std::vector<std::shared_ptr<AggregationFunction>> aggregationFunctions,
    std::unique_ptr<Interface::HashFunction> hashFunction,
    std::vector<FieldOffsets> fieldKeys,
    std::vector<FieldOffsets> fieldValues,
    uint64_t entriesPerPage,
    uint64_t entrySize)
    : aggregationFunctions(std::move(aggregationFunctions))
    , hashFunction(std::move(hashFunction))
    , fieldKeys(std::move(fieldKeys))
    , fieldValues(std::move(fieldValues))
    , entriesPerPage(entriesPerPage)
    , entrySize(entrySize)
{
    INVARIANT(entriesPerPage > 0, "The number of entries per page must be greater than 0");
    INVARIANT(entrySize > 0, "The entry size must be greater than 0");
    PRECONDITION(
        this->aggregationFunctions.size() == this->fieldValues.size(),
        "The number of aggregation functions (here: {}) must match the number of field values (here: {})",
        this->aggregationFunctions.size(),
        this->fieldValues.size());
}

WindowAggregation::WindowAggregation(const WindowAggregation& other)
    : aggregationFunctions(other.aggregationFunctions)
    , hashFunction(other.hashFunction ? other.hashFunction->clone() : nullptr)
    , fieldKeys(other.fieldKeys)
    , fieldValues(other.fieldValues)
    , entriesPerPage(other.entriesPerPage)
    , entrySize(other.entrySize)
{
}

WindowAggregation::WindowAggregation(const std::shared_ptr<WindowAggregation>& other)
    : aggregationFunctions(other->aggregationFunctions)
    , hashFunction(std::move(other->hashFunction))
    , fieldKeys(other->fieldKeys)
    , fieldValues(other->fieldValues)
    , entriesPerPage(other->entriesPerPage)
    , entrySize(other->entrySize)
{
}

}
