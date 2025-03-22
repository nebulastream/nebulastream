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

#include <string>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <Streaming/Aggregation/Function/AggregationFunction.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// Stores members that are needed for both phases of the aggregation, build and probe
class WindowAggregation : public PhysicalOperatorConcept
{
public:
    WindowAggregation(
        std::vector<std::shared_ptr<AggregationFunction>> aggregationFunctions,
        std::shared_ptr<Nautilus::Interface::HashFunction> hashFunction,
        std::vector<Nautilus::Interface::MemoryProvider::FieldOffsets> fieldKeys,
        std::vector<Nautilus::Interface::MemoryProvider::FieldOffsets> fieldValues,
        const uint64_t entriesPerPage,
        const uint64_t entrySize)
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
            "The number of aggregation functions must match the number of field values");
    }

    WindowAggregation(const std::shared_ptr<WindowAggregation>& other) noexcept
        : aggregationFunctions(other->aggregationFunctions)
        , hashFunction(other->hashFunction->clone())
        , fieldKeys(other->fieldKeys)
        , fieldValues(other->fieldValues)
        , entriesPerPage(other->entriesPerPage)
        , entrySize(other->entrySize)
    {
    }

protected:
    /// It is fine that these are not nautilus types, because they are only used in the tracing and not in the actual execution
    std::vector<std::shared_ptr<AggregationFunction>> aggregationFunctions;
    std::shared_ptr<Nautilus::Interface::HashFunction> hashFunction;
    std::vector<Nautilus::Interface::MemoryProvider::FieldOffsets> fieldKeys;
    std::vector<Nautilus::Interface::MemoryProvider::FieldOffsets> fieldValues;
    uint64_t entriesPerPage;
    uint64_t entrySize;
};

}
