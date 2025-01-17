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

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <Execution/Operators/Streaming/Aggregation/Function/AggregationFunction.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>


namespace NES::Runtime::Execution::Operators
{

/// Stores members that are needed for both phases of the aggregation, build and probe
class WindowAggregationOperator
{
public:
    WindowAggregationOperator(
        std::vector<std::unique_ptr<Aggregation::AggregationFunction>> aggregationFunctions,
        std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction,
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
    }

protected:
    /// It is fine that these are not nautilus types, because they are only used in the tracing and not in the actual execution
    const std::vector<std::unique_ptr<Aggregation::AggregationFunction>> aggregationFunctions;
    const std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction;
    const std::vector<Nautilus::Interface::MemoryProvider::FieldOffsets> fieldKeys;
    const std::vector<Nautilus::Interface::MemoryProvider::FieldOffsets> fieldValues;
    const uint64_t entriesPerPage;
    const uint64_t entrySize;
};

}
