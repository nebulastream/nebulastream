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
#include <vector>
#include <Aggregation/Function/AggregationFunction.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>

namespace NES
{
using namespace Interface::MemoryProvider;

/// Stores members that are needed for both phases of the aggregation, build and probe
class WindowAggregation
{
public:
    WindowAggregation(
        std::vector<std::shared_ptr<AggregationFunction>> aggregationFunctions,
        std::unique_ptr<Interface::HashFunction> hashFunction,
        std::vector<FieldOffsets> fieldKeys,
        std::vector<FieldOffsets> fieldValues,
        uint64_t entriesPerPage,
        uint64_t entrySize);
    WindowAggregation(const WindowAggregation& other);
    explicit WindowAggregation(const std::shared_ptr<WindowAggregation>& other);

protected:
    /// It is fine that these are not nautilus types, because they are only used in the tracing and not in the actual execution
    /// The aggregation function is a shared_ptr, because it is used in the aggregation build and in the getStateCleanupFunction()
    std::vector<std::shared_ptr<AggregationFunction>> aggregationFunctions;
    std::unique_ptr<Interface::HashFunction> hashFunction;
    std::vector<FieldOffsets> fieldKeys;
    std::vector<FieldOffsets> fieldValues;
    uint64_t entriesPerPage;
    uint64_t entrySize;
};

}
