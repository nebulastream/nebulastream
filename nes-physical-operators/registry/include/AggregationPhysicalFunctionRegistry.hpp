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

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

using AggregationPhysicalFunctionRegistryReturnType = std::shared_ptr<AggregationPhysicalFunction>;

struct AggregationPhysicalFunctionRegistryArguments
{
    DataType inputType;
    DataType resultType;
    PhysicalFunction inputFunction;
    Record::RecordFieldIdentifier resultFieldIdentifier;
    std::optional<std::shared_ptr<PagedVectorTupleLayout>> tupleLayout;
    bool includeNullValues;
    /// Number of buckets of the hash set that COUNT(DISTINCT x) keeps per key (count_distinct_number_of_buckets).
    uint64_t countDistinctNumberOfBuckets = 64;
    /// Page size of hash maps that an aggregation function keeps in its state.
    uint64_t pageSize = 4096;
};

using AggregationPhysicalFunctionFn
    = std::function<AggregationPhysicalFunctionRegistryReturnType(AggregationPhysicalFunctionRegistryArguments)>;

/// Entries are static create members on the aggregation function classes (constructor
/// signatures differ between aggregations).
class AggregationPhysicalFunctionRegistry : public RuntimeRegistry<
                                                AggregationPhysicalFunctionRegistry,
                                                std::string,
                                                AggregationPhysicalFunctionFn,
                                                /*CaseSensitive*/ false>
{
public:
    static AggregationPhysicalFunctionRegistry& instance();
};

}
