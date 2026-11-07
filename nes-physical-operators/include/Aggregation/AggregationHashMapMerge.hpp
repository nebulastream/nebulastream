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

#include <memory>
#include <vector>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// Allocates an empty chained hash map with the sizing of hashMapConfig in an unpooled buffer and pins it in hashMapBuffer.
/// The sizing is a query-compile-time constant, so it comes from hashMapConfig rather than from one of the maps the new map merges.
void createAggregationHashMap(ExecutionContext& executionCtx, const ChainedHashMapConfig& hashMapConfig, OwnedNautilusBuffer& hashMapBuffer);

/// Combines the aggregation states of every key in the hash map source into the hash map target. A key that target does not have
/// yet is inserted with reset states first. Both maps use hashMapConfig and hold the states of aggregationFunctions in this order.
void combineAggregationHashMap(
    ExecutionContext& executionCtx,
    const ChainedHashMapConfig& hashMapConfig,
    const std::vector<std::shared_ptr<AggregationPhysicalFunction>>& aggregationFunctions,
    const BorrowedNautilusBuffer& target,
    const BorrowedNautilusBuffer& source);

}
