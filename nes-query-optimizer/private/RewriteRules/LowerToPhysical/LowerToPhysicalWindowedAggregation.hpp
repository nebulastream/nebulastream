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
#include <cstddef>
#include <memory>
#include <numeric>
#include <Configurations/Worker/QueryOptimizerConfiguration.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/Operator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Streaming/Aggregation/AggregationBuildPhysicalOperator.hpp>
#include <Streaming/Aggregation/AggregationProbePhysicalOperator.hpp>
#include <Streaming/Aggregation/Function/AvgAggregationFunction.hpp>
#include <Streaming/Aggregation/Function/CountAggregationFunction.hpp>
#include <Streaming/Aggregation/Function/MaxAggregationFunction.hpp>
#include <Streaming/Aggregation/Function/MedianAggregationFunction.hpp>
#include <Streaming/Aggregation/Function/MinAggregationFunction.hpp>
#include <Streaming/Aggregation/Function/SumAggregationFunction.hpp>
#include <Streaming/Aggregation/WindowAggregation.hpp>
#include <Traits/QueryForSubtree.hpp>
#include <Traits/TraitSet.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <MapPhysicalOperator.hpp>
#include <magic_enum.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

namespace NES::Optimizer
{

struct LowerToPhysicalWindowedAggregation : AbstractLowerToPhysicalRewriteRule<QueryForSubtree, Operator>
{
    LowerToPhysicalWindowedAggregation(const NES::Configurations::QueryOptimizerConfiguration& conf) : conf(conf) {}
    std::vector<std::unique_ptr<PhysicalOperator>> applyToPhysical(DynamicTraitSet<QueryForSubtree, Operator>*) override;
    const NES::Configurations::QueryOptimizerConfiguration& conf;
};

}
