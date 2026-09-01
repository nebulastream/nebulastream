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
#include <utility>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <LoweringRuleRegistry.hpp>
#include <QueryExecutionConfiguration.hpp>

namespace NES
{

/// Lowers a StatisticStoreWriterLogicalOperator into a single StatisticStoreWriterPhysicalOperator that writes
/// statistic blobs into the worker's statistic store.
struct LowerToPhysicalStatisticStoreWriter : AbstractLoweringRule
{
    explicit LowerToPhysicalStatisticStoreWriter(LoweringRuleRegistryArguments arguments)
        : conf(std::move(arguments.conf)), statisticStore(std::move(arguments.statisticStore))
    {
    }

    LoweringRuleResultSubgraph apply(LogicalOperator logicalOperator) override;

private:
    QueryExecutionConfiguration conf;
    std::shared_ptr<AbstractStatisticStore> statisticStore;
};

}
