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
#include <QueryExecutionConfiguration.hpp>

namespace NES
{

class AbstractStatisticStore;

/// Lowers a StatisticStoreWriterLogicalOperator to the physical StatisticStoreWriter, wiring it to a
/// StatisticStoreOperatorHandler that wraps the shared store injected via the LoweringRuleRegistryArguments
/// (constructor DI from the QueryCompiler).
struct LowerToPhysicalStatisticStoreWriter : AbstractLoweringRule
{
    LowerToPhysicalStatisticStoreWriter(QueryExecutionConfiguration conf, std::shared_ptr<AbstractStatisticStore> statisticStore)
        : conf(std::move(conf)), statisticStore(std::move(statisticStore))
    {
    }

    LoweringRuleResultSubgraph apply(LogicalOperator logicalOperator) override;

private:
    QueryExecutionConfiguration conf;
    std::shared_ptr<AbstractStatisticStore> statisticStore;
};

}
