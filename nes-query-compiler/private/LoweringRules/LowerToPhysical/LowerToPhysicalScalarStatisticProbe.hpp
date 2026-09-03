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

#include <utility>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <StatisticStore/AbstractStatisticStore.hpp>
#include <LoweringRuleRegistry.hpp>
#include <Operators/LogicalOperator.hpp>
#include <QueryExecutionConfiguration.hpp>

namespace NES
{

/// Lowers a ScalarStatisticProbe to a StatisticStoreReader. This is the only place a reader-side
/// StatisticStoreOperatorHandler is built, and so the only route by which the read path reaches the store.
struct LowerToPhysicalScalarStatisticProbe : AbstractLoweringRule
{
    explicit LowerToPhysicalScalarStatisticProbe(LoweringRuleRegistryArguments arguments)
        : conf(std::move(arguments.conf)), statisticStore(std::move(arguments.statisticStore))
    {
    }

    LoweringRuleResultSubgraph apply(LogicalOperator logicalOperator) override;

private:
    QueryExecutionConfiguration conf;
    std::shared_ptr<AbstractStatisticStore> statisticStore;
};

template <>
inline LoweringRuleFn makeLoweringRule<LowerToPhysicalScalarStatisticProbe>()
{
    return [](LoweringRuleRegistryArguments arguments) -> LoweringRuleRegistryReturnType
    { return std::make_unique<LowerToPhysicalScalarStatisticProbe>(std::move(arguments)); };
}

}
