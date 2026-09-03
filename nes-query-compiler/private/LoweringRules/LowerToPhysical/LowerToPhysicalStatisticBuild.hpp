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
#include <Operators/LogicalOperator.hpp>
#include <QueryExecutionConfiguration.hpp>

namespace NES
{

/// Lowers a StatisticBuildLogicalOperator into the same build/probe physical operator pair as a windowed
/// aggregation, with a single directly-constructed ReservoirSamplePhysicalFunction as the aggregation function.
struct LowerToPhysicalStatisticBuild : AbstractLoweringRule
{
    explicit LowerToPhysicalStatisticBuild(QueryExecutionConfiguration conf) : conf(std::move(conf)) { }

    LoweringRuleResultSubgraph apply(LogicalOperator logicalOperator) override;

private:
    QueryExecutionConfiguration conf;
};

}
