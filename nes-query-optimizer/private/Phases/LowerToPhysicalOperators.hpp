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
#include <Configurations/Worker/QueryOptimizerConfiguration.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <PhysicalPlan.hpp>
#include <RewriteRuleRegistry.hpp>

namespace NES
{
class LowerToPhysicalOperators
{
public:
    explicit LowerToPhysicalOperators(NES::Configurations::QueryOptimizerConfiguration queryOptimizerConfiguration);
    PhysicalPlan apply(const LogicalPlan& queryPlan);
    RewriteRuleResultSubgraph::SubGraphRoot
    lowerOperatorRecursively(const LogicalOperator& logicalOperator, const RewriteRuleRegistryArguments& registryArgument);


private:
    NES::Configurations::QueryOptimizerConfiguration queryOptimizerConfiguration;
};

}
