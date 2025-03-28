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

#include <memory>
#include <string>
#include <vector>
#include <Iterators/BFSIterator.hpp>
#include <Plans/QueryPlan.hpp>
#include <ErrorHandling.hpp>
#include <RewriteRuleRegistry.hpp>
#include <Operators/LogicalOperator.hpp>
#include <PhysicalPlan.hpp>

namespace NES::Optimizer::LowerToPhysicalOperators
{

std::unique_ptr<PhysicalPlan> apply(QueryPlan queryPlan)
{
    /// default query optimizer configuration
    NES::Configurations::QueryOptimizerConfiguration conf;
    const auto registryArgument = RewriteRuleRegistryArguments{conf};

    std::vector<std::unique_ptr<PhysicalOperatorWrapper>> rootOperators;
    for (auto logicalOperator : BFSRange<LogicalOperator>(queryPlan.getRootOperators()[0]))
    {
        /// Currently, we directly map logical operators to rewrite rules.
        /// Once our operator traits are more mature we should consider here to map logical operator + traits to rules
        if (auto rule = RewriteRuleRegistry::instance().create(std::string(logicalOperator.getName()), registryArgument); rule.has_value())
        {
            auto physicalOperatorWrapper = rule.value()->apply(logicalOperator);
        }
        else
        {
            throw UnknownPhysicalOperator("{} cant be resolved into a PhysicalOperator", logicalOperator.getName());
        }
    }
    return std::make_unique<PhysicalPlan>(queryPlan.getQueryId(), std::move(rootOperators));
}
}
