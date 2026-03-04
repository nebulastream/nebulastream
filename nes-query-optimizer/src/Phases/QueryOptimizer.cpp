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

#include <Phases/QueryOptimizer.hpp>

#include <Plans/LogicalPlan.hpp>
#include <QueryOptimizerConfiguration.hpp>

#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <Rules/Static/DecideJoinTypesRule.hpp>
#include <Rules/Static/DecideMemoryLayoutRule.hpp>
#include <Rules/Static/RedundantProjectionRemovalRule.hpp>
#include <Rules/Static/RedundantUnionRemovalRule.hpp>

namespace NES
{

LogicalPlan QueryOptimizer::optimize(const LogicalPlan& plan) const
{
    return optimize(plan, defaultQueryOptimization);
}

LogicalPlan QueryOptimizer::optimize(const LogicalPlan& plan, const QueryOptimizerConfiguration& defaultQueryOptimization)
{
    /// In the future, we will have a real rule matching engine / rule driver for our optimizer.
    /// For now, we just decide the join type (if one exists in the query), set the memory layout type and lower to physical operators in a pure function.
    DecideJoinTypesRule joinTypeDecider(defaultQueryOptimization.joinStrategy);
    DecideMemoryLayoutRule memoryLayoutDecider;
    constexpr RedundantUnionRemovalRule redundantUnionRemovalRule{};
    constexpr RedundantProjectionRemovalRule  redundantProjectionRemovalRule{};
    constexpr TypeInferenceRule typeInferenceRule{};

    auto optimizedPlan = LogicalPlan{plan};
    redundantProjectionRemovalRule.apply(optimizedPlan);
    redundantUnionRemovalRule.apply(optimizedPlan);
    typeInferenceRule.apply(optimizedPlan);
    optimizedPlan = joinTypeDecider.apply(optimizedPlan);
    optimizedPlan = memoryLayoutDecider.apply(optimizedPlan);

    return optimizedPlan;
}

}
