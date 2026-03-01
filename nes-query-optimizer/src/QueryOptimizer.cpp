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

#include <QueryOptimizer.hpp>
#include <QueryOptimizerConfiguration.hpp>

#include <Phases/DecideJoinTypes.hpp>
#include <Phases/DecideMemoryLayout.hpp>
#include <Plans/LogicalPlan.hpp>

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
    DecideJoinTypes joinTypeDecider(defaultQueryOptimization.joinStrategy);
    DecideMemoryLayout memoryLayoutDecider;
    auto optimizedPlan = joinTypeDecider.apply(plan);
    return memoryLayoutDecider.apply(optimizedPlan);
}

}
