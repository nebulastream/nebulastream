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

#include <Plans/LogicalPlan.hpp>
#include <QueryOptimizerConfiguration.hpp>

#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <Rules/Static/DecideJoinTypesRule.hpp>
#include <Rules/Static/DecideMemoryLayoutRule.hpp>
#include <Rules/Static/RedundantProjectionRemovalRule.hpp>
#include <Rules/Static/RedundantUnionRemovalRule.hpp>

namespace NES
{


QueryOptimizer::QueryOptimizer(QueryOptimizerConfiguration defaultQueryOptimization):defaultQueryOptimization(std::move(defaultQueryOptimization))
{
    ruleManager.addRule(DecideJoinTypesRule{defaultQueryOptimization.joinStrategy});
    ruleManager.addRule(DecideMemoryLayoutRule{});
    ruleManager.addRule(RedundantUnionRemovalRule{});
    ruleManager.addRule(RedundantProjectionRemovalRule{});
    ruleManager.addRule(TypeInferenceRule{});
}

LogicalPlan QueryOptimizer::optimize(LogicalPlan plan) const
{
    return ruleManager.apply(std::move(plan));
}

}
