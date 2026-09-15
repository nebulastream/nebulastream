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

#include <unordered_map>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>

namespace NES
{

OptimizedPlan QueryOptimizer::optimize(LogicalPlan plan) const
{
    auto globalPlan = ruleBasedOptimization.optimize(std::move(plan));
    auto localPlans = operatorPlacement.place(globalPlan);
    return OptimizedPlan{.globalPlan = std::move(globalPlan), .localPlans = std::move(localPlans)};
}

}
