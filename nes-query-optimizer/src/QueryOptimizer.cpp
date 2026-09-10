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

std::unordered_map<Host, std::vector<LogicalPlan>> QueryOptimizer::optimize(LogicalPlan plan) const
{
    return place(optimizeGlobalPlan(std::move(plan)));
}

LogicalPlan QueryOptimizer::optimizeGlobalPlan(LogicalPlan plan) const
{
    return ruleBasedOptimization.optimize(std::move(plan));
}

std::unordered_map<Host, std::vector<LogicalPlan>> QueryOptimizer::place(LogicalPlan globalPlan) const
{
    return operatorPlacement.place(std::move(globalPlan));
}

}
