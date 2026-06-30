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

#include <optional>
#include <string>
#include <Plans/LogicalPlan.hpp>
#include <DistributedLogicalPlan.hpp>

namespace NES
{

DistributedLogicalPlan QueryOptimizer::optimize(LogicalPlan plan, const std::optional<std::string>& useStatisticSource) const
{
    plan = semanticAnalyzer.analyse(plan);
    plan = ruleBasedOptimization.optimize(plan, useStatisticSource);
    return operatorPlacement.place(plan);
}

}
