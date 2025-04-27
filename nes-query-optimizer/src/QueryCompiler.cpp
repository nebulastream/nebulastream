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
#include <Phases/LowerToPhysicalOperators.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryOptimizer.hpp>

namespace NES::Optimizer
{
PhysicalPlan QueryOptimizer::optimize(LogicalPlan plan)
{
    /// In the future, we will have a real rule matching engine / rule driver for our optimizer.
    /// For now, we just lower to physical operators in a pure function.
    auto physicalPlan = LowerToPhysicalOperators::apply(plan,  conf);
    auto flippedPlan = flip(physicalPlan);
    return flippedPlan;
}

}
