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
#include <Plans/QueryPlan.hpp>
#include <QueryOptimizer.hpp>
#include <Phases/LowerToPhysicalOperators.hpp>

namespace NES::Optimizer
{
/// Takes the query plan as a logical plan and returns a fully physical plan
std::unique_ptr<QueryPlan> QueryOptimizer::optimize(std::unique_ptr<QueryPlan> plan)
{
    /// In the future, we will have a real rule matching engine / rule driver for our optimizer.
    /// For now, we just 'purely' lower to physical operators here.
    return LowerToPhysicalOperators::apply(std::move(plan));
}

}