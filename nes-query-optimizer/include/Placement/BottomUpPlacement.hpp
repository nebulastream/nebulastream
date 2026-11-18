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

#include <cstddef>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Pointers.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

namespace NES
{

/// Number of operator slots an operator occupies on its worker. Sources and sinks do not occupy a slot.
size_t getOperatorCapacityDemand(const LogicalOperator& op);

/// Placement that traverses the topology from bottom-to-top based on the given capacities of the nodes.
/// This does not change the structure of the plan in any way, it only assigns placement traits with node placements to the given
/// logical operators.
class BottomUpOperatorPlacer final
{
    SharedPtr<const WorkerCatalog> workerCatalog;

public:
    explicit BottomUpOperatorPlacer(SharedPtr<const WorkerCatalog> workerCatalog) : workerCatalog(std::move(workerCatalog)) { }

    void apply(LogicalPlan& logicalPlan);

    /// Places the operators of a placed plan that are not part of `fixedPlacement` (the placement addition of ISQP). The fixed operators
    /// keep their worker and do not occupy additional slots. The other operators are placed on the candidate workers within their
    /// available slots; workers without an entry in `availableSlots` have no free slot. Returns the workers of the placed operators.
    [[nodiscard]] std::unordered_map<OperatorId, Host> placeIncrementally(
        const LogicalPlan& logicalPlan,
        const std::unordered_map<OperatorId, Host>& fixedPlacement,
        const std::unordered_set<Host>& candidateWorkers,
        const std::unordered_map<Host, Capacity>& availableSlots) const;
};
}
