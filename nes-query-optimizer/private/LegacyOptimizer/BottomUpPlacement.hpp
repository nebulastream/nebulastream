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

#include <utility>
#include <LegacyOptimizer/OperatorPlacement.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Pointers.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{

/// Placement that traverses the topology from bottom-to-top based on the given capacities of the nodes.
/// This does not change the structure of the plan in any way, it only assigns placement traits with node placements to the given
/// logical operators.
class BottomUpOperatorPlacer final : OperatorPlacer
{
    SharedPtr<const WorkerCatalog> workerCatalog;

public:
    explicit BottomUpOperatorPlacer(SharedPtr<const WorkerCatalog> workerCatalog) : workerCatalog(std::move(workerCatalog)) { }

    void apply(LogicalPlan& logicalPlan) override;
};
}
