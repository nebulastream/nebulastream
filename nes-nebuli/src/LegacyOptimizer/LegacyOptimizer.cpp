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

#include <LegacyOptimizer/LegacyOptimizer.hpp>

#include <functional>
#include <numeric>
#include <utility>
#include <vector>

#include <LegacyOptimizer/Phases/LogicalSourceExpansionPhase.hpp>
#include <LegacyOptimizer/Phases/OriginIdInferencePhase.hpp>
#include <LegacyOptimizer/Phases/RedundantProjectionRemovalPhase.hpp>
#include <LegacyOptimizer/Phases/RedundantUnionRemovalPhase.hpp>
#include <LegacyOptimizer/Phases/SinkBindingPhase.hpp>
#include <LegacyOptimizer/Phases/SourceInferencePhase.hpp>
#include <LegacyOptimizer/Phases/TypeInferencePhase.hpp>
#include <LegacyOptimizer/QueryPlanning.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>

namespace NES
{

///NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved) Future changes might actually require the move.
PlanStage::OptimizedLogicalPlan LegacyOptimizer::optimize(PlanStage::BoundLogicalPlan&& inputPlan) &&
{
    using Transform = std::function<LogicalPlan(LogicalPlan)>;

    std::vector<Transform> phases
        = {[&](const LogicalPlan& plan) { return SinkBindingPhase::apply(plan, copyPtr(ctx.sinkCatalog)); },
           [&](const LogicalPlan& plan) { return SourceInferencePhase::apply(plan, copyPtr(ctx.sourceCatalog)); },
           [&](const LogicalPlan& plan) { return LogicalSourceExpansionPhase::apply(plan, copyPtr(ctx.sourceCatalog)); },
           [](const LogicalPlan& plan) { return RedundantUnionRemovalPhase::apply(plan); },
           [](const LogicalPlan& plan) { return TypeInferencePhase::apply(plan); },
           [](const LogicalPlan& plan) { return RedundantProjectionRemovalPhase::apply(plan); },
           [](const LogicalPlan& plan) { return OriginIdInferencePhase::apply(plan); },
           [](const LogicalPlan& plan) { return TypeInferencePhase::apply(plan); }};

    LogicalPlan optimizedPlan = std::accumulate(
        phases.begin(),
        phases.end(),
        std::move(inputPlan.plan),
        [](LogicalPlan plan, const Transform& phase) { return phase(std::move(plan)); });

    NES_DEBUG("Plan after optimization passes: {}", optimizedPlan);
    return PlanStage::OptimizedLogicalPlan{std::move(optimizedPlan)};
}
}
