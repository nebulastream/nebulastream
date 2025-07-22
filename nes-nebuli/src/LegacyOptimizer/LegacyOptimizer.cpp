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
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Pointers.hpp>

namespace NES
{

LegacyOptimizer::OptimizedLogicalPlan
LegacyOptimizer::optimize(LogicalPlan inputPlan, SharedPtr<SourceCatalog> sourceCatalog, SharedPtr<SinkCatalog> sinkCatalog)
{
    using Transform = std::function<LogicalPlan(LogicalPlan)>;

    std::vector<Transform> phases
        = {[&](const LogicalPlan& p) { return SinkBindingPhase::apply(p, copyPtr(sinkCatalog)); },
           [&](const LogicalPlan& p) { return SourceInferencePhase::apply(p, copyPtr(sourceCatalog)); },
           [&](const LogicalPlan& p) { return LogicalSourceExpansionPhase::apply(p, copyPtr(sourceCatalog)); },
           [](const LogicalPlan& p) { return RedundantUnionRemovalPhase::apply(p); },
           [](const LogicalPlan& p) { return TypeInferencePhase::apply(p); },
           [](const LogicalPlan& p) { return RedundantProjectionRemovalPhase::apply(p); },
           [](const LogicalPlan& p) { return OriginIdInferencePhase::apply(p); },
           [](const LogicalPlan& p) { return TypeInferencePhase::apply(p); }};

    LogicalPlan optimizedPlan = std::accumulate(
        phases.begin(),
        phases.end(),
        std::move(inputPlan),
        [](LogicalPlan plan, const Transform& phase) { return phase(std::move(plan)); });

    NES_DEBUG("Plan after optimization passes: {}", optimizedPlan);
    return OptimizedLogicalPlan{std::move(optimizedPlan)};
}
}
