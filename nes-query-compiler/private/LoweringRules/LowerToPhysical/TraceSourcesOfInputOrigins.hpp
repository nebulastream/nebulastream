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

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Watermark/WatermarkGapThrottle.hpp>

namespace NES
{

/// Per-compilation immutable watermark-gap analysis. LogicalOperator keys use instance identity, not operator ids or structural equality.
struct WatermarkGapAnalysis
{
    std::unordered_map<LogicalOperator, SourcesOfInputOrigins> sourcesByWindow;
    /// Multi-origin windows sharing sources with an upstream or sibling multi-origin window must not throttle.
    std::unordered_set<LogicalOperator> windowsWithoutThrottling;
};

using WatermarkGapAnalysisPtr = std::shared_ptr<const WatermarkGapAnalysis>;

/// Uses PlanVisitor to compute source provenance and window ancestry once per needed DAG node, then checks siblings using cached sets.
/// The shared result is reused by all lowering rules of this compilation; it must not be reused for a different plan.
WatermarkGapAnalysisPtr analyzeWatermarkGaps(const LogicalPlan& plan);

/// Maps every input origin of a window-based operator with the given children to source origins below it.
/// This is the worker's plan, so a network source counts as a source of its own.
SourcesOfInputOrigins traceSourcesOfInputOrigins(const std::vector<LogicalOperator>& children);

/// Returns windows whose throttle conflicts with an upstream or sibling multi-origin window. Preferring upstream controllers
/// avoids circular source pressure without assuming that nested windows preserve the same watermark domain.
std::unordered_set<LogicalOperator> findWindowsWithConflictingThrottles(const LogicalPlan& plan);

/// Like traceSourcesOfInputOrigins, but clears sources for windows whose throttle must remain disabled.
SourcesOfInputOrigins
traceThrottleableSourcesOfInputOrigins(const LogicalOperator& window, const std::unordered_set<LogicalOperator>& windowsWithoutThrottling);

/// Reuses the immutable per-compilation analysis without retraversing the plan. A null analysis falls back to standalone tracing.
SourcesOfInputOrigins traceThrottleableSourcesOfInputOrigins(const LogicalOperator& window, const WatermarkGapAnalysisPtr& analysis);

}
