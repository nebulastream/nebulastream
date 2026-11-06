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

#include <unordered_set>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Watermark/WatermarkGapThrottle.hpp>

namespace NES
{

/// Maps every input origin of a window-based operator with the given children to the origins of the sources in the plan below it.
/// The plan is the one of this worker, so a network source counts as a source of its own.
SourcesOfInputOrigins traceSourcesOfInputOrigins(const std::vector<LogicalOperator>& children);

/// Returns the windows with several input origins that share a source with a sibling window, i.e., one that is neither above nor
/// below them. Below a single window, throttling cannot deadlock, as the sources of its slowest origin are never throttled. Two sibling
/// windows, however, may each throttle a source that the other one waits for.
std::unordered_set<OperatorId> findWindowsSharingSourcesWithSiblings(const LogicalPlan& plan);

/// Like traceSourcesOfInputOrigins for the children of `window`, but maps every input origin to no sources if `window` is one of
/// `windowsWithoutThrottling`, so that its watermark gap throttle stays disabled.
SourcesOfInputOrigins
traceThrottleableSourcesOfInputOrigins(const LogicalOperator& window, const std::unordered_set<OperatorId>& windowsWithoutThrottling);

}
