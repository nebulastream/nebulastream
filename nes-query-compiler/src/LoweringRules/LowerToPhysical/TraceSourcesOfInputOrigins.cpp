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

#include <LoweringRules/LowerToPhysical/TraceSourcesOfInputOrigins.hpp>

#include <algorithm>
#include <functional>
#include <memory>
#include <ranges>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Watermark/WatermarkGapThrottle.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>

namespace NES
{

namespace
{
bool isWindow(const LogicalOperator& op)
{
    return op.tryGetAs<JoinLogicalOperator>().has_value() || op.tryGetAs<WindowedAggregationLogicalOperator>().has_value();
}

struct Producer
{
    LogicalOperator op;
    bool ambiguous = false;
};

struct SubgraphSummary
{
    std::unordered_set<OriginId> sources;
    std::unordered_map<OriginId, Producer> producers;
    /// Includes the current operator if it is a window.
    std::unordered_set<LogicalOperator> windows;
};

void mergeProducer(std::unordered_map<OriginId, Producer>& producers, const OriginId origin, const Producer& producer)
{
    const auto [it, inserted] = producers.try_emplace(origin, producer);
    if (!inserted)
    {
        it->second.ambiguous |= producer.ambiguous || it->second.op != producer.op;
    }
}

using SummaryPtr = std::shared_ptr<const SubgraphSummary>;
using ChildSummaries = std::unordered_map<LogicalOperator, SummaryPtr>;
using SummaryVisitor = PlanVisitor<bool, bool, SummaryPtr>;

SummaryPtr summarize(const LogicalOperator& op, const ChildSummaries& children)
{
    auto summary = std::make_shared<SubgraphSummary>();
    for (const auto& [child, childSummary] : children)
    {
        summary->sources.insert(childSummary->sources.begin(), childSummary->sources.end());
        summary->windows.insert(childSummary->windows.begin(), childSummary->windows.end());
        for (const auto& [origin, producer] : childSummary->producers)
        {
            mergeProducer(summary->producers, origin, producer);
        }
    }
    if (op.tryGetAs<SourceDescriptorLogicalOperator>().has_value())
    {
        /// Match LowerToPhysicalSource: a network source carries several origins but owns only its first origin's channel.
        const auto& outputOrigins = *op.getTraitSet().get<OutputOriginIdsTrait>();
        INVARIANT(outputOrigins.size() > 0, "A source must have at least one output origin");
        summary->sources.insert(*outputOrigins.begin());
    }
    if (op.tryGetAs<OriginIdAssigner>().has_value())
    {
        for (const auto origin : *op.getTraitSet().get<OutputOriginIdsTrait>())
        {
            mergeProducer(summary->producers, origin, Producer{.op = op});
        }
    }
    if (isWindow(op))
    {
        summary->windows.insert(op);
    }
    return summary;
}

/// PlanVisitor computes a shared immutable summary once per needed DAG node. Its downward contexts limit provenance to subgraphs
/// below windows; its upward contexts avoid copying summaries for every parent. Sibling comparisons only inspect the cached sets.
class SourceOriginCache
{
public:
    explicit SourceOriginCache(const LogicalPlan& plan, const bool traceAll)
    {
        SummaryVisitor visitor{
            [traceAll](const LogicalOperator& op, const std::vector<bool>& parentNeeds) -> SummaryVisitor::DownResult
            {
                const bool needed = traceAll || isWindow(op) || std::ranges::any_of(parentNeeds, std::identity{});
                SummaryVisitor::DownResult result{.operatorContext = needed, .downContexts = {}};
                if (needed)
                {
                    for (const auto& child : op.getChildren())
                    {
                        result.downContexts.emplace(child, true);
                    }
                }
                return result;
            },
            [this](const LogicalOperator& op, const std::vector<LogicalOperator>&, const bool needed, const ChildSummaries& children)
                -> SummaryVisitor::UpResult
            {
                if (!needed)
                {
                    return {op, {}};
                }
                auto summary = summarize(op, children);
                summaries.emplace(op, summary);
                /// This is analysis, not rewriting: keep the original operator instance and its id.
                return {op, std::move(summary)};
            }};
        static_cast<void>(visitor.apply(plan));
    }

    [[nodiscard]] SourcesOfInputOrigins trace(const std::vector<LogicalOperator>& children) const
    {
        SourcesOfInputOrigins result;
        for (const auto& child : children)
        {
            const auto& producers = summaries.at(child)->producers;
            for (const auto origin : *child.getTraitSet().get<OutputOriginIdsTrait>())
            {
                const auto it = producers.find(origin);
                INVARIANT(it != producers.end(), "No operator below the window produces its input origin {}", origin);
                INVARIANT(!it->second.ambiguous, "Several operators below the window assign the origin {}", origin);
                const auto& sources = summaries.at(it->second.op)->sources;
                result[origin] = {sources.begin(), sources.end()};
            }
        }
        return result;
    }

    [[nodiscard]] auto getWindows() const { return summaries | std::views::keys | std::views::filter(isWindow); }

    [[nodiscard]] const std::unordered_set<OriginId>& getSourceOrigins(const LogicalOperator& op) const
    {
        return summaries.at(op)->sources;
    }

    [[nodiscard]] bool hasUpstreamWindow(const LogicalOperator& downstream, const LogicalOperator& upstream) const
    {
        return summaries.at(downstream)->windows.contains(upstream);
    }

private:
    std::unordered_map<LogicalOperator, SummaryPtr> summaries;
};

void clearSources(SourcesOfInputOrigins& sourcesOfInputOrigins)
{
    for (auto& sources : sourcesOfInputOrigins | std::views::values)
    {
        sources.clear();
    }
}
}

SourcesOfInputOrigins traceSourcesOfInputOrigins(const std::vector<LogicalOperator>& children)
{
    /// Standalone inputs can overlap, e.g., a window and one of its sources. An analysis-only root makes that a single DAG for the
    /// visitor. No schema is needed and none of the original operators are rebuilt.
    const auto root = UnionLogicalOperator::create().withChildrenUnsafe(children);
    return SourceOriginCache(LogicalPlan{INVALID_QUERY_ID, {root}}, true).trace(children);
}

WatermarkGapAnalysisPtr analyzeWatermarkGaps(const LogicalPlan& plan)
{
    const SourceOriginCache cache(plan, false);
    auto analysis = std::make_shared<WatermarkGapAnalysis>();
    for (const auto& window : cache.getWindows())
    {
        analysis->sourcesByWindow.emplace(window, cache.trace(window.getChildren()));
    }

    /// Reuse the analysis's window keys and cached source unions instead of maintaining duplicate operator/source collections.
    auto throttlingWindows = analysis->sourcesByWindow
        | std::views::filter([](const auto& windowAndSources) { return windowAndSources.second.size() > 1; }) | std::views::keys;
    for (const auto& window : throttlingWindows)
    {
        const auto& sources = cache.getSourceOrigins(window);
        for (const auto& otherWindow : throttlingWindows)
        {
            /// Prefer the upstream controller. Nested windows can otherwise hold each other's sources when watermarks are
            /// reassigned between them. Siblings disable each other, leaving no two eligible controllers sharing a source.
            if (window == otherWindow || cache.hasUpstreamWindow(otherWindow, window))
            {
                continue;
            }
            const auto& otherSources = cache.getSourceOrigins(otherWindow);
            const auto& smaller = sources.size() <= otherSources.size() ? sources : otherSources;
            const auto& larger = sources.size() <= otherSources.size() ? otherSources : sources;
            if (std::ranges::any_of(smaller, [&](const auto source) { return larger.contains(source); }))
            {
                analysis->windowsWithoutThrottling.insert(window);
                break;
            }
        }
    }
    return analysis;
}

std::unordered_set<LogicalOperator> findWindowsWithConflictingThrottles(const LogicalPlan& plan)
{
    return analyzeWatermarkGaps(plan)->windowsWithoutThrottling;
}

SourcesOfInputOrigins
traceThrottleableSourcesOfInputOrigins(const LogicalOperator& window, const std::unordered_set<LogicalOperator>& windowsWithoutThrottling)
{
    auto sources = traceSourcesOfInputOrigins(window.getChildren());
    if (windowsWithoutThrottling.contains(window))
    {
        clearSources(sources);
    }
    return sources;
}

SourcesOfInputOrigins traceThrottleableSourcesOfInputOrigins(const LogicalOperator& window, const WatermarkGapAnalysisPtr& analysis)
{
    if (!analysis)
    {
        return traceSourcesOfInputOrigins(window.getChildren());
    }
    auto sources = analysis->sourcesByWindow.at(window);
    if (analysis->windowsWithoutThrottling.contains(window))
    {
        clearSources(sources);
    }
    return sources;
}

}
