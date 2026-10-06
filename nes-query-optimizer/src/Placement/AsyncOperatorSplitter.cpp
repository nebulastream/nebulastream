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

#include <Placement/AsyncOperatorSplitter.hpp>

#include <ranges>
#include <string>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Async/AsyncWiring.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Traits/AsyncExecutionTrait.hpp>
#include <Traits/FieldOrderingTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Pointers.hpp>
#include <Util/UUID.hpp>
#include <DistributedLogicalPlan.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>
#include <QueryId.hpp>

namespace NES
{

namespace
{

struct SplitContext
{
    SharedPtr<const SourceCatalog> sourceCatalog;
    SharedPtr<const SinkCatalog> sinkCatalog;
    Host host;
    /// Producer halves discovered while rewriting; they become additional local plans.
    std::vector<LogicalPlan> producerPlans;
};

/// Everything except the marker itself. A marker left on an operator would make a later pass try
/// to split it again, and `AsyncOperatorSplitter` is meant to be idempotent.
TraitSet withoutAsyncExecution(const TraitSet& traits)
{
    return traits | std::views::filter([](const auto& existing) { return existing.getTypeInfo() != typeid(AsyncExecutionTrait); })
        | std::ranges::to<TraitSet>();
}

/// Replaces one marked operator by a sink/source pair and records the producer half.
LogicalOperator cut(SplitContext& context, const LogicalOperator& asyncOperator, const LogicalOperator& child, const AsyncExecutionTrait& trait)
{
    /// What crosses the channel is the child's output; what the source produces is the operator's.
    const auto inputSchema = child.getTraitSet().get<FieldOrderingTrait>()->getOrderedFields();
    const auto outputSchema = asyncOperator.getTraitSet().get<FieldOrderingTrait>()->getOrderedFields();

    /// The one value both halves must agree on. Everything else they learn from their own config.
    const auto channelId = UUIDToString(generateUUID());

    const std::unordered_map<Identifier, std::string> sinkConfig{
        {Identifier::parse("channel"), channelId},
        {Identifier::parse("channel_capacity"), std::to_string(trait.channelCapacity)}};

    const std::unordered_map<Identifier, std::string> sourceConfig{
        {Identifier::parse("channel"), channelId},
        {Identifier::parse("channel_capacity"), std::to_string(trait.channelCapacity)},
        {Identifier::parse("executor_type"), trait.executorType},
        {Identifier::parse("executor_config"), encodeConfig(trait.config)},
        /// The descriptor's own schema describes what the source emits, so the schema of the
        /// records arriving through the channel has to travel separately.
        {Identifier::parse("input_schema"), encodeSchema(inputSchema)},
        {Identifier::parse("batch_size"), std::to_string(trait.batchSize)},
        {Identifier::parse("max_concurrency"), std::to_string(trait.maxConcurrency)},
        {Identifier::parse("preserve_order"), trait.preserveOrder ? "true" : "false"}};

    const auto sourceDescriptor = context.sourceCatalog->getAnonymousSource(
        Identifier::parse("Async"),
        outputSchema,
        context.host,
        /// NATIVE: records are handed over as they are, nothing is parsed.
        {{Identifier::parse(InputFormatterDescriptor::getTypeString()), "NATIVE"}},
        sourceConfig);
    INVARIANT(sourceDescriptor.has_value(), "Failed to create the source descriptor for an asynchronous operator");

    const auto sinkDescriptor
        = context.sinkCatalog->getAnonymousSink(inputSchema, Identifier::parse("Handoff"), context.host, sinkConfig, {});
    INVARIANT(sinkDescriptor.has_value(), "Failed to create the sink descriptor for an asynchronous operator");

    /// Trait sets are taken over unchanged, and deliberately from two different operators: the
    /// sink continues the child, the source stands in for the operator. That is what carries the
    /// OutputOriginIdsTrait to the source, so downstream watermark processing keeps seeing the
    /// origin it was compiled for. Both operators already carry the PlacementTrait of this host —
    /// everything inside one local plan is placed on the same worker.
    auto sinkOperator = SinkLogicalOperator::create(child, sinkDescriptor.value())->withTraitSet(child.getTraitSet()).withInferredSchema();
    context.producerPlans.emplace_back(INVALID_QUERY_ID, std::vector<LogicalOperator>{sinkOperator});

    /// The source now *is* the operator, but it must not keep the marker.
    const auto sourceTraits = withoutAsyncExecution(asyncOperator.getTraitSet());

    NES_DEBUG("Split out asynchronous operator '{}' onto channel {}", trait.executorType, channelId);
    return SourceDescriptorLogicalOperator::create(sourceDescriptor.value())->withTraitSet(sourceTraits);
}

LogicalOperator splitRecursive(SplitContext& context, const LogicalOperator& op)
{
    /// Children first, so that a marked operator inside the producer half is already replaced
    /// by the time we look at its parent. Several asynchronous operators in one query therefore
    /// simply produce several plans.
    std::vector<LogicalOperator> newChildren;
    newChildren.reserve(op.getChildren().size());
    for (const auto& child : op.getChildren())
    {
        newChildren.emplace_back(splitRecursive(context, child));
    }

    if (const auto trait = op.getTraitSet().tryGet<AsyncExecutionTrait>(); trait.has_value())
    {
        /// The marker belongs to an inner operator — one that reads from a child and feeds a
        /// parent. `QueryDecomposition::connect()` copies the whole trait set of the operator it
        /// cuts onto *both* boundary operators it generates, so the network sink ending the
        /// producer half and the network source starting the consumer half both carry this marker
        /// without being that operator. The real operator sits in the local plan of the host it
        /// was placed on and is split there; everywhere else the marker is an artifact.
        ///
        /// Without this, the generated network sink would be split as well, and since it is the
        /// root of its local plan the consumer half would come out rooted in a source.
        if (op.tryGetAs<SourceDescriptorLogicalOperator>() || op.tryGetAs<SinkLogicalOperator>())
        {
            NES_DEBUG("Dropping the asynchronous execution marker from a generated boundary operator for '{}'", trait.value()->executorType);
            return op.withChildren(std::move(newChildren)).withTraitSet(withoutAsyncExecution(op.getTraitSet()));
        }

        if (newChildren.size() != 1)
        {
            throw UnsupportedQuery(
                "An asynchronous operator must have exactly one input, but '{}' has {}: {}",
                trait.value()->executorType,
                newChildren.size(),
                op.explain(ExplainVerbosity::Debug));
        }
        return cut(context, op, newChildren.front(), *trait.value());
    }

    return op.withChildren(std::move(newChildren));
}

}

AsyncOperatorSplitter::AsyncOperatorSplitter(SharedPtr<const SourceCatalog> sourceCatalog, SharedPtr<const SinkCatalog> sinkCatalog)
    : sourceCatalog(std::move(sourceCatalog)), sinkCatalog(std::move(sinkCatalog))
{
}

DistributedLogicalPlan AsyncOperatorSplitter::split(const DistributedLogicalPlan& placedPlan) const
{
    std::unordered_map<Host, std::vector<LogicalPlan>> newPlans;

    for (const auto& [host, localPlans] : placedPlan)
    {
        for (const auto& localPlan : localPlans)
        {
            const auto roots = localPlan.getRootOperators();
            INVARIANT(roots.size() == 1, "A local plan is expected to have exactly one root, but has {}", roots.size());

            NES_DEBUG("Splitting local plan on host {}:\n{}", host, explain(localPlan, ExplainVerbosity::Debug));

            SplitContext context{
                .sourceCatalog = copyPtr(sourceCatalog), .sinkCatalog = copyPtr(sinkCatalog), .host = host, .producerPlans = {}};
            auto newRoot = splitRecursive(context, roots.front());

            /// Producer halves first — they only matter for readability, since the plans are
            /// deployed independently and the channel tolerates either side starting first.
            for (auto& producerPlan : context.producerPlans)
            {
                newPlans[host].emplace_back(std::move(producerPlan));
            }
            newPlans[host].emplace_back(localPlan.getQueryId(), std::vector<LogicalOperator>{std::move(newRoot)});
        }
    }

    return {std::move(newPlans), placedPlan.getGlobalPlan()};
}

}
