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

#include <Rules/Static/AsyncOperatorDecompositionRule.hpp>

#include <memory>
#include <ranges>
#include <string>
#include <unordered_map>
#include <vector>
#include <AsyncDelayLogicalOperator.hpp>
#include <utility>
#include <optional>
#include <algorithm>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/AsynchronousOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/HandoffSinkConfig.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <Sources/AsyncSourceConfig.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/FieldOrderingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/PlacementTrait.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <OptimizerTestUtils.hpp>
#include <QueryOptimizerAsyncConfiguration.hpp>

namespace NES
{
namespace
{
Field field(const LogicalOperator& op, const std::string& name)
{
    const auto result = op.getOutputSchema().getFieldByName(Identifier::parse(name));
    PRECONDITION(result.has_value(), "Missing field {} in test fixture", name);
    return *result;
}

LogicalOperator placed(const LogicalOperator& op, const Schema<UnqualifiedUnboundField, Ordered>& fields, Host host)
{
    auto traits = op.getTraitSet();
    traits.insert(PlacementTrait{std::move(host)});
    traits.insert(MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT});
    traits.insert(FieldOrderingTrait{fields});
    if (!traits.contains<FieldMappingTrait>())
    {
        std::unordered_map<UnqualifiedUnboundField, QualifiedIdentifier> mapping;
        for (const auto& output : fields)
        {
            mapping.emplace(output, QualifiedIdentifier{output.getFullyQualifiedName()});
        }
        traits.insert(FieldMappingTrait{std::move(mapping)});
    }
    return op.withTraitSet(std::move(traits));
}

LogicalOperator placed(const LogicalOperator& op, const Schema<UnqualifiedUnboundField, Ordered>& fields)
{
    return placed(op, fields, Host{"localhost"});
}

LogicalOperator placedOn(const LogicalOperator& op, Host host)
{
    const Schema<UnqualifiedUnboundField, Ordered> fields{op.getOutputSchema()
                                                          | std::views::transform([](const Field& output) { return output.unbound(); })};
    return placed(op, fields, std::move(host));
}

LogicalOperator placed(const LogicalOperator& op)
{
    const Schema<UnqualifiedUnboundField, Ordered> fields{op.getOutputSchema()
                                                          | std::views::transform([](const Field& output) { return output.unbound(); })};
    return placed(op, fields);
}

std::vector<TypedLogicalOperator<SourceDescriptorLogicalOperator>> asyncSources(const LogicalPlan& plan)
{
    std::vector<TypedLogicalOperator<SourceDescriptorLogicalOperator>> sources;
    for (const auto& source : getOperatorByType<SourceDescriptorLogicalOperator>(plan))
    {
        if (source->getSourceDescriptor().getSourceType() == Identifier::parse("Async"))
        {
            sources.push_back(source);
        }
    }
    return sources;
}

LogicalOperator delay(LogicalOperator child, const Field& value)
{
    return AsyncDelayLogicalOperator::create(
        std::move(child),
        FieldAccessLogicalFunction{value},
        ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "0"});
}

TEST(AsyncOperatorDecompositionRuleTest, LowersOrdinaryLogicalOperatorWithPairedHandoffAndRequiredOutput)
{
    OptimizerTestUtils utils;
    const auto sourceCatalog = std::make_shared<SourceCatalog>();
    const auto sinkCatalog = std::make_shared<SinkCatalog>();
    const AsyncOperatorDecompositionRule rule{QueryOptimizerAsyncConfiguration{3, 5}, sourceCatalog, sinkCatalog};
    const auto source = placed(utils.createSource("input", std::vector<std::string>{"seed"}));
    const auto invocation = placed(delay(source, field(source, "seed")));
    const auto plan = utils.createPlan(utils.createSink(invocation, "output", std::vector<std::string>{"seed", "value"}));
    const auto lowered = rule.apply(plan);

    EXPECT_EQ(std::ranges::count_if(BFSRange(lowered.getRootOperators().front()),
                                    [](const LogicalOperator& op) { return op.tryGetAs<AsynchronousOperator>().has_value(); }),
              0);
    const auto sources = asyncSources(lowered);
    ASSERT_EQ(sources.size(), 1);
    const auto& descriptor = sources.front()->getSourceDescriptor();
    const auto& config = descriptor.getPluginData().getAs<AsyncSourceConfig>();
    EXPECT_EQ(descriptor.getHost(), Host{"localhost"});
    EXPECT_EQ(config.maxConcurrency, 3);
    EXPECT_EQ(config.channel->getCapacity(), 5);
    ASSERT_EQ(config.calls.getCalls().size(), 1);
    ASSERT_EQ(config.calls.getOutputs().size(), 1);
    EXPECT_EQ(config.calls.getCalls().front()->getDependencies().size(), 0);

    const auto handoff = lowered.getRootOperators().front().getAs<SinkLogicalOperator>();
    const auto sinkDescriptor = handoff->getSinkDescriptor();
    ASSERT_TRUE(sinkDescriptor.has_value());
    EXPECT_EQ(sinkDescriptor->getHost(), descriptor.getHost());
    EXPECT_EQ(sinkDescriptor->getPluginData().getAs<HandoffSinkConfig>().channel, config.channel);
    const auto sinkSchema = std::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(sinkDescriptor->getSchema());
    ASSERT_NE(sinkSchema, nullptr);
    EXPECT_EQ(*sinkSchema, descriptor.getSchema());
    EXPECT_EQ(plan.getRootOperators().size(), 1);
}

TEST(AsyncOperatorDecompositionRuleTest, FusesSameWorkerAliasChainsButKeepsEveryInvocationDistinct)
{
    OptimizerTestUtils utils;
    const AsyncOperatorDecompositionRule rule{QueryOptimizerAsyncConfiguration{std::nullopt, std::nullopt},
                                              std::make_shared<SourceCatalog>(),
                                              std::make_shared<SinkCatalog>()};
    const auto source = placed(utils.createSource("input", std::vector<std::string>{"seed"}));
    const auto first = placed(delay(source, field(source, "seed")));
    const auto firstAlias
        = placed(ProjectionLogicalOperator::create(first,
                                                   {{Identifier::parse("one"), FieldAccessLogicalFunction{field(first, "value")}}},
                                                   ProjectionLogicalOperator::Asterisk{false}));
    const auto second = placed(delay(firstAlias, field(firstAlias, "one")));
    const auto secondAlias
        = placed(ProjectionLogicalOperator::create(second,
                                                   {{Identifier::parse("two"), FieldAccessLogicalFunction{field(second, "value")}}},
                                                   ProjectionLogicalOperator::Asterisk{false}));
    const auto third = placed(delay(secondAlias, field(secondAlias, "two")));
    const auto plan = utils.createPlan(utils.createSink(third, "output", std::vector<std::string>{"two", "value"}));
    const auto lowered = rule.apply(plan);
    const auto sources = asyncSources(lowered);
    ASSERT_EQ(sources.size(), 1);
    const auto& graph = sources.front()->getSourceDescriptor().getPluginData().getAs<AsyncSourceConfig>().calls;
    ASSERT_EQ(graph.getCalls().size(), 3);
    ASSERT_EQ(graph.getRequiredCalls().size(), 3);
    EXPECT_NE(graph.getCalls()[0], graph.getCalls()[1]);
    EXPECT_NE(graph.getCalls()[1], graph.getCalls()[2]);
    EXPECT_EQ(graph.getCalls()[1]->getDependencies().size(), 1);
    EXPECT_EQ(graph.getCalls()[2]->getDependencies().size(), 1);
}

TEST(AsyncOperatorDecompositionRuleTest, NarrowingProjectionBelowAsyncOperatorStaysUpstreamOfHandoff)
{
    OptimizerTestUtils utils;
    const AsyncOperatorDecompositionRule rule{QueryOptimizerAsyncConfiguration{std::nullopt, std::nullopt},
                                              std::make_shared<SourceCatalog>(),
                                              std::make_shared<SinkCatalog>()};
    const auto source = placed(utils.createSource("input", std::vector<std::string>{"used", "pruned_payload"}));
    const auto pruned
        = placed(ProjectionLogicalOperator::create(source,
                                                   {{Identifier::parse("used"), FieldAccessLogicalFunction{field(source, "used")}}},
                                                   ProjectionLogicalOperator::Asterisk{false}));
    const auto invocation = placed(delay(pruned, field(pruned, "used")));
    const auto plan = utils.createPlan(utils.createSink(invocation, "output", std::vector<std::string>{"used", "value"}));
    const auto lowered = rule.apply(plan);

    const auto sources = asyncSources(lowered);
    ASSERT_EQ(sources.size(), 1);
    const auto& descriptor = sources.front()->getSourceDescriptor();
    EXPECT_EQ(descriptor.getSchema().size(), 2);
    EXPECT_TRUE(descriptor.getSchema().getFieldByName(Identifier::parse("used")).has_value());
    EXPECT_TRUE(descriptor.getSchema().getFieldByName(Identifier::parse("value")).has_value());
    EXPECT_FALSE(descriptor.getSchema().getFieldByName(Identifier::parse("pruned_payload")).has_value());

    const auto handoff = lowered.getRootOperators().front().getAs<SinkLogicalOperator>();
    const auto sinkDescriptor = handoff->getSinkDescriptor();
    ASSERT_TRUE(sinkDescriptor.has_value());
    const auto sinkSchema = std::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(sinkDescriptor->getSchema());
    ASSERT_NE(sinkSchema, nullptr);
    EXPECT_EQ(*sinkSchema, descriptor.getSchema());
    EXPECT_FALSE(sinkSchema->getFieldByName(Identifier::parse("pruned_payload")).has_value());
}

TEST(AsyncOperatorDecompositionRuleTest, PreservesAllUpstreamOriginIdsOnAsyncSource)
{
    OptimizerTestUtils utils;
    const AsyncOperatorDecompositionRule rule{QueryOptimizerAsyncConfiguration{std::nullopt, std::nullopt},
                                              std::make_shared<SourceCatalog>(),
                                              std::make_shared<SinkCatalog>()};
    auto withOrigins = [](LogicalOperator op, std::vector<OriginId> origins)
    {
        auto traits = op.getTraitSet();
        traits.insert(OutputOriginIdsTrait{std::move(origins)});
        return op.withTraitSet(std::move(traits));
    };
    const auto left = withOrigins(placed(utils.createSource("left_input", std::vector<std::string>{"seed"})), {OriginId{1}});
    const auto right = withOrigins(placed(utils.createSource("right_input", std::vector<std::string>{"seed"})), {OriginId{2}});
    const auto combined = withOrigins(placed(UnionLogicalOperator::create({left, right})), {OriginId{1}, OriginId{2}});
    const auto invocation = withOrigins(placed(delay(combined, field(combined, "seed"))), {OriginId{1}, OriginId{2}});
    const auto plan = utils.createPlan(utils.createSink(invocation, "output", std::vector<std::string>{"seed", "value"}));
    const auto lowered = rule.apply(plan);

    const auto sources = asyncSources(lowered);
    ASSERT_EQ(sources.size(), 1);
    const auto originsTrait = sources.front()->getTraitSet().tryGet<OutputOriginIdsTrait>();
    ASSERT_TRUE(originsTrait.has_value());
    const std::vector<OriginId> actualOrigins{(*originsTrait)->begin(), (*originsTrait)->end()};
    EXPECT_EQ(actualOrigins, (std::vector<OriginId>{OriginId{1}, OriginId{2}}));
}

TEST(AsyncOperatorDecompositionRuleTest, ComputedProjectionRemainsAnAsyncFusionBoundary)
{
    OptimizerTestUtils utils;
    const AsyncOperatorDecompositionRule rule{QueryOptimizerAsyncConfiguration{std::nullopt, std::nullopt},
                                              std::make_shared<SourceCatalog>(),
                                              std::make_shared<SinkCatalog>()};
    const auto source = placed(utils.createSource("input", std::vector<std::string>{"seed"}));
    const auto first = placed(delay(source, field(source, "seed")));
    const auto computed = placed(ProjectionLogicalOperator::create(
        first,
        {{Identifier::parse("computed"), ConstantValueLogicalFunction{field(first, "value").getDataType(), "1"}}},
        ProjectionLogicalOperator::Asterisk{false}));
    const auto second = placed(delay(computed, field(computed, "computed")));
    const auto plan = utils.createPlan(utils.createSink(second, "output", std::vector<std::string>{"computed", "value"}));
    const auto lowered = rule.apply(plan);
    EXPECT_EQ(asyncSources(lowered).size(), 2);
}

TEST(AsyncOperatorDecompositionRuleTest, DifferentWorkerPlacementStopsFusionAcrossAnAlias)
{
    OptimizerTestUtils utils;
    const AsyncOperatorDecompositionRule rule{QueryOptimizerAsyncConfiguration{std::nullopt, std::nullopt},
                                              std::make_shared<SourceCatalog>(),
                                              std::make_shared<SinkCatalog>()};
    const auto source = placed(utils.createSource("input", std::vector<std::string>{"seed"}));
    const auto first = placed(delay(source, field(source, "seed")));
    const auto alias
        = placed(ProjectionLogicalOperator::create(first,
                                                   {{Identifier::parse("intermediate"), FieldAccessLogicalFunction{field(first, "value")}}},
                                                   ProjectionLogicalOperator::Asterisk{false}));
    const auto second = placedOn(delay(alias, field(alias, "intermediate")), Host{"worker-two"});
    const auto plan = utils.createPlan(utils.createSink(second, "output", std::vector<std::string>{"intermediate", "value"}));
    const auto lowered = rule.apply(plan);
    const auto sources = asyncSources(lowered);
    ASSERT_EQ(sources.size(), 2);
    for (const auto& asyncSource : sources)
    {
        const auto& graph = asyncSource->getSourceDescriptor().getPluginData().getAs<AsyncSourceConfig>().calls;
        ASSERT_EQ(graph.getCalls().size(), 1);
        EXPECT_TRUE(graph.getCalls().front()->getDependencies().empty());
    }
}

TEST(AsyncOperatorDecompositionRuleTest, RejectsAsyncBelowASharedAncestorBeforeCreatingStages)
{
    OptimizerTestUtils utils;
    const AsyncOperatorDecompositionRule rule{QueryOptimizerAsyncConfiguration{std::nullopt, std::nullopt},
                                              std::make_shared<SourceCatalog>(),
                                              std::make_shared<SinkCatalog>()};
    const auto source = placed(utils.createSource("input", std::vector<std::string>{"seed"}));
    const auto invocation = placed(delay(source, field(source, "seed")));
    const auto shared
        = ProjectionLogicalOperator::create(invocation,
                                            {{Identifier::parse("value"), FieldAccessLogicalFunction{field(invocation, "value")}}},
                                            ProjectionLogicalOperator::Asterisk{false});
    const auto combined = UnionLogicalOperator::create({shared, shared});
    const auto plan = utils.createPlan(utils.createSink(combined, "output", std::vector<std::string>{"value"}));
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(rule.apply(plan)), ErrorCode::UnsupportedQuery);
}
}
}
