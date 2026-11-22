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

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant> /// NOLINT(misc-include-cleaner) public provider of the variant std::get overloads used below
#include <vector>
#include <Async/HandoffChannel.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Operators/AsyncLogicalOperator.hpp>
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
#include <Sources/AsyncError.hpp>
#include <Sources/AsyncExecutionContext.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Sources/AsyncSourceConfig.hpp>
#include <Sources/PreparedAsyncCall.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/FieldOrderingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Util/Reflection.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <OptimizerTestUtils.hpp>
#include <QueryOptimizerAsyncConfiguration.hpp>

namespace NES
{
namespace
{
struct TestAsyncOperator
{
    std::vector<DataType> resultTypes;
    int marker;

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) native async operator interface
    [[nodiscard]] std::string_view getType() const { return "TEST_DECOMPOSITION"; }

    [[nodiscard]] Schema<UnqualifiedUnboundField, Ordered> getOutputSchema() const
    {
        std::vector<UnqualifiedUnboundField> fields;
        for (size_t index = 0; index < resultTypes.size(); ++index)
        {
            fields.emplace_back(Identifier::parse("OUT" + std::to_string(index)), resultTypes.at(index));
        }
        return Schema<UnqualifiedUnboundField, Ordered>{std::move(fields)};
    }

    [[nodiscard]] TestAsyncOperator withInferredArguments(const AsyncArgumentsWithConstants&) const { return *this; }

    /// NOLINTBEGIN(readability-convert-member-functions-to-static,performance-unnecessary-value-param) native owning prepare interface
    [[nodiscard]] AsyncResult<std::unique_ptr<PreparedAsyncCall>> prepare(AsyncExecutionContext&, SharedRuntimeValues) const
    {
        return std::unexpected(AsyncError{IOError("The decomposition test operator is not executable")});
    }

    /// NOLINTEND(readability-convert-member-functions-to-static,performance-unnecessary-value-param)
};

Field field(const LogicalOperator& op, const std::string& name)
{
    const auto result = op.getOutputSchema().getFieldByName(Identifier::parse(name));
    PRECONDITION(result.has_value(), "Missing field {} in test fixture", name);
    return *result;
}

LogicalOperator placed(const LogicalOperator& op, const Schema<UnqualifiedUnboundField, Ordered>& fields)
{
    auto traits = op.getTraitSet();
    traits.insert(PlacementTrait{Host{"localhost"}});
    traits.insert(MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT});
    traits.insert(FieldOrderingTrait{fields});
    /// Match inferred materialization mappings unless the fixture supplies a physical alias.
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

LogicalOperator placed(const LogicalOperator& op)
{
    const Schema<UnqualifiedUnboundField, Ordered> fields{
        op.getOutputSchema() | std::views::transform([](const Field& output) { return output.unbound(); })};
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

TEST(AsyncOperatorDecompositionRuleTest, PreservesMultiOutputPayloadPaddingLayoutAndPairedChannel)
{
    OptimizerTestUtils utils;
    const auto sourceCatalog = std::make_shared<SourceCatalog>();
    const auto sinkCatalog = std::make_shared<SinkCatalog>();
    const AsyncOperatorDecompositionRule rule{QueryOptimizerAsyncConfiguration{3, 5, 0}, sourceCatalog, sinkCatalog};
    const auto rawSource = utils.createSource("input", std::vector<std::string>{"value"});
    const auto input = field(rawSource, "value");
    auto inputTraits = rawSource.getTraitSet();
    inputTraits.insert(FieldMappingTrait{{{input.unbound(), QualifiedIdentifier::parse("physical_input")}}});
    const auto source = placed(rawSource.withTraitSet(std::move(inputTraits)));

    const DataType letter{DataType::Type::CHAR, DataType::NULLABLE::NOT_NULLABLE};
    const DataType flag{DataType::Type::BOOLEAN, DataType::NULLABLE::NOT_NULLABLE};
    const DataType text{DataType::Type::VARSIZED, DataType::NULLABLE::IS_NULLABLE};
    const auto invocation = AsyncLogicalOperator::create(
        source,
        TestAsyncOperator{{letter, flag, text}, 73},
        {FieldAccessLogicalFunction{field(source, "value")}, ConstantValueLogicalFunction{input.getDataType(), "7"}},
        {Identifier::parse("letter"), Identifier::parse("flag"), Identifier::parse("text")});
    const Schema<UnqualifiedUnboundField, Ordered> order{
        field(invocation, "text").unbound(), input.unbound(), field(invocation, "letter").unbound(), field(invocation, "flag").unbound()};
    const auto plan = utils.createPlan(utils.createSink(placed(invocation, order), "output", order));
    const auto lowered = rule.apply(plan);

    ASSERT_EQ(lowered.getRootOperators().size(), 2);
    EXPECT_TRUE(getOperatorByType<AsyncLogicalOperator>(lowered).empty());
    const auto sources = asyncSources(lowered);
    ASSERT_EQ(sources.size(), 1);
    const auto& descriptor = sources.front()->getSourceDescriptor();
    const auto& config = descriptor.getPluginData().getAs<AsyncSourceConfig>();
    EXPECT_EQ(descriptor.getHost(), Host{"localhost"});
    EXPECT_EQ(config.maxConcurrency, 3);
    EXPECT_EQ(config.maxRetries, 0);
    EXPECT_EQ(config.channel->getCapacity(), 5);
    ASSERT_EQ(config.calls.getCalls().size(), 1);
    EXPECT_EQ(config.calls.getCalls().front()->getAsyncOperator().getAs<TestAsyncOperator>()->marker, 73);
    ASSERT_EQ(config.calls.getOutputs().size(), 3);
    EXPECT_EQ(config.calls.getOutputs().at(0).field, QualifiedIdentifier::parse("letter"));
    EXPECT_EQ(config.calls.getOutputs().at(1).field, QualifiedIdentifier::parse("flag"));
    EXPECT_EQ(config.calls.getOutputs().at(2).field, QualifiedIdentifier::parse("text"));
    const auto& arguments = config.calls.getCalls().front()->getArguments();
    ASSERT_EQ(arguments.size(), 2);
    EXPECT_EQ(std::get<InputFieldReference>(arguments.at(0)).getField(), QualifiedIdentifier{input.getFullyQualifiedName()});
    EXPECT_EQ(std::get<RuntimeValue>(arguments.at(1)).view().get<uint64_t>(), 7);

    const auto handoff = lowered.getRootOperators().front().getAs<SinkLogicalOperator>();
    const auto sinkDescriptor = handoff->getSinkDescriptor();
    ASSERT_TRUE(sinkDescriptor.has_value());
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access) guarded by the fatal GoogleTest assertion
    const auto& sink = *sinkDescriptor;
    EXPECT_EQ(sink.getHost(), descriptor.getHost());
    EXPECT_EQ(sink.getPluginData().getAs<HandoffSinkConfig>().channel, config.channel);
    EXPECT_EQ(descriptor.getSchema(), order);
    const auto sinkSchema = std::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(sink.getSchema());
    ASSERT_NE(sinkSchema, nullptr);
    EXPECT_EQ(*sinkSchema, order);
    const auto padding = handoff->getChild().getAs<ProjectionLogicalOperator>();
    EXPECT_EQ(padding.getTraitSet().get<FieldOrderingTrait>()->getOrderedFields(), order);
    for (const auto& [output, expression] : padding->getProjections())
    {
        if (output.getLastName() == Identifier::parse("letter"))
        {
            EXPECT_EQ(expression.getAs<ConstantValueLogicalFunction>()->getConstantValue(), std::string(1, '\0'));
        }
    }
    const auto mapping = sources.front().getTraitSet().get<FieldMappingTrait>()->getMapping(input.unbound());
    ASSERT_TRUE(mapping.has_value());
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access) guarded by the fatal GoogleTest assertion
    EXPECT_EQ(*mapping, QualifiedIdentifier{input.getFullyQualifiedName()});
    EXPECT_EQ(plan.getRootOperators().size(), 1);
    EXPECT_EQ(getOperatorByType<AsyncLogicalOperator>(plan).size(), 1);
}

TEST(AsyncOperatorDecompositionRuleTest, KeepsEqualOccurrencesSeparateAndChainsMaterializedOutputs)
{
    OptimizerTestUtils utils;
    const auto sourceCatalog = std::make_shared<SourceCatalog>();
    const auto sinkCatalog = std::make_shared<SinkCatalog>();
    const AsyncOperatorDecompositionRule rule{
        QueryOptimizerAsyncConfiguration{std::nullopt, std::nullopt, std::nullopt}, sourceCatalog, sinkCatalog};
    const auto source = placed(utils.createSource("input", std::vector<std::string>{"value"}));
    const auto input = field(source, "value");
    const AsyncOperator config = TestAsyncOperator{{input.getDataType()}, 11};
    const auto first
        = placed(AsyncLogicalOperator::create(source, config, {FieldAccessLogicalFunction{input}}, {Identifier::parse("first")}));
    const auto second = placed(
        AsyncLogicalOperator::create(first, config, {FieldAccessLogicalFunction{field(first, "value")}}, {Identifier::parse("second")}));
    const auto third = placed(
        AsyncLogicalOperator::create(second, config, {FieldAccessLogicalFunction{field(second, "first")}}, {Identifier::parse("third")}));
    const auto plan = utils.createPlan(utils.createSink(third, "output", std::vector<std::string>{"value", "first", "second", "third"}));
    const auto lowered = rule.apply(plan);
    ASSERT_EQ(lowered.getRootOperators().size(), 4);
    const auto sources = asyncSources(lowered);
    ASSERT_EQ(sources.size(), 3);
    std::unordered_set<const HandoffChannel*> channels;
    bool foundChainedInput = false;
    for (const auto& sourceOp : sources)
    {
        const auto& stage = sourceOp->getSourceDescriptor().getPluginData().getAs<AsyncSourceConfig>();
        channels.insert(stage.channel.get());
        ASSERT_EQ(stage.calls.getCalls().size(), 1);
        ASSERT_EQ(stage.calls.getOutputs().size(), 1);
        if (stage.calls.getOutputs().front().field == QualifiedIdentifier::parse("third"))
        {
            foundChainedInput = true;
            EXPECT_EQ(
                std::get<InputFieldReference>(stage.calls.getCalls().front()->getArguments().front()).getField(),
                QualifiedIdentifier::parse("first"));
        }
    }
    EXPECT_EQ(channels.size(), 3);
    EXPECT_TRUE(foundChainedInput);
    EXPECT_TRUE(getOperatorByType<AsyncLogicalOperator>(lowered).empty());
}

TEST(AsyncOperatorDecompositionRuleTest, RejectsAsyncBelowASharedAncestorBeforeCreatingStages)
{
    OptimizerTestUtils utils;
    const AsyncOperatorDecompositionRule rule{
        QueryOptimizerAsyncConfiguration{std::nullopt, std::nullopt, std::nullopt},
        std::make_shared<SourceCatalog>(),
        std::make_shared<SinkCatalog>()};
    const auto source = placed(utils.createSource("input", std::vector<std::string>{"value"}));
    const auto input = field(source, "value");
    const auto invocation = placed(
        AsyncLogicalOperator::create(
            source, TestAsyncOperator{{input.getDataType()}, 0}, {FieldAccessLogicalFunction{input}}, {Identifier::parse("result")}));
    const auto shared = ProjectionLogicalOperator::create(
        invocation,
        {{Identifier::parse("value"), FieldAccessLogicalFunction{field(invocation, "value")}}},
        ProjectionLogicalOperator::Asterisk{false});
    const auto combined = UnionLogicalOperator::create({shared, shared});
    const auto plan = utils.createPlan(utils.createSink(combined, "output", std::vector<std::string>{"value"}));
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(rule.apply(plan)), ErrorCode::UnsupportedQuery);
}
}
}
