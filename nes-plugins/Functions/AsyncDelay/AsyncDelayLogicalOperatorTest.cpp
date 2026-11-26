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
#include <AsyncDelayLogicalOperator.hpp>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Sources/AsyncInputAccessor.hpp>
#include <Functions/LogicalFunctionProvider.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/AsynchronousOperator.hpp>
#include <Operators/AsyncLoweringContext.hpp>
#include <Sources/AsyncInputAccessor.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Static/OperatorFunctionLiftingRule.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/UUID.hpp>
#include <gtest/gtest.h>
#include <AsyncOperatorLoweringRegistry.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>

namespace NES
{
namespace
{
const DataType integer{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
const DataType uint64{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};

LogicalOperator child(Schema<UnqualifiedUnboundField, Ordered> schema)
{
    const auto logical = LogicalSource{Identifier::parse("delay_input"), std::move(schema)};
    const std::unordered_map<Identifier, std::string> sourceConfig{{Identifier::parse("file_path"), "/dev/null"}};
    const auto descriptor = SourceDescriptor::create(PhysicalSourceId{1},
                                                     logical,
                                                     Identifier::parse("File"),
                                                     Host{"localhost"},
                                                     sourceConfig,
                                                     {{Identifier::parse("type"), "CSV"}},
                                                     false)
                                .value();
    return SourceDescriptorLogicalOperator::create(descriptor);
}

LogicalOperator child()
{
    return child(Schema<UnqualifiedUnboundField, Ordered>{});
}

TypedLogicalOperator<AsyncDelayLogicalOperator> delay()
{
    return AsyncDelayLogicalOperator::create(
        child(), ConstantValueLogicalFunction{integer, "7"}, ConstantValueLogicalFunction{uint64, "0"});
}
}

TEST(AsyncDelayLogicalOperatorTest, DirectCreationAndValueRewritesPreserveConcreteType)
{
    auto op = delay();
    static_assert(std::same_as<decltype(op->withChildren(op->getChildren())), AsyncDelayLogicalOperator>);
    static_assert(std::same_as<decltype(op->withChildren(op->getChildren())), AsyncDelayLogicalOperator>);
    static_assert(std::same_as<decltype(op->withChildrenUnsafe(op->getChildren())), AsyncDelayLogicalOperator>);
    static_assert(std::same_as<decltype(op->withInferredSchema()), AsyncDelayLogicalOperator>);
    static_assert(std::same_as<decltype(op->withTraitSet(TraitSet{})), AsyncDelayLogicalOperator>);

    EXPECT_EQ(op->getName(), "AsyncDelay");
    EXPECT_EQ(op->getValue().getDataType(), integer);
    EXPECT_TRUE(op->getOutputSchema()[Identifier::parse("value")].has_value());
    EXPECT_TRUE(LogicalOperator{op}.tryGetAs<AsynchronousOperator>().has_value());
    EXPECT_TRUE(LogicalOperator{op->withChildren(op->getChildren())}.tryGetAs<AsyncDelayLogicalOperator>().has_value());
}

TEST(AsyncDelayLogicalOperatorTest, ScalarShorthandLiftsThroughLogicalFactory)
{
    const auto scalar = LogicalFunctionProvider::provide(
                            "ASYNC_DELAY", {ConstantValueLogicalFunction{integer, "7"}, ConstantValueLogicalFunction{uint64, "0"}})
                            .withInferredDataType(Schema<Field, Unordered>{});
    const auto projection
        = ProjectionLogicalOperator::create(child(), {{Identifier::parse("result"), scalar}}, ProjectionLogicalOperator::Asterisk{false});
    const LogicalPlan plan{QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()), {LogicalOperator{projection}}};

    const auto normalized = OperatorFunctionLiftingRule{}.apply(plan);
    const auto calls = getOperatorByType<AsyncDelayLogicalOperator>(normalized);
    ASSERT_EQ(calls.size(), 1U);
    EXPECT_EQ(calls.front()->getName(), "AsyncDelay");
}

TEST(AsyncDelayLogicalOperatorTest, LoweringResolvesNamedRuntimeInputs)
{
    const auto input
        = child(Schema<UnqualifiedUnboundField, Ordered>{{Identifier::parse("input"), integer}, {Identifier::parse("delay"), uint64}});
    const auto op
        = AsyncDelayLogicalOperator::create(input,
                                            FieldAccessLogicalFunction{input.getOutputSchema()[Identifier::parse("input")].value()},
                                            FieldAccessLogicalFunction{input.getOutputSchema()[Identifier::parse("delay")].value()});
    std::unordered_map<QualifiedIdentifierBase<1>, AsyncInputAccessor> fields;
    fields.emplace(op->getValue().getAs<FieldAccessLogicalFunction>()->getField().getFullyQualifiedName(),
                   AsyncInputAccessor{RuntimeValue{int64_t{7}}});
    fields.emplace(op->getMillis().getAs<FieldAccessLogicalFunction>()->getField().getFullyQualifiedName(),
                   AsyncInputAccessor{RuntimeValue{uint64_t{0}}});
    AsyncLoweringContext context{std::move(fields)};
    const auto lowered = AsyncDelayLogicalOperator::lowerToAsync(op, context);
    EXPECT_EQ(lowered.getType(), "ASYNC_DELAY");
}

TEST(AsyncDelayLogicalOperatorTest, ReflectionRoundTripRetainsConcreteTypeAndNamedInputs)
{
    const auto op = delay();
    const auto schema = Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("value"), integer}};
    const auto descriptor
        = SinkDescriptor::createNamed(INVALID_SINK_ID,
                                      Identifier::parse("delay_test_sink"),
                                      Identifier::parse("file"),
                                      schema,
                                      Host{"localhost"},
                                      {{Identifier::parse("FILE_PATH"), "/dev/null"}, {Identifier::parse("OUTPUT_FORMAT"), "CSV"}},
                                      {})
              .value();
    const auto sinkOp = SinkLogicalOperator::create(LogicalOperator{op}, descriptor).withInferredSchema();
    const LogicalPlan plan{QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()), {LogicalOperator{sinkOp}}};
    const auto restored = QueryPlanSerializationUtil::deserializeQueryPlan(QueryPlanSerializationUtil::serializeQueryPlan(plan));
    const auto roots = restored.getRootOperators();
    ASSERT_EQ(roots.size(), 1U);
    const auto concrete = roots.front().getAs<SinkLogicalOperator>()->getChild().tryGetAs<AsyncDelayLogicalOperator>();
    ASSERT_TRUE(concrete.has_value());
    EXPECT_TRUE(StructuralFunctionEqual{}(concrete.value()->getValue(), op->getValue()));
    EXPECT_TRUE(StructuralFunctionEqual{}(concrete.value()->getMillis(), op->getMillis()));
}
}
