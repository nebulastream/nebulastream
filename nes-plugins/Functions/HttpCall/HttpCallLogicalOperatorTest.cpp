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
#include <HttpCallLogicalOperator.hpp>

#include <concepts>
#include <string>
#include <unordered_map>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/AsynchronousOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
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
const DataType text{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE};

LogicalOperator child(Schema<UnqualifiedUnboundField, Ordered> schema)
{
    const auto logical = LogicalSource{Identifier::parse("http_input"), std::move(schema)};
    const std::unordered_map<Identifier, std::string> sourceConfig{{Identifier::parse("file_path"), "/dev/null"}};
    const auto descriptor = SourceDescriptor::create(
                                PhysicalSourceId{1},
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

TypedLogicalOperator<HttpCallLogicalOperator> call()
{
    return HttpCallLogicalOperator::create(
        child(), ConstantValueLogicalFunction{text, "body"}, ConstantValueLogicalFunction{text, "http://localhost/"});
}
}

TEST(HttpCallLogicalOperatorTest, DirectCreationAndValueRewritesPreserveConcreteType)
{
    auto op = call();
    static_assert(std::same_as<decltype(op->withChildren(op->getChildren())), HttpCallLogicalOperator>);
    static_assert(std::same_as<decltype(op->withChildren(op->getChildren())), HttpCallLogicalOperator>);
    static_assert(std::same_as<decltype(op->withChildrenUnsafe(op->getChildren())), HttpCallLogicalOperator>);
    static_assert(std::same_as<decltype(op->withInferredSchema()), HttpCallLogicalOperator>);
    static_assert(std::same_as<decltype(op->withTraitSet(TraitSet{})), HttpCallLogicalOperator>);

    EXPECT_EQ(op->getName(), "HttpCall");
    EXPECT_EQ(op->getBody().getDataType(), text);
    EXPECT_EQ(op->getUrl().getDataType(), text);
    EXPECT_TRUE(op->getOutputSchema()[Identifier::parse("response")].has_value());
    EXPECT_TRUE(LogicalOperator{op}.tryGetAs<AsynchronousOperator>().has_value());
    EXPECT_TRUE(LogicalOperator{op.withChildren(op->getChildren())}.tryGetAs<HttpCallLogicalOperator>().has_value());
}

TEST(HttpCallLogicalOperatorTest, FieldInputsRemainWiredAndOtherFieldsAreForwarded)
{
    const auto bodyName = Identifier::parse("body");
    const auto forwardedName = Identifier::parse("forwarded");
    const DataType nullableInteger{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE};
    const auto input = child(Schema<UnqualifiedUnboundField, Ordered>{{bodyName, text}, {forwardedName, nullableInteger}});
    const auto body = input.getOutputSchema()[bodyName].value();
    const auto op
        = HttpCallLogicalOperator::create(input, FieldAccessLogicalFunction{body}, ConstantValueLogicalFunction{text, "http://localhost/"});
    EXPECT_EQ(op->getChild(), input);
    EXPECT_EQ(op->getBody().getAs<FieldAccessLogicalFunction>()->getField(), body);
    const auto schema = op->getOutputSchema();
    EXPECT_EQ(schema.size(), 3U);
    const auto bodyOutput = schema[bodyName];
    const auto forwardedOutput = schema[forwardedName];
    const auto resultOutput = schema[Identifier::parse("response")];
    ASSERT_TRUE(bodyOutput);
    ASSERT_TRUE(forwardedOutput);
    ASSERT_TRUE(resultOutput);
    EXPECT_EQ(bodyOutput->getDataType(), text);
    EXPECT_EQ(forwardedOutput->getDataType(), nullableInteger);
    EXPECT_EQ(resultOutput->getDataType(), text);
}

TEST(HttpCallLogicalOperatorTest, RepeatedOperatorsUseAnOrdinaryProjectionForResultRenaming)
{
    const auto responseName = Identifier::parse("response");
    const auto firstName = Identifier::parse("first");
    const auto secondName = Identifier::parse("second");
    const auto first = call();
    const auto response = first->getOutputSchema()[responseName].value();
    const LogicalFunction url = ConstantValueLogicalFunction{text, "http://localhost/next"};
    EXPECT_THROW(static_cast<void>(HttpCallLogicalOperator::create(first, FieldAccessLogicalFunction{response}, url)), Exception);

    const auto renamed = ProjectionLogicalOperator::create(
        first, {{firstName, FieldAccessLogicalFunction{response}}}, ProjectionLogicalOperator::Asterisk{false});
    const auto preservedFirst = renamed->getOutputSchema()[firstName].value();
    const auto second = HttpCallLogicalOperator::create(renamed, FieldAccessLogicalFunction{preservedFirst}, url);
    const auto secondResponse = second->getOutputSchema()[responseName].value();
    const auto projected = ProjectionLogicalOperator::create(
        second,
        {{firstName, FieldAccessLogicalFunction{second->getOutputSchema()[firstName].value()}},
         {secondName, FieldAccessLogicalFunction{secondResponse}}},
        ProjectionLogicalOperator::Asterisk{false});
    const auto schema = projected->getOutputSchema();
    EXPECT_EQ(schema.size(), 2U);
    EXPECT_TRUE(schema[firstName]);
    EXPECT_TRUE(schema[secondName]);
    EXPECT_FALSE(schema[responseName]);
    EXPECT_EQ(second->getBody().getAs<FieldAccessLogicalFunction>()->getField(), preservedFirst);
}

TEST(HttpCallLogicalOperatorTest, DynamicNamedInputsAreValidatedLogicallyAndConstructRuntimeOnlyOnLowering)
{
    const DataType millisType{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE};
    const auto input = child(
        Schema<UnqualifiedUnboundField, Ordered>{
            {Identifier::parse("body"), text}, {Identifier::parse("url"), text}, {Identifier::parse("timeout"), millisType}});
    const auto op = HttpCallLogicalOperator::create(
        input,
        FieldAccessLogicalFunction{input.getOutputSchema()[Identifier::parse("body")].value()},
        FieldAccessLogicalFunction{input.getOutputSchema()[Identifier::parse("url")].value()},
        FieldAccessLogicalFunction{input.getOutputSchema()[Identifier::parse("timeout")].value()},
        {});
    EXPECT_EQ(op->getTimeoutMillis()->getDataType(), millisType);
    const auto lowered = HttpCallLogicalOperator::lowerToAsync(op);
    EXPECT_EQ(lowered.implementation.getType(), "HTTP_CALL");
    ASSERT_EQ(lowered.arguments.size(), 3U);
    EXPECT_EQ(lowered.arguments.at(0), op->getBody());
    EXPECT_EQ(lowered.arguments.at(1), op->getUrl());
}

TEST(HttpCallLogicalOperatorTest, LogicalInferenceRejectsANonintegerOrNullableRetriesField)
{
    for (const auto& retriesType :
         {DataType{DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE},
          DataType{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE}})
    {
        const auto input = child(Schema<UnqualifiedUnboundField, Ordered>{{Identifier::parse("retries"), retriesType}});
        const auto retries = FieldAccessLogicalFunction{input.getOutputSchema()[Identifier::parse("retries")].value()};
        EXPECT_THROW(
            static_cast<void>(
                HttpCallLogicalOperator::create(
                    input, ConstantValueLogicalFunction{text, "body"}, ConstantValueLogicalFunction{text, "http://localhost/"}, {}, retries)
                    .withInferredSchema()),
            Exception);
    }
}

TEST(HttpCallLogicalOperatorTest, ReflectionRoundTripRetainsConcreteTypeAndNamedLogicalInputs)
{
    const auto op = call();
    const auto schema = Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("response"), text}};
    const auto descriptor = SinkDescriptor::createNamed(
                                INVALID_SINK_ID,
                                Identifier::parse("http_test_sink"),
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
    const auto restoredSink = roots.front().getAs<SinkLogicalOperator>();
    const auto concrete = restoredSink->getChild().tryGetAs<HttpCallLogicalOperator>();
    ASSERT_TRUE(concrete.has_value());
    EXPECT_TRUE(StructuralFunctionEqual{}(concrete.value()->getBody(), op->getBody()));
    EXPECT_TRUE(StructuralFunctionEqual{}(concrete.value()->getUrl(), op->getUrl()));
}
}
