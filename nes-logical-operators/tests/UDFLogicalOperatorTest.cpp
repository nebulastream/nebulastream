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

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/UDFCallLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/UDFLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/UUID.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{
namespace
{

/// NOLINTBEGIN(bugprone-unchecked-optional-access)
class UDFLogicalOperatorTest : public ::testing::Test
{
public:
    static TypedLogicalOperator<SourceDescriptorLogicalOperator> makeSource()
    {
        const auto logical = LogicalSource{
            Identifier::parse("src"),
            Schema<UnqualifiedUnboundField, Ordered>{{Identifier::parse("a"), DataTypeProvider::provideDataType(DataType::Type::INT64)}}};
        const std::unordered_map<Identifier, std::string> sourceConfig{{Identifier::parse("file_path"), "/dev/null"}};
        return SourceDescriptorLogicalOperator::create(
            SourceDescriptor::create(
                PhysicalSourceId{1},
                logical,
                Identifier::parse("File"),
                Host{"localhost"},
                sourceConfig,
                {{Identifier::parse("type"), "CSV"}},
                false)
                .value());
    }

    static SinkDescriptor createSinkDescriptor(const Identifier& sinkName, const Schema<UnqualifiedUnboundField, Ordered>& schema)
    {
        const std::unordered_map<Identifier, std::string> sinkConfig{
            {Identifier::parse("FILE_PATH"), "/dev/null"}, {Identifier::parse("OUTPUT_FORMAT"), "CSV"}};
        return SinkDescriptor::createNamed(INVALID_SINK_ID, sinkName, Identifier::parse("file"), schema, Host{"localhost"}, sinkConfig, {})
            .value();
    }

    static LogicalFunction makeCall(const LogicalOperator& child)
    {
        const auto int64 = DataTypeProvider::provideDataType(DataType::Type::INT64);
        return UDFCallLogicalFunction{
            "inc",
            UdfDescriptor::fromSource(
                "inc",
                "/opt/bridge.so",
                InlineUdfSource{.parameterNames = {"x"}, .body = "return x + 1"},
                {int64},
                int64,
                UdfExecution::InProcess),
            {FieldAccessLogicalFunction{child.getOutputSchema().getFieldByName(Identifier::parse("a")).value()}}};
    }
};

TEST_F(UDFLogicalOperatorTest, AppendsNullableResultsToTheChildsFields)
{
    const LogicalOperator source = makeSource();
    const auto udf = UDFLogicalOperator::create(source, {{Identifier::parse("result"), makeCall(source)}});

    EXPECT_EQ(udf->getExecution(), UdfExecution::InProcess);
    const auto schema = udf->getOutputSchema();
    EXPECT_EQ(schema.size(), 2U);
    EXPECT_TRUE(schema.contains(Identifier::parse("a")));
    const auto result = schema.getFieldByName(Identifier::parse("result"));
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->getDataType().nullable);
}

TEST_F(UDFLogicalOperatorTest, RejectsAResultNamedLikeAChildField)
{
    const LogicalOperator source = makeSource();
    EXPECT_ANY_THROW(UDFLogicalOperator::create(source, {{Identifier::parse("a"), makeCall(source)}}));
}

TEST_F(UDFLogicalOperatorTest, ReflectionRoundTripKeepsTheCalls)
{
    const LogicalOperator source = makeSource();
    const auto udf = UDFLogicalOperator::create(source, {{Identifier::parse("result"), makeCall(source)}});
    const auto sinkDescriptor = createSinkDescriptor(
        Identifier::parse("test_sink"),
        Schema<UnqualifiedUnboundField, Ordered>{
            {Identifier::parse("a"), DataTypeProvider::provideDataType(DataType::Type::INT64)},
            {Identifier::parse("result"), DataTypeProvider::provideDataType(DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE)}});
    const auto sink = SinkLogicalOperator::create(sinkDescriptor).withChildrenUnsafe({udf});
    const LogicalPlan plan{QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()), {sink->withInferredSchema()}};

    const auto restored = QueryPlanSerializationUtil::deserializeQueryPlan(QueryPlanSerializationUtil::serializeQueryPlan(plan));

    const auto udfs = getOperatorByType<UDFLogicalOperator>(restored);
    ASSERT_EQ(udfs.size(), 1U);
    ASSERT_EQ(udfs[0]->getCalls().size(), 1U);
    const auto call = udfs[0]->getCalls().front().second.tryGetAs<UDFCallLogicalFunction>();
    ASSERT_TRUE(call.has_value());
    ASSERT_TRUE(call.value()->getDescriptor().has_value());
    EXPECT_EQ(call.value()->getDescriptor()->getExecution(), UdfExecution::InProcess);
    EXPECT_EQ(call.value()->getDescriptor()->getSource()->body, "return x + 1");
    EXPECT_EQ(udfs[0]->getExecution(), UdfExecution::InProcess);
}

/// NOLINTEND(bugprone-unchecked-optional-access)

}
}
