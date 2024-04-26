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

#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/QueryAPI.hpp>
#include <API/Windowing.hpp>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/TCPSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowOperator.hpp>
#include <Operators/Serialization/OperatorSerializationUtil.hpp>
#include <Operators/Serialization/QueryPlanSerializationUtil.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <SerializableOperator.pb.h>
#include <Types/SlidingWindow.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <tuple>

class WindowSerializationTest : public Testing::BaseUnitTest,
                                public ::testing::WithParamInterface<std::tuple<TimeMeasure, TimeUnit>> {

  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SerializationUtilTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup WindowSerializationTest test class.");
    }
};

TEST_P(WindowSerializationTest, testSerializeDeserializeWindowOperators) {

    auto operatorId = OperatorId(1);

    auto descriptors = {
        SlidingWindow::of(EventTime(Attribute("event_time"), std::get<1>(GetParam())),
                          std::get<0>(GetParam()),
                          TimeMeasure(std::get<0>(GetParam()).getTime() / 2)),
        SlidingWindow::of(IngestionTime(), std::get<0>(GetParam()), TimeMeasure(std::get<0>(GetParam()).getTime() / 2)),
        TumblingWindow::of(EventTime(Attribute("event_time"), std::get<1>(GetParam())), std::get<0>(GetParam())),
        TumblingWindow::of(IngestionTime(), std::get<0>(GetParam()))};

    for (const auto& window_descriptor : descriptors) {
        auto logical_window_descriptor = LogicalWindowDescriptor::create({}, window_descriptor, 0xC0FFEE);
        auto windowOperator = LogicalOperatorFactory::createWindowOperator(logical_window_descriptor, operatorId);

        auto serializedOperator = OperatorSerializationUtil::serializeOperator(windowOperator);

        auto deserializedOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);

        // Two distinct objects
        EXPECT_NE(windowOperator, deserializedOperator);
        EXPECT_TRUE(deserializedOperator->instanceOf<LogicalWindowOperator>());
        EXPECT_TRUE(windowOperator->equal(deserializedOperator));
    }
}

TEST_P(WindowSerializationTest, testSerializeDeserializeWindowJoinOperators) {

    auto operatorId = OperatorId(1);

    auto descriptors = {
        SlidingWindow::of(EventTime(Attribute("event_time"), std::get<1>(GetParam())),
                          std::get<0>(GetParam()),
                          TimeMeasure(std::get<0>(GetParam()).getTime() / 2)),

        SlidingWindow::of(IngestionTime(), std::get<0>(GetParam()), TimeMeasure(std::get<0>(GetParam()).getTime() / 2)),
        TumblingWindow::of(EventTime(Attribute("event_time"), std::get<1>(GetParam())), std::get<0>(GetParam())),
        TumblingWindow::of(IngestionTime(), std::get<0>(GetParam()))};

    for (const auto& logical_window_descriptor : descriptors) {
        auto joinDescriptor = Join::LogicalJoinDescriptor::create(
            FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "key")->as<FieldAccessExpressionNode>(),
            FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "key")->as<FieldAccessExpressionNode>(),
            logical_window_descriptor,
            1,
            1,
            Join::LogicalJoinDescriptor::JoinType::INNER_JOIN);

        auto joinOperator = LogicalOperatorFactory::createJoinOperator(joinDescriptor, operatorId);
        std::dynamic_pointer_cast<OriginIdAssignmentOperator>(joinOperator)->setOriginId(OriginId(1));

        auto serializedOperator = OperatorSerializationUtil::serializeOperator(joinOperator->as<LogicalJoinOperator>());
        auto deserializedOperator = OperatorSerializationUtil::deserializeOperator(serializedOperator);

        // Two distinct objects
        EXPECT_NE(joinOperator, deserializedOperator);
        EXPECT_TRUE(deserializedOperator->instanceOf<LogicalJoinOperator>());
        EXPECT_TRUE(joinOperator->equal(deserializedOperator));
    }
}

// Run Deserialization Test over all supported Time Units
INSTANTIATE_TEST_SUITE_P(MyInstantiation,
                         WindowSerializationTest,
                         ::testing::Combine(::testing::Values(Milliseconds(1),
                                                              Milliseconds(200),
                                                              Milliseconds(1000),
                                                              Hours(1),
                                                              Hours(6),
                                                              Hours(24),
                                                              Days(1),
                                                              Days(10),
                                                              Minutes(1),
                                                              Minutes(5),
                                                              Minutes(60),
                                                              Seconds(1),
                                                              Seconds(5),
                                                              Seconds(60)),
                                            ::testing::Values(Milliseconds(), Hours(), Days(), Minutes(), Seconds())));
