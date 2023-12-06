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

#include <BaseIntegrationTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

#include <Operators/LogicalOperators/Statistics/StatisticCollectorType.hpp>
#include <Statistics/Requests/StatisticCreateRequest.hpp>
#include <Statistics/Requests/StatisticDeleteRequest.hpp>
#include <Statistics/Requests/StatisticProbeRequest.hpp>
#include <Statistics/Requests/StatisticRequest.hpp>

#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/Statistics/CountMinDescriptor.hpp>
#include <Operators/LogicalOperators/Statistics/StatisticCollectorType.hpp>
#include <Operators/LogicalOperators/Statistics/WindowStatisticDescriptor.hpp>
#include <Operators/LogicalOperators/Statistics/WindowStatisticLogicalOperatorNode.hpp>

namespace NES {

class StatisticsTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StatisticsTest.log", NES::LogLevel::LOG_DEBUG);

        NES_INFO("Setup StatisticsTest test class.");
    }

    static void TearDownTestCase() { NES_DEBUG("Tear down StatisticsTest test class."); }
};

TEST_F(StatisticsTest, requestsTest) {
    std::string defaultLogicalSourceName = "defaultLogicalSourceName";
    std::vector<std::string> physicalSourceNames(10, "defaultPhysicalSourceName");
    std::string defaultFieldName = "defaultFieldName";
    Experimental::Statistics::StatisticCollectorType statisticCollectorType =
        Experimental::Statistics::StatisticCollectorType::COUNT_MIN;
    std::string probeExpression = "x == 15";
    time_t startTime = 100;
    time_t endTime = 10;

    auto createObj =
        Experimental::Statistics::StatisticCreateRequest(defaultLogicalSourceName, defaultFieldName, statisticCollectorType);

    auto probeObj = Experimental::Statistics::StatisticProbeRequest(defaultLogicalSourceName,
                                                                    defaultFieldName,
                                                                    statisticCollectorType,
                                                                    probeExpression,
                                                                    physicalSourceNames,
                                                                    startTime,
                                                                    endTime);

    auto deleteObj = Experimental::Statistics::StatisticDeleteRequest(defaultLogicalSourceName,
                                                                      defaultFieldName,
                                                                      statisticCollectorType,
                                                                      endTime);

    EXPECT_EQ(createObj.getLogicalSourceName(), defaultLogicalSourceName);
    EXPECT_EQ(probeObj.getLogicalSourceName(), defaultLogicalSourceName);
    EXPECT_EQ(deleteObj.getLogicalSourceName(), defaultLogicalSourceName);
    EXPECT_EQ(probeObj.getPhysicalSourceNames(), physicalSourceNames);
    EXPECT_EQ(createObj.getFieldName(), defaultFieldName);
    EXPECT_EQ(probeObj.getFieldName(), defaultFieldName);
    EXPECT_EQ(deleteObj.getFieldName(), defaultFieldName);
    EXPECT_EQ(createObj.getStatisticCollectorType(), statisticCollectorType);
    EXPECT_EQ(probeObj.getStatisticCollectorType(), statisticCollectorType);
    EXPECT_EQ(deleteObj.getStatisticCollectorType(), statisticCollectorType);
    EXPECT_EQ(probeObj.getProbeExpression(), probeExpression);
    EXPECT_EQ(probeObj.getStartTime(), startTime);
    EXPECT_EQ(probeObj.getEndTime(), endTime);
    EXPECT_EQ(deleteObj.getEndTime(), endTime);
};

TEST_F(StatisticsTest, statisticLogicalOperatorTest) {
    auto depth = 5;
    auto width = 10000;

    auto logicalSourceName = "defaultLogicalSourceName";
    auto physicalSourceName = "defaultPhysicalSourceName";
    auto synopsisFieldName = "synopsisSourceDataFieldName";
    auto topologyNodeId = 0;
    auto statisticCollectorType = NES::Experimental::Statistics::StatisticCollectorType::COUNT_MIN;
    auto windowSize = 5;
    auto slideFactor = 1;

    auto statisticDescriptor = std::make_shared<Experimental::Statistics::CountMinDescriptor>(depth, width);

    auto logicalUnaryOperatorNode = LogicalOperatorFactory::createStatisticOperator(statisticDescriptor,
                                                                                   synopsisFieldName,
                                                                                   windowSize,
                                                                                   slideFactor);

    auto statisticOperator = std::dynamic_pointer_cast<NES::Experimental::Statistics::WindowStatisticLogicalOperatorNode>(logicalUnaryOperatorNode);

    EXPECT_EQ(statisticOperator->getSynopsisFieldName(), synopsisFieldName);
    EXPECT_EQ(statisticOperator->getWindowSize(), windowSize);
    EXPECT_EQ(statisticOperator->getSlideFactor(), slideFactor);

    auto desc= statisticOperator->getStatisticDescriptor();
    auto cmDesc = std::dynamic_pointer_cast<NES::Experimental::Statistics::CountMinDescriptor>(desc);

    EXPECT_EQ(cmDesc->getDepth(), depth);
    EXPECT_EQ(cmDesc->getWidth(), width);
}

}// namespace NES
