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

#include <Statistics/Requests/StatCreateRequest.hpp>
#include <Statistics/Requests/StatDeleteRequest.hpp>
#include <Statistics/Requests/StatProbeRequest.hpp>
#include <Statistics/Requests/StatRequest.hpp>
#include <Statistics/StatCollectors/StatCollectorType.hpp>

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
    Experimental::Statistics::StatCollectorType statCollectorType = Experimental::Statistics::StatCollectorType::COUNT_MIN;
    std::string probeExpression = "x == 15";
    time_t startTime = 100;
    time_t endTime = 10;

    auto createObj = Experimental::Statistics::StatCreateRequest(defaultLogicalSourceName,
                                                                 defaultFieldName,
                                                                 Experimental::Statistics::StatCollectorType::COUNT_MIN);

    auto probeObj = Experimental::Statistics::StatProbeRequest(defaultLogicalSourceName,
                                                               defaultFieldName,
                                                               Experimental::Statistics::StatCollectorType::COUNT_MIN,
                                                               probeExpression,
                                                               physicalSourceNames,
                                                               startTime,
                                                               endTime);

    auto deleteObj = Experimental::Statistics::StatDeleteRequest(defaultLogicalSourceName,
                                                                 defaultFieldName,
                                                                 Experimental::Statistics::StatCollectorType::COUNT_MIN,
                                                                 endTime);

    EXPECT_EQ(createObj.getLogicalSourceName(), defaultLogicalSourceName);
    EXPECT_EQ(probeObj.getLogicalSourceName(), defaultLogicalSourceName);
    EXPECT_EQ(deleteObj.getLogicalSourceName(), defaultLogicalSourceName);
    EXPECT_EQ(probeObj.getPhysicalSourceNames(), physicalSourceNames);
    EXPECT_EQ(createObj.getFieldName(), defaultFieldName);
    EXPECT_EQ(probeObj.getFieldName(), defaultFieldName);
    EXPECT_EQ(deleteObj.getFieldName(), defaultFieldName);
    EXPECT_EQ(createObj.getStatCollectorType(), Experimental::Statistics::StatCollectorType::COUNT_MIN);
    EXPECT_EQ(probeObj.getStatCollectorType(), Experimental::Statistics::StatCollectorType::COUNT_MIN);
    EXPECT_EQ(deleteObj.getStatCollectorType(), Experimental::Statistics::StatCollectorType::COUNT_MIN);
    EXPECT_EQ(probeObj.getProbeExpression(), probeExpression);
    EXPECT_EQ(probeObj.getStartTime(), startTime);
    EXPECT_EQ(probeObj.getEndTime(), endTime);
    EXPECT_EQ(deleteObj.getEndTime(), endTime);
};

}// namespace NES
