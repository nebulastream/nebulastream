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

#include <Statistics/Requests/RequestParamObj.hpp>
#include <Statistics/Requests/BuildRequestParamObj.hpp>
#include <Statistics/Requests/ProbeRequestParamObj.hpp>
#include <Statistics/Requests/DeleteRequestParamObj.hpp>

namespace NES {

class StatisticsTest : public Testing::BaseUnitTest {
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
    std::string expression = "SELECTIVITIES";
    std::string otherExpression = "x == 15";
    time_t startTime = 100;
    time_t endTime = 10;



    auto buildObj = Experimental::Statistics::BuildRequestParamObj(defaultLogicalSourceName,
                                                                   physicalSourceNames,
                                                                   defaultFieldName,
                                                                   expression);

    auto probeObj = Experimental::Statistics::ProbeRequestParamObj(defaultLogicalSourceName,
                                                                   physicalSourceNames,
                                                                   defaultFieldName,
                                                                   otherExpression,
                                                                   startTime,
                                                                   endTime);

    auto deleteObj = Experimental::Statistics::DeleteRequestParamObj(defaultLogicalSourceName,
                                                                     physicalSourceNames,
                                                                     defaultFieldName,
                                                                     expression,
                                                                     endTime);

    EXPECT_TRUE(buildObj.getLogicalSourceName() == defaultLogicalSourceName);
    EXPECT_TRUE(probeObj.getLogicalSourceName() == defaultLogicalSourceName);
    EXPECT_TRUE(deleteObj.getLogicalSourceName() == defaultLogicalSourceName);
    EXPECT_TRUE(buildObj.getPhysicalSourceNames() == physicalSourceNames);
    EXPECT_TRUE(probeObj.getPhysicalSourceNames() == physicalSourceNames);
    EXPECT_TRUE(deleteObj.getPhysicalSourceNames() == physicalSourceNames);
    EXPECT_TRUE(buildObj.getFieldName() == defaultFieldName);
    EXPECT_TRUE(probeObj.getFieldName() == defaultFieldName);
    EXPECT_TRUE(deleteObj.getFieldName() == defaultFieldName);
    EXPECT_TRUE(buildObj.getExpression() == expression);
    EXPECT_TRUE(probeObj.getExpression() == otherExpression);
    EXPECT_TRUE(deleteObj.getExpression() == expression);
    EXPECT_TRUE(probeObj.getStartTime() == startTime);
    EXPECT_TRUE(probeObj.getEndTime() == endTime);
    EXPECT_TRUE(deleteObj.getEndTime() == endTime);
};
}
