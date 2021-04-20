/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <API/Query.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <gtest/gtest.h>

namespace NES {

class OperatorPropertiesTest : public testing::Test {
  public:
    static void SetUpTestCase() { setupLogging(); }

    void SetUp() {}

    void TearDown() { NES_DEBUG("Tear down OperatorPropertiesTest Test."); }

  protected:
    static void setupLogging() {
        NES::setupLogging("OperatorPropertiesTest.log", NES::LOG_DEBUG);
        NES_DEBUG("Setup OperatorPropertiesTest test class.");
    }
};

TEST_F(OperatorPropertiesTest, testAssignProperties) {
    auto q1 = Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create());

    std::vector<std::map<std::string, std::string>> properties;

    // adding property of the source
    std::map<std::string, std::string> srcProp;
    srcProp.insert(std::make_pair("load","1"));
    srcProp.insert(std::make_pair("dmf","1"));
    properties.push_back(srcProp);

    // adding property of the filter
    std::map<std::string, std::string> filterProp;
    filterProp.insert(std::make_pair("load","2"));
    filterProp.insert(std::make_pair("dmf","0.25"));
    properties.push_back(filterProp);

    // adding property of the sink
    std::map<std::string, std::string> sinkProp;
    sinkProp.insert(std::make_pair("load","3"));
    sinkProp.insert(std::make_pair("dmf","1"));
    properties.push_back(sinkProp);

    UtilityFunctions::assignPropertiesToQueryOperators(q1, properties);

    // Assert if the property are added correctly
    auto queryPlanIterator = QueryPlanIterator(q1.getQueryPlan()).begin();

    ASSERT_EQ((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load"), "1");
    ASSERT_EQ((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("dmf"), "1");
    ++queryPlanIterator;

    ASSERT_EQ((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load"), "2");
    ASSERT_EQ((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("dmf"), "0.25");
    ++queryPlanIterator;

    ASSERT_EQ((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load"), "3");
    ASSERT_EQ((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("dmf"), "1");
    ++queryPlanIterator;
}

} // namespace NES
