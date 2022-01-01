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

#include <API/QueryAPI.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <gtest/gtest.h>

namespace NES {

class OperatorPropertiesTest : public testing::Test {
  public:
    static void SetUpTestCase() { setupLogging(); }

    void SetUp() override {}

    void TearDown() override { NES_DEBUG("Tear down OperatorPropertiesTest Test."); }

  protected:
    static void setupLogging() {
        NES::setupLogging("OperatorPropertiesTest.log", NES::LOG_DEBUG);
        NES_DEBUG("Setup OperatorPropertiesTest test class.");
    }
};

// test assigning operators properties
TEST_F(OperatorPropertiesTest, testAssignProperties) {
    auto query = Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create());

    std::vector<std::map<std::string, std::any>> properties;

    // adding property of the source
    std::map<std::string, std::any> srcProp;
    srcProp.insert(std::make_pair("load", 1));
    srcProp.insert(std::make_pair("dmf", 1));
    properties.push_back(srcProp);

    // adding property of the filter
    std::map<std::string, std::any> filterProp;
    filterProp.insert(std::make_pair("load", 2));
    filterProp.insert(std::make_pair("dmf", 1));
    properties.push_back(filterProp);

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", 3));
    sinkProp.insert(std::make_pair("dmf", 1));
    properties.push_back(sinkProp);

    bool res = Util::assignPropertiesToQueryOperators(query.getQueryPlan(), properties);

    // Assert if the assignment success
    ASSERT_TRUE(res);

    // Assert if the property are added correctly
    auto queryPlanIterator = QueryPlanIterator(query.getQueryPlan()).begin();

    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load")), 1);
    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("dmf")), 1);
    ++queryPlanIterator;

    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load")), 2);
    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("dmf")), 1);
    ++queryPlanIterator;

    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load")), 3);
    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("dmf")), 1);
    ++queryPlanIterator;
}

// test assigning different types of operators properties
TEST_F(OperatorPropertiesTest, testAssignDifferentPropertyTypes) {
    auto query = Query::from("default_logical").sink(PrintSinkDescriptor::create());

    std::vector<std::map<std::string, std::any>> properties;

    // adding property of the source
    std::map<std::string, std::any> srcProp;
    srcProp.insert(std::make_pair("load_int", 1));
    srcProp.insert(std::make_pair("dmf_double", 0.5));
    srcProp.insert(std::make_pair("misc_str", std::string("xyz")));
    properties.push_back(srcProp);

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load_int", 1));
    sinkProp.insert(std::make_pair("dmf_double", 0.5));
    sinkProp.insert(std::make_pair("misc_str", std::string("xyz")));
    properties.push_back(sinkProp);

    bool res = Util::assignPropertiesToQueryOperators(query.getQueryPlan(), properties);

    // Assert if the assignment success
    ASSERT_TRUE(res);

    // Assert if the property are added correctly
    auto queryPlanIterator = QueryPlanIterator(query.getQueryPlan()).begin();

    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load_int")), 1);
    ASSERT_EQ(std::any_cast<double>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("dmf_double")), 0.5);
    ASSERT_EQ(std::any_cast<std::string>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("misc_str")), "xyz");
    ++queryPlanIterator;

    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load_int")), 1);
    ASSERT_EQ(std::any_cast<double>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("dmf_double")), 0.5);
    ASSERT_EQ(std::any_cast<std::string>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("misc_str")), "xyz");
    ++queryPlanIterator;
}

// test on providing more properties than the number of operators in the query
TEST_F(OperatorPropertiesTest, testAssignWithMorePropertiesThanOperators) {
    auto query = Query::from("default_logical").sink(PrintSinkDescriptor::create());

    std::vector<std::map<std::string, std::any>> properties;

    // adding property of the source
    std::map<std::string, std::any> srcProp;
    srcProp.insert(std::make_pair("load", 1));
    srcProp.insert(std::make_pair("dmf", 1));
    properties.push_back(srcProp);

    // adding property of the filter
    std::map<std::string, std::any> filterProp;
    filterProp.insert(std::make_pair("load", 2));
    filterProp.insert(std::make_pair("dmf", 1));
    properties.push_back(filterProp);

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", 3));
    sinkProp.insert(std::make_pair("dmf", 1));
    properties.push_back(sinkProp);

    // this should return false as properties of all operators has to be supplied if one of them is supplied
    bool res = Util::assignPropertiesToQueryOperators(query.getQueryPlan(), properties);
    ASSERT_FALSE(res);
}

// test on providing less properties than the number of operators in the query
TEST_F(OperatorPropertiesTest, testAssignWithLessPropertiesThanOperators) {
    auto query = Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create());

    std::vector<std::map<std::string, std::any>> properties;

    // adding property of the source
    std::map<std::string, std::any> srcProp;
    srcProp.insert(std::make_pair("load", "1"));
    srcProp.insert(std::make_pair("dmf", "1"));
    properties.push_back(srcProp);

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", "3"));
    sinkProp.insert(std::make_pair("dmf", "1"));
    properties.push_back(sinkProp);

    // this should return false as properties of all operators has to be supplied if one of them is supplied
    bool res = Util::assignPropertiesToQueryOperators(query.getQueryPlan(), properties);
    ASSERT_FALSE(res);
}

// test with binary operator
TEST_F(OperatorPropertiesTest, testAssignWithBinaryOperator) {

    auto subQuery = Query::from("default_logical").filter(Attribute("field_1") <= 10);

    auto query = Query::from("default_logical")
                     .joinWith(subQuery)
                     .where(Attribute("id"))
                     .equalsTo(Attribute("id"))
                     .window(TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)))
                     .sink(PrintSinkDescriptor::create());

    std::vector<std::map<std::string, std::any>> properties;

    // Adding properties of each operator. The order should be the same as used in the QueryPlanIterator
    // adding property of the source
    std::map<std::string, std::any> srcProp;
    srcProp.insert(std::make_pair("load", 1));
    properties.push_back(srcProp);

    // adding property of the source watermark (watermarks are added automatically)
    std::map<std::string, std::any> srcWatermarkProp;
    srcWatermarkProp.insert(std::make_pair("load", 2));
    properties.push_back(srcWatermarkProp);

    // adding property of the source in the sub query
    std::map<std::string, std::any> srcSubProp;
    srcSubProp.insert(std::make_pair("load", 3));
    properties.push_back(srcSubProp);

    // adding property of the watermark in the sub query (watermarks are added automatically)
    std::map<std::string, std::any> srcSubWatermarkProp;
    srcSubWatermarkProp.insert(std::make_pair("load", 4));
    properties.push_back(srcSubWatermarkProp);

    // adding property of the filter in the sub query
    std::map<std::string, std::any> filterSubProp;
    filterSubProp.insert(std::make_pair("load", 5));
    properties.push_back(filterSubProp);

    // adding property of the join
    std::map<std::string, std::any> joinProp;
    joinProp.insert(std::make_pair("load", 6));
    properties.push_back(joinProp);

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", 7));
    properties.push_back(sinkProp);

    bool res = Util::assignPropertiesToQueryOperators(query.getQueryPlan(), properties);
    ASSERT_TRUE(res);

    auto queryPlanIterator = QueryPlanIterator(query.getQueryPlan()).begin();

    // Assert the property values
    // source (main)
    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load")), 1);
    ++queryPlanIterator;
    // source (main) watermark
    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load")), 2);
    ++queryPlanIterator;
    // source of subquery
    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load")), 3);
    ++queryPlanIterator;
    // watermark of subquery
    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load")), 4);
    ++queryPlanIterator;
    // filter in the subquery
    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load")), 5);
    ++queryPlanIterator;
    // join
    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load")), 6);
    ++queryPlanIterator;
    // sink
    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load")), 7);
    ++queryPlanIterator;
}

}// namespace NES
