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

// clang-format off
#include <gtest/gtest.h>
// clang-format on
#include <API/QueryAPI.hpp>
#include <Catalogs/SourceCatalog.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/BinaryOperatorSortRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <iostream>

using namespace NES;

class BinaryOperatorSortRuleTest : public testing::Test {

  public:
    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::setupLogging("BinaryOperatorSortRuleTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup BinaryOperatorSortRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {}

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Setup BinaryOperatorSortRuleTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down BinaryOperatorSortRuleTest test class."); }
};

void setupSensorNodeAndStreamCatalog(const SourceCatalogPtr& streamCatalog) {
    NES_INFO("Setup BinaryOperatorSortRuleTest test case.");
    auto schema1 = Schema::create()
                       ->addField("id", BasicType::UINT32)
                       ->addField("value", BasicType::UINT64)
                       ->addField("ts", BasicType::UINT64);
    auto schema2 = Schema::create()
                       ->addField("id", BasicType::UINT32)
                       ->addField("value", BasicType::UINT64)
                       ->addField("ts", BasicType::UINT64);
    streamCatalog->addLogicalStream("src1", schema1);
    streamCatalog->addLogicalStream("src2", schema2);
}

TEST_F(BinaryOperatorSortRuleTest, testBinaryOperatorSortRuleForUnionWithUnSortedChildren) {

    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery = Query::from("src1");
    Query query = Query::from("src2").unionWith(subQuery).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto unionOperators = queryPlan->getOperatorByType<UnionLogicalOperatorNode>();
    EXPECT_EQ(unionOperators.size(), 1U);
    auto unionChildren = unionOperators[0]->getChildren();

    typeInferencePhase->execute(queryPlan);

    auto binaryOperatorSortRule = Optimizer::BinaryOperatorSortRule::create();
    binaryOperatorSortRule->apply(queryPlan);

    auto updatedUnionOperators = queryPlan->getOperatorByType<UnionLogicalOperatorNode>();
    EXPECT_EQ(updatedUnionOperators.size(), 1U);

    auto updatedUnionChildren = updatedUnionOperators[0]->getChildren();

    EXPECT_EQ(unionChildren[0], updatedUnionChildren[1]);
    EXPECT_EQ(unionChildren[1], updatedUnionChildren[0]);
}

TEST_F(BinaryOperatorSortRuleTest, testBinaryOperatorSortRuleForUnionWithSortedChildren) {

    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery = Query::from("src2");
    Query query = Query::from("src1").unionWith(subQuery).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto unionOperators = queryPlan->getOperatorByType<UnionLogicalOperatorNode>();
    EXPECT_EQ(unionOperators.size(), 1U);
    auto unionChildren = unionOperators[0]->getChildren();

    typeInferencePhase->execute(queryPlan);

    auto binaryOperatorSortRule = Optimizer::BinaryOperatorSortRule::create();
    binaryOperatorSortRule->apply(queryPlan);

    auto updatedUnionOperators = queryPlan->getOperatorByType<UnionLogicalOperatorNode>();
    EXPECT_EQ(updatedUnionOperators.size(), 1U);
    auto updatedUnionChildren = updatedUnionOperators[0]->getChildren();

    EXPECT_EQ(unionChildren[0], updatedUnionChildren[0]);
    EXPECT_EQ(unionChildren[1], updatedUnionChildren[1]);
}

TEST_F(BinaryOperatorSortRuleTest, testBinaryOperatorSortRuleForJoinWithUnSortedChildren) {

    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);

    auto windowType1 = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(4), Milliseconds(2));

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery = Query::from("src1");
    Query query = Query::from("src2")
                      .joinWith(subQuery)
                      .where(Attribute("id"))
                      .equalsTo(Attribute("id"))
                      .window(windowType1)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto joinOperators = queryPlan->getOperatorByType<JoinLogicalOperatorNode>();
    EXPECT_EQ(joinOperators.size(), 1U);
    auto joinChildren = joinOperators[0]->getChildren();

    typeInferencePhase->execute(queryPlan);

    auto binaryOperatorSortRule = Optimizer::BinaryOperatorSortRule::create();
    binaryOperatorSortRule->apply(queryPlan);

    auto updatedJoinOperators = queryPlan->getOperatorByType<JoinLogicalOperatorNode>();
    EXPECT_EQ(updatedJoinOperators.size(), 1U);
    auto updatedJoinChildren = updatedJoinOperators[0]->getChildren();

    EXPECT_EQ(joinChildren[0], updatedJoinChildren[1]);
    EXPECT_EQ(joinChildren[1], updatedJoinChildren[0]);
}

TEST_F(BinaryOperatorSortRuleTest, testBinaryOperatorSortRuleForJoinWithSortedChildren) {

    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);

    auto windowType1 = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(4), Milliseconds(2));

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery = Query::from("src2");
    Query query = Query::from("src1")
                      .joinWith(subQuery)
                      .where(Attribute("id"))
                      .equalsTo(Attribute("id"))
                      .window(windowType1)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto joinOperators = queryPlan->getOperatorByType<JoinLogicalOperatorNode>();
    EXPECT_EQ(joinOperators.size(), 1U);
    auto joinChildren = joinOperators[0]->getChildren();

    typeInferencePhase->execute(queryPlan);

    auto binaryOperatorSortRule = Optimizer::BinaryOperatorSortRule::create();
    binaryOperatorSortRule->apply(queryPlan);

    auto updatedJoinOperators = queryPlan->getOperatorByType<JoinLogicalOperatorNode>();
    EXPECT_EQ(updatedJoinOperators.size(), 1U);
    auto updatedJoinChildren = updatedJoinOperators[0]->getChildren();

    EXPECT_EQ(joinChildren[0], updatedJoinChildren[0]);
    EXPECT_EQ(joinChildren[1], updatedJoinChildren[1]);
}

TEST_F(BinaryOperatorSortRuleTest, testBinaryOperatorSortRuleForJoinAnUnionWithUnSortedChildren) {

    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);

    auto windowType1 = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(4), Milliseconds(2));

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery1 = Query::from("src1");
    Query subQuery2 = Query::from("src1");
    Query query = Query::from("src2")
                      .unionWith(subQuery1)
                      .joinWith(subQuery2)
                      .where(Attribute("id"))
                      .equalsTo(Attribute("id"))
                      .window(windowType1)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto unionOperators = queryPlan->getOperatorByType<UnionLogicalOperatorNode>();
    EXPECT_EQ(unionOperators.size(), 1U);
    auto unionChildren = unionOperators[0]->getChildren();

    auto joinOperators = queryPlan->getOperatorByType<JoinLogicalOperatorNode>();
    EXPECT_EQ(joinOperators.size(), 1U);
    auto joinChildren = joinOperators[0]->getChildren();

    typeInferencePhase->execute(queryPlan);

    auto binaryOperatorSortRule = Optimizer::BinaryOperatorSortRule::create();
    binaryOperatorSortRule->apply(queryPlan);

    auto updatedUnionOperators = queryPlan->getOperatorByType<UnionLogicalOperatorNode>();
    EXPECT_EQ(updatedUnionOperators.size(), 1U);
    auto updatedUnionChildren = updatedUnionOperators[0]->getChildren();

    EXPECT_EQ(unionChildren[0], updatedUnionChildren[1]);
    EXPECT_EQ(unionChildren[1], updatedUnionChildren[0]);

    auto updatedJoinOperators = queryPlan->getOperatorByType<JoinLogicalOperatorNode>();
    EXPECT_EQ(updatedJoinOperators.size(), 1U);
    auto updatedJoinChildren = updatedJoinOperators[0]->getChildren();

    EXPECT_EQ(joinChildren[0], updatedJoinChildren[1]);
    EXPECT_EQ(joinChildren[1], updatedJoinChildren[0]);
}

TEST_F(BinaryOperatorSortRuleTest, testBinaryOperatorSortRuleForJoinAndUnionWithSortedChildren) {

    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);

    auto windowType1 = SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(4), Milliseconds(2));

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery1 = Query::from("src2");
    Query subQuery2 = Query::from("src2");
    Query query = Query::from("src1")
                      .unionWith(subQuery1)
                      .joinWith(subQuery2)
                      .where(Attribute("id"))
                      .equalsTo(Attribute("id"))
                      .window(windowType1)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto unionOperators = queryPlan->getOperatorByType<UnionLogicalOperatorNode>();
    EXPECT_EQ(unionOperators.size(), 1U);
    auto unionChildren = unionOperators[0]->getChildren();

    auto joinOperators = queryPlan->getOperatorByType<JoinLogicalOperatorNode>();
    EXPECT_EQ(joinOperators.size(), 1U);
    auto joinChildren = joinOperators[0]->getChildren();

    typeInferencePhase->execute(queryPlan);

    auto binaryOperatorSortRule = Optimizer::BinaryOperatorSortRule::create();
    binaryOperatorSortRule->apply(queryPlan);

    auto updatedUnionOperators = queryPlan->getOperatorByType<UnionLogicalOperatorNode>();
    EXPECT_EQ(updatedUnionOperators.size(), 1U);
    auto updatedUnionChildren = updatedUnionOperators[0]->getChildren();

    EXPECT_EQ(unionChildren[0], updatedUnionChildren[0]);
    EXPECT_EQ(unionChildren[1], updatedUnionChildren[1]);

    auto updatedJoinOperators = queryPlan->getOperatorByType<JoinLogicalOperatorNode>();
    EXPECT_EQ(updatedJoinOperators.size(), 1U);
    auto updatedJoinChildren = updatedJoinOperators[0]->getChildren();

    EXPECT_EQ(joinChildren[0], updatedJoinChildren[0]);
    EXPECT_EQ(joinChildren[1], updatedJoinChildren[1]);
}