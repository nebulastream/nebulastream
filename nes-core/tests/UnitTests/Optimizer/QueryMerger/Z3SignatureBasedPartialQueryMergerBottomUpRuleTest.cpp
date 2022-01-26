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

// clang-format off
#include <gtest/gtest.h>
// clang-format on
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerBottomUpRule.hpp>
#include <Optimizer/QuerySignatures/SignatureEqualityUtil.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlanChangeLog.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <z3++.h>

using namespace NES;
using namespace Configurations;

class Z3SignatureBasedPartialQueryMergerBottomUpRuleTest : public testing::Test {

  public:
    SchemaPtr schema;
    SourceCatalogPtr streamCatalog;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::setupLogging("Z3SignatureBasedPartialQueryMergerBottomUpRuleTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup Z3SignatureBasedPartialQueryMergerBottomUpRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        schema = Schema::create()
                     ->addField("ts", BasicType::UINT32)
                     ->addField("type", BasicType::UINT32)
                     ->addField("id", BasicType::UINT32)
                     ->addField("value", BasicType::UINT64)
                     ->addField("id1", BasicType::UINT32)
                     ->addField("value1", BasicType::UINT64);
        streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
        streamCatalog->addLogicalStream("car", schema);
        streamCatalog->addLogicalStream("bike", schema);
        streamCatalog->addLogicalStream("truck", schema);

        TopologyNodePtr sourceNode1 = TopologyNode::create(2, "localhost", 123, 124, 4);

        auto logicalSourceCar = streamCatalog->getStreamForLogicalStream("car");
        auto physicalSourceCar = PhysicalSource::create("car", "testCar", DefaultSourceType::create());
        SourceCatalogEntryPtr streamCatalogEntry1 =
            std::make_shared<SourceCatalogEntry>(physicalSourceCar, logicalSourceCar, sourceNode1);
        streamCatalog->addPhysicalSource("car", streamCatalogEntry1);

        auto logicalSourceBike = streamCatalog->getStreamForLogicalStream("bike");
        auto physicalSourceBike = PhysicalSource::create("bike", "testBike", DefaultSourceType::create());
        SourceCatalogEntryPtr streamCatalogEntry2 =
            std::make_shared<SourceCatalogEntry>(physicalSourceBike, logicalSourceBike, sourceNode1);
        streamCatalog->addPhysicalSource("bike", streamCatalogEntry2);

        auto logicalSourceTruck = streamCatalog->getStreamForLogicalStream("truck");
        auto physicalSourceTruck = PhysicalSource::create("truck", "testTruck", DefaultSourceType::create());
        SourceCatalogEntryPtr streamCatalogEntry3 =
            std::make_shared<SourceCatalogEntry>(physicalSourceCar, logicalSourceCar, sourceNode1);
        streamCatalog->addPhysicalSource("truck", streamCatalogEntry3);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Setup Z3SignatureBasedPartialQueryMergerRuleTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down Z3SignatureBasedPartialQueryMergerRuleTest test class."); }
};

/**
 * @brief Test applying SignatureBasedPartialQueryMergerRuleTest on Global query plan with same queries
 */
TEST_F(Z3SignatureBasedPartialQueryMergerBottomUpRuleTest, testMergingEqualQueries) {

    auto topologySpecificReWrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog, false, 2, 4);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context, Optimizer::QueryMergerRule::Z3SignatureBasedPartialQueryMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan2 = topologySpecificReWrite->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerBottomUpRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    // assert children equality
    for (const auto& sink1Child : updatedRootOperators1[0]->getChildren()) {
        bool found = false;
        for (const auto& sink2Child : updatedRootOperators1[1]->getChildren()) {
            if (sink1Child->equal(sink2Child)) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found);
    }
}

/**
 * @brief Test applying SignatureBasedPartialQueryMergerRuleTest on Global query plan with partially same queries
 */
TEST_F(Z3SignatureBasedPartialQueryMergerBottomUpRuleTest, testMergingPartiallyEqualQueries) {

    auto topologySpecificReWrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog, false, 2, 4);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id1") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("value1") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan2 = topologySpecificReWrite->execute(queryPlan2);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context, Optimizer::QueryMergerRule::Z3SignatureBasedPartialQueryMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerBottomUpRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    NES_INFO(updatedSharedQueryPlan1->toString());

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (const auto& sink1Child : updatedRootOperators1[0]->getChildren()) {
        bool found = false;
        for (const auto& sink2Child : updatedRootOperators1[1]->getChildren()) {
            EXPECT_NE(sink1Child, sink2Child);
            auto sink1ChildGrandChild = sink1Child->getChildren();
            auto sink2ChildGrandChild = sink2Child->getChildren();
            EXPECT_TRUE(sink1ChildGrandChild.size() == 1);
            EXPECT_TRUE(sink2ChildGrandChild.size() == 1);
            if (sink1ChildGrandChild[0]->equal(sink2ChildGrandChild[0])) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found);
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with different source
 */
TEST_F(Z3SignatureBasedPartialQueryMergerBottomUpRuleTest, testMergingQueriesWithDifferentSources) {

    // Prepare
    auto topologySpecificReWrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog, false, 2, 4);

    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("truck").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan2 = topologySpecificReWrite->execute(queryPlan2);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context, Optimizer::QueryMergerRule::Z3SignatureBasedPartialQueryMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerBottomUpRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (const auto& sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (const auto& sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

TEST_F(Z3SignatureBasedPartialQueryMergerBottomUpRuleTest, testMergingMoreThanTwoPartiallyEqualQueries) {

    auto topologySpecificReWrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog, false, 2, 4);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").filter(Attribute("id") < 45).map(Attribute("queryId") = 1).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car").filter(Attribute("id") < 45).map(Attribute("queryId") = 2).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    Query query3 = Query::from("car").filter(Attribute("id") < 45).map(Attribute("queryId") = 3).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan3 = query3.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator3 = queryPlan3->getSinkOperators()[0];
    QueryId queryId3 = PlanIdGenerator::getNextQueryId();
    queryPlan3->setQueryId(queryId3);

    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan2 = topologySpecificReWrite->execute(queryPlan2);
    queryPlan3 = topologySpecificReWrite->execute(queryPlan3);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);
    typeInferencePhase->execute(queryPlan3);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context, Optimizer::QueryMergerRule::Z3SignatureBasedPartialQueryMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);
    z3InferencePhase->execute(queryPlan3);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute merging shared query plan 1 and 2
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerBottomUpRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQueryPlansDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    NES_INFO("Shared Plan After Merging with Query 2\n" << updatedSharedQueryPlansDeploy[0]->getQueryPlan()->toString());
    updatedSharedQueryPlansDeploy[0]->markAsDeployed();

    // execute merging shared query plan 1 and 3
    globalQueryPlan->addQueryPlan(queryPlan3);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    updatedSharedQueryPlansDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_EQ(updatedSharedQueryPlansDeploy.size(), 1U);

    auto updatedSharedQueryPlan1 = updatedSharedQueryPlansDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    NES_INFO("Shared Plan After Merging with Query 3\n" << updatedSharedQueryPlan1->toString());

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_EQ(updatedRootOperators1.size(), 3U);
    EXPECT_EQ(updatedSharedQueryPlan1->getSourceOperators().size(), 1U);

    auto changeLog = updatedSharedQueryPlansDeploy[0]->getChangeLog();
    EXPECT_EQ(changeLog->getAddition().size(), 1U);

    for (const auto& addition : changeLog->getAddition()) {
        auto operatorNewBranchAddedTo = updatedSharedQueryPlan1->getOperatorWithId(addition.first->getId());
        EXPECT_TRUE(operatorNewBranchAddedTo->instanceOf<FilterLogicalOperatorNode>());
        // Three different map operators are added
        EXPECT_TRUE(operatorNewBranchAddedTo->getParents().size() == 3U);
        for (const auto& parent : operatorNewBranchAddedTo->getParents()) {
            EXPECT_TRUE(parent->instanceOf<MapLogicalOperatorNode>());
        }
        // There are one physical sources
        EXPECT_TRUE(operatorNewBranchAddedTo->getChildren().size() == 1U);
        for (const auto& child : operatorNewBranchAddedTo->getChildren()) {
            EXPECT_TRUE(child->instanceOf<SourceLogicalOperatorNode>());
        }
    }
}

TEST_F(Z3SignatureBasedPartialQueryMergerBottomUpRuleTest, testMergingPartiallyEqualQueriesWithQueryStopChangeLog) {

    auto topologySpecificReWrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog, false, 2, 4);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car").sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan2 = topologySpecificReWrite->execute(queryPlan2);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context, Optimizer::QueryMergerRule::Z3SignatureBasedPartialQueryMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerBottomUpRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    NES_INFO(updatedSharedQueryPlan1->toString());

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    // stop query 1
    globalQueryPlan->removeQuery(queryPlan1->getQueryId());

    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedPlanAfterStopToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedPlanAfterStopToDeploy.size() == 1);

    auto updatedSharedPlanAfterStop = updatedSharedPlanAfterStopToDeploy[0]->getQueryPlan();
    auto changeLog = updatedSharedPlanAfterStopToDeploy[0]->getChangeLog();

    EXPECT_TRUE(updatedSharedPlanAfterStop);
    NES_INFO(updatedSharedPlanAfterStop->toString());

    ASSERT_EQ(changeLog->getRemoval().size(), 1UL);

    //assert that the sink operators have same up-stream operator
    auto rootOperatorsAfterStop = updatedSharedPlanAfterStop->getRootOperators();
    EXPECT_TRUE(rootOperatorsAfterStop.size() == 1);

    EXPECT_TRUE(updatedSharedPlanAfterStop->getSourceOperators().size() == 1);

    auto operatorsInQueryPlan2 = QueryPlanIterator(queryPlan2).snapshot();
    auto operatorsInSharedPlanAfterStop = QueryPlanIterator(updatedSharedPlanAfterStop).snapshot();

    EXPECT_TRUE(operatorsInQueryPlan2.size() == operatorsInSharedPlanAfterStop.size());
}

TEST_F(Z3SignatureBasedPartialQueryMergerBottomUpRuleTest, testMergingPartiallyEqualQueriesWithQueryStop) {

    auto topologySpecificReWrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog, false, 2, 4);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id1") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("value1") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan2 = topologySpecificReWrite->execute(queryPlan2);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context, Optimizer::QueryMergerRule::Z3SignatureBasedPartialQueryMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerBottomUpRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    NES_INFO(updatedSharedQueryPlan1->toString());

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (const auto& sink1Child : updatedRootOperators1[0]->getChildren()) {
        bool found = false;
        for (const auto& sink2Child : updatedRootOperators1[1]->getChildren()) {
            EXPECT_NE(sink1Child, sink2Child);
            auto sink1ChildGrandChild = sink1Child->getChildren();
            auto sink2ChildGrandChild = sink2Child->getChildren();
            EXPECT_TRUE(sink1ChildGrandChild.size() == 1);
            EXPECT_TRUE(sink2ChildGrandChild.size() == 1);
            if (sink1ChildGrandChild[0]->equal(sink2ChildGrandChild[0])) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found);
    }

    // stop query 1
    globalQueryPlan->removeQuery(queryPlan1->getQueryId());

    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedPlanAfterStopToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedPlanAfterStopToDeploy.size() == 1);

    auto updatedSharedPlanAfterStop = updatedSharedPlanAfterStopToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    NES_INFO(updatedSharedQueryPlan1->toString());

    //assert that the sink operators have same up-stream operator
    auto rootOperatorsAfterStop = updatedSharedPlanAfterStop->getRootOperators();
    EXPECT_TRUE(rootOperatorsAfterStop.size() == 1);

    EXPECT_TRUE(updatedSharedPlanAfterStop->getSourceOperators().size() == 1);

    auto operatorsInQueryPlan2 = QueryPlanIterator(queryPlan2).snapshot();
    auto operatorsInSharedPlanAfterStop = QueryPlanIterator(updatedSharedPlanAfterStop).snapshot();

    EXPECT_TRUE(operatorsInQueryPlan2.size() == operatorsInSharedPlanAfterStop.size());
}