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
#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerRule.hpp>
#include <Optimizer/Utils/SignatureEqualityUtil.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryMetaData.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Util/Logger.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <iostream>
#include <z3++.h>

using namespace NES;

class Z3SignatureBasedPartialQueryMergerRuleTest : public testing::Test {

  public:
    SchemaPtr schema;
    StreamCatalogPtr streamCatalog;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::setupLogging("Z3SignatureBasedPartialQueryMergerRuleTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup Z3SignatureBasedPartialQueryMergerRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        schema = Schema::create()
                     ->addField("ts", BasicType::UINT32)
                     ->addField("type", BasicType::UINT32)
                     ->addField("id", BasicType::UINT32)
                     ->addField("value", BasicType::UINT64)
                     ->addField("id1", BasicType::UINT32)
                     ->addField("value1", BasicType::UINT64);
        streamCatalog = std::make_shared<StreamCatalog>();
        streamCatalog->addLogicalStream("car", schema);
        streamCatalog->addLogicalStream("bike", schema);
        streamCatalog->addLogicalStream("truck", schema);
    }

    /* Will be called before a test is executed. */
    void TearDown() { NES_INFO("Setup Z3SignatureBasedPartialQueryMergerRuleTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down Z3SignatureBasedPartialQueryMergerRuleTest test class."); }
};

/**
 * @brief Test applying SignatureBasedPartialQueryMergerRuleTest on Global query plan with same queries
 */
TEST_F(Z3SignatureBasedPartialQueryMergerRuleTest, testMergingEqualQueries) {
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

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context, true);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (auto sink1Children : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2Children : updatedRootOperators1[1]->getChildren()) {
            EXPECT_EQ(sink1Children, sink2Children);
        }
    }
}

/**
 * @brief Test applying SignatureBasedPartialQueryMergerRuleTest on Global query plan with partially same queries
 */
TEST_F(Z3SignatureBasedPartialQueryMergerRuleTest, testMergingPartiallyEqualQueries) {
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

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (auto sink1Child : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2Child : updatedRootOperators1[1]->getChildren()) {
            EXPECT_NE(sink1Child, sink2Child);
            auto sink1ChildGrandChild = sink1Child->getChildren();
            auto sink2ChildGrandChild = sink2Child->getChildren();
            EXPECT_TRUE(sink1ChildGrandChild.size() == 1);
            EXPECT_TRUE(sink2ChildGrandChild.size() == 1);
            EXPECT_EQ(sink1ChildGrandChild[0], sink2ChildGrandChild[0]);
        }
    }
}

/**
 * @brief Test applying SignatureBasedEqualQueryMergerRule on Global query plan with two queries with different source
 */
TEST_F(Z3SignatureBasedPartialQueryMergerRuleTest, testMergingQueriesWithDifferentSources) {
    // Prepare
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

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //assert
    auto sharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQMToDeploy.size() == 2);

    auto sharedQueryPlan1 = sharedQMToDeploy[0]->getQueryPlan();
    auto sharedQueryPlan2 = sharedQMToDeploy[1]->getQueryPlan();

    //assert that the up-stream operator of sink has only one down-stream operator
    auto rootOperators1 = sharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(rootOperators1.size() == 1);

    auto root1Children = rootOperators1[0]->getChildren();
    EXPECT_TRUE(root1Children.size() == 1);
    EXPECT_TRUE(root1Children[0]->getParents().size() == 1);

    auto rootOperators2 = sharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(rootOperators2.size() == 1);

    auto root2Children = rootOperators2[0]->getChildren();
    EXPECT_TRUE(root2Children.size() == 1);
    EXPECT_TRUE(root2Children[0]->getParents().size() == 1);

    //execute
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerRule::create(context);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (auto sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (auto sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}
