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
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Operators/OperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Util/Logger.hpp>
#include <Optimizer/QueryMerger/L0QueryMergerRule.hpp>
#include <iostream>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>

using namespace NES;

class L0QueryMergerRuleTest : public testing::Test {

  public:
    SchemaPtr schema;

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("L0QueryMergerRuleTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup L0QueryMergerRuleTest test case.");
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    }

    /* Will be called before a test is executed. */
    void TearDown() { NES_INFO("Setup L0QueryMergerRuleTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down L0QueryMergerRuleTest test class."); }
};

/**
 * @brief Test applying L0 merge on Global query plan with same queries
 */
TEST_F(L0QueryMergerRuleTest, testMergingEqualQueries) {
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

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()[0]->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()[0]->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto l0QueryMergerRule = L0QueryMergerRule::create();
    l0QueryMergerRule->apply(globalQueryPlan);

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_EQ(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying L0 merge on Global query plan with same queries with multiple same source
 */
TEST_F(L0QueryMergerRuleTest, testMergingEqualQueriesWithMultipleSameSources) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    auto sourceOperator11 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car", 11));

    auto sourceOperator21 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car", 21));

    auto sinkOperator11 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator11->addChild(sourceOperator11);
    sinkOperator11->addChild(sourceOperator21);

    QueryPlanPtr queryPlan1 = QueryPlan::create();
    queryPlan1->addRootOperator(sinkOperator11);
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    auto sourceOperator12 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car", 12));

    auto sourceOperator22 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car", 22));

    auto sinkOperator12 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator12->addChild(sourceOperator12);
    sinkOperator12->addChild(sourceOperator22);

    QueryPlanPtr queryPlan2 = QueryPlan::create();
    queryPlan2->addRootOperator(sinkOperator12);
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    ASSERT_EQ(sinkGQNs.size(), 2);

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()[0]->getId() == sinkOperator11->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()[0]->getId() == sinkOperator12->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    ASSERT_FALSE(sinkOperator1GQN->getChildren().empty());
    ASSERT_FALSE(sinkOperator2GQN->getChildren().empty());

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto l0QueryMergerRule = L0QueryMergerRule::create();
    l0QueryMergerRule->apply(globalQueryPlan);

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        bool found = false;
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            if (sink1GQNChild->equal(sink2GQNChild)) {
                found = true;
            }
        }
        ASSERT_TRUE(found);
    }
}

/**
 * @brief Test applying L0 merge on Global query plan with two queries with different source
 */
TEST_F(L0QueryMergerRuleTest, testMergingQueriesWithDifferentSources) {
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

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()[0]->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()[0]->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto l0QueryMergerRule = L0QueryMergerRule::create();
    l0QueryMergerRule->apply(globalQueryPlan);

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying L0 merge on Global query plan with same queries with merge operators
 */
TEST_F(L0QueryMergerRuleTest, testMergingQueriesWithMergeOperators) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery1 = Query::from("truck");
    Query query1 =
        Query::from("car").merge(&subQuery1).map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query subQuery2 = Query::from("truck");
    Query query2 =
        Query::from("car").merge(&subQuery2).map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()[0]->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()[0]->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto l0QueryMergerRule = L0QueryMergerRule::create();
    l0QueryMergerRule->apply(globalQueryPlan);

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_EQ(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying L0 merge on Global query plan with queries with different order of merge operator children
 */
TEST_F(L0QueryMergerRuleTest, testMergingQueriesWithMergeOperatorChildrenOrder) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery1 = Query::from("car");
    Query query1 = Query::from("truck")
                       .merge(&subQuery1)
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query subQuery2 = Query::from("truck");
    Query query2 =
        Query::from("car").merge(&subQuery2).map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()[0]->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()[0]->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto l0QueryMergerRule = L0QueryMergerRule::create();
    l0QueryMergerRule->apply(globalQueryPlan);

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_EQ(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying L0 merge on Global query plan with queries with merge operators but different children
 */
TEST_F(L0QueryMergerRuleTest, testMergingQueriesWithMergeOperatorsWithDifferentChildren) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery1 = Query::from("bike");
    Query query1 = Query::from("truck")
                       .merge(&subQuery1)
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query subQuery2 = Query::from("truck");
    Query query2 =
        Query::from("car").merge(&subQuery2).map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()[0]->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()[0]->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto l0QueryMergerRule = L0QueryMergerRule::create();
    l0QueryMergerRule->apply(globalQueryPlan);

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying L0 merge on Global query plan with two queries with different filters
 */
TEST_F(L0QueryMergerRuleTest, testMergingQueriesWithDifferentFilters) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()[0]->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()[0]->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto l0QueryMergerRule = L0QueryMergerRule::create();
    l0QueryMergerRule->apply(globalQueryPlan);

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying L0 merge on Global query plan with two queries with different filters
 */
TEST_F(L0QueryMergerRuleTest, testMergingQueriesWithDifferentFiltersField) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id1") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()[0]->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()[0]->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto l0QueryMergerRule = L0QueryMergerRule::create();
    l0QueryMergerRule->apply(globalQueryPlan);

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying L0 merge on Global query plan with two queries with different map
 */
TEST_F(L0QueryMergerRuleTest, testMergingQueriesWithDifferentMapAttribute) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car").map(Attribute("value1") = 40).filter(Attribute("id") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()[0]->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()[0]->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto l0QueryMergerRule = L0QueryMergerRule::create();
    l0QueryMergerRule->apply(globalQueryPlan);

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}

/**
 * @brief Test applying L0 merge on Global query plan with two queries with different map
 */
TEST_F(L0QueryMergerRuleTest, testMergingQueriesWithDifferentMapValue) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car").map(Attribute("value") = 50).filter(Attribute("id") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    std::vector<GlobalQueryNodePtr> sinkGQNs = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()[0]->getId() == sinkOperator1->getId();
    });
    GlobalQueryNodePtr sinkOperator1GQN = *found;

    found = std::find_if(sinkGQNs.begin(), sinkGQNs.end(), [&](GlobalQueryNodePtr sinkGQN) {
        return sinkGQN->getOperator()[0]->getId() == sinkOperator2->getId();
    });
    GlobalQueryNodePtr sinkOperator2GQN = *found;

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }

    sinkOperator2GQN->getChildren();

    //execute
    auto l0QueryMergerRule = L0QueryMergerRule::create();
    l0QueryMergerRule->apply(globalQueryPlan);

    //assert
    for (NodePtr sink1GQNChild : sinkOperator1GQN->getChildren()) {
        for (auto sink2GQNChild : sinkOperator2GQN->getChildren()) {
            ASSERT_NE(sink1GQNChild, sink2GQNChild);
        }
    }
}