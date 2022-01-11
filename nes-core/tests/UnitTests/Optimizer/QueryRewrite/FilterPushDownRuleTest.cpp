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
#include "../../../util/NesBaseTest.hpp"
// clang-format on
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <iostream>

using namespace NES;

class FilterPushDownRuleTest : public Testing::NESBaseTest {

  public:
    SchemaPtr schema;

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::setupLogging("FilterPushDownTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup FilterPushDownTest test case.");
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Setup FilterPushDownTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down FilterPushDownTest test class."); }
};

void setupSensorNodeAndStreamCatalog(const SourceCatalogPtr& streamCatalog) {
    NES_INFO("Setup FilterPushDownTest test case.");
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);
    auto csvSourceType = CSVSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("default_logical", "test_stream", csvSourceType);
    LogicalSourcePtr logicalSource = LogicalSource::create("default_logical", Schema::create());
    SourceCatalogEntryPtr sce1 = std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);
    streamCatalog->addPhysicalSource("default_logical", sce1);
}

TEST_F(FilterPushDownRuleTest, testPushingOneFilterBelowMap) {
    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("default_logical").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr mapOperator = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingOneFilterBelowMapAndBeforeFilter) {
    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .filter(Attribute("id") > 45)
                      .map(Attribute("value") = 40)
                      .filter(Attribute("id") < 45)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator1 = (*itr);
    ++itr;
    const NodePtr mapOperator = (*itr);
    ++itr;
    const NodePtr filterOperator2 = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingFiltersBelowAllMapOperators) {
    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .map(Attribute("value") = 80)
                      .filter(Attribute("id") > 45)
                      .map(Attribute("value") = 40)
                      .filter(Attribute("id") < 45)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator1 = (*itr);
    ++itr;
    const NodePtr mapOperator1 = (*itr);
    ++itr;
    const NodePtr filterOperator2 = (*itr);
    ++itr;
    const NodePtr mapOperator2 = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingTwoFilterBelowMap) {
    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .map(Attribute("value") = 40)
                      .filter(Attribute("id") > 45)
                      .filter(Attribute("id") < 45)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator1 = (*itr);
    ++itr;
    const NodePtr filterOperator2 = (*itr);
    ++itr;
    const NodePtr mapOperator = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingFilterAlreadyAtBottom) {
    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("default_logical").filter(Attribute("id") > 45).map(Attribute("value") = 40).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr mapOperator = (*itr);
    ++itr;
    const NodePtr filterOperator2 = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingOneFilterBelowABinaryOperator) {
    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query subQuery = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 45);

    Query query = Query::from("default_logical")
                      .unionWith(subQuery)
                      .map(Attribute("value") = 80)
                      .filter(Attribute("id") > 45)
                      .sink(printSinkDescriptor);

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ = (*itr);
    ++itr;
    const NodePtr unionOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorSQ = (*itr);
    ++itr;
    const NodePtr mapOperatorSQ = (*itr);
    ++itr;
    const NodePtr srcOperatorSQ = (*itr);
    ++itr;
    const NodePtr srcOperatorPQ = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    NES_DEBUG("Input Query Plan: " + (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG("Updated Query Plan: " + (updatedPlan)->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(unionOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorSQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorSQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorSQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingTwoFiltersAlreadyBelowABinaryOperator) {
    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query subQuery = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 45);

    Query query = Query::from("default_logical")
                      .map(Attribute("value") = 80)
                      .filter(Attribute("id") > 45)
                      .unionWith(subQuery)
                      .sink(printSinkDescriptor);

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr mergeOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorSQ = (*itr);
    ++itr;
    const NodePtr mapOperatorSQ = (*itr);
    ++itr;
    const NodePtr srcOperatorSQ = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ = (*itr);
    ++itr;
    const NodePtr srcOperatorPQ = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    NES_DEBUG("Input Query Plan: " + (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG("Updated Query Plan: " + (updatedPlan)->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mergeOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorSQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorSQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorSQ->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingTwoFiltersBelowABinaryOperator) {
    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query subQuery = Query::from("car");

    Query query = Query::from("default_logical")
                      .unionWith(subQuery)
                      .map(Attribute("value") = 80)
                      .filter(Attribute("id") > 45)
                      .map(Attribute("value") = 40)
                      .filter(Attribute("id") < 55)
                      .sink(printSinkDescriptor);

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ1 = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ1 = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ2 = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ2 = (*itr);
    ++itr;
    const NodePtr unionOperator = (*itr);
    ++itr;
    const NodePtr srcOperatorPQ = (*itr);
    ++itr;
    const NodePtr srcOperatorSQ = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    NES_DEBUG("Input Query Plan: " + (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG("Updated Query Plan: " + (updatedPlan)->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(unionOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorSQ->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingOneFilterAlreadyBelowAndTwoFiltersBelowABinaryOperator) {
    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query subQuery = Query::from("car").map(Attribute("value") = 90).filter(Attribute("id") > 35);

    Query query = Query::from("default_logical")
                      .unionWith(subQuery)
                      .map(Attribute("value") = 80)
                      .filter(Attribute("id") > 45)
                      .map(Attribute("value") = 40)
                      .filter(Attribute("id") < 55)
                      .sink(printSinkDescriptor);

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();
    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ1 = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ1 = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ2 = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ2 = (*itr);
    ++itr;
    const NodePtr unionOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorSQ = (*itr);
    ++itr;
    const NodePtr mapOperatorSQ = (*itr);
    ++itr;
    const NodePtr srcOperatorSQ = (*itr);
    ++itr;
    const NodePtr srcOperatorPQ = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    NES_DEBUG("Input Query Plan: " + (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG("Updated Query Plan: " + (updatedPlan)->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(unionOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorSQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorSQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorSQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingTwoFiltersAlreadyAtBottomAndTwoFiltersBelowABinaryOperator) {
    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query subQuery = Query::from("car").filter(Attribute("id") > 35);

    Query query = Query::from("default_logical")
                      .filter(Attribute("id") > 25)
                      .unionWith(subQuery)
                      .map(Attribute("value") = 80)
                      .filter(Attribute("id") > 45)
                      .map(Attribute("value") = 40)
                      .filter(Attribute("id") < 55)
                      .sink(printSinkDescriptor);

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();
    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ1 = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ1 = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ2 = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ2 = (*itr);
    ++itr;
    const NodePtr mergeOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ3 = (*itr);
    ++itr;
    const NodePtr srcOperatorPQ = (*itr);
    ++itr;
    const NodePtr filterOperatorSQ = (*itr);
    ++itr;
    const NodePtr srcOperatorSQ = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    NES_DEBUG("Input Query Plan: " + (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG("Updated Query Plan: " + (updatedPlan)->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mergeOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorSQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorSQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ3->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingFilterBetweenTwoMaps) {
    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    NES::SchemaPtr schema = NES::Schema::create()
                                ->addField("id", NES::UINT64)
                                ->addField("val", NES::UINT64)
                                ->addField("X", NES::UINT64)
                                ->addField("Y", NES::UINT64);
    streamCatalog->addLogicalStream("example", schema);

    NES_INFO("Setup FilterPushDownTest test case.");
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    auto csvSourceType = CSVSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("example", "test_stream", csvSourceType);
    LogicalSourcePtr logicalSource = LogicalSource::create("example", Schema::create());
    SourceCatalogEntryPtr sce1 = std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);

    streamCatalog->addPhysicalSource("example", sce1);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    auto query = Query::from("example")
                     .map(Attribute("Y") = Attribute("Y") - 2)
                     .map(Attribute("NEW_id2") = Attribute("Y") / Attribute("Y"))
                     .filter(Attribute("Y") >= 49)
                     .sink(NullOutputSinkDescriptor::create());

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();
    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ1 = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ1 = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ2 = (*itr);
    ++itr;
    const NodePtr srcOperatorPQ = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    NES_DEBUG("Input Query Plan: " + (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG("Updated Query Plan: " + (updatedPlan)->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
}
