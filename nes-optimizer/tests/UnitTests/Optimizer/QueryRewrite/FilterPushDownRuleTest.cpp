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
#include <BaseIntegrationTest.hpp>
// clang-format on
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Operators/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Operators/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/SpatialType.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>

using namespace NES;

class FilterPushDownRuleTest : public Testing::BaseIntegrationTest {

  public:
    SchemaPtr schema;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("FilterPushDownRuleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup FilterPushDownRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    }
};

void setupSensorNodeAndSourceCatalog(const Catalogs::Source::SourceCatalogPtr& sourceCatalog) {
    NES_INFO("Setup FilterPushDownTest test case.");
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4, properties);
    auto csvSourceType = CSVSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("default_logical", "test_stream", csvSourceType);
    LogicalSourcePtr logicalSource = LogicalSource::create("default_logical", Schema::create());
    Catalogs::Source::SourceCatalogEntryPtr sce1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);
    sourceCatalog->addPhysicalSource("default_logical", sce1);
}

bool isFilterAndAccessesCorrectFields(NodePtr filter, std::vector<std::string> accessedFields) {
    if (!filter->instanceOf<FilterLogicalOperatorNode>()) {
        return false;
    }

    auto count = accessedFields.size();

    DepthFirstNodeIterator depthFirstNodeIterator(filter->as<FilterLogicalOperatorNode>()->getPredicate());
    for (auto itr = depthFirstNodeIterator.begin(); itr != NES::DepthFirstNodeIterator::end(); ++itr) {
        if ((*itr)->instanceOf<FieldAccessExpressionNode>()) {
            const FieldAccessExpressionNodePtr accessExpressionNode = (*itr)->as<FieldAccessExpressionNode>();
            if (std::find(accessedFields.begin(), accessedFields.end(), accessExpressionNode->getFieldName())
                == accessedFields.end()) {
                return false;
            }
            count--;
        }
    }
    return count == 0;
}

TEST_F(FilterPushDownRuleTest, testPushingFilterBelowProjectionWithoutRename) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("default_logical").project(Attribute("value")).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr projectOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
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
    EXPECT_TRUE(filterOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(projectOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingFilterBelowProjectionWithRename) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .project(Attribute("value").as("value2"))
                      .filter(Attribute("value2") < 45)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr projectOperator = (*itr);
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
    EXPECT_TRUE(projectOperator->equal((*itr)));

    ++itr;
    EXPECT_TRUE(filterOperator->equal((*itr)));
    FilterLogicalOperatorNodePtr filterLogicalOperatorNode = (*itr)->as<FilterLogicalOperatorNode>();
    std::vector<FieldAccessExpressionNodePtr> filterAttributeNames =
        Optimizer::FilterPushDownRule::getFilterAccessExpressions(filterLogicalOperatorNode->getPredicate());
    bool attributeChangedName = std::any_of(filterAttributeNames.begin(),
                                            filterAttributeNames.end(),
                                            [&](const FieldAccessExpressionNodePtr& filterAttributeName) {
                                                return filterAttributeName->getFieldName() == "value";
                                            });

    EXPECT_TRUE(attributeChangedName);

    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingOneFilterBelowMap) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

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
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

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
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

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
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

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
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

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
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

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
    NES_DEBUG("Input Query Plan: {}", (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG("Updated Query Plan: {}", (updatedPlan)->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(unionOperator->equal((*itr)));

    //the order of the children of the union operator might have changed, but it both orders are valid.
    auto leavesLeft = unionOperator->getChildren()[0]->getAllLeafNodes();
    if (std::find(leavesLeft.begin(), leavesLeft.end(), srcOperatorSQ) == leavesLeft.end()) {
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
    } else {
        ++itr;
        EXPECT_TRUE(filterOperatorPQ->equal((*itr)));
        ++itr;
        EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
        ++itr;
        EXPECT_TRUE(mapOperatorSQ->equal((*itr)));
        ++itr;
        EXPECT_TRUE(filterOperatorPQ->equal((*itr)));
        ++itr;
        EXPECT_TRUE(filterOperatorSQ->equal((*itr)));
        ++itr;
        EXPECT_TRUE(srcOperatorSQ->equal((*itr)));
    }
}

TEST_F(FilterPushDownRuleTest, testPushingTwoFiltersAlreadyBelowABinaryOperator) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

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
    NES_DEBUG("Input Query Plan: {}", (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG("Updated Query Plan: {}", (updatedPlan)->toString());

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
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

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
    NES_DEBUG("Input Query Plan: {}", (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG("Updated Query Plan: {}", (updatedPlan)->toString());

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
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

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
    NES_DEBUG("Input Query Plan: {}", (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG("Updated Query Plan: {}", (updatedPlan)->toString());

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

    //the order of the children of the union operator might have changed, but it both orders are valid.
    auto leavesLeft = unionOperator->getChildren()[0]->getAllLeafNodes();
    if (std::find(leavesLeft.begin(), leavesLeft.end(), srcOperatorSQ) == leavesLeft.end()) {
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
    } else {
        ++itr;
        EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
        ++itr;
        EXPECT_TRUE(filterOperatorPQ2->equal((*itr)));
        ++itr;
        EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
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
    }
}

TEST_F(FilterPushDownRuleTest, testPushingTwoFiltersAlreadyAtBottomAndTwoFiltersBelowABinaryOperator) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

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
    NES_DEBUG("Input Query Plan: {}", (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG("Updated Query Plan: {}", (updatedPlan)->toString());

    // Validate
    // The order of the branches of the union operator matters for this tests and stays the same in this test.
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    NES_DEBUG("Expected Plan Node: {}  Actual in updated Query plan: {}", sinkOperator->toString(), (*itr)->toString());
    ++itr;
    EXPECT_TRUE(mapOperatorPQ1->equal((*itr)));
    NES_DEBUG("Expected Plan Node: {}  Actual in updated Query plan: {}", mapOperatorPQ1->toString(), (*itr)->toString());
    ++itr;
    EXPECT_TRUE(mapOperatorPQ2->equal((*itr)));
    NES_DEBUG("Expected Plan Node: {}  Actual in updated Query plan: {}", mapOperatorPQ2->toString(), (*itr)->toString());
    ++itr;
    EXPECT_TRUE(mergeOperator->equal((*itr)));
    NES_DEBUG("Expected Plan Node: {}  Actual in updated Query plan: {}", mergeOperator->toString(), (*itr)->toString());

    //the order of the children of the union operator might have changed, but it both orders are valid.
    auto leavesLeft = mergeOperator->getChildren()[0]->getAllLeafNodes();
    if (std::find(leavesLeft.begin(), leavesLeft.end(), srcOperatorPQ) == leavesLeft.end()) {
        ++itr;
        EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
        NES_DEBUG("Expected Plan Node: {}  Actual in updated Query plan: {}", filterOperatorPQ1->toString(), (*itr)->toString());
        ++itr;
        EXPECT_TRUE(filterOperatorPQ2->equal((*itr)));
        NES_DEBUG("Expected Plan Node: {}  Actual in updated Query plan: {}", filterOperatorPQ2->toString(), (*itr)->toString());
        ++itr;
        EXPECT_TRUE(filterOperatorPQ3->equal((*itr)));
        NES_DEBUG("Expected Plan Node: {}  Actual in updated Query plan: {}", filterOperatorPQ3->toString(), (*itr)->toString());
        ++itr;
        EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
        NES_DEBUG("Expected Plan Node: {}  Actual in updated Query plan: {}", srcOperatorPQ->toString(), (*itr)->toString());
        ++itr;
        EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
        NES_DEBUG("Expected Plan Node: {}  Actual in updated Query plan: {}", filterOperatorPQ1->toString(), (*itr)->toString());
        ++itr;
        EXPECT_TRUE(filterOperatorPQ2->equal((*itr)));
        NES_DEBUG("Expected Plan Node: {}  Actual in updated Query plan: {}", filterOperatorPQ2->toString(), (*itr)->toString());
        ++itr;
        EXPECT_TRUE(filterOperatorSQ->equal((*itr)));
        NES_DEBUG("Expected Plan Node: {}  Actual in updated Query plan: {}", filterOperatorSQ->toString(), (*itr)->toString());
        ++itr;
        EXPECT_TRUE(srcOperatorSQ->equal((*itr)));
        NES_DEBUG("Expected Plan Node: {}  Actual in updated Query plan: {}", srcOperatorSQ->toString(), (*itr)->toString());
    } else {
        ++itr;
        EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
        NES_DEBUG("Expected Plan Node: {}  Actual in updated Query plan: {}", filterOperatorPQ1->toString(), (*itr)->toString());
        ++itr;
        EXPECT_TRUE(filterOperatorPQ2->equal((*itr)));
        NES_DEBUG("Expected Plan Node: {}  Actual in updated Query plan: {}", filterOperatorPQ2->toString(), (*itr)->toString());
        ++itr;
        EXPECT_TRUE(filterOperatorSQ->equal((*itr)));
        NES_DEBUG("Expected Plan Node: {}  Actual in updated Query plan: {}", filterOperatorSQ->toString(), (*itr)->toString());
        ++itr;
        EXPECT_TRUE(srcOperatorSQ->equal((*itr)));
        NES_DEBUG("Expected Plan Node: {}  Actual in updated Query plan: {}", srcOperatorSQ->toString(), (*itr)->toString());
        ++itr;
        EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
        NES_DEBUG("Expected Plan Node: {}  Actual in updated Query plan: {}", filterOperatorPQ1->toString(), (*itr)->toString());
        ++itr;
        EXPECT_TRUE(filterOperatorPQ2->equal((*itr)));
        NES_DEBUG("Expected Plan Node: {}  Actual in updated Query plan: {}", filterOperatorPQ2->toString(), (*itr)->toString());
        ++itr;
        EXPECT_TRUE(filterOperatorPQ3->equal((*itr)));
        NES_DEBUG("Expected Plan Node: {}  Actual in updated Query plan: {}", filterOperatorPQ3->toString(), (*itr)->toString());
        ++itr;
        EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
        NES_DEBUG("Expected Plan Node: {}  Actual in updated Query plan: {}", srcOperatorPQ->toString(), (*itr)->toString());
    }
}

TEST_F(FilterPushDownRuleTest, testPushingFilterBelowThreeMapsWithOneFieldSubstitution) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    NES::SchemaPtr schema = NES::Schema::create()
                                ->addField("id", NES::BasicType::UINT64)
                                ->addField("val", NES::BasicType::UINT64)
                                ->addField("X", NES::BasicType::UINT64)
                                ->addField("Y", NES::BasicType::UINT64);
    sourceCatalog->addLogicalSource("example", schema);

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    NES_INFO("Setup FilterPushDownTest test case.");
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4, properties);

    auto csvSourceType = CSVSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("example", "test_stream", csvSourceType);
    LogicalSourcePtr logicalSource = LogicalSource::create("example", Schema::create());
    Catalogs::Source::SourceCatalogEntryPtr sce1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);

    sourceCatalog->addPhysicalSource("example", sce1);

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
    NES_DEBUG("Input Query Plan: {}", (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG("Updated Query Plan: {}", (updatedPlan)->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    NES_DEBUG("New filter Predicate: {}", filterOperatorPQ1->as<FilterLogicalOperatorNode>()->getPredicate()->toString());
    ++itr;
    EXPECT_TRUE(mapOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
    NES_DEBUG("filterOperatorPQ1: {}", filterOperatorPQ1->toString());
    NES_DEBUG(
        "filterOperatorPQ1 Predicate: {}",
        filterOperatorPQ1->as<FilterLogicalOperatorNode>()->getPredicate()->getNodesByType<SubExpressionNode>()[0]->toString());
    NES_DEBUG("mapOperatorPQ2 map expression: {}",
              mapOperatorPQ2->as<MapLogicalOperatorNode>()->getMapExpression()->getAssignment()->toString());
    EXPECT_TRUE(filterOperatorPQ1->as<FilterLogicalOperatorNode>()->getPredicate()->getNodesByType<SubExpressionNode>()[0]->equal(
        mapOperatorPQ2->as<MapLogicalOperatorNode>()->getMapExpression()->getAssignment()));
    ++itr;
    EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingFilterBelowTwoMapsWithTwoFieldSubstitutions) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    NES::SchemaPtr schema = NES::Schema::create()
                                ->addField("id", NES::BasicType::UINT64)
                                ->addField("val", NES::BasicType::UINT64)
                                ->addField("X", NES::BasicType::UINT64)
                                ->addField("Y", NES::BasicType::UINT64);
    sourceCatalog->addLogicalSource("example", schema);

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    NES_INFO("Setup FilterPushDownTest test case.");
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4, properties);

    auto csvSourceType = CSVSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("example", "test_stream", csvSourceType);
    LogicalSourcePtr logicalSource = LogicalSource::create("example", Schema::create());
    Catalogs::Source::SourceCatalogEntryPtr sce1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);

    sourceCatalog->addPhysicalSource("example", sce1);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    auto query = Query::from("example")
                     .map(Attribute("Y") = Attribute("Y") * 2)
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
    const NodePtr mapOperatorPQ3 = (*itr);
    ++itr;
    const NodePtr srcOperatorPQ = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    NES_DEBUG("Input Query Plan: {}", (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG("Updated Query Plan: {}", (updatedPlan)->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    NES_DEBUG("New filter Predicate: {}", filterOperatorPQ1->as<FilterLogicalOperatorNode>()->getPredicate()->toString());
    ++itr;
    EXPECT_TRUE(mapOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ3->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
    NES_DEBUG("filterOperatorPQ1: {}", filterOperatorPQ1->toString());
    NES_DEBUG(
        "filterOperatorPQ1 Predicate: {}",
        filterOperatorPQ1->as<FilterLogicalOperatorNode>()->getPredicate()->getNodesByType<MulExpressionNode>()[0]->toString());
    NES_DEBUG("mapOperatorPQ3 map expression: {}",
              mapOperatorPQ3->as<MapLogicalOperatorNode>()->getMapExpression()->getAssignment()->toString());
    EXPECT_TRUE(filterOperatorPQ1->as<FilterLogicalOperatorNode>()->getPredicate()->getNodesByType<MulExpressionNode>()[0]->equal(
        mapOperatorPQ3->as<MapLogicalOperatorNode>()->getMapExpression()->getAssignment()));
    ++itr;
    EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
}

/* tests if a filter is correctly pushed below a join if all its attributes belong to source 1. The order of the operators in the
updated query plan is validated, and it is checked that the input and output schema of the filter that is now at a new position
is still correct */
TEST_F(FilterPushDownRuleTest, testPushingFilterBelowJoinToSrc1) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());

    //setup source 1
    NES::SchemaPtr schema = NES::Schema::create()
                                ->addField("id", NES::BasicType::UINT64)
                                ->addField("A", NES::BasicType::UINT64)
                                ->addField("ts", NES::BasicType::UINT64);
    sourceCatalog->addLogicalSource("src1", schema);

    //setup source two
    NES::SchemaPtr schema2 = NES::Schema::create()
                                 ->addField("id", NES::BasicType::UINT64)
                                 ->addField("X", NES::BasicType::UINT64)
                                 ->addField("ts", NES::BasicType::UINT64);
    sourceCatalog->addLogicalSource("src2", schema2);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery = Query::from("src2");

    Query query = Query::from("src1")
                      .joinWith(subQuery)
                      .where(Attribute("id"))
                      .equalsTo(Attribute("id"))
                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                      .filter(Attribute("A") < 9999)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    //type inference
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, Catalogs::UDF::UDFCatalog::create());
    typeInferencePhase->execute(queryPlan);

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();
    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr joinOperator = (*itr);
    ++itr;
    const NodePtr watermarkOperatorAboveSrc2 = (*itr);
    ++itr;
    const NodePtr srcOperatorSrc2 = (*itr);
    ++itr;
    const NodePtr watermarkOperatorAboveSrc1 = (*itr);
    ++itr;
    const NodePtr srcOperatorSrc1 = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    NES_DEBUG("Input Query Plan: {}", (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG("Updated Query Plan: {}", (updatedPlan)->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(joinOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(watermarkOperatorAboveSrc2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorSrc2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(watermarkOperatorAboveSrc1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorSrc1->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingFilterBelowJoinNotPossible) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());

    //setup source 1
    NES::SchemaPtr schema = NES::Schema::create()
                                ->addField("id", NES::BasicType::UINT64)
                                ->addField("A", NES::BasicType::UINT64)
                                ->addField("ts", NES::BasicType::UINT64);
    sourceCatalog->addLogicalSource("src1", schema);

    //setup source two
    NES::SchemaPtr schema2 = NES::Schema::create()
                                 ->addField("id", NES::BasicType::UINT64)
                                 ->addField("X", NES::BasicType::UINT64)
                                 ->addField("ts", NES::BasicType::UINT64);
    sourceCatalog->addLogicalSource("src2", schema2);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery = Query::from("src2");

    Query query = Query::from("src1")
                      .joinWith(subQuery)
                      .where(Attribute("id"))
                      .equalsTo(Attribute("id"))
                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                      .filter(Attribute("src1$A") < 9999 || Attribute("src2$X") < 9999)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    //type inference
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, Catalogs::UDF::UDFCatalog::create());
    typeInferencePhase->execute(queryPlan);

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();
    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr joinOperator = (*itr);
    ++itr;
    const NodePtr watermarkOperatorAboveSrc2 = (*itr);
    ++itr;
    const NodePtr srcOperatorSrc2 = (*itr);
    ++itr;
    const NodePtr watermarkOperatorAboveSrc1 = (*itr);
    ++itr;
    const NodePtr srcOperatorSrc1 = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    NES_DEBUG("Input Query Plan: {}", (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG("Updated Query Plan: {}", (updatedPlan)->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(joinOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(watermarkOperatorAboveSrc2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorSrc2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(watermarkOperatorAboveSrc1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorSrc1->equal((*itr)));
}

/* tests if a filter is correctly pushed below a join if all its attributes are part of the join condition. The order of the
operators in the updated query plan is validated, and it is checked that the input and output schema of the filter that is now at
a new position is still correct. Original filter would go to the left branch*/
TEST_F(FilterPushDownRuleTest, testPushingFilterBelowJoinToBothSourcesLeft) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());

    //setup source 1
    NES::SchemaPtr schema = NES::Schema::create()
                                ->addField("id", NES::BasicType::UINT64)
                                ->addField("A", NES::BasicType::UINT64)
                                ->addField("ts", NES::BasicType::UINT64);
    sourceCatalog->addLogicalSource("src1", schema);

    //setup source two
    NES::SchemaPtr schema2 = NES::Schema::create()
                                 ->addField("id", NES::BasicType::UINT64)
                                 ->addField("X", NES::BasicType::UINT64)
                                 ->addField("ts", NES::BasicType::UINT64);
    sourceCatalog->addLogicalSource("src2", schema2);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery = Query::from("src2");

    Query query = Query::from("src1")
                      .joinWith(subQuery)
                      .where(Attribute("id"))
                      .equalsTo(Attribute("id"))
                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                      .filter(Attribute("src1$id") < 9999)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    //type inference
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, Catalogs::UDF::UDFCatalog::create());
    typeInferencePhase->execute(queryPlan);

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();
    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr joinOperator = (*itr);
    ++itr;
    const NodePtr watermarkOperatorAboveSrc2 = (*itr);
    ++itr;
    const NodePtr srcOperatorSrc2 = (*itr);
    ++itr;
    const NodePtr watermarkOperatorAboveSrc1 = (*itr);
    ++itr;
    const NodePtr srcOperatorSrc1 = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    NES_DEBUG("Input Query Plan: {}", (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG("Updated Query Plan: {}", (updatedPlan)->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(joinOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(watermarkOperatorAboveSrc2->equal((*itr)));
    ++itr;
    std::vector<std::string> accessedFields;
    accessedFields.push_back("src2$id");//a duplicate filter that accesses src2$id should be pushed down
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), accessedFields));
    //check if schema updated correctly
    EXPECT_TRUE((*itr)->as<UnaryOperatorNode>()->getOutputSchema()->equals(
        watermarkOperatorAboveSrc2->as<UnaryOperatorNode>()->getInputSchema()));
    EXPECT_TRUE(
        (*itr)->as<UnaryOperatorNode>()->getInputSchema()->equals(srcOperatorSrc2->as<UnaryOperatorNode>()->getOutputSchema()));
    ++itr;
    EXPECT_TRUE(srcOperatorSrc2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(watermarkOperatorAboveSrc1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator->equal((*itr)));
    //check if schema updated correctly
    EXPECT_TRUE((*itr)->as<UnaryOperatorNode>()->getOutputSchema()->equals(
        watermarkOperatorAboveSrc1->as<UnaryOperatorNode>()->getInputSchema()));
    EXPECT_TRUE(
        (*itr)->as<UnaryOperatorNode>()->getInputSchema()->equals(srcOperatorSrc1->as<UnaryOperatorNode>()->getOutputSchema()));
    ++itr;
    EXPECT_TRUE(srcOperatorSrc1->equal((*itr)));
}

/* tests if a filter is correctly pushed below a join if all its attributes are part of the join condition. The order of the
operators in the updated query plan is validated, and it is checked that the input and output schema of the filter that is now at
a new position is still correct. Original filter would go to the right branch */
TEST_F(FilterPushDownRuleTest, testPushingFilterBelowJoinToBothSourcesRight) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());

    //setup source 1
    NES::SchemaPtr schema = NES::Schema::create()
                                ->addField("id", NES::BasicType::UINT64)
                                ->addField("A", NES::BasicType::UINT64)
                                ->addField("ts", NES::BasicType::UINT64);
    sourceCatalog->addLogicalSource("src1", schema);

    //setup source two
    NES::SchemaPtr schema2 = NES::Schema::create()
                                 ->addField("id", NES::BasicType::UINT64)
                                 ->addField("X", NES::BasicType::UINT64)
                                 ->addField("ts", NES::BasicType::UINT64);
    sourceCatalog->addLogicalSource("src2", schema2);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery = Query::from("src2");

    Query query = Query::from("src1")
                      .joinWith(subQuery)
                      .where(Attribute("id"))
                      .equalsTo(Attribute("id"))
                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                      .filter(Attribute("src2$id") < 9999)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    //type inference
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, Catalogs::UDF::UDFCatalog::create());
    typeInferencePhase->execute(queryPlan);

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();
    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr joinOperator = (*itr);
    ++itr;
    const NodePtr watermarkOperatorAboveSrc2 = (*itr);
    ++itr;
    const NodePtr srcOperatorSrc2 = (*itr);
    ++itr;
    const NodePtr watermarkOperatorAboveSrc1 = (*itr);
    ++itr;
    const NodePtr srcOperatorSrc1 = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    NES_DEBUG("Input Query Plan: {}", (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);
    NES_DEBUG("Updated Query Plan: {}", (updatedPlan)->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = updatedQueryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(joinOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(watermarkOperatorAboveSrc2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator->equal((*itr)));
    //check if schema updated correctly
    EXPECT_TRUE((*itr)->as<UnaryOperatorNode>()->getOutputSchema()->equals(
        watermarkOperatorAboveSrc2->as<UnaryOperatorNode>()->getInputSchema()));
    EXPECT_TRUE(
        (*itr)->as<UnaryOperatorNode>()->getInputSchema()->equals(srcOperatorSrc2->as<UnaryOperatorNode>()->getOutputSchema()));
    ++itr;
    EXPECT_TRUE(srcOperatorSrc2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(watermarkOperatorAboveSrc1->equal((*itr)));
    ++itr;
    std::vector<std::string> accessedFields;
    accessedFields.push_back("src1$id");//a duplicate filter that accesses src2$id should be pushed down
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), accessedFields));
    //check if schema updated correctly
    EXPECT_TRUE((*itr)->as<UnaryOperatorNode>()->getOutputSchema()->equals(
        watermarkOperatorAboveSrc1->as<UnaryOperatorNode>()->getInputSchema()));
    EXPECT_TRUE(
        (*itr)->as<UnaryOperatorNode>()->getInputSchema()->equals(srcOperatorSrc1->as<UnaryOperatorNode>()->getOutputSchema()));
    ++itr;
    EXPECT_TRUE(srcOperatorSrc1->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingOneFilterBelowWindow) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query query = Query::from("vehicles")
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(10)))
                      .byKey(Attribute("type"))
                      .apply(Count()->as(Attribute("count_value")))
                      .filter(Attribute("type") == 1)
                      .sink(printSinkDescriptor);

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr windowOperator = (*itr);
    ++itr;
    const NodePtr watermarkOperator = (*itr);
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
    EXPECT_TRUE(windowOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(watermarkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingOneFilterBelowWindowNotPossible) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query query = Query::from("vehicles")
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(10)))
                      .byKey(Attribute("type"))
                      .apply(Count()->as(Attribute("count_value")))
                      .filter(Attribute("size") > 5)
                      .sink(printSinkDescriptor);

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr windowOperator = (*itr);
    ++itr;
    const NodePtr watermarkOperator = (*itr);
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
    EXPECT_TRUE(filterOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(windowOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(watermarkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingOneFilterBelowWindowNotPossibleMultipleAttributes) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query query = Query::from("vehicles")
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(10)))
                      .byKey(Attribute("type"))
                      .apply(Count()->as(Attribute("count_value")))
                      .filter(Attribute("type") == 1 && Attribute("size") > 5)
                      .sink(printSinkDescriptor);

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr windowOperator = (*itr);
    ++itr;
    const NodePtr watermarkOperator = (*itr);
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
    EXPECT_TRUE(filterOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(windowOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(watermarkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterPushDownRuleTest, testPushingDifferentFiltersThroughDifferentOperators) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());

    //setup source 1
    NES::SchemaPtr schema = NES::Schema::create()
                                ->addField("id", NES::BasicType::UINT64)
                                ->addField("A", NES::BasicType::UINT64)
                                ->addField("ts", NES::BasicType::UINT64);
    sourceCatalog->addLogicalSource("src1", schema);

    //setup source two
    NES::SchemaPtr schema2 = NES::Schema::create()
                                 ->addField("id", NES::BasicType::UINT64)
                                 ->addField("B", NES::BasicType::UINT64)
                                 ->addField("ts", NES::BasicType::UINT64);
    sourceCatalog->addLogicalSource("src2", schema2);

    //setup source three
    NES::SchemaPtr schema3 = NES::Schema::create()
                                 ->addField("id", NES::BasicType::UINT64)
                                 ->addField("C", NES::BasicType::UINT64)
                                 ->addField("ts", NES::BasicType::UINT64);
    sourceCatalog->addLogicalSource("src3", schema3);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query query =
        Query::from("src1")
            .map(Attribute("A") = Attribute("A") * 3)
            .joinWith(Query::from("src2"))
            .where(Attribute("id"))
            .equalsTo(Attribute("id"))
            .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
            .joinWith(Query::from("src3").map(Attribute("ts") = Attribute("ts") * 2))
            .where(Attribute("id"))
            .equalsTo(Attribute("id"))
            .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
            .filter(
                Attribute("src3$id")
                != 0)// should be pushed directly above every src with the adequate predicate for each source as the filter predicate is applied to the join key
            .filter(
                Attribute("A")
                != 1)// should be pushed above src1, plus, substitute field access with map transformation (Attribute("A") = Attribute("A") * 3)
            .filter(Attribute("B") != 2)                     // should be pushed above id filter above src2
            .filter(Attribute("C") != 3)                     // should be pushed above id filter above src3
            .filter(Attribute("A") > 0 || Attribute("B") > 0)// should be pushed above join src1 & src2
            .filter(Attribute("A") > 0
                    || Attribute("C")
                        > 0)// can not be pushed through any join; would need to split the filter otherwise (Not implemented)
            .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    //type inference
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, Catalogs::UDF::UDFCatalog::create());
    typeInferencePhase->execute(queryPlan);

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();
    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorAorC = (*itr);
    ++itr;
    const NodePtr filterOperatorAorB = (*itr);
    ++itr;
    const NodePtr filterOperatorC = (*itr);
    ++itr;
    const NodePtr filterOperatorB = (*itr);
    ++itr;
    const NodePtr filterOperatorA = (*itr);
    ++itr;
    const NodePtr filterOperatorId = (*itr);
    ++itr;
    const NodePtr joinOperator1and2and3 = (*itr);
    ++itr;
    const NodePtr watermarkOperator1 = (*itr);
    ++itr;
    const NodePtr mapOperatorTs = (*itr);
    ++itr;
    const NodePtr srcOperatorSrc3 = (*itr);
    ++itr;
    const NodePtr joinOperator1and2 = (*itr);
    ++itr;
    const NodePtr watermarkOperator2 = (*itr);
    ++itr;
    const NodePtr srcOperatorSrc2 = (*itr);
    ++itr;
    const NodePtr watermarkOperator3 = (*itr);
    ++itr;
    const NodePtr mapOperatorA = (*itr);
    ++itr;
    const NodePtr srcOperatorSrc1 = (*itr);

    // Execute
    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorAorC->equal((*itr)));
    NES_DEBUG("FilterOperatorAorC: {}", filterOperatorAorC->toString());
    NES_DEBUG("FilterOperatorAorC Predicate: {}",
              filterOperatorAorC->as<FilterLogicalOperatorNode>()->getPredicate()->toString());
    //check if schema still correct
    EXPECT_TRUE(
        (*itr)->as<UnaryOperatorNode>()->getOutputSchema()->equals(sinkOperator->as<UnaryOperatorNode>()->getInputSchema()));
    EXPECT_TRUE((*itr)->as<UnaryOperatorNode>()->getInputSchema()->equals(
        joinOperator1and2and3->as<BinaryOperatorNode>()->getOutputSchema()));
    ++itr;
    EXPECT_TRUE(joinOperator1and2and3->equal((*itr)));
    ++itr;
    EXPECT_TRUE(watermarkOperator1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorTs->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorC->equal((*itr)));
    NES_DEBUG("filterOperatorC: {}", filterOperatorC->toString());
    NES_DEBUG("filterOperatorC Predicate: {}", filterOperatorC->as<FilterLogicalOperatorNode>()->getPredicate()->toString());
    //check if schema updated correctly
    EXPECT_TRUE(
        (*itr)->as<UnaryOperatorNode>()->getOutputSchema()->equals(mapOperatorTs->as<UnaryOperatorNode>()->getInputSchema()));
    EXPECT_TRUE(
        (*itr)->as<UnaryOperatorNode>()->getInputSchema()->equals(srcOperatorSrc3->as<UnaryOperatorNode>()->getOutputSchema()));
    ++itr;
    EXPECT_TRUE(filterOperatorId->equal((*itr)));
    NES_DEBUG("filterOperatorId: {}", filterOperatorId->toString());
    NES_DEBUG("filterOperatorId Predicate: {}", filterOperatorId->as<FilterLogicalOperatorNode>()->getPredicate()->toString());
    //check if schema updated correctly
    EXPECT_TRUE(
        (*itr)->as<UnaryOperatorNode>()->getOutputSchema()->equals(mapOperatorTs->as<UnaryOperatorNode>()->getInputSchema()));
    EXPECT_TRUE(
        (*itr)->as<UnaryOperatorNode>()->getInputSchema()->equals(srcOperatorSrc3->as<UnaryOperatorNode>()->getOutputSchema()));
    ++itr;
    EXPECT_TRUE(srcOperatorSrc3->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorAorB->equal((*itr)));
    NES_DEBUG("filterOperatorAorB: {}", filterOperatorAorB->toString());
    NES_DEBUG("filterOperatorAorB Predicate: {}",
              filterOperatorAorB->as<FilterLogicalOperatorNode>()->getPredicate()->toString());
    //check if schema updated correctly
    EXPECT_TRUE((*itr)->as<UnaryOperatorNode>()->getOutputSchema()->equals(
        joinOperator1and2and3->as<BinaryOperatorNode>()->getLeftInputSchema()));
    EXPECT_TRUE((*itr)->as<UnaryOperatorNode>()->getInputSchema()->equals(
        joinOperator1and2->as<BinaryOperatorNode>()->getOutputSchema()));
    ++itr;
    EXPECT_TRUE(joinOperator1and2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(watermarkOperator2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorB->equal((*itr)));
    NES_DEBUG("filterOperatorB: {}", filterOperatorB->toString());
    NES_DEBUG("filterOperatorB Predicate: {}", filterOperatorB->as<FilterLogicalOperatorNode>()->getPredicate()->toString());
    //check if schema updated correctly
    EXPECT_TRUE((*itr)->as<UnaryOperatorNode>()->getOutputSchema()->equals(
        watermarkOperator2->as<UnaryOperatorNode>()->getInputSchema()));
    EXPECT_TRUE(
        (*itr)->as<UnaryOperatorNode>()->getInputSchema()->equals(srcOperatorSrc2->as<UnaryOperatorNode>()->getOutputSchema()));
    ++itr;
    std::vector<std::string> accessedFields;
    accessedFields.push_back("src2$id");//a duplicate filter that accesses src2$id should be pushed down
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), accessedFields));
    //check if schema updated correctly
    EXPECT_TRUE((*itr)->as<UnaryOperatorNode>()->getOutputSchema()->equals(
        watermarkOperator2->as<UnaryOperatorNode>()->getInputSchema()));
    EXPECT_TRUE(
        (*itr)->as<UnaryOperatorNode>()->getInputSchema()->equals(srcOperatorSrc2->as<UnaryOperatorNode>()->getOutputSchema()));
    ++itr;
    EXPECT_TRUE(srcOperatorSrc2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(watermarkOperator3->equal((*itr)));
    //check if schema updated correctly
    EXPECT_TRUE((*itr)->as<UnaryOperatorNode>()->getOutputSchema()->equals(
        watermarkOperator3->as<UnaryOperatorNode>()->getInputSchema()));
    EXPECT_TRUE(
        (*itr)->as<UnaryOperatorNode>()->getInputSchema()->equals(mapOperatorA->as<UnaryOperatorNode>()->getOutputSchema()));
    ++itr;
    EXPECT_TRUE(mapOperatorA->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorA->equal((*itr)));
    NES_DEBUG("filterOperatorA: {}", filterOperatorA->toString());
    NES_DEBUG(
        "filterOperatorA Predicate: {}",
        filterOperatorA->as<FilterLogicalOperatorNode>()->getPredicate()->getNodesByType<MulExpressionNode>()[0]->toString());
    NES_DEBUG("mapOperatorA map expression: {}",
              mapOperatorA->as<MapLogicalOperatorNode>()->getMapExpression()->getAssignment()->toString());
    EXPECT_TRUE(filterOperatorA->as<FilterLogicalOperatorNode>()->getPredicate()->getNodesByType<MulExpressionNode>()[0]->equal(
        mapOperatorA->as<MapLogicalOperatorNode>()->getMapExpression()->getAssignment()));
    ++itr;
    std::vector<std::string> accessedFields2;
    accessedFields2.push_back("src1$id");//a duplicate filter that accesses src2$id should be pushed down
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), accessedFields2));
    //check if schema updated correctly
    EXPECT_TRUE(
        (*itr)->as<UnaryOperatorNode>()->getOutputSchema()->equals(mapOperatorA->as<UnaryOperatorNode>()->getInputSchema()));
    EXPECT_TRUE(
        (*itr)->as<UnaryOperatorNode>()->getInputSchema()->equals(srcOperatorSrc1->as<UnaryOperatorNode>()->getOutputSchema()));
    ++itr;
    EXPECT_TRUE(srcOperatorSrc1->equal((*itr)));
    ++itr;
}
