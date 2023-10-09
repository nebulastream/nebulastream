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
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Operators/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Optimizer/QueryRewrite/FilterMergeRule.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/SpatialType.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>

using namespace NES;

class FilterMergeRuleTest : public Testing::BaseIntegrationTest {

  public:
    SchemaPtr schema;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("PredicateReorderingRuleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup PredicateReorderingRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    }
};

void setupSensorNodeAndSourceCatalog(const Catalogs::Source::SourceCatalogPtr& sourceCatalog) {
    NES_INFO("Setup FilterMergeRule test case.");
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

bool isFilterAndHasCorrectPredicate(NodePtr filter, ExpressionNodePtr expectedPredicate) {
    if (!filter->instanceOf<FilterLogicalOperatorNode>()) {
        return false;
    }

    auto filterPredicates = filter->as<FilterLogicalOperatorNode>()->getPredicate()->getAndFlattenAllChildren(true);
    auto expectedPredicates = expectedPredicate->getAndFlattenAllChildren(true);

    for (unsigned int i = 0; i < filterPredicates.size(); i++) {
        if (!filterPredicates.at(i)->equal(expectedPredicates.at(i))) {
            return false;
        }
    }
    auto filterPredicate = filter->as<FilterLogicalOperatorNode>()->getPredicate();
    return true;
}

TEST_F(FilterMergeRuleTest, testMergeTwoConsecutiveFilters) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .filter(Attribute("id") < 5)
                      .filter(Attribute("vehicle") == "car")
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator1 = (*itr);
    auto predicate1 = filterOperator1->as<FilterLogicalOperatorNode>()->getPredicate();
    ++itr;
    const NodePtr filterOperator2 = (*itr);
    auto predicate2 = filterOperator2->as<FilterLogicalOperatorNode>()->getPredicate();
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterMergeRule = Optimizer::FilterMergeRule::create();
    const QueryPlanPtr updatedPlan = filterMergeRule->apply(queryPlan);

    // Validate
    auto filterOperators = queryPlan->getOperatorByType<FilterLogicalOperatorNode>();
    EXPECT_TRUE(filterOperators.size() == 1);
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    ExpressionNodePtr expectedPredicate = AndExpressionNode::create(predicate1, predicate2);
    EXPECT_TRUE(isFilterAndHasCorrectPredicate((*itr), expectedPredicate));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterMergeRuleTest, testMergeThreeConsecutiveComplexFilters) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .filter(Attribute("id") < 5 || Attribute("id") > 10)
                      .filter(Attribute("vehicle") == "car" && Attribute("value") != 1)
                      .filter(Attribute("vehicle") == "car" && !Attribute("value") < 0)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator1 = (*itr);
    auto predicate1 = filterOperator1->as<FilterLogicalOperatorNode>()->getPredicate();
    ++itr;
    const NodePtr filterOperator2 = (*itr);
    auto predicate2 = filterOperator2->as<FilterLogicalOperatorNode>()->getPredicate();
    ++itr;
    const NodePtr filterOperator3 = (*itr);
    auto predicate3 = filterOperator3->as<FilterLogicalOperatorNode>()->getPredicate();
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterMergeRule = Optimizer::FilterMergeRule::create();
    const QueryPlanPtr updatedPlan = filterMergeRule->apply(queryPlan);

    // Validate
    auto filterOperators = queryPlan->getOperatorByType<FilterLogicalOperatorNode>();
    EXPECT_TRUE(filterOperators.size() == 1);
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    ExpressionNodePtr expectedPredicate = AndExpressionNode::create(predicate1, predicate2);
    expectedPredicate = AndExpressionNode::create(expectedPredicate, predicate3);
    EXPECT_TRUE(isFilterAndHasCorrectPredicate((*itr), expectedPredicate));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterMergeRuleTest, testMergeDifferentFilterGroups) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery = Query::from("car").filter(Attribute("id") > 35);
    Query query = Query::from("default_logical")
                      .filter(Attribute("id") > 1)
                      .filter(Attribute("value") > 10)
                      .unionWith(subQuery)
                      .map(Attribute("value") = 80)
                      .project(Attribute("type").as("t"), Attribute("value").as("v"))
                      .filter(Attribute("id2") > 2)
                      .filter(Attribute("vehicle") == "car")
                      .filter(Attribute("id") > 45)
                      .sink(printSinkDescriptor);

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ1 = (*itr);
    auto predicatePQ1 = filterOperatorPQ1->as<FilterLogicalOperatorNode>()->getPredicate();
    ++itr;
    const NodePtr filterOperatorPQ2 = (*itr);
    auto predicatePQ2 = filterOperatorPQ2->as<FilterLogicalOperatorNode>()->getPredicate();
    ++itr;
    const NodePtr filterOperatorPQ3 = (*itr);
    auto predicatePQ3 = filterOperatorPQ3->as<FilterLogicalOperatorNode>()->getPredicate();
    ++itr;
    const NodePtr projectOperatorPQ = (*itr);
    ++itr;
    const NodePtr mapOperatorPQ = (*itr);
    ++itr;
    const NodePtr mergeOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorSQ = (*itr);
    auto predicateSQ = filterOperatorSQ->as<FilterLogicalOperatorNode>()->getPredicate();
    ++itr;
    const NodePtr srcOperatorSQ = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ4 = (*itr);
    auto predicatePQ4 = filterOperatorPQ4->as<FilterLogicalOperatorNode>()->getPredicate();
    ++itr;
    const NodePtr filterOperatorPQ5 = (*itr);
    auto predicatePQ5 = filterOperatorPQ5->as<FilterLogicalOperatorNode>()->getPredicate();
    ++itr;
    const NodePtr srcOperatorPQ = (*itr);
    ++itr;

    // Execute
    auto filterMergeRule = Optimizer::FilterMergeRule::create();
    const QueryPlanPtr updatedPlan = filterMergeRule->apply(queryPlan);

    // Validate
    auto filterOperators = queryPlan->getOperatorByType<FilterLogicalOperatorNode>();
    EXPECT_TRUE(filterOperators.size() == 3);
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    ExpressionNodePtr expectedPredicate1 = AndExpressionNode::create(predicatePQ1, predicatePQ2);
    expectedPredicate1 = AndExpressionNode::create(expectedPredicate1, predicatePQ3);
    EXPECT_TRUE(isFilterAndHasCorrectPredicate((*itr), expectedPredicate1));
    ++itr;
    EXPECT_TRUE(projectOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mergeOperator->equal((*itr)));
    ++itr;
    ExpressionNodePtr expectedPredicate2 = AndExpressionNode::create(predicatePQ4, predicatePQ5);
    EXPECT_TRUE(isFilterAndHasCorrectPredicate((*itr), expectedPredicate2));
    ++itr;
    EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(isFilterAndHasCorrectPredicate((*itr), predicateSQ));
    ++itr;
    EXPECT_TRUE(srcOperatorSQ->equal((*itr)));
}

TEST_F(FilterMergeRuleTest, testMergeNotPossibleOperatorsInBetween) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .filter(Attribute("id") < 5)
                      .map(Attribute("id") = Attribute("id") * 2)
                      .filter(Attribute("vehicle") == "car")
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator1 = (*itr);
    auto predicate1 = filterOperator1->as<FilterLogicalOperatorNode>()->getPredicate();
    ++itr;
    const NodePtr mapOperator = (*itr);
    ++itr;
    const NodePtr filterOperator2 = (*itr);
    auto predicate2 = filterOperator2->as<FilterLogicalOperatorNode>()->getPredicate();
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterMergeRule = Optimizer::FilterMergeRule::create();
    const QueryPlanPtr updatedPlan = filterMergeRule->apply(queryPlan);

    // Validate
    auto filterOperators = queryPlan->getOperatorByType<FilterLogicalOperatorNode>();
    EXPECT_TRUE(filterOperators.size() == 2);
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(isFilterAndHasCorrectPredicate((*itr), predicate1));
    ++itr;
    EXPECT_TRUE(mapOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(isFilterAndHasCorrectPredicate((*itr), predicate2));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterMergeRuleTest, testMergeNotPossibleOneFilter) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .filter(Attribute("id") < 5)
                      .map(Attribute("id") = Attribute("id") * 2)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr mapOperator = (*itr);
    ++itr;
    const NodePtr filterOperator1 = (*itr);
    auto predicate1 = filterOperator1->as<FilterLogicalOperatorNode>()->getPredicate();
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterMergeRule = Optimizer::FilterMergeRule::create();
    const QueryPlanPtr updatedPlan = filterMergeRule->apply(queryPlan);

    // Validate
    auto filterOperators = queryPlan->getOperatorByType<FilterLogicalOperatorNode>();
    EXPECT_TRUE(filterOperators.size() == 1);
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(isFilterAndHasCorrectPredicate((*itr), predicate1));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}