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
#include <NesBaseTest.hpp>
// clang-format on
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Optimizer/QueryRewrite/PredicateReorderingRule.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/SpatialType.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>

using namespace NES;

class PredicateReorderingRuleTest : public Testing::NESBaseTest {

  public:
    SchemaPtr schema;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("PredicateReorderingRuleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup PredicateReorderingRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::NESBaseTest::SetUp();
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

TEST_F(PredicateReorderingRuleTest, testReorderingChain) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                       .filter(Attribute("id") < 45 && Attribute("id2") > 2)
                      .filter(Attribute("vehicle") == "car")
                      .filter(Attribute("type") == 2 && Attribute("id2") > 2)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorHighSelectivity = (*itr);
    (*itr)->as<FilterLogicalOperatorNode>()->setSelectivity(0.9);
    ++itr;
    const NodePtr filterOperatorMediumSelectivity = (*itr);
    (*itr)->as<FilterLogicalOperatorNode>()->setSelectivity(0.5);
    ++itr;
    const NodePtr filterOperatorLowSelectivity = (*itr);
    (*itr)->as<FilterLogicalOperatorNode>()->setSelectivity(0.1);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto predicateReorderingRule = Optimizer::PredicateReorderingRule::create();
    const QueryPlanPtr updatedPlan = predicateReorderingRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorLowSelectivity->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorMediumSelectivity->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorHighSelectivity->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(PredicateReorderingRuleTest, testReorderingChainNotApplicable) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());    \
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .filter(Attribute("id") < 45 && Attribute("id2") > 2)
                      .filter(Attribute("vehicle") == "car")
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorLowSelectivity = (*itr);
    (*itr)->as<FilterLogicalOperatorNode>()->setSelectivity(0.2);
    ++itr;
    const NodePtr filterOperatorMediumSelectivity = (*itr);
    (*itr)->as<FilterLogicalOperatorNode>()->setSelectivity(0.6);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto predicateReorderingRule = Optimizer::PredicateReorderingRule::create();
    const QueryPlanPtr updatedPlan = predicateReorderingRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorLowSelectivity->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorMediumSelectivity->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(PredicateReorderingRuleTest, testReorderingFiltersNotAlignedConsecutively) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());    \
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .filter(Attribute("id") < 45 && Attribute("id2") > 2)
                      .map(Attribute("value") = 40)
                      .filter(Attribute("vehicle") == "car")
                      .map(Attribute("id") = 30)
                      .filter(Attribute("id") > 1)
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
    const NodePtr filterOperator3 = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto predicateReorderingRule = Optimizer::PredicateReorderingRule::create();
    const QueryPlanPtr updatedPlan = predicateReorderingRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator3->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(PredicateReorderingRuleTest, testReorderingFiltersAfterBinaryOperator) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());    \
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query subQuery = Query::from("car").filter(Attribute("id") > 35);

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
    const NodePtr mergeOperator = (*itr);
    ++itr;
    const NodePtr srcOperatorPQ = (*itr);
    ++itr;
    const NodePtr filterOperatorSQ = (*itr);
    ++itr;
    const NodePtr srcOperatorSQ = (*itr);

    // Execute
    auto predicateReorderingRule = Optimizer::PredicateReorderingRule::create();
    const QueryPlanPtr updatedPlan = predicateReorderingRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mergeOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorSQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorSQ->equal((*itr)));
}
