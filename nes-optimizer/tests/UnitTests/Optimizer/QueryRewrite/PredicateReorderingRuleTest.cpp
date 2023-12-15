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
#include "Operators/LogicalOperators/Operators/LogicalFilterOperator.hpp"
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Optimizer/QueryRewrite/PredicateReorderingRule.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <iostream>

using namespace NES;

class PredicateReorderingRuleTest : public Testing::BaseIntegrationTest {

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

    void setupSensorNodeAndSourceCatalog(const Catalogs::Source::SourceCatalogPtr& sourceCatalog) {
        NES_INFO("Setup FilterPushDownTest test case.");
        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

        TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4, properties);
        auto csvSourceType = CSVSourceType::create("default_logical", "test_stream");
        PhysicalSourcePtr physicalSource = PhysicalSource::create(csvSourceType);
        LogicalSourcePtr logicalSource = LogicalSource::create("default_logical", Schema::create());
        Catalogs::Source::SourceCatalogEntryPtr sce1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);
        sourceCatalog->addPhysicalSource("default_logical", sce1);
    }
};

TEST_F(PredicateReorderingRuleTest, testReorderingChain) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>();
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
    (*itr)->as<LogicalFilterOperator>()->setSelectivity(0.9);
    ++itr;
    const NodePtr filterOperatorMediumSelectivity = (*itr);
    (*itr)->as<LogicalFilterOperator>()->setSelectivity(0.5);
    ++itr;
    const NodePtr filterOperatorLowSelectivity = (*itr);
    (*itr)->as<LogicalFilterOperator>()->setSelectivity(0.1);
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
        std::make_shared<Catalogs::Source::SourceCatalog>();
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
    (*itr)->as<LogicalFilterOperator>()->setSelectivity(0.2);
    ++itr;
    const NodePtr filterOperatorMediumSelectivity = (*itr);
    (*itr)->as<LogicalFilterOperator>()->setSelectivity(0.6);
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
        std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .filter(Attribute("id2") > 2)
                      .filter(Attribute("vehicle") == "car")
                      .map(Attribute("value") = 40)
                      .filter(Attribute("id") > 1)
                      .filter(Attribute("value") > 10)
                      .project(Attribute("type").as("t"), Attribute("value").as("v"))
                      .filter(Attribute("id3") > 3)
                      .filter(Attribute("id4") > 4)
                      .filter(Attribute("id5") > 5)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator1 = (*itr);
    (*itr)->as<LogicalFilterOperator>()->setSelectivity(0.8);
    ++itr;
    const NodePtr filterOperator2 = (*itr);
    (*itr)->as<LogicalFilterOperator>()->setSelectivity(0.2);
    ++itr;
    const NodePtr filterOperator3 = (*itr);
    (*itr)->as<LogicalFilterOperator>()->setSelectivity(0.6);
    ++itr;
    const NodePtr projectionOperator = (*itr);
    ++itr;
    const NodePtr filterOperator4 = (*itr);
    (*itr)->as<LogicalFilterOperator>()->setSelectivity(0.2);
    ++itr;
    const NodePtr filterOperator5 = (*itr);
    (*itr)->as<LogicalFilterOperator>()->setSelectivity(0.2);
    ++itr;
    const NodePtr mapOperator = (*itr);
    ++itr;
    const NodePtr filterOperator6 = (*itr);
    (*itr)->as<LogicalFilterOperator>()->setSelectivity(0.6);
    ++itr;
    const NodePtr filterOperator7 = (*itr);
    (*itr)->as<LogicalFilterOperator>()->setSelectivity(0.4);
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
    EXPECT_TRUE(filterOperator2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator3->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(projectionOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator4->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator5->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator7->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator6->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(PredicateReorderingRuleTest, testReorderingFiltersAfterBinaryOperator) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    Query subQuery = Query::from("car").filter(Attribute("id") > 35);

    Query query = Query::from("default_logical")
                      .filter(Attribute("id") > 1)
                      .filter(Attribute("value") > 10)
                      .unionWith(subQuery)
                      .map(Attribute("value") = 80)
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
    (*itr)->as<LogicalFilterOperator>()->setSelectivity(0.2);
    ++itr;
    const NodePtr filterOperatorPQ2 = (*itr);
    (*itr)->as<LogicalFilterOperator>()->setSelectivity(0.6);
    ++itr;
    const NodePtr filterOperatorPQ3 = (*itr);
    (*itr)->as<LogicalFilterOperator>()->setSelectivity(0.4);
    ++itr;
    const NodePtr mapOperatorPQ = (*itr);
    ++itr;
    const NodePtr mergeOperator = (*itr);
    ++itr;
    const NodePtr filterOperatorSQ = (*itr);
    ++itr;
    const NodePtr srcOperatorSQ = (*itr);
    ++itr;
    const NodePtr filterOperatorPQ4 = (*itr);
    (*itr)->as<LogicalFilterOperator>()->setSelectivity(0.7);
    ++itr;
    const NodePtr filterOperatorPQ5 = (*itr);
    (*itr)->as<LogicalFilterOperator>()->setSelectivity(0.3);
    ++itr;
    const NodePtr srcOperatorPQ = (*itr);
    ++itr;

    // Execute
    auto predicateReorderingRule = Optimizer::PredicateReorderingRule::create();
    const QueryPlanPtr updatedPlan = predicateReorderingRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ1->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ3->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ2->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mergeOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ5->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorPQ4->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorPQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperatorSQ->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperatorSQ->equal((*itr)));
}
