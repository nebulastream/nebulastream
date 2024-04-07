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
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Optimizer/QueryRewrite/FilterSplitUpRule.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <iostream>

using namespace NES;

class FilterSplitUpRuleTest : public Testing::BaseIntegrationTest {

  public:
    SchemaPtr schema;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("FilterSplitUpRuleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup FilterSplitUpRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    }

    void setupSensorNodeAndSourceCatalog(const Catalogs::Source::SourceCatalogPtr& sourceCatalog) {
        NES_INFO("Setup FilterSplitUpRuleTest test case.");
        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

        TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4, properties);
        auto csvSourceType = CSVSourceType::create("default_logical", "test_stream");
        PhysicalSourcePtr physicalSource = PhysicalSource::create(csvSourceType);
        LogicalSourcePtr logicalSource = LogicalSource::create("default_logical", Schema::create());
        auto sce1 = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, physicalNode->getId());
        sourceCatalog->addPhysicalSource("default_logical", sce1);
    }

    bool isFilterAndAccessesCorrectFields(NodePtr filter, std::vector<std::string> accessedFields) {
        if (!filter->instanceOf<LogicalFilterOperator>()) {
            return false;
        }

        auto count = accessedFields.size();

        DepthFirstNodeIterator depthFirstNodeIterator(filter->as<LogicalFilterOperator>()->getPredicate());
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
};

TEST_F(FilterSplitUpRuleTest, testSplittingFilterWithOneAnd) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical").filter(Attribute("id") == 1 && Attribute("ts") == 2).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterSplitUpRule = Optimizer::FilterSplitUpRule::create();
    const QueryPlanPtr updatedPlan = filterSplitUpRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    std::vector<std::string> accessedFields;
    accessedFields.push_back("id");
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), accessedFields));
    ++itr;
    std::replace(accessedFields.begin(), accessedFields.end(), "id", "ts");
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), accessedFields));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterSplitUpRuleTest, testSplittingFilterWithThreeAnd) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("default_logical")
            .filter(Attribute("id") == 1 && Attribute("ts") == 2 && Attribute("fictional") == 3 && Attribute("fictional2") == 4)
            .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterSplitUpRule = Optimizer::FilterSplitUpRule::create();
    const QueryPlanPtr updatedPlan = filterSplitUpRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    std::vector<std::string> id;
    id.emplace_back("id");
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), id));
    ++itr;
    std::vector<std::string> ts;
    ts.emplace_back("ts");
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), ts));
    ++itr;
    std::vector<std::string> fictional;
    fictional.emplace_back("fictional");
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), fictional));
    ++itr;
    std::vector<std::string> fictional2;
    fictional2.emplace_back("fictional2");
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), fictional2));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterSplitUpRuleTest, testSplittingFilterWithAndOr) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("default_logical")
            .filter(Attribute("id") == 1 && (Attribute("ts") == 2 || Attribute("fictional") == 3 && Attribute("fictional2") == 4))
            .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterSplitUpRule = Optimizer::FilterSplitUpRule::create();
    const QueryPlanPtr updatedPlan = filterSplitUpRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    std::vector<std::string> id;
    id.emplace_back("id");
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), id));
    ++itr;
    std::vector<std::string> tsFictionalFictional2;
    tsFictionalFictional2.emplace_back("ts");
    tsFictionalFictional2.emplace_back("fictional");
    tsFictionalFictional2.emplace_back("fictional2");
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), tsFictionalFictional2));
    ++itr;

    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterSplitUpRuleTest, testSplittingFilterWithAndOrImpossible) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .filter((Attribute("id") == 1 && Attribute("ts") == 2)
                              || (Attribute("fictional") == 3 && Attribute("fictional2") == 4))
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterSplitUpRule = Optimizer::FilterSplitUpRule::create();
    const QueryPlanPtr updatedPlan = filterSplitUpRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    std::vector<std::string> all;
    all.emplace_back("id");
    all.emplace_back("ts");
    all.emplace_back("fictional");
    all.emplace_back("fictional2");
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), all));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterSplitUpRuleTest, testSplittingFilterImpossibleNotAnd) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("default_logical").filter(!(Attribute("id") == 1 && Attribute("ts") == 2)).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterSplitUpRule = Optimizer::FilterSplitUpRule::create();
    const QueryPlanPtr updatedPlan = filterSplitUpRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    std::vector<std::string> accessedFields;
    accessedFields.emplace_back("id");
    accessedFields.emplace_back("ts");
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), accessedFields));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterSplitUpRuleTest, testSplittingFilterNotOr) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("default_logical").filter(!(Attribute("id") == 1 || Attribute("ts") == 2)).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterSplitUpRule = Optimizer::FilterSplitUpRule::create();
    const QueryPlanPtr updatedPlan = filterSplitUpRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    std::vector<std::string> id;
    id.emplace_back("id");
    EXPECT_TRUE((*itr)->as<LogicalFilterOperator>()->getPredicate()->instanceOf<NegateExpressionNode>());
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), id));
    ++itr;
    std::vector<std::string> ts;
    ts.emplace_back("ts");
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), ts));
    EXPECT_TRUE((*itr)->as<LogicalFilterOperator>()->getPredicate()->instanceOf<NegateExpressionNode>());
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterSplitUpRuleTest, testSplittingFilterOrImpossible) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical").filter(Attribute("id") == 1 || Attribute("ts") == 2).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterSplitUpRule = Optimizer::FilterSplitUpRule::create();
    const QueryPlanPtr updatedPlan = filterSplitUpRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterSplitUpRuleTest, testSplittingFilterNotTwoOrs) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .filter(!((Attribute("id") == 1 || Attribute("ts") == 2) || Attribute("fict") == 3))
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterSplitUpRule = Optimizer::FilterSplitUpRule::create();
    const QueryPlanPtr updatedPlan = filterSplitUpRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    std::vector<std::string> id;
    id.emplace_back("id");
    EXPECT_TRUE((*itr)->as<LogicalFilterOperator>()->getPredicate()->instanceOf<NegateExpressionNode>());
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), id));
    ++itr;
    std::vector<std::string> ts;
    ts.emplace_back("ts");
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), ts));
    EXPECT_TRUE((*itr)->as<LogicalFilterOperator>()->getPredicate()->instanceOf<NegateExpressionNode>());
    ++itr;
    std::vector<std::string> fict;
    fict.emplace_back("fict");
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), fict));
    EXPECT_TRUE((*itr)->as<LogicalFilterOperator>()->getPredicate()->instanceOf<NegateExpressionNode>());
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterSplitUpRuleTest, testSplittingFilterImpossibleAndOrNot) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .filter(!((Attribute("id") == 1 || Attribute("ts") == 2) && Attribute("fict") == 3))
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterSplitUpRule = Optimizer::FilterSplitUpRule::create();
    const QueryPlanPtr updatedPlan = filterSplitUpRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterSplitUpRuleTest, testSplittingFilterAndOrNot) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .filter(!((Attribute("id") == 1 && Attribute("ts") == 2) || Attribute("fict") == 3))
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterSplitUpRule = Optimizer::FilterSplitUpRule::create();
    const QueryPlanPtr updatedPlan = filterSplitUpRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    std::vector<std::string> idTs;
    idTs.emplace_back("id");
    idTs.emplace_back("ts");
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), idTs));
    EXPECT_TRUE((*itr)->as<LogicalFilterOperator>()->getPredicate()->instanceOf<NegateExpressionNode>());
    ++itr;
    std::vector<std::string> fict;
    fict.emplace_back("fict");
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), fict));
    EXPECT_TRUE((*itr)->as<LogicalFilterOperator>()->getPredicate()->instanceOf<NegateExpressionNode>());
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

TEST_F(FilterSplitUpRuleTest, testSplittingFilterDoubleNot) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical").filter(!!(Attribute("id") == 1)).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterSplitUpRule = Optimizer::FilterSplitUpRule::create();
    const QueryPlanPtr updatedPlan = filterSplitUpRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator->equal((*itr)));
    EXPECT_TRUE(!(*itr)->as<LogicalFilterOperator>()->getPredicate()->instanceOf<NegateExpressionNode>());
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}

//filter(!(!e1) || (!e2)) translates temporarily to filter( !(!(e1)) && !(!(e2)) ) which translates to filter(e1)->filter(e2)
TEST_F(FilterSplitUpRuleTest, testSplittingFilterNotNegatedOr) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("default_logical").filter(!(!(Attribute("id") == 1) || !(Attribute("id2") == 2))).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto filterSplitUpRule = Optimizer::FilterSplitUpRule::create();
    const QueryPlanPtr updatedPlan = filterSplitUpRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    std::vector<std::string> id;
    id.emplace_back("id");
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), id));
    EXPECT_TRUE((*itr)->as<LogicalFilterOperator>()->getPredicate()->instanceOf<EqualsExpressionNode>());
    ++itr;
    std::vector<std::string> id2;
    id2.emplace_back("id2");
    EXPECT_TRUE(isFilterAndAccessesCorrectFields((*itr), id2));
    EXPECT_TRUE((*itr)->as<LogicalFilterOperator>()->getPredicate()->instanceOf<EqualsExpressionNode>());
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}