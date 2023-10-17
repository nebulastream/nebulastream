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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperatorNode.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedTreeBasedQueryContainmentMergerRule.hpp>
#include <Optimizer/QueryValidation/SyntacticQueryValidation.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <iostream>
#include <z3++.h>

using namespace NES;

class Z3SignatureBasedTreeBasedQueryContainmentMergerRuleTest : public Testing::BaseUnitTest {

  public:
    SchemaPtr schema;
    SchemaPtr schemaHouseholds;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog;
    Optimizer::SyntacticQueryValidationPtr syntacticQueryValidation;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("Z3SignatureBasedBottomUpQueryContainmentRuleTest.log", NES::LogLevel::LOG_TRACE);
        NES_INFO("Setup Z3SignatureBasedBottomUpQueryContainmentRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        schema = Schema::create()
                     ->addField("ts", BasicType::UINT32)
                     ->addField("type", BasicType::UINT32)
                     ->addField("id", BasicType::UINT32)
                     ->addField("value", BasicType::UINT64)
                     ->addField("id1", BasicType::UINT32)
                     ->addField("value1", BasicType::UINT64)
                     ->addField("value2", BasicType::UINT64);

        schemaHouseholds = Schema::create()
                               ->addField("ts", BasicType::UINT32)
                               ->addField("type", DataTypeFactory::createFixedChar(8))
                               ->addField("id", BasicType::UINT32)
                               ->addField("value", BasicType::UINT64)
                               ->addField("id1", BasicType::UINT32)
                               ->addField("value1", BasicType::FLOAT32)
                               ->addField("value2", BasicType::FLOAT64)
                               ->addField("value3", BasicType::UINT64);

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
        sourceCatalog->addLogicalSource("windTurbines", schema);
        sourceCatalog->addLogicalSource("solarPanels1", schema);
        sourceCatalog->addLogicalSource("solarPanels2", schema);
        sourceCatalog->addLogicalSource("households", schemaHouseholds);
        /*udfCatalog = Catalogs::UDF::UDFCatalog::create();
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(QueryParsingService::create(jitCompiler));*/
    }

    /* Will be called after a test is executed. */
    void TearDown() override { NES_DEBUG("Tear down Z3SignatureBasedBottomUpQueryContainmentRuleTest test case."); }

    void setupSensorNodeAndSourceCatalog() {
        NES_INFO("Setup FilterPushDownTest test case.");
        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

        TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4, properties);
        auto csvSourceWindTurbine = CSVSourceType::create("windTurbines", "windTurbines");
        LogicalSourcePtr windTurbinesLogicalSource = LogicalSource::create("windTurbines", schema);
        PhysicalSourcePtr windTurbinesPhysicalSource = PhysicalSource::create(csvSourceWindTurbine);
        Catalogs::Source::SourceCatalogEntryPtr sce1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(windTurbinesPhysicalSource,
                                                                   windTurbinesLogicalSource,
                                                                   physicalNode);
        sourceCatalog->addPhysicalSource("windTurbines", sce1);

        auto csvSourceSolarPanel1 = CSVSourceType::create("solarPanels1", "solarPanels1");
        LogicalSourcePtr solarPanels1LogicalSource = LogicalSource::create("solarPanels1", schema);
        PhysicalSourcePtr solarPanels1PhysicalSource = PhysicalSource::create(csvSourceSolarPanel1);
        Catalogs::Source::SourceCatalogEntryPtr sce2 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(solarPanels1PhysicalSource,
                                                                   solarPanels1LogicalSource,
                                                                   physicalNode);
        sourceCatalog->addPhysicalSource("solarPanels2", sce2);

        LogicalSourcePtr solarPanels2LogicalSource = LogicalSource::create("solarPanels2", schema);
        auto csvSourceSolarPanel2 = CSVSourceType::create("solarPanels2", "solarPanels2");
        PhysicalSourcePtr solarPanels2PhysicalSource = PhysicalSource::create(csvSourceSolarPanel2);
        Catalogs::Source::SourceCatalogEntryPtr sce3 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(solarPanels2PhysicalSource,
                                                                   solarPanels2LogicalSource,
                                                                   physicalNode);
        sourceCatalog->addPhysicalSource("solarPanels2", sce3);

        auto csvSourceHouseHolds = CSVSourceType::create("households", "households");
        LogicalSourcePtr householdsLogicalSource = LogicalSource::create("households", schemaHouseholds);
        PhysicalSourcePtr householdsPhysicalSource = PhysicalSource::create(csvSourceHouseHolds);
        Catalogs::Source::SourceCatalogEntryPtr sce4 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(householdsPhysicalSource,
                                                                   householdsLogicalSource,
                                                                   physicalNode);
        sourceCatalog->addPhysicalSource("households", sce4);
    }
};

/**
 * @brief Test that TD-CQM correctly identifies the equivalent filter and map operations here and merges them.
 */
TEST_F(Z3SignatureBasedTreeBasedQueryContainmentMergerRuleTest, testMultipleEqualFilters) {
    setupSensorNodeAndSourceCatalog();

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("windTurbines")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .sink(PrintSinkDescriptor::create());
    Query query2 = Query::from("windTurbines")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .sink(PrintSinkDescriptor::create());
    const QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    const QueryPlanPtr queryPlan2 = query2.getQueryPlan();

    DepthFirstNodeIterator queryPlan1NodeIterator(queryPlan1->getRootOperators()[0]);
    auto leftItr = queryPlan1NodeIterator.begin();

    const NodePtr query1SinkOperator = (*leftItr);
    ++leftItr;
    const NodePtr query1FilterOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr query1FilterOperator2 = (*leftItr);
    ++leftItr;
    const NodePtr query1FilterOperator3 = (*leftItr);
    ++leftItr;
    const NodePtr query1FilterOperator4 = (*leftItr);
    ++leftItr;
    const NodePtr query1SrcOperator = (*leftItr);

    DepthFirstNodeIterator queryPlan2NodeIterator(queryPlan2->getRootOperators()[0]);
    auto rightItr = queryPlan2NodeIterator.begin();

    const NodePtr query2SinkOperator = (*rightItr);

    // Execute
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   Optimizer::QueryMergerRule::Z3SignatureBasedTopDownQueryContainmentMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    queryPlan1->setQueryId(1);
    queryPlan2->setQueryId(2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    auto signatureBasedEqualQueryMergerRule =
        Optimizer::Z3SignatureBasedTreeBasedQueryContainmentMergerRule::create(context, true);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan);

    // UpdatedSharedQueryPlan should have 2 sink operators
    EXPECT_EQ(updatedSharedQueryPlan->getRootOperators().size(), 2);

    NES_TRACE("UpdatedSharedQueryPlan: {}", updatedSharedQueryPlan->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIteratorFirstSink(updatedSharedQueryPlan->getRootOperators()[0]);
    auto itrFirstSink = updatedQueryPlanNodeIteratorFirstSink.begin();
    EXPECT_TRUE(query1SinkOperator->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1FilterOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1FilterOperator2->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1FilterOperator3->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1FilterOperator4->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1SrcOperator->equal((*itrFirstSink)));

    DepthFirstNodeIterator updatedQueryPlanNodeIteratorSecondSink(updatedSharedQueryPlan->getRootOperators()[1]);
    auto itrSecondSink = updatedQueryPlanNodeIteratorSecondSink.begin();
    EXPECT_TRUE(query2SinkOperator->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(query1FilterOperator1->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(query1FilterOperator2->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(query1FilterOperator3->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(query1FilterOperator4->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(query1SrcOperator->equal((*itrSecondSink)));
}

/**
 * @brief: Tests that TD-CQM correctly identifies and merges the equivalent map operation
 *
 * Query 1:
 * Source -> Map1 -> Map2 -> Sink
 *
 * Query 2:
 * Source -> Map1 -> Sink
 *
 * Expected Result:
 * Source -> Map1 -> Map2 -> Sink
 *              \-> Sink
 */
TEST_F(Z3SignatureBasedTreeBasedQueryContainmentMergerRuleTest, testPartialEqualityWithMaps) {
    setupSensorNodeAndSourceCatalog();

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("windTurbines")
                       .map(Attribute("value") = 40)
                       .map(Attribute("value") = Attribute("value") + 10)
                       .sink(PrintSinkDescriptor::create());
    Query query2 = Query::from("windTurbines").map(Attribute("value") = 40).sink(PrintSinkDescriptor::create());
    const QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    const QueryPlanPtr queryPlan2 = query2.getQueryPlan();

    DepthFirstNodeIterator queryPlan1NodeIterator(queryPlan1->getRootOperators()[0]);
    auto leftItr = queryPlan1NodeIterator.begin();

    const NodePtr query1SinkOperator = (*leftItr);
    ++leftItr;
    const NodePtr query1MapOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr query1MapOperator2 = (*leftItr);
    ++leftItr;
    const NodePtr query1SrcOperator = (*leftItr);

    DepthFirstNodeIterator queryPlan2NodeIterator(queryPlan2->getRootOperators()[0]);
    auto rightItr = queryPlan2NodeIterator.begin();

    const NodePtr query2SinkOperator = (*rightItr);

    // Execute
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   Optimizer::QueryMergerRule::Z3SignatureBasedTopDownQueryContainmentMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    queryPlan1->setQueryId(1);
    queryPlan2->setQueryId(2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    auto signatureBasedEqualQueryMergerRule =
        Optimizer::Z3SignatureBasedTreeBasedQueryContainmentMergerRule::create(context, true);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan);

    // UpdatedSharedQueryPlan should have 2 sink operators
    EXPECT_EQ(updatedSharedQueryPlan->getRootOperators().size(), 2);

    NES_TRACE("UpdatedSharedQueryPlan: {}", updatedSharedQueryPlan->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIteratorFirstSink(updatedSharedQueryPlan->getRootOperators()[0]);
    auto itrFirstSink = updatedQueryPlanNodeIteratorFirstSink.begin();
    EXPECT_TRUE(query1SinkOperator->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1MapOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1MapOperator2->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1SrcOperator->equal((*itrFirstSink)));

    DepthFirstNodeIterator updatedQueryPlanNodeIteratorSecondSink(updatedSharedQueryPlan->getRootOperators()[1]);
    auto itrSecondSink = updatedQueryPlanNodeIteratorSecondSink.begin();
    EXPECT_TRUE(query2SinkOperator->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(query1MapOperator2->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(query1SrcOperator->equal((*itrSecondSink)));
}

/**
 * @brief: Tests that only the sources are merged here
 *
 * Query 1:
 * Source -> Filter1 -> Window1 -> Sink
 *
 * Query 2:
 * Source -> Window2 -> Sink
 *
 * Equivalent sources:
 * Source -> Filter1 -> Window1 -> Sink
 *     \-> Window2 -> Sink
 */
TEST_F(Z3SignatureBasedTreeBasedQueryContainmentMergerRuleTest, testEqualSourceOperationsDistinctQueries) {
    setupSensorNodeAndSourceCatalog();

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("windTurbines")
                       .filter(Attribute("value") < 40)
                       .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(20)))
                       .apply(Sum(Attribute("value1")))
                       .sink(PrintSinkDescriptor::create());
    Query query2 = Query::from("windTurbines")
                       .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10)))
                       .apply(Sum(Attribute("value1")))
                       .sink(PrintSinkDescriptor::create());
    const QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    const QueryPlanPtr queryPlan2 = query2.getQueryPlan();

    DepthFirstNodeIterator queryPlan1NodeIterator(queryPlan1->getRootOperators()[0]);
    auto leftItr = queryPlan1NodeIterator.begin();

    const NodePtr query1SinkOperator = (*leftItr);
    ++leftItr;
    const NodePtr windowOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr watermarkAssigner1 = (*leftItr);
    ++leftItr;
    const NodePtr query1FilterOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr query1SrcOperator = (*leftItr);

    DepthFirstNodeIterator queryPlan2NodeIterator(queryPlan2->getRootOperators()[0]);
    auto rightItr = queryPlan2NodeIterator.begin();

    const NodePtr query2SinkOperator = (*rightItr);
    ++rightItr;
    const NodePtr windowOperator2 = (*rightItr);
    ++rightItr;
    const NodePtr watermarkAssigner2 = (*rightItr);

    // Execute
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   Optimizer::QueryMergerRule::Z3SignatureBasedTopDownQueryContainmentMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    queryPlan1->setQueryId(1);
    queryPlan2->setQueryId(2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    auto signatureBasedEqualQueryMergerRule =
        Optimizer::Z3SignatureBasedTreeBasedQueryContainmentMergerRule::create(context, true);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan);

    // UpdatedSharedQueryPlan should have 2 sink operators
    EXPECT_EQ(updatedSharedQueryPlan->getRootOperators().size(), 2);

    NES_TRACE("UpdatedSharedQueryPlan: {}", updatedSharedQueryPlan->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIteratorFirstSink(updatedSharedQueryPlan->getRootOperators()[0]);
    auto itrFirstSink = updatedQueryPlanNodeIteratorFirstSink.begin();
    EXPECT_TRUE(query1SinkOperator->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(windowOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(watermarkAssigner1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1FilterOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1SrcOperator->equal((*itrFirstSink)));

    DepthFirstNodeIterator updatedQueryPlanNodeIteratorSecondSink(updatedSharedQueryPlan->getRootOperators()[1]);
    auto itrSecondSink = updatedQueryPlanNodeIteratorSecondSink.begin();
    EXPECT_TRUE(query2SinkOperator->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(windowOperator2->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(watermarkAssigner2->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(query1SrcOperator->equal((*itrSecondSink)));
}

TEST_F(Z3SignatureBasedTreeBasedQueryContainmentMergerRuleTest, testContainedFilterOperation) {
    setupSensorNodeAndSourceCatalog();

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("windTurbines")
                       .filter(Attribute("value") < 40)
                       .map(Attribute("value2") = Attribute("value2") * 5)
                       .sink(PrintSinkDescriptor::create());
    Query query2 = Query::from("windTurbines")
                       .filter(Attribute("value") < 30)
                       .map(Attribute("value2") = Attribute("value2") * 5)
                       .sink(PrintSinkDescriptor::create());
    const QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    const QueryPlanPtr queryPlan2 = query2.getQueryPlan();

    DepthFirstNodeIterator queryPlan1NodeIterator(queryPlan1->getRootOperators()[0]);
    auto leftItr = queryPlan1NodeIterator.begin();

    const NodePtr query1SinkOperator = (*leftItr);
    ++leftItr;
    const NodePtr mapOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr filterOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr query1SrcOperator = (*leftItr);

    DepthFirstNodeIterator queryPlan2NodeIterator(queryPlan2->getRootOperators()[0]);
    auto rightItr = queryPlan2NodeIterator.begin();

    const NodePtr query2SinkOperator = (*rightItr);
    ++rightItr;
    const NodePtr mapOperator2 = (*rightItr);
    ++rightItr;
    const NodePtr filterOperator2 = (*rightItr);
    ++rightItr;
    const NodePtr query2SrcOperator = (*rightItr);

    // Execute
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   Optimizer::QueryMergerRule::Z3SignatureBasedTopDownQueryContainmentMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    queryPlan1->setQueryId(1);
    queryPlan2->setQueryId(2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    auto signatureBasedEqualQueryMergerRule =
        Optimizer::Z3SignatureBasedTreeBasedQueryContainmentMergerRule::create(context, true);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan);

    // UpdatedSharedQueryPlan should have 2 sink operators
    EXPECT_EQ(updatedSharedQueryPlan->getRootOperators().size(), 2);

    NES_TRACE("UpdatedSharedQueryPlan: {}", updatedSharedQueryPlan->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIteratorFirstSink(updatedSharedQueryPlan->getRootOperators()[0]);
    auto itrFirstSink = updatedQueryPlanNodeIteratorFirstSink.begin();
    EXPECT_TRUE(query1SinkOperator->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(mapOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(filterOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1SrcOperator->equal((*itrFirstSink)));

    DepthFirstNodeIterator updatedQueryPlanNodeIteratorSecondSink(updatedSharedQueryPlan->getRootOperators()[1]);
    auto itrSecondSink = updatedQueryPlanNodeIteratorSecondSink.begin();
    EXPECT_TRUE(query2SinkOperator->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(filterOperator2->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(mapOperator1->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(filterOperator1->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(query1SrcOperator->equal((*itrSecondSink)));
}

/**
 * @brief Tests that the merger correctly identifies that the joins are different
 *
 * Query 1:
 * Source1 -> Filter -> (Source2) -> Join1 -> Sink
 *
 * Query 2:
 * Source1 -> (Source2) -> Join2 -> Sink
 *
 * Query 2 contains Query 1:
 * Source1 -> (Source2) -> Join1 -> Filter -> Sink
 *                 \-> Join2 -> Sink
 */
TEST_F(Z3SignatureBasedTreeBasedQueryContainmentMergerRuleTest, testEqualSourcesDifferentJoins) {
    setupSensorNodeAndSourceCatalog();

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("windTurbines")
                       .filter(Attribute("value") < 5)
                       .joinWith(Query::from("solarPanels1"))
                       .where(Attribute("id1"))
                       .equalsTo(Attribute("id"))
                       .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                       .sink(PrintSinkDescriptor::create());
    Query query2 = Query::from("windTurbines")
                       .joinWith(Query::from("solarPanels1"))
                       .where(Attribute("id1"))
                       .equalsTo(Attribute("id"))
                       .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(20)))
                       .sink(PrintSinkDescriptor::create());
    const QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    const QueryPlanPtr queryPlan2 = query2.getQueryPlan();

    DepthFirstNodeIterator queryPlan1NodeIterator(queryPlan1->getRootOperators()[0]);
    auto leftItr = queryPlan1NodeIterator.begin();

    const NodePtr query1SinkOperator = (*leftItr);
    ++leftItr;
    const NodePtr joinOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr watermarkAssigner1 = (*leftItr);
    ++leftItr;
    const NodePtr query1SrcOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr watermarkAssigner2 = (*leftItr);
    ++leftItr;
    const NodePtr query1FilterOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr query1SrcOperator2 = (*leftItr);

    DepthFirstNodeIterator queryPlan2NodeIterator(queryPlan2->getRootOperators()[0]);
    auto rightItr = queryPlan2NodeIterator.begin();

    const NodePtr query2SinkOperator = (*rightItr);
    ++rightItr;
    const NodePtr joinOperator2 = (*rightItr);
    ++rightItr;
    const NodePtr watermarkAssigner3 = (*rightItr);
    ++rightItr;
    const NodePtr query2SrcOperator1 = (*rightItr);
    ++rightItr;
    const NodePtr watermarkAssigner4 = (*rightItr);
    ++rightItr;
    const NodePtr query2SrcOperator2 = (*rightItr);

    // Execute
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   Optimizer::QueryMergerRule::Z3SignatureBasedTopDownQueryContainmentMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    queryPlan1->setQueryId(1);
    queryPlan2->setQueryId(2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    auto signatureBasedEqualQueryMergerRule =
        Optimizer::Z3SignatureBasedTreeBasedQueryContainmentMergerRule::create(context, true);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan);

    // UpdatedSharedQueryPlan should have 2 sink operators
    EXPECT_EQ(updatedSharedQueryPlan->getRootOperators().size(), 2);

    NES_TRACE("UpdatedSharedQueryPlan: {}", updatedSharedQueryPlan->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIteratorFirstSink(updatedSharedQueryPlan->getRootOperators()[0]);
    auto itrFirstSink = updatedQueryPlanNodeIteratorFirstSink.begin();
    EXPECT_TRUE(query1SinkOperator->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(joinOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(watermarkAssigner1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1SrcOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(watermarkAssigner2->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1FilterOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1SrcOperator2->equal((*itrFirstSink)));

    DepthFirstNodeIterator updatedQueryPlanNodeIteratorSecondSink(updatedSharedQueryPlan->getRootOperators()[1]);
    auto itrSecondSink = updatedQueryPlanNodeIteratorSecondSink.begin();
    EXPECT_TRUE(query2SinkOperator->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(joinOperator2->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(watermarkAssigner3->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(query1SrcOperator1->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(watermarkAssigner4->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(query1SrcOperator2->equal((*itrSecondSink)));
}

/**
 * @brief Tests that the contained filter will be extreacted correctly
 *
 * Query 1:
 * Source1 -> Filter -> (Source2) -> Join -> Sink
 *
 * Query 2:
 * Source1 -> (Source2) -> Join -> Sink
 *
 * Query 2 contains Query 1:
 * Source1 -> (Source2) -> Join -> Filter -> Sink
 *                          \-> Sink
 */
TEST_F(Z3SignatureBasedTreeBasedQueryContainmentMergerRuleTest, testEqualSourcesWithEqualJoinsAndFilterContainment) {
    setupSensorNodeAndSourceCatalog();

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("windTurbines")
                       .filter(Attribute("value") < 5)
                       .joinWith(Query::from("solarPanels1"))
                       .where(Attribute("id1"))
                       .equalsTo(Attribute("id"))
                       .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                       .sink(PrintSinkDescriptor::create());
    Query query2 = Query::from("windTurbines")
                       .joinWith(Query::from("solarPanels1"))
                       .where(Attribute("id1"))
                       .equalsTo(Attribute("id"))
                       .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                       .sink(PrintSinkDescriptor::create());
    const QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    const QueryPlanPtr queryPlan2 = query2.getQueryPlan();

    DepthFirstNodeIterator queryPlan1NodeIterator(queryPlan1->getRootOperators()[0]);
    auto leftItr = queryPlan1NodeIterator.begin();

    const NodePtr query1SinkOperator = (*leftItr);
    ++leftItr;
    const NodePtr joinOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr watermarkAssigner1 = (*leftItr);
    ++leftItr;
    const NodePtr query1SrcOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr watermarkAssigner2 = (*leftItr);
    ++leftItr;
    const NodePtr query1FilterOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr query1SrcOperator2 = (*leftItr);

    DepthFirstNodeIterator queryPlan2NodeIterator(queryPlan2->getRootOperators()[0]);
    auto rightItr = queryPlan2NodeIterator.begin();

    const NodePtr query2SinkOperator = (*rightItr);
    ++rightItr;
    const NodePtr joinOperator2 = (*rightItr);
    ++rightItr;
    const NodePtr watermarkAssigner3 = (*rightItr);
    ++rightItr;
    const NodePtr query2SrcOperator1 = (*rightItr);
    ++rightItr;
    const NodePtr watermarkAssigner4 = (*rightItr);
    ++rightItr;
    const NodePtr query2SrcOperator2 = (*rightItr);

    // Execute
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   Optimizer::QueryMergerRule::Z3SignatureBasedTopDownQueryContainmentMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    queryPlan1->setQueryId(1);
    queryPlan2->setQueryId(2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    auto signatureBasedEqualQueryMergerRule =
        Optimizer::Z3SignatureBasedTreeBasedQueryContainmentMergerRule::create(context, true);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan);

    // UpdatedSharedQueryPlan should have 2 sink operators
    EXPECT_EQ(updatedSharedQueryPlan->getRootOperators().size(), 2);

    NES_TRACE("UpdatedSharedQueryPlan: {}", updatedSharedQueryPlan->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIteratorFirstSink(updatedSharedQueryPlan->getRootOperators()[0]);
    auto itrFirstSink = updatedQueryPlanNodeIteratorFirstSink.begin();
    EXPECT_TRUE(query1SinkOperator->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1FilterOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(joinOperator2->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(watermarkAssigner3->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1SrcOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(watermarkAssigner4->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1SrcOperator2->equal((*itrFirstSink)));

    DepthFirstNodeIterator updatedQueryPlanNodeIteratorSecondSink(updatedSharedQueryPlan->getRootOperators()[1]);
    auto itrSecondSink = updatedQueryPlanNodeIteratorSecondSink.begin();
    EXPECT_TRUE(query2SinkOperator->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(joinOperator2->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(watermarkAssigner3->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(query1SrcOperator1->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(watermarkAssigner4->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(query1SrcOperator2->equal((*itrSecondSink)));
}

/**
 * @brief Here we are testing for correct merging when there is a different number of windows involved
 *
 * Query 1:
 * Source -> Window1 -> Window2 -> Sink
 *
 * Query 2:
 * Source -> Window1 -> Sink
 *
 * Query 1 containes Query 2:
 * Source -> Window1 -> Window2 -> Sink
 *                \-> Sink
 */
TEST_F(Z3SignatureBasedTreeBasedQueryContainmentMergerRuleTest, testDifferentNumberOfWindows) {
    setupSensorNodeAndSourceCatalog();

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("windTurbines")
                       .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                       .apply(Sum(Attribute("value")))
                       .window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(10000)))
                       .apply(Min(Attribute("value")))
                       .sink(PrintSinkDescriptor::create());
    Query query2 = Query::from("windTurbines")
                       .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                       .apply(Sum(Attribute("value")))
                       .sink(PrintSinkDescriptor::create());
    const QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    const QueryPlanPtr queryPlan2 = query2.getQueryPlan();

    DepthFirstNodeIterator queryPlan1NodeIterator(queryPlan1->getRootOperators()[0]);
    auto leftItr = queryPlan1NodeIterator.begin();

    const NodePtr query1SinkOperator = (*leftItr);
    ++leftItr;
    const NodePtr windowOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr watermarkAssigner1 = (*leftItr);
    ++leftItr;
    const NodePtr windowOperator2 = (*leftItr);
    ++leftItr;
    const NodePtr watermarkAssigner2 = (*leftItr);
    ++leftItr;
    const NodePtr query1SrcOperator = (*leftItr);

    DepthFirstNodeIterator queryPlan2NodeIterator(queryPlan2->getRootOperators()[0]);
    auto rightItr = queryPlan2NodeIterator.begin();

    const NodePtr query2SinkOperator = (*rightItr);
    ++rightItr;
    const NodePtr windowOperator3 = (*rightItr);
    ++rightItr;
    const NodePtr watermarkAssigner3 = (*rightItr);
    ++rightItr;
    const NodePtr query2SrcOperator2 = (*rightItr);

    // Execute
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   Optimizer::QueryMergerRule::Z3SignatureBasedTopDownQueryContainmentMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    queryPlan1->setQueryId(1);
    queryPlan2->setQueryId(2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    auto signatureBasedEqualQueryMergerRule =
        Optimizer::Z3SignatureBasedTreeBasedQueryContainmentMergerRule::create(context, true);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan);

    // UpdatedSharedQueryPlan should have 2 sink operators
    EXPECT_EQ(updatedSharedQueryPlan->getRootOperators().size(), 2);

    NES_TRACE("UpdatedSharedQueryPlan: {}", updatedSharedQueryPlan->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIteratorFirstSink(updatedSharedQueryPlan->getRootOperators()[0]);
    auto itrFirstSink = updatedQueryPlanNodeIteratorFirstSink.begin();
    EXPECT_TRUE(query1SinkOperator->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(windowOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(watermarkAssigner1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(windowOperator2->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(watermarkAssigner2->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1SrcOperator->equal((*itrFirstSink)));

    DepthFirstNodeIterator updatedQueryPlanNodeIteratorSecondSink(updatedSharedQueryPlan->getRootOperators()[1]);
    auto itrSecondSink = updatedQueryPlanNodeIteratorSecondSink.begin();
    EXPECT_TRUE(query2SinkOperator->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(windowOperator2->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(watermarkAssigner2->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(query1SrcOperator->equal((*itrSecondSink)));
}

/**
 * @brief: In this test, we verify that given two queries with two windows each and multiple containment relationships, the merger will create a correct query plan:
 *
 * Query 1:
 * Source -> Window1 -> Window2 -> Sink
 * Query 2:
 * Source -> Window3 -> Window4 -> Sink
 *
 * Query 1 contains Query 2:
 * Source -> Window1 ->Window2 -> Sink
 *                  \-> Window3 -> Window4 -> Sink
 */
TEST_F(Z3SignatureBasedTreeBasedQueryContainmentMergerRuleTest, testSameNumberOfWindowsTwoContainmentRelationships) {
    setupSensorNodeAndSourceCatalog();

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("windTurbines")
                       .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(100)))
                       .apply(Sum(Attribute("value")))
                       .window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(1000)))
                       .apply(Max(Attribute("value")))
                       .sink(PrintSinkDescriptor::create());
    Query query2 = Query::from("windTurbines")
                       .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                       .apply(Sum(Attribute("value")))
                       .window(TumblingWindow::of(EventTime(Attribute("start")), Milliseconds(10000)))
                       .apply(Min(Attribute("value")))
                       .sink(PrintSinkDescriptor::create());
    const QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    const QueryPlanPtr queryPlan2 = query2.getQueryPlan();

    DepthFirstNodeIterator queryPlan1NodeIterator(queryPlan1->getRootOperators()[0]);
    auto leftItr = queryPlan1NodeIterator.begin();

    const NodePtr query1SinkOperator = (*leftItr);
    ++leftItr;
    const NodePtr windowOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr watermarkAssigner1 = (*leftItr);
    ++leftItr;
    const NodePtr windowOperator2 = (*leftItr);
    ++leftItr;
    const NodePtr watermarkAssigner2 = (*leftItr);
    ++leftItr;
    const NodePtr query1SrcOperator = (*leftItr);

    DepthFirstNodeIterator queryPlan2NodeIterator(queryPlan2->getRootOperators()[0]);
    auto rightItr = queryPlan2NodeIterator.begin();

    const NodePtr query2SinkOperator = (*rightItr);
    ++rightItr;
    const NodePtr windowOperator3 = (*rightItr);
    ++rightItr;
    const NodePtr watermarkAssigner3 = (*rightItr);
    ++rightItr;
    const NodePtr windowOperator4 = (*rightItr);
    ++rightItr;
    const NodePtr watermarkAssigner4 = (*rightItr);
    ++rightItr;
    const NodePtr query2SrcOperator2 = (*rightItr);

    // Execute
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   Optimizer::QueryMergerRule::Z3SignatureBasedTopDownQueryContainmentMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    queryPlan1->setQueryId(1);
    queryPlan2->setQueryId(2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    auto signatureBasedEqualQueryMergerRule =
        Optimizer::Z3SignatureBasedTreeBasedQueryContainmentMergerRule::create(context, true);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan);

    // UpdatedSharedQueryPlan should have 2 sink operators
    EXPECT_EQ(updatedSharedQueryPlan->getRootOperators().size(), 2);

    NES_TRACE("UpdatedSharedQueryPlan: {}", updatedSharedQueryPlan->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIteratorFirstSink(updatedSharedQueryPlan->getRootOperators()[0]);
    auto itrFirstSink = updatedQueryPlanNodeIteratorFirstSink.begin();
    EXPECT_TRUE(query1SinkOperator->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(windowOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(watermarkAssigner1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(windowOperator2->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(watermarkAssigner2->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1SrcOperator->equal((*itrFirstSink)));

    DepthFirstNodeIterator updatedQueryPlanNodeIteratorSecondSink(updatedSharedQueryPlan->getRootOperators()[1]);
    auto itrSecondSink = updatedQueryPlanNodeIteratorSecondSink.begin();
    EXPECT_TRUE(query2SinkOperator->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(windowOperator3->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(watermarkAssigner3->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(windowOperator4->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(watermarkAssigner4->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(windowOperator2->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(watermarkAssigner2->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(query1SrcOperator->equal((*itrSecondSink)));
}

/**
 * @brief: In this test, we verify that TD-CQM correctly merges equivalent union operations followed by distinct window operations:
 *
 * Query 1:
 * Source1 -> (Source2) -> Union1 -> Window1 -> Sink
 * Query 2:
 * Source1 -> (Source2) -> Union1 -> Window2 -> Sink
 *
 * Query 1 contains Query 2:
 * Source1 -> (Source2) -> Union1 -> Window1 -> Sink
 *                              \-> Window2 -> Sink
 */
TEST_F(Z3SignatureBasedTreeBasedQueryContainmentMergerRuleTest, testEquivalentUnionDifferentWindows) {
    setupSensorNodeAndSourceCatalog();

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("windTurbines")
                       .unionWith(Query::from("solarPanels1"))
                       .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000)))
                       .apply(Avg(Attribute("value")))
                       .sink(PrintSinkDescriptor::create());
    Query query2 = Query::from("windTurbines")
                       .unionWith(Query::from("solarPanels1"))
                       .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10000)))
                       .apply(Sum(Attribute("value")))
                       .sink(PrintSinkDescriptor::create());
    const QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    const QueryPlanPtr queryPlan2 = query2.getQueryPlan();

    DepthFirstNodeIterator queryPlan1NodeIterator(queryPlan1->getRootOperators()[0]);
    auto leftItr = queryPlan1NodeIterator.begin();

    const NodePtr query1SinkOperator = (*leftItr);
    ++leftItr;
    const NodePtr windowOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr watermarkAssigner1 = (*leftItr);
    ++leftItr;
    const NodePtr unionOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr query1SrcOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr query1SrcOperator2 = (*leftItr);

    DepthFirstNodeIterator queryPlan2NodeIterator(queryPlan2->getRootOperators()[0]);
    auto rightItr = queryPlan2NodeIterator.begin();

    const NodePtr query2SinkOperator = (*rightItr);
    ++rightItr;
    const NodePtr windowOperator2 = (*rightItr);
    ++rightItr;
    const NodePtr watermarkAssigner2 = (*rightItr);
    ++rightItr;
    const NodePtr unionOperator2 = (*rightItr);
    ++rightItr;
    const NodePtr query2SrcOperator1 = (*rightItr);
    ++rightItr;
    const NodePtr query2SrcOperator2 = (*rightItr);

    // Execute
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   Optimizer::QueryMergerRule::Z3SignatureBasedTopDownQueryContainmentMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    queryPlan1->setQueryId(1);
    queryPlan2->setQueryId(2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    auto signatureBasedEqualQueryMergerRule =
        Optimizer::Z3SignatureBasedTreeBasedQueryContainmentMergerRule::create(context, true);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan);

    // UpdatedSharedQueryPlan should have 2 sink operators
    EXPECT_EQ(updatedSharedQueryPlan->getRootOperators().size(), 2);

    NES_TRACE("UpdatedSharedQueryPlan: {}", updatedSharedQueryPlan->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIteratorFirstSink(updatedSharedQueryPlan->getRootOperators()[0]);
    auto itrFirstSink = updatedQueryPlanNodeIteratorFirstSink.begin();
    EXPECT_TRUE(query1SinkOperator->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(windowOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(watermarkAssigner1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(unionOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1SrcOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1SrcOperator2->equal((*itrFirstSink)));

    DepthFirstNodeIterator updatedQueryPlanNodeIteratorSecondSink(updatedSharedQueryPlan->getRootOperators()[1]);
    auto itrSecondSink = updatedQueryPlanNodeIteratorSecondSink.begin();
    EXPECT_TRUE(query2SinkOperator->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(windowOperator2->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(watermarkAssigner2->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(unionOperator1->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(query1SrcOperator1->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(query1SrcOperator2->equal((*itrSecondSink)));
}

/**
 * @brief: In this test, we verify that TD-CQM correctly merges equivalent union operations followed by distinct window operations:
 *
 * Query 1:
 * Source1 -> (Source2) -> Union1 -> Filter1 -> Project1 -> Join1 -> (Source3) -> Sink1
 * Query 2:
 * Source1 -> (Source2) -> Union1 -> (Source3) -> Join1 -> Filter1 -> Project2 -> Sink1
 *
 * Query 1 contains Query 2:
 * Source1 -> (Source2) -> Union1 -> Filter1 -> Project1 -> Join1 -> (Source3) -> Sink1
 *                                                              \-> Project2 -> Sink
 */
TEST_F(Z3SignatureBasedTreeBasedQueryContainmentMergerRuleTest, testProjectionContainment) {
    setupSensorNodeAndSourceCatalog();

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 =
        Query::from("windTurbines")
            .unionWith(Query::from("solarPanels1"))
            .filter(Attribute("value") > 4)
            .project(Attribute("value"), Attribute("id1"), Attribute("value1"), Attribute("ts"))
            .joinWith(
                Query::from("households").project(Attribute("value"), Attribute("id"), Attribute("value1"), Attribute("ts")))
            .where(Attribute("windTurbines$id1"))
            .equalsTo(Attribute("households$id"))
            .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
            .sink(PrintSinkDescriptor::create());
    Query query2 = Query::from("windTurbines")
                       .unionWith(Query::from("solarPanels1"))
                       .joinWith(Query::from("households"))
                       .where(Attribute("id1"))
                       .equalsTo(Attribute("id"))
                       .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                       .filter(Attribute("value") > 4)
                       .project(Attribute("windTurbines$value"),
                                Attribute("windTurbines$id1"),
                                Attribute("households$value"),
                                Attribute("households$id"),
                                Attribute("windTurbines$ts"),
                                Attribute("households$ts"))
                       .sink(PrintSinkDescriptor::create());
    const QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    const QueryPlanPtr queryPlan2 = query2.getQueryPlan();

    DepthFirstNodeIterator queryPlan1NodeIterator(queryPlan1->getRootOperators()[0]);
    auto leftItr = queryPlan1NodeIterator.begin();

    const NodePtr query1SinkOperator = (*leftItr);
    ++leftItr;
    const NodePtr joinOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr watermarkAssigner1 = (*leftItr);
    ++leftItr;
    const NodePtr projectOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr query1SrcOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr watermarkAssigner2 = (*leftItr);
    ++leftItr;
    const NodePtr projectOperator2 = (*leftItr);
    ++leftItr;
    const NodePtr filterOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr unionOperator1 = (*leftItr);
    ++leftItr;
    const NodePtr query1SrcOperator2 = (*leftItr);
    ++leftItr;
    const NodePtr query1SrcOperator3 = (*leftItr);

    DepthFirstNodeIterator queryPlan2NodeIterator(queryPlan2->getRootOperators()[0]);
    auto rightItr = queryPlan2NodeIterator.begin();

    const NodePtr query2SinkOperator = (*rightItr);
    ++rightItr;
    const NodePtr projectOperator3 = (*rightItr);

    // Execute
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   Optimizer::QueryMergerRule::Z3SignatureBasedTopDownQueryContainmentMergerRule);
    z3InferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan2);

    queryPlan1->setQueryId(1);
    queryPlan2->setQueryId(2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    auto signatureBasedEqualQueryMergerRule =
        Optimizer::Z3SignatureBasedTreeBasedQueryContainmentMergerRule::create(context, true);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan);

    // UpdatedSharedQueryPlan should have 2 sink operators
    EXPECT_EQ(updatedSharedQueryPlan->getRootOperators().size(), 2);

    NES_TRACE("UpdatedSharedQueryPlan: {}", updatedSharedQueryPlan->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIteratorFirstSink(updatedSharedQueryPlan->getRootOperators()[0]);
    auto itrFirstSink = updatedQueryPlanNodeIteratorFirstSink.begin();
    EXPECT_TRUE(query1SinkOperator->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(joinOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(watermarkAssigner1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(projectOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1SrcOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(watermarkAssigner2->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(projectOperator2->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(filterOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(unionOperator1->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1SrcOperator2->equal((*itrFirstSink)));
    ++itrFirstSink;
    EXPECT_TRUE(query1SrcOperator3->equal((*itrFirstSink)));

    DepthFirstNodeIterator updatedQueryPlanNodeIteratorSecondSink(updatedSharedQueryPlan->getRootOperators()[1]);
    auto itrSecondSink = updatedQueryPlanNodeIteratorSecondSink.begin();
    EXPECT_TRUE(query2SinkOperator->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(projectOperator3->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(joinOperator1->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(watermarkAssigner1->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(projectOperator1->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(query1SrcOperator1->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(watermarkAssigner2->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(projectOperator2->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(filterOperator1->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(unionOperator1->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(query1SrcOperator2->equal((*itrSecondSink)));
    ++itrSecondSink;
    EXPECT_TRUE(query1SrcOperator3->equal((*itrSecondSink)));
}