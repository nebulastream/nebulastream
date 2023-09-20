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
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedTopDownQueryContainmentMergerRule.hpp>
#include <Optimizer/QueryValidation/SyntacticQueryValidation.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Services/QueryParsingService.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/SpatialType.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>
#include <z3++.h>

using namespace NES;

class Z3SignatureBasedTopDownQueryContainmentMergerRuleTest
    : public Testing::BaseUnitTest {

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
        auto csvSourceType = CSVSourceType::create();
        PhysicalSourcePtr windTurbinesPhysicalSource = PhysicalSource::create("windTurbines", "windTurbines", csvSourceType);
        LogicalSourcePtr windTurbinesLogicalSource = LogicalSource::create("windTurbines", schema);
        Catalogs::Source::SourceCatalogEntryPtr sce1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(windTurbinesPhysicalSource,
                                                                   windTurbinesLogicalSource,
                                                                   physicalNode);
        sourceCatalog->addPhysicalSource("windTurbines", sce1);
        PhysicalSourcePtr solarPanels1PhysicalSource = PhysicalSource::create("solarPanels1", "solarPanels1", csvSourceType);
        LogicalSourcePtr solarPanels1LogicalSource = LogicalSource::create("solarPanels1", schema);
        Catalogs::Source::SourceCatalogEntryPtr sce2 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(solarPanels1PhysicalSource,
                                                                   solarPanels1LogicalSource,
                                                                   physicalNode);
        sourceCatalog->addPhysicalSource("solarPanels1", sce2);
        PhysicalSourcePtr solarPanels2PhysicalSource = PhysicalSource::create("solarPanels2", "solarPanels2", csvSourceType);
        LogicalSourcePtr solarPanels2LogicalSource = LogicalSource::create("solarPanels2", schema);
        Catalogs::Source::SourceCatalogEntryPtr sce3 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(solarPanels2PhysicalSource,
                                                                   solarPanels2LogicalSource,
                                                                   physicalNode);
        sourceCatalog->addPhysicalSource("solarPanels2", sce3);
        PhysicalSourcePtr householdsPhysicalSource = PhysicalSource::create("households", "households", csvSourceType);
        LogicalSourcePtr householdsLogicalSource = LogicalSource::create("households", schemaHouseholds);
        Catalogs::Source::SourceCatalogEntryPtr sce4 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(householdsPhysicalSource, householdsLogicalSource, physicalNode);
        sourceCatalog->addPhysicalSource("households", sce4);
    }
};

/**
 * @brief Test applying Z3SignatureBasedBottomUpQueryContainmentRuleTest on Global query plan
 */
TEST_F(Z3SignatureBasedTopDownQueryContainmentMergerRuleTest, testMultipleEqualFilters) {
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

    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedTopDownQueryContainmentMergerRule::create(context, true);
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

TEST_F(Z3SignatureBasedTopDownQueryContainmentMergerRuleTest, testPartialEqualityWithMaps) {
    setupSensorNodeAndSourceCatalog();

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("windTurbines").map(Attribute("value") = 40).map(Attribute("value") = Attribute("value") + 10).sink(PrintSinkDescriptor::create());
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

    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedTopDownQueryContainmentMergerRule::create(context, true);
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

TEST_F(Z3SignatureBasedTopDownQueryContainmentMergerRuleTest, testEqualSourceOperationsDistinctQueries) {
    setupSensorNodeAndSourceCatalog();

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("windTurbines").filter(Attribute("value") < 40).window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(20))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());
    Query query2 = Query::from("windTurbines").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10))).apply(Sum(Attribute("value1"))).sink(PrintSinkDescriptor::create());
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

    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedTopDownQueryContainmentMergerRule::create(context, true);
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


TEST_F(Z3SignatureBasedTopDownQueryContainmentMergerRuleTest, testContainedFilterOperation) {
    setupSensorNodeAndSourceCatalog();

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("windTurbines").filter(Attribute("value") < 40).map(Attribute("value2") = Attribute("value2") * 5).sink(PrintSinkDescriptor::create());
    Query query2 = Query::from("windTurbines").filter(Attribute("value") < 30).map(Attribute("value2") = Attribute("value2") * 5).sink(PrintSinkDescriptor::create());
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

    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedTopDownQueryContainmentMergerRule::create(context, true);
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

//todo: enable test
/*TEST_F(Z3SignatureBasedTopDownQueryContainmentMergerRuleTest, testEqualSourcesWithEqualJoinsButFilterPreventsMerging) {
    setupSensorNodeAndSourceCatalog();

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("windTurbines").filter(Attribute("value") < 5).joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).sink(PrintSinkDescriptor::create());
    Query query2 = Query::from("windTurbines").joinWith(Query::from("solarPanels")).where(Attribute("id1")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(20))).sink(PrintSinkDescriptor::create());
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
    const NodePtr watermarkAssigner2 = (*leftItr);
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

    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedTopDownQueryContainmentMergerRule::create(context);
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
}*/