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
// clang-format on
#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/OpenCLLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UDFs/MapUDF/MapUDFLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/MapUDFsToOpenCLOperatorsRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/JavaUDFDescriptorBuilder.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <iostream>

using namespace NES;

class MapUDFsToOpenCLOperatorsRuleTest : public Testing::BaseUnitTest {

  public:
    SchemaPtr schema;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("RenameSourceToProjectOperatorRuleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup RenameSourceToProjectOperatorRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        schema = Schema::create()->addField("a", BasicType::UINT32)->addField("b", BasicType::UINT32);
        udfCatalog = Catalogs::UDF::UDFCatalog::create();
    }

    void setupSensorNodeAndSourceCatalog(const Catalogs::Source::SourceCatalogPtr& sourceCatalog) const {
        NES_INFO("Setup FilterPushDownTest test case.");
        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
        TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4, properties);
        PhysicalSourcePtr physicalSource = PhysicalSource::create("x", "x1");
        LogicalSourcePtr logicalSource = LogicalSource::create("x", schema);
        auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, physicalNode->getId());
        sourceCatalog->addLogicalSource("src", schema);
        sourceCatalog->addPhysicalSource("src", sce);

        // given
        auto udfName = "my_udf";
        auto udfDescriptor = Catalogs::UDF::JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor();
        // when
        udfCatalog->registerUDF(udfName, udfDescriptor);
        // then
        ASSERT_EQ(udfDescriptor,
                  Catalogs::UDF::UDFDescriptor::as<Catalogs::UDF::JavaUDFDescriptor>(udfCatalog->getUDFDescriptor(udfName)));
    }
};

TEST_F(MapUDFsToOpenCLOperatorsRuleTest, testAddingSingleSourceRenameOperator) {

    // Prepare
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    auto javaUDFDescriptor =
        Catalogs::UDF::UDFDescriptor::as<Catalogs::UDF::JavaUDFDescriptor>(udfCatalog->getUDFDescriptor("my_udf"));
    Query query = Query::from("src").mapUDF(javaUDFDescriptor).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto mapJavaUDFOperators = queryPlan->getOperatorByType<MapUDFLogicalOperatorNode>();
    EXPECT_EQ(mapJavaUDFOperators.size(), 1);

    auto udFsToOpenClOperatorsRule = Optimizer::MapUDFsToOpenCLOperatorsRule::create();
    auto updatedQueryPlan = udFsToOpenClOperatorsRule->apply(queryPlan);
    NES_INFO("{}", updatedQueryPlan->toString());

    mapJavaUDFOperators = updatedQueryPlan->getOperatorByType<MapUDFLogicalOperatorNode>();
    EXPECT_TRUE(mapJavaUDFOperators.empty());

    auto openCLOperators = updatedQueryPlan->getOperatorByType<OpenCLLogicalOperatorNode>();
    EXPECT_TRUE(openCLOperators.size() == 1);

    //Check if the insertion happened at the correct location
    EXPECT_EQ(openCLOperators[0]->getParents().size(), 1);
    EXPECT_TRUE(openCLOperators[0]->getParents()[0]->instanceOf<SinkLogicalOperatorNode>());
    EXPECT_EQ(openCLOperators[0]->getChildren().size(), 1);
    EXPECT_TRUE(openCLOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
}

TEST_F(MapUDFsToOpenCLOperatorsRuleTest, testAddingMultipleSourceRenameOperator) {

    // Prepare
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto javaUDFDescriptor =
        Catalogs::UDF::UDFDescriptor::as<Catalogs::UDF::JavaUDFDescriptor>(udfCatalog->getUDFDescriptor("my_udf"));
    Query query = Query::from("src")
                      .mapUDF(javaUDFDescriptor)
                      .map(Attribute("b") = Attribute("b") + Attribute("a"))
                      .mapUDF(javaUDFDescriptor)
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto mapJavaUDFOperators = queryPlan->getOperatorByType<MapUDFLogicalOperatorNode>();
    EXPECT_EQ(mapJavaUDFOperators.size(), 2);

    auto udFsToOpenClOperatorsRule = Optimizer::MapUDFsToOpenCLOperatorsRule::create();
    auto updatedQueryPlan = udFsToOpenClOperatorsRule->apply(queryPlan);
    NES_INFO("{}", updatedQueryPlan->toString());

    mapJavaUDFOperators = queryPlan->getOperatorByType<MapUDFLogicalOperatorNode>();
    EXPECT_TRUE(mapJavaUDFOperators.empty());

    auto openCLOperators = updatedQueryPlan->getOperatorByType<OpenCLLogicalOperatorNode>();
    EXPECT_EQ(openCLOperators.size(), 2);

    //Check if the insertion happened at the correct location
    EXPECT_EQ(openCLOperators[0]->getParents().size(), 1);
    EXPECT_TRUE(openCLOperators[0]->getParents()[0]->instanceOf<SinkLogicalOperatorNode>());
    EXPECT_EQ(openCLOperators[0]->getChildren().size(), 1);
    EXPECT_TRUE(openCLOperators[0]->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());

    EXPECT_EQ(openCLOperators[1]->getParents().size(), 1);
    EXPECT_TRUE(openCLOperators[1]->getParents()[0]->instanceOf<MapLogicalOperatorNode>());
    EXPECT_EQ(openCLOperators[1]->getChildren().size(), 1);
    EXPECT_TRUE(openCLOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
}
