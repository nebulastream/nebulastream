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

#include "z3++.h"
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <NesBaseTest.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/AdaptiveActiveStandby.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Services/QueryParsingService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

using namespace NES;
using namespace z3;
using namespace Configurations;

class AdaptiveActiveStandbyTest : public Testing::TestWithErrorHandling<testing::Test> {

  protected:
    z3::ContextPtr z3Context;

  public:
    std::shared_ptr<QueryParsingService> queryParsingService;
    Catalogs::UDF::UdfCatalogPtr udfCatalog;
    TopologyPtr topology;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup AdaptiveActiveStandbyTest test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::Logger::setupLogging("AdaptiveActiveStandbyTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup AdaptiveActiveStandbyTest test case." << std::endl;
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
        udfCatalog = Catalogs::UDF::UdfCatalog::create();

        z3::config cfg;
        cfg.set("timeout", 50000);
        cfg.set("model", false);
        cfg.set("type_check", false);
        z3Context = std::make_shared<z3::context>(cfg);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Setup AdaptiveActiveStandbyTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down AdaptiveActiveStandbyTest test class." << std::endl; }

    /*
     * 3
     * 2
     * 1
     */
    void setupTopologyAndSourceCatalogSequential3() {
        topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(3, "localhost", 123, 124, 100);
        rootNode->addNodeProperty("slots", 100);
        topology->setAsRoot(rootNode);

        TopologyNodePtr middleNode1 = TopologyNode::create(2, "localhost", 123, 124, 2);
        middleNode1->addNodeProperty("slots", 2);
        topology->addNewTopologyNodeAsChild(rootNode, middleNode1);

        TopologyNodePtr sourceNode = TopologyNode::create(1, "localhost", 123, 124, 0);
        sourceNode->addNodeProperty("slots", 0);
        topology->addNewTopologyNodeAsChild(middleNode1, sourceNode);

        LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));

        sourceNode->addLinkProperty(middleNode1, linkProperty);

        middleNode1->addLinkProperty(sourceNode, linkProperty);
        middleNode1->addLinkProperty(rootNode, linkProperty);

        rootNode->addLinkProperty(middleNode1, linkProperty);

        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        const std::string sourceName = "car";

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        sourceCatalog->addLogicalSource(sourceName, schema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
        CSVSourceTypePtr csvSourceType = CSVSourceType::create();
        csvSourceType->setGatheringInterval(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
        auto physicalSource = PhysicalSource::create(sourceName, "test2", csvSourceType);
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNode);
        sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
    }

    /*
     * 4
     * 3
     * 2
     * 1
     */
    void setupTopologyAndSourceCatalogSequential4() {
        topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(4, "localhost", 123, 124, 100);
        rootNode->addNodeProperty("slots", 100);
        topology->setAsRoot(rootNode);

        TopologyNodePtr middleNode2 = TopologyNode::create(3, "localhost", 123, 124, 10);
        middleNode2->addNodeProperty("slots", 10);
        topology->addNewTopologyNodeAsChild(rootNode, middleNode2);

        TopologyNodePtr middleNode1 = TopologyNode::create(2, "localhost", 123, 124, 2);
        middleNode1->addNodeProperty("slots", 2);
        topology->addNewTopologyNodeAsChild(middleNode2, middleNode1);

        TopologyNodePtr sourceNode = TopologyNode::create(1, "localhost", 123, 124, 0);
        sourceNode->addNodeProperty("slots", 0);
        topology->addNewTopologyNodeAsChild(middleNode1, sourceNode);

        LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));

        sourceNode->addLinkProperty(middleNode1, linkProperty);

        middleNode1->addLinkProperty(sourceNode, linkProperty);
        middleNode1->addLinkProperty(middleNode2, linkProperty);

        middleNode2->addLinkProperty(middleNode1, linkProperty);
        middleNode2->addLinkProperty(rootNode, linkProperty);

        rootNode->addLinkProperty(middleNode2, linkProperty);

        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        const std::string sourceName = "car";

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        sourceCatalog->addLogicalSource(sourceName, schema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
        CSVSourceTypePtr csvSourceType = CSVSourceType::create();
        csvSourceType->setGatheringInterval(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
        auto physicalSource = PhysicalSource::create(sourceName, "test2", csvSourceType);
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNode);
        sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
    }


    /*
     *   4
     * 2   3
     *   1
     */
    void setupTopologyAndSourceCatalogSimpleShortDiamond() {
        topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(4, "localhost", 123, 124, 100);
        rootNode->addNodeProperty("slots", 100);
        topology->setAsRoot(rootNode);

        TopologyNodePtr middleNode1 = TopologyNode::create(3, "localhost", 123, 124, 2);
        middleNode1->addNodeProperty("slots", 2);
        topology->addNewTopologyNodeAsChild(rootNode, middleNode1);

        TopologyNodePtr middleNode2 = TopologyNode::create(2, "localhost", 123, 124, 2);
        middleNode2->addNodeProperty("slots", 2);
        topology->addNewTopologyNodeAsChild(rootNode, middleNode2);

        TopologyNodePtr sourceNode = TopologyNode::create(1, "localhost", 123, 124, 0);
        sourceNode->addNodeProperty("slots", 0);
        topology->addNewTopologyNodeAsChild(middleNode1, sourceNode);
        topology->addNewTopologyNodeAsChild(middleNode2, sourceNode);

        LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));

        sourceNode->addLinkProperty(middleNode1, linkProperty);
        sourceNode->addLinkProperty(middleNode2, linkProperty);

        middleNode1->addLinkProperty(sourceNode, linkProperty);
        middleNode1->addLinkProperty(rootNode, linkProperty);

        middleNode2->addLinkProperty(sourceNode, linkProperty);
        middleNode2->addLinkProperty(rootNode, linkProperty);

        rootNode->addLinkProperty(middleNode1, linkProperty);
        rootNode->addLinkProperty(middleNode2, linkProperty);

        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        const std::string sourceName = "car";

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        sourceCatalog->addLogicalSource(sourceName, schema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
        CSVSourceTypePtr csvSourceType = CSVSourceType::create();
        csvSourceType->setGatheringInterval(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
        auto physicalSource = PhysicalSource::create(sourceName, "test2", csvSourceType);
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNode);
        sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
    }

    /*
     *   4
     * 3   6
     * 2   5
     *   1
     */
    void setupTopologyAndSourceCatalogTallDiamond() {
        topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(4, "localhost", 123, 124, 100);
        rootNode->addNodeProperty("slots", 100);
        topology->setAsRoot(rootNode);

        TopologyNodePtr backupNode1 = TopologyNode::create(6, "localhost", 123, 124, 1);
        backupNode1->addNodeProperty("slots", 1);   // low amount of resources, not fit for deployment of primaries
        topology->addNewTopologyNodeAsChild(rootNode, backupNode1);

        TopologyNodePtr backupNode2 = TopologyNode::create(5, "localhost", 123, 124, 1);
        backupNode2->addNodeProperty("slots", 1);   // low amount of resources, not fit for deployment of primaries
        topology->addNewTopologyNodeAsChild(backupNode1, backupNode2);

        TopologyNodePtr middleNode1 = TopologyNode::create(3, "localhost", 123, 124, 10);
        middleNode1->addNodeProperty("slots", 10);
        topology->addNewTopologyNodeAsChild(rootNode, middleNode1);

        TopologyNodePtr middleNode2 = TopologyNode::create(2, "localhost", 123, 124, 5);
        middleNode2->addNodeProperty("slots", 5);
        topology->addNewTopologyNodeAsChild(middleNode1, middleNode2);

        TopologyNodePtr sourceNode = TopologyNode::create(1, "localhost", 123, 124, 0);
        sourceNode->addNodeProperty("slots", 0);
        topology->addNewTopologyNodeAsChild(middleNode2, sourceNode);
        topology->addNewTopologyNodeAsChild(backupNode2, sourceNode);

        LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));

        sourceNode->addLinkProperty(backupNode2, linkProperty);
        sourceNode->addLinkProperty(middleNode2, linkProperty);

        middleNode1->addLinkProperty(middleNode2, linkProperty);
        middleNode1->addLinkProperty(rootNode, linkProperty);

        middleNode2->addLinkProperty(sourceNode, linkProperty);
        middleNode2->addLinkProperty(middleNode1, linkProperty);

        rootNode->addLinkProperty(middleNode1, linkProperty);
        rootNode->addLinkProperty(backupNode1, linkProperty);

        backupNode1->addLinkProperty(rootNode, linkProperty);
        backupNode1->addLinkProperty(backupNode2, linkProperty);

        backupNode2->addLinkProperty(sourceNode, linkProperty);
        backupNode2->addLinkProperty(backupNode1, linkProperty);

        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        const std::string sourceName = "car";

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        sourceCatalog->addLogicalSource(sourceName, schema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
        CSVSourceTypePtr csvSourceType = CSVSourceType::create();
        csvSourceType->setGatheringInterval(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
        auto physicalSource = PhysicalSource::create(sourceName, "test2", csvSourceType);
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNode);
        sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
    }

    /*
     * 5 -\
     * |  4
     * 3 -/
     * 2
     * 1
     */
    void setupTopologyAndSourceCatalogTriangle() {
        topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(5, "localhost", 123, 124, 100);
        rootNode->addNodeProperty("slots", 100);
        topology->setAsRoot(rootNode);

        TopologyNodePtr middleNode4 = TopologyNode::create(4, "localhost", 123, 124, 10);
        middleNode4->addNodeProperty("slots", 10);
        topology->addNewTopologyNodeAsChild(rootNode, middleNode4);

        TopologyNodePtr middleNode3 = TopologyNode::create(3, "localhost", 123, 124, 10);
        middleNode3->addNodeProperty("slots", 10);
        topology->addNewTopologyNodeAsChild(rootNode, middleNode3);
        topology->addNewTopologyNodeAsChild(middleNode4, middleNode3);

        TopologyNodePtr middleNode2 = TopologyNode::create(2, "localhost", 123, 124, 2);
        middleNode2->addNodeProperty("slots", 2);
        topology->addNewTopologyNodeAsChild(middleNode3, middleNode2);

        TopologyNodePtr sourceNode = TopologyNode::create(1, "localhost", 123, 124, 0);
        sourceNode->addNodeProperty("slots", 0);
        topology->addNewTopologyNodeAsChild(middleNode2, sourceNode);

        LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));

        sourceNode->addLinkProperty(middleNode2, linkProperty);

        middleNode2->addLinkProperty(sourceNode, linkProperty);
        middleNode2->addLinkProperty(middleNode3, linkProperty);

        middleNode3->addLinkProperty(middleNode2, linkProperty);
        middleNode3->addLinkProperty(middleNode4, linkProperty);
        middleNode3->addLinkProperty(rootNode, linkProperty);

        middleNode4->addLinkProperty(middleNode3, linkProperty);
        middleNode4->addLinkProperty(rootNode, linkProperty);

        rootNode->addLinkProperty(middleNode3, linkProperty);
        rootNode->addLinkProperty(middleNode4, linkProperty);

        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        const std::string sourceName = "car";

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        sourceCatalog->addLogicalSource(sourceName, schema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
        CSVSourceTypePtr csvSourceType = CSVSourceType::create();
        csvSourceType->setGatheringInterval(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
        auto physicalSource = PhysicalSource::create(sourceName, "test2", csvSourceType);
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNode);
        sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
    }

    /*
     * /- 5 -\
     * 2     4
     * 1     3
     */
    void setupTopologyAndSourceCatalog2PhysicalSourcesSeparatePath() {
        topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(5, "localhost", 123, 124, 100);
        rootNode->addNodeProperty("slots", 100);
        topology->setAsRoot(rootNode);

        TopologyNodePtr middleNode1 = TopologyNode::create(2, "localhost", 123, 124, 10);
        middleNode1->addNodeProperty("slots", 10);
        topology->addNewTopologyNodeAsChild(rootNode, middleNode1);

        TopologyNodePtr middleNode2 = TopologyNode::create(4, "localhost", 123, 124, 10);
        middleNode2->addNodeProperty("slots", 10);
        topology->addNewTopologyNodeAsChild(rootNode, middleNode2);

        TopologyNodePtr sourceNode1 = TopologyNode::create(1, "localhost", 123, 124, 0);
        sourceNode1->addNodeProperty("slots", 0);
        topology->addNewTopologyNodeAsChild(middleNode1, sourceNode1);

        TopologyNodePtr sourceNode2 = TopologyNode::create(3, "localhost", 123, 124, 0);
        sourceNode2->addNodeProperty("slots", 0);
        topology->addNewTopologyNodeAsChild(middleNode2, sourceNode2);

        LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));

        sourceNode1->addLinkProperty(middleNode1, linkProperty);
        sourceNode2->addLinkProperty(middleNode2, linkProperty);

        middleNode1->addLinkProperty(sourceNode1, linkProperty);
        middleNode1->addLinkProperty(rootNode, linkProperty);
        middleNode2->addLinkProperty(sourceNode2, linkProperty);
        middleNode2->addLinkProperty(rootNode, linkProperty);

        rootNode->addLinkProperty(middleNode1, linkProperty);
        rootNode->addLinkProperty(middleNode2, linkProperty);

        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        const std::string sourceName = "car";

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        sourceCatalog->addLogicalSource(sourceName, schema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
        CSVSourceTypePtr csvSourceType = CSVSourceType::create();
        csvSourceType->setGatheringInterval(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
        auto physicalSource1 = PhysicalSource::create(sourceName, "test2", csvSourceType);
        auto physicalSource2 = PhysicalSource::create(sourceName, "test3", csvSourceType);
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource1, logicalSource, sourceNode1);
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry2 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource2, logicalSource, sourceNode2);
        sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
        sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry2);
    }

    /*
     *  2*(n-1)+3
     * /   |     \
     * 2   4 ..  2*(n-1)+2
     * 1   3 ..  2*(n-1)+1
     */
    void setupTopologyAndSourceCatalogNPhysicalSourcesSeparatePath(int n) {
        topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(2*(n-1)+3, "localhost", 123, 124, 100);
        rootNode->addNodeProperty("slots", 100);
        topology->setAsRoot(rootNode);

        std::map<int, TopologyNodePtr> middleNodes;
        for (int i = 0; i < n; ++i) {
            TopologyNodePtr middleNode = TopologyNode::create(2*i+2, "localhost", 123, 124, 10);
            middleNode->addNodeProperty("slots", 10);
            topology->addNewTopologyNodeAsChild(rootNode, middleNode);

            middleNodes[i] = middleNode;
        }

        std::map<int, TopologyNodePtr> sourceNodes;
        for (int i = 0; i < n; ++i) {
            TopologyNodePtr sourceNode = TopologyNode::create(2*i+1, "localhost", 123, 124, 0);
            sourceNode->addNodeProperty("slots", 0);
            topology->addNewTopologyNodeAsChild(middleNodes[i], sourceNode);

            sourceNodes[i] = sourceNode;
        }

        LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));

        for (int i = 0; i < n; ++i) {
            sourceNodes[i]->addLinkProperty(middleNodes[i], linkProperty);
            middleNodes[i]->addLinkProperty(sourceNodes[i], linkProperty);

            middleNodes[i]->addLinkProperty(rootNode, linkProperty);
            rootNode->addLinkProperty(middleNodes[i], linkProperty);
        }

        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        const std::string sourceName = "car";

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        sourceCatalog->addLogicalSource(sourceName, schema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
        CSVSourceTypePtr csvSourceType = CSVSourceType::create();
        csvSourceType->setGatheringInterval(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);

        for (int i = 0; i < n; ++i) {
            std::stringstream name;
            name << "test" << 2*i+1;

            auto physicalSource = PhysicalSource::create(sourceName, name.str(), csvSourceType);
            Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry =
                std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNodes[i]);
            sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry);
        }
    }

    /*
     *   3*(n-1)+4
     * /    |      \
     * 3    6  ..  3*(n-1)+3
     * 2    5  ..  3*(n-1)+2
     * 1    4  ..  3*(n-1)+1
     */
    void setupTopologyAndSourceCatalogNPhysicalSourcesSeparatePathDepth4(int n) {
        topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(3*(n-1)+4, "localhost", 123, 124, 100);
        rootNode->addNodeProperty("slots", 100);
        topology->setAsRoot(rootNode);

        std::map<int, TopologyNodePtr> middleNodes2, middleNodes1;
        for (int i = 0; i < n; ++i) {
            TopologyNodePtr middleNode2 = TopologyNode::create(3*i+3, "localhost", 123, 124, 10);
            middleNode2->addNodeProperty("slots", 10);
            topology->addNewTopologyNodeAsChild(rootNode, middleNode2);
            middleNodes2[i] = middleNode2;

            TopologyNodePtr middleNode1 = TopologyNode::create(3*i+2, "localhost", 123, 124, 5);
            middleNode1->addNodeProperty("slots", 5);
            topology->addNewTopologyNodeAsChild(middleNode2, middleNode1);
            middleNodes1[i] = middleNode1;
        }

        std::map<int, TopologyNodePtr> sourceNodes;
        for (int i = 0; i < n; ++i) {
            TopologyNodePtr sourceNode = TopologyNode::create(3*i+1, "localhost", 123, 124, 0);
            sourceNode->addNodeProperty("slots", 0);
            topology->addNewTopologyNodeAsChild(middleNodes1[i], sourceNode);
            sourceNodes[i] = sourceNode;
        }

        LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));

        for (int i = 0; i < n; ++i) {
            sourceNodes[i]->addLinkProperty(middleNodes1[i], linkProperty);
            middleNodes1[i]->addLinkProperty(sourceNodes[i], linkProperty);

            middleNodes2[i]->addLinkProperty(middleNodes1[i], linkProperty);
            middleNodes1[i]->addLinkProperty(middleNodes2[i], linkProperty);

            middleNodes2[i]->addLinkProperty(rootNode, linkProperty);
            rootNode->addLinkProperty(middleNodes2[i], linkProperty);
        }

        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        const std::string sourceName = "car";

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        sourceCatalog->addLogicalSource(sourceName, schema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
        CSVSourceTypePtr csvSourceType = CSVSourceType::create();
        csvSourceType->setGatheringInterval(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);

        for (int i = 0; i < n; ++i) {
            std::stringstream name;
            name << "test" << 2*i+1;

            auto physicalSource = PhysicalSource::create(sourceName, name.str(), csvSourceType);
            Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry =
                std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNodes[i]);
            sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry);
        }
    }

    /*
     *    4
     * /- 2 -\
     * 1     3
     */
    void setupTopologyAndSourceCatalog2PhysicalSourcesSamePath() {
        topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(4, "localhost", 123, 124, 100);
        rootNode->addNodeProperty("slots", 100);
        topology->setAsRoot(rootNode);

        TopologyNodePtr middleNode1 = TopologyNode::create(2, "localhost", 123, 124, 10);
        middleNode1->addNodeProperty("slots", 10);
        topology->addNewTopologyNodeAsChild(rootNode, middleNode1);

        TopologyNodePtr sourceNode1 = TopologyNode::create(1, "localhost", 123, 124, 0);
        sourceNode1->addNodeProperty("slots", 0);
        topology->addNewTopologyNodeAsChild(middleNode1, sourceNode1);

        TopologyNodePtr sourceNode2 = TopologyNode::create(3, "localhost", 123, 124, 0);
        sourceNode2->addNodeProperty("slots", 0);
        topology->addNewTopologyNodeAsChild(middleNode1, sourceNode2);

        LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));

        sourceNode1->addLinkProperty(middleNode1, linkProperty);
        sourceNode2->addLinkProperty(middleNode1, linkProperty);

        middleNode1->addLinkProperty(sourceNode1, linkProperty);
        middleNode1->addLinkProperty(rootNode, linkProperty);
        middleNode1->addLinkProperty(sourceNode2, linkProperty);

        rootNode->addLinkProperty(middleNode1, linkProperty);

        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        const std::string sourceName = "car";

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        sourceCatalog->addLogicalSource(sourceName, schema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
        CSVSourceTypePtr csvSourceType = CSVSourceType::create();
        csvSourceType->setGatheringInterval(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
        auto physicalSource1 = PhysicalSource::create(sourceName, "test2", csvSourceType);
        auto physicalSource2 = PhysicalSource::create(sourceName, "test3", csvSourceType);
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource1, logicalSource, sourceNode1);
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry2 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource2, logicalSource, sourceNode2);
        sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
        sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry2);
    }

    /*
     *   (n-1)+4
     *   (n-1)+3
     *   (n-1)+2
     * /   |    \
     * 1   2 .. (n-1)+1
     */
    void setupTopologyAndSourceCatalogNPhysicalSourcesSamePath(int n) {
        topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create((n-1)+4, "localhost", 123, 124, 100);
        rootNode->addNodeProperty("slots", 100);
        topology->setAsRoot(rootNode);

        TopologyNodePtr middleNode2 = TopologyNode::create((n-1)+3, "localhost", 123, 124, 10);
        middleNode2->addNodeProperty("slots", 10);
        topology->addNewTopologyNodeAsChild(rootNode, middleNode2);

        TopologyNodePtr middleNode1 = TopologyNode::create((n-1)+2, "localhost", 123, 124, 5);
        middleNode1->addNodeProperty("slots", 5);
        topology->addNewTopologyNodeAsChild(middleNode2, middleNode1);

        std::map<int, TopologyNodePtr> sourceNodes;
        for (int i = 0; i < n; ++i) {
            TopologyNodePtr sourceNode = TopologyNode::create(i+1, "localhost", 123, 124, 0);
            sourceNode->addNodeProperty("slots", 0);
            topology->addNewTopologyNodeAsChild(middleNode1, sourceNode);

            sourceNodes[i] = sourceNode;
        }

        LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));

        rootNode->addLinkProperty(middleNode2, linkProperty);
        middleNode2->addLinkProperty(rootNode, linkProperty);

        middleNode2->addLinkProperty(middleNode1, linkProperty);
        middleNode1->addLinkProperty(middleNode2, linkProperty);

        for (int i = 0; i < n; ++i) {
            sourceNodes[i]->addLinkProperty(middleNode1, linkProperty);
            middleNode1->addLinkProperty(sourceNodes[i], linkProperty);
        }

        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        const std::string sourceName = "car";

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        sourceCatalog->addLogicalSource(sourceName, schema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
        CSVSourceTypePtr csvSourceType = CSVSourceType::create();
        csvSourceType->setGatheringInterval(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);

        for (int i = 0; i < n; ++i) {
            std::stringstream name;
            name << "test" << i+1;

            auto physicalSource = PhysicalSource::create(sourceName, name.str(), csvSourceType);
            Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry =
                std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNodes[i]);
            sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry);
        }
    }

    void assignOperatorPropertiesRecursive(const LogicalOperatorNodePtr& operatorNode) {
        int cost = 1;
        double dmf = 1;
        double input = 0;

        for (const auto& child : operatorNode->getChildren()) {
            LogicalOperatorNodePtr op = child->as<LogicalOperatorNode>();
            assignOperatorPropertiesRecursive(op);
            std::any output = op->getProperty("output");
            input += std::any_cast<double>(output);
        }

        NodePtr nodePtr = operatorNode->as<Node>();
        if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
            dmf = 0;
            cost = 0;
        } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
            dmf = 0.5;
            cost = 1;
        } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
            dmf = 2;
            cost = 2;
        } else if (operatorNode->instanceOf<JoinLogicalOperatorNode>()) {
            cost = 2;
        } else if (operatorNode->instanceOf<UnionLogicalOperatorNode>()) {
            cost = 2;
        } else if (operatorNode->instanceOf<ProjectionLogicalOperatorNode>()) {
            cost = 1;
        } else if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
            cost = 0;
            input = 100;
        }

        double output = input * dmf;
        operatorNode->addProperty("output", output);
        operatorNode->addProperty("cost", cost);
    }

    Catalogs::Source::SourceCatalogPtr sourceCatalog;
};



/// --------------------------------------------------- GREEDY ----------------------------------------------------------------

/* Test replica placements on a simple query, on a sequential topology of 3 nodes
 * 3
 * 2
 * 1
 */
TEST_F(AdaptiveActiveStandbyTest, testAASGreedySequential3) {

    setupTopologyAndSourceCatalogSequential3();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::Greedy_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 4U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 3) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
        } else {
            // filters should be placed on other nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        }
    }
}

/* Test replica placements on a simple query, on a sequential topology of 4 nodes
 * 4
 * 3
 * 2
 * 1
 */
TEST_F(AdaptiveActiveStandbyTest, testAASGreedySequential4) {

    setupTopologyAndSourceCatalogSequential4();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::Greedy_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 6U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 4) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
        } else if (executionNode->getId() == 2 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart+1) {
            // filters should be placed on node 2 and node newTopologyNodeIdStart+1
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/* Test replica placements on a simple query, on a simple short diamond topology
 *   4
 * 2   3
 *   1
 */
TEST_F(AdaptiveActiveStandbyTest, testAASGreedySimpleShortDiamond) {

    setupTopologyAndSourceCatalogSimpleShortDiamond();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::Greedy_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 4U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 4) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
        } else {
            // filters should be placed on other nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        }
    }
}

/* Test replica placements on a simple query, on a tall diamond topology
 *   4
 * 3   6
 * 2   5
 *   1
 */
TEST_F(AdaptiveActiveStandbyTest, testAASGreedyTallDiamond) {

    setupTopologyAndSourceCatalogTallDiamond();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto filterOperator = queryPlan->getOperatorByType<FilterLogicalOperatorNode>()[0]->as<FilterLogicalOperatorNode>();
    auto mapOperator = queryPlan->getOperatorByType<MapLogicalOperatorNode>()[0]->as<MapLogicalOperatorNode>();
    auto sourceOperator = queryPlan->getOperatorByType<SourceLogicalOperatorNode>()[0]->as<SourceLogicalOperatorNode>();
    auto sinkOperator = queryPlan->getOperatorByType<SinkLogicalOperatorNode>()[0]->as<SinkLogicalOperatorNode>();


    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 NES::PlacementStrategy::Greedy_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 6U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 4) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
        } else if (executionNode->getId() == 2 || executionNode->getId() == 5) {
            // filters should be placed on node 2 and node 5
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/* Test replica placements on a simple query, on a topology with a triangle
 * 5 -\
 * |  4
 * 3 -/
 * 2
 * 1
 */
TEST_F(AdaptiveActiveStandbyTest, testAASGreedyTriangle) {

    setupTopologyAndSourceCatalogTriangle();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::Greedy_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 6U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 5) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
        } else if (executionNode->getId() == 2 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart) {
            // filters should be placed on node 2 and node newTopologyNodeIdStart
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/* Test replica placements on a simple query, in case of 2 physical sources
 * /- 5 -\
 * 2     4
 * 1     3
 */
TEST_F(AdaptiveActiveStandbyTest, testAASGreedy2PhysicalSourcesSeparatePath) {
    setupTopologyAndSourceCatalog2PhysicalSourcesSeparatePath();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::Greedy_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 7U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1 || executionNode->getId() == 3) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 5) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 2U); // 2 maps
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[1]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
            ASSERT_EQ(actualRootOperator->getChildren()[1]->getChildren().size(), 2U);
        } else if (executionNode->getId() == 2 || executionNode->getId() == 4) {
            // primary filters should be placed on node 2 and node 4
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 1) {
            // secondary filters should be placed on the two new nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}


/* Test replica placements on a simple query, in case of 2 physical sources
 *    4
 * /- 2 -\
 * 1     3
 */
TEST_F(AdaptiveActiveStandbyTest, testAASGreedy2PhysicalSourcesSamePath) {
    setupTopologyAndSourceCatalog2PhysicalSourcesSamePath();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::Greedy_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 6U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1 || executionNode->getId() == 3) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 4) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 2U); // 2 maps
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[1]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
            ASSERT_EQ(actualRootOperator->getChildren()[1]->getChildren().size(), 2U);
        } else if (executionNode->getId() == 2) {
            // primary filters should be placed on node 2
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 2U);
            for (auto const& querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                            instanceOf<SourceLogicalOperatorNode>());
            }
        } else if (executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 1) {
            // secondary filters should be placed on the two new nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/* Test replica placements on a simple query, in case of 2 physical sources
 *    6
 *    5
 *    4
 * /  |  \
 * 1  2  3
 */
TEST_F(AdaptiveActiveStandbyTest, testAASGreedy3PhysicalSourcesSamePath) {
    setupTopologyAndSourceCatalogNPhysicalSourcesSamePath(3);

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::Greedy_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 10U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1 ||
            executionNode->getId() == 2 ||
            executionNode->getId() == 3) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 6) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 3U); // 3 maps
            // map should have 2 sources
            for(int i = 0; i < 3; i++) {
                EXPECT_TRUE(actualRootOperator->getChildren()[i]->instanceOf<MapLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren()[i]->getChildren().size(), 2U);
            }
        } else if (executionNode->getId() == 4) {
            // 5 primary filters should be placed on node 4
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 3U);
            for (auto const& querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            }
            ASSERT_EQ(topology->findNodeWithId(executionNode->getId())->getAvailableResources(), 5-5);
        } else if (executionNode->getId() == 5) {
            // 1 primary filter should be placed on node 5
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 3U);
            for (auto const& querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>() ||
                            actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
            ASSERT_EQ(topology->findNodeWithId(executionNode->getId())->getAvailableResources(), 10-1);
        } else if (executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 1||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 2 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 3) {
            // secondary filters should be placed on the bottom 3 new nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/* Test replica placements on a simple query, in case of N physical sources
 *      11
 * /  |  |  |  \
 * 2  4  6  8  10
 * 1  3  5  7   9
 */
TEST_F(AdaptiveActiveStandbyTest, testAASGreedy5PhysicalSourcesSeparatePath) {
    setupTopologyAndSourceCatalogNPhysicalSourcesSeparatePath(5);

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::Greedy_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 16U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1 ||
            executionNode->getId() == 3 ||
            executionNode->getId() == 5 ||
            executionNode->getId() == 7 ||
            executionNode->getId() == 9) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 11) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 5U);// 5 maps
            // map should have 2 sources
            for (int i = 0; i < 5; i++) {
                EXPECT_TRUE(actualRootOperator->getChildren()[i]->instanceOf<MapLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren()[i]->getChildren().size(), 2U);
            }
        } else if (executionNode->getId() == 2 ||
                   executionNode->getId() == 4 ||
                   executionNode->getId() == 6 ||
                   executionNode->getId() == 8 ||
                   executionNode->getId() == 10) {
            // primary filters
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[0U]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]
                            ->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 1 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 2 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 3 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 4) {
            // secondary filters on new nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[0U]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]
                            ->getChildren()[0]
                            ->getChildren()[0]
                            ->instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/* Test replica placements on a simple query, in case of N physical sources with the depth of 4
 *     13
 * /  |  |  \
 * 3  6  9  12
 * 2  5  8  11
 * 1  4  7  10
 */
TEST_F(AdaptiveActiveStandbyTest, testAASGreedy4PhysicalSourcesSeparatePathDepth4) {
    setupTopologyAndSourceCatalogNPhysicalSourcesSeparatePathDepth4(4);

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::Greedy_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 17U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1 ||
            executionNode->getId() == 4 ||
            executionNode->getId() == 7 ||
            executionNode->getId() == 10) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 13) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 4U);// 4 maps
            // map should have 2 sources
            for (int i = 0; i < 4; i++) {
                EXPECT_TRUE(actualRootOperator->getChildren()[i]->instanceOf<MapLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren()[i]->getChildren().size(), 2U);
            }
        } else if (executionNode->getId() == 2 ||
                   executionNode->getId() == 5 ||
                   executionNode->getId() == 8 ||
                   executionNode->getId() == 11) {
            // primary filters
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[0U]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]
                            ->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 1 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 2 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 3) {
            // secondary filters on new nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[0U]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]
                            ->getChildren()[0]
                            ->getChildren()[0]
                            ->instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}


/// ------------------------------------------ LOCAL SEARCH TESTS --------------------------------------------------------------

/* Test replica placements on a simple query, on a sequential topology of 3 nodes
 * 3
 * 2
 * 1
 */
TEST_F(AdaptiveActiveStandbyTest, testAASLocalSearchSequential3) {

    setupTopologyAndSourceCatalogSequential3();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::LocalSearch_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 4U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 3) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
        } else {
            // filters should be placed on other nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        }
    }
}

/* Test replica placements on a simple query, on a sequential topology of 4 nodes
 * 4
 * 3
 * 2
 * 1
 */
TEST_F(AdaptiveActiveStandbyTest, testAASLocalSearchSequential4) {

    setupTopologyAndSourceCatalogSequential4();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::LocalSearch_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 6U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 4) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
        } else if (executionNode->getId() == 2 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart+1) {
            // filters should be placed on node 2 and node newTopologyNodeIdStart+1
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        }
    }
}


/* Test replica placements on a simple query, on a simple short diamond topology
 *   4
 * 2   3
 *   1
 */
TEST_F(AdaptiveActiveStandbyTest, testAASLocalSearchSimpleShortDiamond) {

    setupTopologyAndSourceCatalogSimpleShortDiamond();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::LocalSearch_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 4U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 4) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
        } else {
            // filters should be placed on other nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        }
    }
}

/* Test replica placements on a simple query, on a tall diamond topology
 *   4
 * 3   6
 * 2   5
 *   1
 */
TEST_F(AdaptiveActiveStandbyTest, testAASLocalSearchTallDiamond) {

    setupTopologyAndSourceCatalogTallDiamond();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto filterOperator = queryPlan->getOperatorByType<FilterLogicalOperatorNode>()[0]->as<FilterLogicalOperatorNode>();
    auto mapOperator = queryPlan->getOperatorByType<MapLogicalOperatorNode>()[0]->as<MapLogicalOperatorNode>();
    auto sourceOperator = queryPlan->getOperatorByType<SourceLogicalOperatorNode>()[0]->as<SourceLogicalOperatorNode>();
    auto sinkOperator = queryPlan->getOperatorByType<SinkLogicalOperatorNode>()[0]->as<SinkLogicalOperatorNode>();

    sourceOperator->addProperty("PINNED_NODE_ID", uint64_t(1));
    filterOperator->addProperty("PINNED_NODE_ID", uint64_t(2));
    mapOperator->addProperty("PINNED_NODE_ID", uint64_t(3));
    sinkOperator->addProperty("PINNED_NODE_ID", uint64_t(4));

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 NES::PlacementStrategy::LocalSearch_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 6U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 4) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
        } else if (executionNode->getId() == 2) {
            // filters should be placed on node 2
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 5 || executionNode->getId() == 6) {
            // filters should be placed on node 5 and node 6
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/* Test replica placements on a simple query, on a topology with a triangle
 * 5 -\
 * |  4
 * 3 -/
 * 2
 * 1
 */
TEST_F(AdaptiveActiveStandbyTest, testAASLocalSearchTriangle) {

    setupTopologyAndSourceCatalogTriangle();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::LocalSearch_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 6U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 5) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
        } else if (executionNode->getId() == 2 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart) {
            // filters should be placed on node 2 and node newTopologyNodeIdStart
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/* Test replica placements on a simple query, in case of 2 physical sources
 * /- 5 -\
 * 2     4
 * 1     3
 */
TEST_F(AdaptiveActiveStandbyTest, testAASLocalSearch2PhysicalSourcesSeparatePath) {
    setupTopologyAndSourceCatalog2PhysicalSourcesSeparatePath();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::LocalSearch_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 7U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1 || executionNode->getId() == 3) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 5) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 2U); // 2 maps
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[1]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
            ASSERT_EQ(actualRootOperator->getChildren()[1]->getChildren().size(), 2U);
        } else if (executionNode->getId() == 2 || executionNode->getId() == 4) {
            // primary filters should be placed on node 2 and node 4
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 1) {
            // secondary filters should be placed on the two new nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}


/* Test replica placements on a simple query, in case of 2 physical sources
 *    4
 * /- 2 -\
 * 1     3
 */
TEST_F(AdaptiveActiveStandbyTest, testAASLocalSearch2PhysicalSourcesSamePath) {
    setupTopologyAndSourceCatalog2PhysicalSourcesSamePath();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::LocalSearch_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 6U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1 || executionNode->getId() == 3) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 4) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 2U); // 2 maps
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[1]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
            ASSERT_EQ(actualRootOperator->getChildren()[1]->getChildren().size(), 2U);
        } else if (executionNode->getId() == 2) {
            // primary filters should be placed on node 2
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 2U);
            for (auto const& querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                            instanceOf<SourceLogicalOperatorNode>());
            }
        } else if (executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 1) {
            // secondary filters should be placed on the two new nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/* Test replica placements on a simple query, in case of 2 physical sources
 *    6
 *    5
 *    4
 * /  |  \
 * 1  2  3
 */
TEST_F(AdaptiveActiveStandbyTest, testAASLocalSearch3PhysicalSourcesSamePath) {
    setupTopologyAndSourceCatalogNPhysicalSourcesSamePath(3);

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::LocalSearch_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 10U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1 ||
            executionNode->getId() == 2 ||
            executionNode->getId() == 3) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 6) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 3U); // 3 maps
            // map should have 2 sources
            for(int i = 0; i < 3; i++) {
                EXPECT_TRUE(actualRootOperator->getChildren()[i]->instanceOf<MapLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren()[i]->getChildren().size(), 2U);
            }
        } else if (executionNode->getId() == 4) {
            // 5 primary filters should be placed on node 4
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 3U);
            for (auto const& querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            }
            ASSERT_EQ(topology->findNodeWithId(executionNode->getId())->getAvailableResources(), 5-5);
        } else if (executionNode->getId() == 5) {
            // 1 primary filter should be placed on node 5
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 3U);
            for (auto const& querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>() ||
                            actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
            ASSERT_EQ(topology->findNodeWithId(executionNode->getId())->getAvailableResources(), 10-1);
        } else if (executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 1||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 2 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 3) {
            // secondary filters should be placed on the bottom 3 new nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/* Test replica placements on a simple query, in case of N physical sources
 *      11
 * /  |  |  |  \
 * 2  4  6  8  10
 * 1  3  5  7   9
 */
TEST_F(AdaptiveActiveStandbyTest, testAASLocalSearch5PhysicalSourcesSeparatePath) {
    setupTopologyAndSourceCatalogNPhysicalSourcesSeparatePath(5);

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::LocalSearch_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 16U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1 ||
            executionNode->getId() == 3 ||
            executionNode->getId() == 5 ||
            executionNode->getId() == 7 ||
            executionNode->getId() == 9) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 11) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 5U);// 5 maps
            // map should have 2 sources
            for (int i = 0; i < 5; i++) {
                EXPECT_TRUE(actualRootOperator->getChildren()[i]->instanceOf<MapLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren()[i]->getChildren().size(), 2U);
            }
        } else if (executionNode->getId() == 2 ||
                   executionNode->getId() == 4 ||
                   executionNode->getId() == 6 ||
                   executionNode->getId() == 8 ||
                   executionNode->getId() == 10) {
            // primary filters
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[0U]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]
                            ->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 1 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 2 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 3 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 4) {
            // secondary filters on new nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[0U]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]
                            ->getChildren()[0]
                            ->getChildren()[0]
                            ->instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/* Test replica placements on a simple query, in case of N physical sources with the depth of 4
 *     13
 * /  |  |  \
 * 3  6  9  12
 * 2  5  8  11
 * 1  4  7  10
 */
TEST_F(AdaptiveActiveStandbyTest, testAASLocalSearch4PhysicalSourcesSeparatePathDepth4) {
    setupTopologyAndSourceCatalogNPhysicalSourcesSeparatePathDepth4(4);

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::LocalSearch_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 17U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1 ||
            executionNode->getId() == 4 ||
            executionNode->getId() == 7 ||
            executionNode->getId() == 10) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 13) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 4U);// 4 maps
            // map should have 2 sources
            for (int i = 0; i < 4; i++) {
                EXPECT_TRUE(actualRootOperator->getChildren()[i]->instanceOf<MapLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren()[i]->getChildren().size(), 2U);
            }
        } else if (executionNode->getId() == 2 ||
                   executionNode->getId() == 5 ||
                   executionNode->getId() == 8 ||
                   executionNode->getId() == 11) {
            // primary filters
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[0U]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]
                            ->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 1 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 2 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 3) {
            // secondary filters on new nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[0U]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]
                            ->getChildren()[0]
                            ->getChildren()[0]
                            ->instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/// ---------------------------------------------------- ILP TESTS -------------------------------------------------------------

/* Test replica placements on a simple query, on a sequential topology of 3 nodes
 * 3
 * 2
 * 1
 */
TEST_F(AdaptiveActiveStandbyTest, testAASILPSequential3) {

    setupTopologyAndSourceCatalogSequential3();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::ILP_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 4U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 3) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
        } else {
            // filters should be placed on other nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        }
    }
}

/* Test replica placements on a simple query, on a sequential topology of 4 nodes
 * 4
 * 3
 * 2
 * 1
 */
TEST_F(AdaptiveActiveStandbyTest, testAASILPSequential4) {

    setupTopologyAndSourceCatalogSequential4();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::ILP_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 6U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 4) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
        } else if (executionNode->getId() == 2 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart+1) {
            // filters should be placed on node 2 and node newTopologyNodeIdStart+1
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        }
    }
}


/* Test replica placements on a simple query, on a simple short diamond topology
 *   4
 * 2   3
 *   1
 */
TEST_F(AdaptiveActiveStandbyTest, testAASILPSimpleShortDiamond) {

    setupTopologyAndSourceCatalogSimpleShortDiamond();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::ILP_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 4U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 4) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
        } else {
            // filters should be placed on other nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        }
    }
}

/* Test replica placements on a simple query, on a tall diamond topology
 *   4
 * 3   6
 * 2   5
 *   1
 */
TEST_F(AdaptiveActiveStandbyTest, testAASILPTallDiamond) {

    setupTopologyAndSourceCatalogTallDiamond();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto filterOperator = queryPlan->getOperatorByType<FilterLogicalOperatorNode>()[0]->as<FilterLogicalOperatorNode>();
    auto mapOperator = queryPlan->getOperatorByType<MapLogicalOperatorNode>()[0]->as<MapLogicalOperatorNode>();
    auto sourceOperator = queryPlan->getOperatorByType<SourceLogicalOperatorNode>()[0]->as<SourceLogicalOperatorNode>();
    auto sinkOperator = queryPlan->getOperatorByType<SinkLogicalOperatorNode>()[0]->as<SinkLogicalOperatorNode>();

    sourceOperator->addProperty("PINNED_NODE_ID", uint64_t(1));
    filterOperator->addProperty("PINNED_NODE_ID", uint64_t(2));
    mapOperator->addProperty("PINNED_NODE_ID", uint64_t(3));
    sinkOperator->addProperty("PINNED_NODE_ID", uint64_t(4));

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 NES::PlacementStrategy::ILP_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 6U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 4) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
        } else if (executionNode->getId() == 2) {
            // filters should be placed on node 2
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 5 || executionNode->getId() == 6) {
            // filters should be placed on node 5 and node 6
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/* Test replica placements on a simple query, on a topology with a triangle
 * 5 -\
 * |  4
 * 3 -/
 * 2
 * 1
 */
TEST_F(AdaptiveActiveStandbyTest, testAASILPTriangle) {

    setupTopologyAndSourceCatalogTriangle();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::ILP_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 6U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 5) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
        } else if (executionNode->getId() == 2 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart) {
            // filters should be placed on node 2 and node newTopologyNodeIdStart
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/* Test replica placements on a simple query, in case of 2 physical sources
 * /- 5 -\
 * 2     4
 * 1     3
 */
TEST_F(AdaptiveActiveStandbyTest, testAASILP2PhysicalSourcesSeparatePath) {
    setupTopologyAndSourceCatalog2PhysicalSourcesSeparatePath();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::ILP_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 7U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1 || executionNode->getId() == 3) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 5) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 2U); // 2 maps
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[1]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
            ASSERT_EQ(actualRootOperator->getChildren()[1]->getChildren().size(), 2U);
        } else if (executionNode->getId() == 2 || executionNode->getId() == 4) {
            // primary filters should be placed on node 2 and node 4
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 1) {
            // secondary filters should be placed on the two new nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}


/* Test replica placements on a simple query, in case of 2 physical sources
 *    4
 * /- 2 -\
 * 1     3
 */
TEST_F(AdaptiveActiveStandbyTest, testAASILP2PhysicalSourcesSamePath) {
    setupTopologyAndSourceCatalog2PhysicalSourcesSamePath();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::ILP_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 6U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1 || executionNode->getId() == 3) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 4) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 2U); // 2 maps
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[1]->instanceOf<MapLogicalOperatorNode>());
            // map should have 2 sources
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
            ASSERT_EQ(actualRootOperator->getChildren()[1]->getChildren().size(), 2U);
        } else if (executionNode->getId() == 2) {
            // primary filters should be placed on node 2
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 2U);
            for (auto const& querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                            instanceOf<SourceLogicalOperatorNode>());
            }
        } else if (executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 1) {
            // secondary filters should be placed on the two new nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/* Test replica placements on a simple query, in case of 2 physical sources
 *    6
 *    5
 *    4
 * /  |  \
 * 1  2  3
 */
TEST_F(AdaptiveActiveStandbyTest, testAASILP3PhysicalSourcesSamePath) {
    setupTopologyAndSourceCatalogNPhysicalSourcesSamePath(3);

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::ILP_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 10U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1 ||
            executionNode->getId() == 2 ||
            executionNode->getId() == 3) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 6) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 3U); // 3 maps
            // map should have 2 sources
            for(int i = 0; i < 3; i++) {
                EXPECT_TRUE(actualRootOperator->getChildren()[i]->instanceOf<MapLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren()[i]->getChildren().size(), 2U);
            }
        } else if (executionNode->getId() == 4) {
            // 5 primary filters should be placed on node 4
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 3U);
            for (auto const& querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            }
            ASSERT_EQ(topology->findNodeWithId(executionNode->getId())->getAvailableResources(), 5-5);
        } else if (executionNode->getId() == 5) {
            // 1 primary filter should be placed on node 5
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 3U);
            for (auto const& querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>() ||
                            actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
            ASSERT_EQ(topology->findNodeWithId(executionNode->getId())->getAvailableResources(), 10-1);
        } else if (executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 1||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 2 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 3) {
            // secondary filters should be placed on the bottom 3 new nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]->
                        instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/* Test replica placements on a simple query, in case of N physical sources
 *      11
 * /  |  |  |  \
 * 2  4  6  8  10
 * 1  3  5  7   9
 */
TEST_F(AdaptiveActiveStandbyTest, testAASILP5PhysicalSourcesSeparatePath) {
    setupTopologyAndSourceCatalogNPhysicalSourcesSeparatePath(5);

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::ILP_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 16U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1 ||
            executionNode->getId() == 3 ||
            executionNode->getId() == 5 ||
            executionNode->getId() == 7 ||
            executionNode->getId() == 9) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 11) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 5U);// 5 maps
            // map should have 2 sources
            for (int i = 0; i < 5; i++) {
                EXPECT_TRUE(actualRootOperator->getChildren()[i]->instanceOf<MapLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren()[i]->getChildren().size(), 2U);
            }
        } else if (executionNode->getId() == 2 ||
                   executionNode->getId() == 4 ||
                   executionNode->getId() == 6 ||
                   executionNode->getId() == 8 ||
                   executionNode->getId() == 10) {
            // primary filters
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[0U]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]
                            ->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 1 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 2 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 3 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 4) {
            // secondary filters on new nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[0U]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]
                            ->getChildren()[0]
                            ->getChildren()[0]
                            ->instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/* Test replica placements on a simple query, in case of N physical sources with the depth of 4
 *     13
 * /  |  |  \
 * 3  6  9  12
 * 2  5  8  11
 * 1  4  7  10
 */
TEST_F(AdaptiveActiveStandbyTest, testAASILP4PhysicalSourcesSeparatePathDepth4) {
    setupTopologyAndSourceCatalogNPhysicalSourcesSeparatePathDepth4(4);

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      z3Context,
                                                                      false /*query reconfiguration*/);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .filter(Attribute("value") > 10)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 PlacementStrategy::ILP_AAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 17U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1 ||
            executionNode->getId() == 4 ||
            executionNode->getId() == 7 ||
            executionNode->getId() == 10) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 13) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 4U);// 4 maps
            // map should have 2 sources
            for (int i = 0; i < 4; i++) {
                EXPECT_TRUE(actualRootOperator->getChildren()[i]->instanceOf<MapLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren()[i]->getChildren().size(), 2U);
            }
        } else if (executionNode->getId() == 2 ||
                   executionNode->getId() == 5 ||
                   executionNode->getId() == 8 ||
                   executionNode->getId() == 11) {
            // primary filters
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[0U]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]
                            ->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 1 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 2 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 3) {
            // secondary filters on new nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[0U]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]
                            ->getChildren()[0]
                            ->getChildren()[0]
                            ->instanceOf<SourceLogicalOperatorNode>());
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}