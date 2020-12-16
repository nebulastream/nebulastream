/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Catalogs/StreamCatalogEntry.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/IFCOPStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Phases/QueryRewritePhase.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <gtest/gtest.h>

using namespace NES;
using namespace web;

class IFCOPTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup IFCOPTest test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("IFCOPTest.log", NES::LOG_DEBUG);

    }

    /* Will be called before a test is executed. */
    void TearDown() { std::cout << "Setup IFCOPTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down IFCOPTest test class." << std::endl; }

    void setupTopologyAndStreamCatalog() {
        uint32_t grpcPort = 4000;
        uint32_t dataPort = 5000;

        topology = Topology::create();

        // creater workers
        std::vector<TopologyNodePtr> topologyNodes;
        int resource = 4;
        for (uint32_t i = 0; i < 10; ++i) {
            topologyNodes.push_back(TopologyNode::create(i, "localhost", grpcPort, dataPort, resource));
            grpcPort = grpcPort + 2;
            dataPort = dataPort + 2;
        }

        topology->setAsRoot(topologyNodes.at(0));

        // link each worker with its neighbor
        topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(1));
        topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(2));

        topology->addNewPhysicalNodeAsChild(topologyNodes.at(1), topologyNodes.at(3));
        topology->addNewPhysicalNodeAsChild(topologyNodes.at(1), topologyNodes.at(4));

        topology->addNewPhysicalNodeAsChild(topologyNodes.at(2), topologyNodes.at(5));
        topology->addNewPhysicalNodeAsChild(topologyNodes.at(2), topologyNodes.at(6));

        topology->addNewPhysicalNodeAsChild(topologyNodes.at(4), topologyNodes.at(7));
        topology->addNewPhysicalNodeAsChild(topologyNodes.at(5), topologyNodes.at(7));

        topology->addNewPhysicalNodeAsChild(topologyNodes.at(7), topologyNodes.at(8));
        topology->addNewPhysicalNodeAsChild(topologyNodes.at(7), topologyNodes.at(9));


        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        const std::string streamName = "car";

        streamCatalog = std::make_shared<StreamCatalog>();
        streamCatalog->addLogicalStream(streamName, schema);

        PhysicalStreamConfigPtr conf =
            PhysicalStreamConfig::create(/**Source Type**/ "DefaultSource", /**Source Config**/ "1",
                /**Source Frequence**/ 0, /**Number Of Tuples To Produce Per Buffer**/ 0,
                /**Number of Buffers To Produce**/ 1, /**Physical Stream Name**/ "test2",
                /**Logical Stream Name**/ "car");

        StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf, topologyNodes.at(8));
        StreamCatalogEntryPtr streamCatalogEntry2 = std::make_shared<StreamCatalogEntry>(conf, topologyNodes.at(9));

        streamCatalog->addPhysicalStream("car", streamCatalogEntry1);
        streamCatalog->addPhysicalStream("car", streamCatalogEntry2);
    }

    StreamCatalogPtr streamCatalog;
    TopologyPtr topology;
};

/* Test query placement with bottom up strategy  */
TEST_F(IFCOPTest, testGeneratingRandomExecutionPath) {

    setupTopologyAndStreamCatalog();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    TypeInferencePhasePtr typeInferencePhase = TypeInferencePhase::create(streamCatalog);

    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    QueryRewritePhasePtr queryReWritePhase = QueryRewritePhase::create(streamCatalog);
    queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto ifcop = IFCOPStrategy::create(globalExecutionPlan, topology, typeInferencePhase, streamCatalog);

    auto randomExecutionPath = ifcop->generateRandomExecutionPath(topology, queryPlan);

//    auto randomExecutionPath = ifcop->generateRandomExecutionPath(topology);


}

