
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
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/ManualPlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Services/QueryParsingService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <utility>

using namespace NES;
using namespace web;

class GeneticAlgorithmBenchmark : public testing::Test {
  public:
    std::shared_ptr<QueryParsingService> queryParsingService;
    // Will be called before any test in this class are executed.
    static void SetUpTestCase() { std::cout << "Setup QueryPlacementTest test class." << std::endl; }

    // Will be called before a test is executed.
    void SetUp() override {
        NES::setupLogging("QueryPlacementTest.log", NES::LOG_DEBUG);
        std::cout << "Setup QueryPlacementTest test case." << std::endl;
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
    }

    // Will be called before a test is executed.
    void TearDown() { std::cout << "Setup QueryPlacementTest test case." << std::endl; }

    // Will be called after all tests in this class are finished.
    static void TearDownTestCase() { std::cout << "Tear down QueryPlacementTest test class." << std::endl; }

    void setupTopologyAndStreamCatalogForGA(int numOfMidNodes) {

        topology = Topology::create();
        // create the topology for the test
        uint32_t grpcPort = 4000;
        uint32_t dataPort = 5000;
        int nodeID = 1;
        TopologyNodePtr sinkNode = TopologyNode::create(nodeID++, "localhost", grpcPort, dataPort, 5000);
        topology->setAsRoot(sinkNode);
        LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(100, 64));
        auto previousNode = sinkNode;
        for(int i = 0; i < numOfMidNodes; i++) {
            TopologyNodePtr midNode = TopologyNode::create(nodeID++, "localhost", 123, 124, 100);
            topology->addNewPhysicalNodeAsChild(previousNode, midNode);
            midNode->addLinkProperty(previousNode, linkProperty);
            previousNode->addLinkProperty(midNode, linkProperty);
            previousNode = midNode;
        }


        TopologyNodePtr sourceNode = TopologyNode::create(nodeID++, "localhost", grpcPort, dataPort, 10000);
        topology->addNewPhysicalNodeAsChild(previousNode, sourceNode);
        previousNode->addLinkProperty(sourceNode, linkProperty);
        sourceNode->addLinkProperty(previousNode, linkProperty);

        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32);";

        const std::string streamName = "car";

        streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
        streamCatalog->addLogicalStream(streamName, schema);

        SourceConfigPtr sourceConfig = SourceConfig::create();
        sourceConfig->setSourceFrequency(0);
        sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
        sourceConfig->setPhysicalStreamName("test1");
        sourceConfig->setLogicalStreamName("car");

        PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

        StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf, sourceNode);

        streamCatalog->addPhysicalStream("car", streamCatalogEntry1);
    }

    StreamCatalogPtr streamCatalog;
    TopologyPtr topology;
};

TEST_F(GeneticAlgorithmBenchmark, testPlacingQueryWithGeneticAlgorithmStrategyFixedTopologyWithDynamicQuery) {
    std::list<int> listOfInts( {10});
    std::map<int, std::vector<long>> counts;
    int repetitions = 2;

    setupTopologyAndStreamCatalogForGA(23);
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("GeneticAlgorithm",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);

    std::ofstream outfile;
    outfile.open ("benchmark.txt");
    outfile.close();
    for(int n : listOfInts) {
        Query query = Query::from("car");

        for(int i = 0; i < n; i+=2){
            query.filter(Attribute("id") < n-i);
        }
        for(int i = 1; i < n; i+=2){
            query.map(Attribute("value") = Attribute("id") * 2);
        }

        query.sink(PrintSinkDescriptor::create());
        QueryPlanPtr testQueryPlan = query.getQueryPlan();
        QueryId queryId = PlanIdGenerator::getNextQueryId();
        testQueryPlan->setQueryId(queryId);

        // Execute optimization phases prior to placement
        auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
        testQueryPlan = queryReWritePhase->execute(testQueryPlan);
        typeInferencePhase->execute(testQueryPlan);

        auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
        topologySpecificQueryRewrite->execute(testQueryPlan);
        typeInferencePhase->execute(testQueryPlan);
        std::vector<long> counts_n;

        outfile.open ("benchmark.txt", std::ios_base::app);
        outfile << "\n\nWriting benchmark for testPlacingQueryWithGeneticAlgorithmStrategyFixedTopologyWithDynamicQuery.\n";
        outfile << "The Query Plan is: \n" << testQueryPlan->toString();
        outfile << "The topology is:\n"<<topology->toString();
        outfile.close();
        for(int j = 0; j < repetitions; j++) {
            outfile.open ("benchmark.txt", std::ios_base::app);
            outfile << "\n\n\nRepetition Number: " << j+1 << "\n\n";
            outfile.close();
            auto start = std::chrono::high_resolution_clock::now();
            ASSERT_TRUE(placementStrategy->updateGlobalExecutionPlan(testQueryPlan));
            auto stop = std::chrono::high_resolution_clock::now();
            long count = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
            NES_DEBUG("Found Solution in " << count << "ms");
            counts_n.push_back(count);
            globalExecutionPlan->removeQuerySubPlans(queryId);
        }
        counts.insert(std::make_pair(n, counts_n));
    }
    outfile.open ("benchmark.txt", std::ios_base::app);
    for (auto& [n, counts_n] : counts) {
        std::sort(counts_n.begin(), counts_n.end());
        long median = counts_n[counts_n.size() / 2];
        if (counts_n.size() % 2 == 0) {
            median = (median + counts_n[(counts_n.size() - 1) / 2]) / 2;
        }
        std::stringstream ss;
        for (auto& count : counts_n) {
            ss << count << ", ";
        }

        NES_INFO("N: " << n << ", median: " << median << ", measures: " << ss.str());
        outfile <<"\nN: " << n+2 << ", median: " << median << ", measures: " << ss.str() << "\n";
    }
}

TEST_F(GeneticAlgorithmBenchmark, testPlacingQueryWithGeneticAlgorithmStrategyFixedQueryWithDynamicTopologyHeight) {
    std::list<int> listOfInts( {1,2});
    std::map<int, std::vector<long>> counts;
    int repetitions = 3;

    // Prepare the query
    Query query = Query::from("car");
    uint32_t numOfOperators = 10;
    for(uint32_t i = 0; i < numOfOperators; i+=2){
        query.filter(Attribute("id") < numOfOperators-i);
    }
    for(uint32_t i = 1; i < numOfOperators; i+=2){
        query.map(Attribute("value") = Attribute("id") * 2);
    }

    query.sink(PrintSinkDescriptor::create());
    QueryPlanPtr testQueryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    testQueryPlan->setQueryId(queryId);

    std::ofstream outfile;
    outfile.open ("benchmark.txt");
    outfile.close();
    for(int n : listOfInts) {
        setupTopologyAndStreamCatalogForGA(n);
        GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
        auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("GeneticAlgorithm",
                                                                                  globalExecutionPlan,
                                                                                  topology,
                                                                                  typeInferencePhase,
                                                                                  streamCatalog);
        // Execute optimization phases prior to placement
        auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
        testQueryPlan = queryReWritePhase->execute(testQueryPlan);
        typeInferencePhase->execute(testQueryPlan);

        auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
        topologySpecificQueryRewrite->execute(testQueryPlan);
        typeInferencePhase->execute(testQueryPlan);

        outfile.open ("benchmark.txt", std::ios_base::app);
        outfile << "\n\nWriting benchmark for testPlacingQueryWithGeneticAlgorithmStrategyFixedQueryWithDynamicTopologyHeight.\n";
        outfile << "The Query Plan is: \n" << testQueryPlan->toString();
        outfile << "The topology is:\n"<<topology->toString();
        outfile.close();

        std::vector<long> counts_n;
        for(int j = 0; j < repetitions; j++) {
            outfile.open ("benchmark.txt", std::ios_base::app);
            outfile << "\n\n\nRepetition Number: " << j+1 << "\n\n";
            outfile.close();
            auto start = std::chrono::high_resolution_clock::now();
            ASSERT_TRUE(placementStrategy->updateGlobalExecutionPlan(testQueryPlan));
            auto stop = std::chrono::high_resolution_clock::now();
            long count = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
            NES_DEBUG("Found Solution in " << count << "ms");
            counts_n.push_back(count);
            globalExecutionPlan->removeQuerySubPlans(queryId);
        }
        counts.insert(std::make_pair(n, counts_n));
    }
    outfile.open ("benchmark.txt", std::ios_base::app);
    for (auto& [n, counts_n] : counts) {
        std::sort(counts_n.begin(), counts_n.end());
        long median = counts_n[counts_n.size() / 2];
        if (counts_n.size() % 2 == 0) {
            median = (median + counts_n[(counts_n.size() - 1) / 2]) / 2;
        }
        std::stringstream ss;
        for (auto& count : counts_n) {
            ss << count << ", ";
        }

        NES_INFO("N: " << n+2 << ", median: " << median << ", measures: " << ss.str());
        outfile <<"\nN: " << n+2 << ", median: " << median << ", measures: " << ss.str() << "\n";
    }
    outfile.close();
}