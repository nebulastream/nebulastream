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
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Services/QueryParsingService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <gtest/gtest.h>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <iostream>
#include <fstream>

using namespace NES;
using namespace web;
class GeneticAlgorithmBenchmark : public testing::Test {
  public:
    std::shared_ptr<QueryParsingService> queryParsingService;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup QueryPlacementTest test class."  << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::setupLogging("QueryPlacementTest.log", NES::LOG_DEBUG);
        std::cout << "Setup QueryPlacementTest test case." << std::endl;
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
    }

    /* Will be called before a test is executed. */
    void TearDown() { std::cout << "Setup GeneticAlgorithmBenchmark test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down GeneticAlgorithmBenchmark test class." << std::endl; }

    void setupTopologyAndStreamCatalogForGA(int n, int SourcePerMiddle) {
        int nodeID = 1;

        topology = Topology::create();
        TopologyNodePtr rootNode = TopologyNode::create(nodeID++, "localhost", 123, 124, 1000);
        topology->setAsRoot(rootNode);
        streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
        LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));
        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        std::string streamName = "car";
        streamCatalog->addLogicalStream(streamName, schema);
        SourceConfigPtr sourceConfig = SourceConfig::create();
        sourceConfig->setSourceFrequency(0);
        sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
        sourceConfig->setPhysicalStreamName(  "GeneticAlgorithmBenchmark");
        sourceConfig->setLogicalStreamName(streamName);
        PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

        for(int i = 0; i < n; i++) {
            TopologyNodePtr middleNode = TopologyNode::create(nodeID++, "localhost", 123, 124, 5000);
            topology->addNewPhysicalNodeAsChild(rootNode, middleNode);
            middleNode->addLinkProperty(rootNode, linkProperty);
            rootNode->addLinkProperty(middleNode, linkProperty);

            for(int j = 0; j < SourcePerMiddle; j++) {
                TopologyNodePtr sourceNode = TopologyNode::create(nodeID++, "localhost", 123, 124, 5000);
                topology->addNewPhysicalNodeAsChild(middleNode, sourceNode);
                sourceNode->addLinkProperty(middleNode, linkProperty);
                middleNode->addLinkProperty(sourceNode, linkProperty);
                StreamCatalogEntryPtr streamCatalogEntry = std::make_shared<StreamCatalogEntry>(conf, sourceNode);
                streamCatalog->addPhysicalStream(streamName, streamCatalogEntry);
            }
        }
    }

    StreamCatalogPtr streamCatalog;
    TopologyPtr topology;
};


TEST_F(GeneticAlgorithmBenchmark, testPlacingQueryWithGeneticAlgorithmStrategyFixedTopologyWithDynamicQuery) {

    std::list<int> listOfInts({20});
    std::map<int, std::vector<long>> counts;
    int SourcePerMiddle = 3;
    int repetitions = 5;

    setupTopologyAndStreamCatalogForGA(10,SourcePerMiddle);
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("GeneticAlgorithm",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);
    std::ofstream outfile;
    outfile.open ("benchmark.txt");
    outfile << "Writing benchmark for testPlacingQueryWithGeneticAlgorithmStrategyFixedTopologyWithDynamicQuery.\n";
    outfile << "The topology is:\n"<<topology->toString();
    outfile.close();

    for(int n : listOfInts) {


        Query query = Query::from("car");
        for(int i = 0; i < n; i+=2){
            query.filter(Attribute("id") < n-i);
        }
        for(int i = 1; i < n; i+=2){
            query.map(Attribute("value2") = Attribute("value") * 2);
        }
        query.sink(PrintSinkDescriptor::create());

        std::vector<long> counts_n;
        outfile.open("benchmark.txt", std::ios_base::app);
        outfile << "Current N: " << n << "\n";
        outfile.close();
        bool queryWritten = false;
        for(int j = 0; j < repetitions; j++) {
            outfile.open("benchmark.txt", std::ios_base::app);
            outfile << "Repetition Number: " << j << "\n";
            outfile.close();
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
            if(!queryWritten){
                outfile.open("benchmark.txt", std::ios_base::app);
                outfile << "The Query Plan is: \n" << testQueryPlan->toString();
                outfile.close();
                queryWritten = true;
            }
           // UtilityFunctions::assignPropertiesToQueryOperators(testQueryPlan, properties);

            auto start = std::chrono::high_resolution_clock::now();
            ASSERT_TRUE(placementStrategy->updateGlobalExecutionPlan(testQueryPlan));
            auto stop = std::chrono::high_resolution_clock::now();
            long count = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
            NES_DEBUG("Found Solution in " << count << "ms");
            counts_n.push_back(count);
            globalExecutionPlan->removeQuerySubPlans(queryId);
        }
        counts.insert(std::make_pair(n, counts_n));
        //properties.clear();
    }
    outfile.open("benchmark.txt", std::ios_base::app);
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
        outfile << "N: " << n << ", median: " << median << ", measures: " << ss.str()<<"\n";
    }
    outfile.close();
}