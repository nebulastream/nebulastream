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

#include <NesBaseTest.hpp>
#include <gtest/gtest.h>

#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>
#include <Util/MetricValidator.hpp>

#include <Monitoring/MetricCollectors/MetricCollectorType.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Runtime/BufferManager.hpp>

#include <Services/MonitoringService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cstdint>
#include <memory>
#include <nlohmann/json.hpp>
#include <regex>

using std::cout;
using std::endl;
namespace NES {

uint16_t timeout = 15;

class MonitoringQueriesIntegrationTest : public Testing::NESBaseTest {
  public:
    Runtime::BufferManagerPtr bufferManager;
    std::string outputFilePath = this->getTestResourceFolder().string() + "/windowOut.csv";

    static void SetUpTestCase() {
        NES::Logger::setupLogging("MonitoringQueriesIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MonitoringQueriesIntegrationTest test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        bufferManager = std::make_shared<Runtime::BufferManager>(4096, 10);
        NES_INFO("MonitoringQueriesIntegrationTest: Setting up test with rpc port " << rpcCoordinatorPort << ", rest port "
                                                                                    << restPort);
    }

    std::string testTopology(uint64_t restPort,
                             uint64_t rpcCoordinatorPort,
                             uint64_t layers,
                             uint64_t nodesPerNode,
                             uint64_t leafNodesPerNode,
                             uint64_t childThreshold,
                             uint64_t combinerThreshold,
                             uint64_t expectedNumberBuffers,
                             std::string query) {
        NES_INFO(" start coordinator");
        remove(outputFilePath.c_str());

        uint64_t nodeId = 1;
        uint64_t leafNodes = 0;

        std::vector<uint64_t> nodes;
        std::vector<uint64_t> parents;
        std::vector<std::shared_ptr<Util::Subprocess>> workerProcs;

        // Setup the topology
        auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(rpcCoordinatorPort),
                                                        TestUtils::restPort(restPort),
                                                        TestUtils::enableMonitoring(),
                                                        TestUtils::enableDebug(),
                                                        TestUtils::enableNemoPlacement(),
                                                        TestUtils::setDistributedWindowChildThreshold(childThreshold),
                                                        TestUtils::setDistributedWindowCombinerThreshold(combinerThreshold)});
        assert(TestUtils::waitForWorkers(restPort, timeout, 0));

        std::stringstream schema;
        schema << "{\"logicalSourceName\" : \"window\",\"schema\" "
                  ":\"Schema::create()->addField(createField(\\\"id\\\",UINT64))->addField(createField(\\\"value\\\",UINT64))->"
                  "addField(createField(\\\"timestamp\\\",UINT64));\"}";
        schema << endl;

        NES_INFO("schema submit=" << schema.str());
        bool logSource = TestUtils::addLogicalSource(schema.str(), std::to_string(restPort));
        assert(logSource);

        nodes.emplace_back(1);
        parents.emplace_back(1);

        auto cnt = 1;
        for (uint64_t i = 2; i <= layers; i++) {
            std::vector<uint64_t> newParents;
            for (auto parent : parents) {
                uint64_t nodeCnt = nodesPerNode;
                if (i == layers) {
                    nodeCnt = leafNodesPerNode;
                }
                for (uint64_t j = 0; j < nodeCnt; j++) {
                    nodeId++;
                    nodes.emplace_back(nodeId);
                    newParents.emplace_back(nodeId);

                    if (i == layers) {
                        leafNodes++;
                        auto workerProc = TestUtils::startWorkerPtr(
                            {TestUtils::rpcPort(0),
                             TestUtils::dataPort(0),
                             TestUtils::enableMonitoring(),
                             TestUtils::monitoringWaitTime(500),
                             TestUtils::coordinatorPort(rpcCoordinatorPort),
                             TestUtils::parentId(parent),
                             TestUtils::sourceType("CSVSource"),
                             TestUtils::csvSourceFilePath(std::string(TEST_DATA_DIRECTORY) + "keyed_windows/window_"
                                                          + std::to_string(cnt++) + ".csv"),
                             TestUtils::physicalSourceName("test_stream_" + std::to_string(nodeId)),
                             TestUtils::logicalSourceName("window"),
                             TestUtils::numberOfBuffersToProduce(1),
                             TestUtils::numberOfTuplesToProducePerBuffer(50),
                             TestUtils::sourceGatheringInterval(1000),
                             TestUtils::workerHealthCheckWaitTime(1)});
                        workerProcs.emplace_back(std::move(workerProc));
                        assert(TestUtils::waitForWorkers(restPort, timeout, nodeId - 1));
                        continue;
                    }

                    auto workerProc = TestUtils::startWorkerPtr({TestUtils::rpcPort(0),
                                                                 TestUtils::dataPort(0),
                                                                 TestUtils::coordinatorPort(rpcCoordinatorPort),
                                                                 TestUtils::parentId(parent),
                                                                 TestUtils::workerHealthCheckWaitTime(1)});
                    workerProcs.emplace_back(std::move(workerProc));
                    assert(TestUtils::waitForWorkers(restPort, timeout, nodeId - 1));
                }
            }
            parents = newParents;
        }

        NES_INFO("NemoIntegrationTest: Finished setting up topology.");

        NES_INFO("query string submit=" << query);
        nlohmann::json json_return = TestUtils::startQueryViaRest(query, std::to_string(restPort));
        QueryId queryId = json_return["queryId"].get<uint64_t>();

        NES_INFO("try to acc return");
        NES_INFO("Query ID: " << queryId);

        auto checkCompleted = TestUtils::checkCompleteOrTimeout(queryId, expectedNumberBuffers, std::to_string(restPort));
        assert(checkCompleted);

        auto queryStopped = TestUtils::stopQueryViaRest(queryId, std::to_string(restPort));
        assert(queryStopped);

        std::ifstream ifs(outputFilePath.c_str());
        assert(ifs.good());
        std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
        NES_INFO("content=" << content);

        return content;
    }

    std::string createMonitoringQuery(Monitoring::MetricType metricType, std::string attribute, std::string out) {
        std::stringstream ss;
        ss << "{\"userQuery\" : ";
        ss << "\"Query::from(\\\"%STREAM%\\\")"
              ".window(TumblingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(1)))"
              ".byKey(Attribute(\\\"node_id\\\"))"
              ".apply(Avg(Attribute(\\\"%ATTRIBUTE%\\\"))).sink(FileSinkDescriptor::create(\\\"";
        ss << out;
        ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
        ss << R"());","placement" : "BottomUp"})";
        ss << endl;

        std::string query = ss.str();
        query = std::regex_replace(query, std::regex("%STREAM%"), toString(metricType));
        query = std::regex_replace(query, std::regex("%ATTRIBUTE%"), attribute);
        return query;
    }

    uint64_t countLines(const std::string& str) { return std::count(str.begin(), str.end(), '\n') + 1; }
};

TEST_F(MonitoringQueriesIntegrationTest, testThreeLevelsTopologyTopDown) {
    uint64_t childThreshold = 1000;
    uint64_t combinerThreshold = 1;
    uint64_t expectedNoBuffers = 100;
    std::string query = createMonitoringQuery(Monitoring::WrappedNetworkMetrics, "tBytes", outputFilePath);
    auto content =
        testTopology(*restPort, *rpcCoordinatorPort, 3, 2, 4, childThreshold, combinerThreshold, expectedNoBuffers, query);

    auto lineCnt = countLines(content);

    ASSERT_EQ(lineCnt, 92);
}

TEST_F(MonitoringQueriesIntegrationTest, testThreeLevelsTopologyBottomUp) {
    uint64_t childThreshold = 1;
    uint64_t combinerThreshold = 1000;
    uint64_t expectedNoBuffers = 100;
    std::string query = createMonitoringQuery(Monitoring::WrappedNetworkMetrics, "tBytes", outputFilePath);
    auto content =
        testTopology(*restPort, *rpcCoordinatorPort, 3, 2, 4, childThreshold, combinerThreshold, expectedNoBuffers, query);

    auto lineCnt = countLines(content);

    ASSERT_EQ(lineCnt, 106);
}

TEST_F(MonitoringQueriesIntegrationTest, testNemoThreelevels) {
    uint64_t childThreshold = 1;
    uint64_t combinerThreshold = 1;
    uint64_t expectedNoBuffers = 20;
    std::string query = createMonitoringQuery(Monitoring::WrappedNetworkMetrics, "tBytes", outputFilePath);
    auto content =
        testTopology(*restPort, *rpcCoordinatorPort, 3, 2, 4, childThreshold, combinerThreshold, expectedNoBuffers, query);

    auto lineCnt = countLines(content);

    ASSERT_EQ(lineCnt, 82);
}

}// namespace NES