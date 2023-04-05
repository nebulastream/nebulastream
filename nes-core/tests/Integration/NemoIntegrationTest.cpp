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
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/MetricValidator.hpp>
#include <Util/TestUtils.hpp>
#include <assert.h>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>

using std::cout;
using std::endl;
namespace NES {

uint16_t timeout = 15;

/**
* @brief Test the NEMO placement on different topologies to check if shared nodes contain the window operator based on the configs
* of setDistributedWindowChildThreshold and setDistributedWindowCombinerThreshold.
*/
class NemoIntegrationTest : public Testing::NESBaseTest {
  public:
    Runtime::BufferManagerPtr bufferManager;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("NemoIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup NemoIntegrationTest test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        bufferManager = std::make_shared<Runtime::BufferManager>(4096, 10);
        NES_INFO("NemoIntegrationTest: Setting up test with rpc port " << rpcCoordinatorPort << ", rest port " << restPort);
    }

    std::string testTopology(uint64_t restPort,
                             uint64_t rpcCoordinatorPort,
                             uint64_t layers,
                             uint64_t nodesPerNode,
                             uint64_t leafNodesPerNode,
                             uint64_t childThreshold,
                             uint64_t combinerThreshold,
                             uint64_t expectedNumberBuffers) {
        NES_INFO(" start coordinator");
        std::string outputFilePath = this->getTestResourceFolder().string() + "/windowOut.csv";
        remove(outputFilePath.c_str());

        uint64_t nodeId = 1;
        uint64_t leafNodes = 0;

        std::vector<uint64_t> nodes;
        std::vector<uint64_t> parents;
        std::vector<std::shared_ptr<Util::Subprocess>> workerProcs;

        // Setup the topology
        auto coordinator = TestUtils::startCoordinator({TestUtils::rpcPort(rpcCoordinatorPort),
                                                        TestUtils::restPort(restPort),
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

        std::stringstream ss;
        ss << "{\"userQuery\" : ";
        ss << "\"Query::from(\\\"window\\\")"
              ".window(TumblingWindow::of(EventTime(Attribute(\\\"timestamp\\\")), Seconds(1)))"
              ".byKey(Attribute(\\\"id\\\"))"
              ".apply(Sum(Attribute(\\\"value\\\"))).sink(FileSinkDescriptor::create(\\\"";
        ss << outputFilePath;
        ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
        ss << R"());","placement" : "BottomUp"})";
        ss << endl;

        NES_INFO("query string submit=" << ss.str());

        nlohmann::json json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(restPort));
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

    uint64_t countLines(const std::string& str) { return std::count(str.begin(), str.end(), '\n') + 1; }
};

TEST_F(NemoIntegrationTest, testThreeLevelsTopologyTopDown) {
    uint64_t childThreshold = 1000;
    uint64_t combinerThreshold = 1;
    uint64_t expectedNoBuffers = 9;
    auto content = testTopology(*restPort, *rpcCoordinatorPort, 3, 2, 4, childThreshold, combinerThreshold, expectedNoBuffers);

    NES_INFO("content=" << content);
    auto lineCnt = countLines(content);

    ASSERT_EQ(lineCnt, 74);
}

TEST_F(NemoIntegrationTest, testThreeLevelsTopologyBottomUp) {
    uint64_t childThreshold = 1;
    uint64_t combinerThreshold = 1000;
    uint64_t expectedNoBuffers = 8;
    auto content = testTopology(*restPort, *rpcCoordinatorPort, 3, 2, 4, childThreshold, combinerThreshold, expectedNoBuffers);

    NES_INFO("content=" << content);
    auto lineCnt = countLines(content);

    ASSERT_EQ(lineCnt, 74);
}

TEST_F(NemoIntegrationTest, testNemoThreelevels) {
    uint64_t childThreshold = 1;
    uint64_t combinerThreshold = 1;
    uint64_t expectedNoBuffers = 2;
    auto content = testTopology(*restPort, *rpcCoordinatorPort, 3, 2, 4, childThreshold, combinerThreshold, expectedNoBuffers);

    NES_INFO("content=" << content);
    auto lineCnt = countLines(content);

    ASSERT_EQ(lineCnt, 74);
}

TEST_F(NemoIntegrationTest, DISABLED_testNemoPlacementFourLevelsSparseTopology) {
    uint64_t childThreshold = 0;
    uint64_t combinerThreshold = 0;
    uint64_t expectedNoBuffers = 2;
    auto content = testTopology(*restPort, *rpcCoordinatorPort, 4, 2, 1, childThreshold, combinerThreshold, expectedNoBuffers);

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "0,10000,1,204\n"
                             "10000,20000,1,580\n"
                             "0,10000,4,4\n"
                             "0,10000,11,20\n"
                             "0,10000,12,4\n"
                             "0,10000,16,8\n";

    NES_INFO("content=" << content);
    NES_INFO("expContent=" << expectedContent);
    ASSERT_EQ(content, expectedContent);
}

TEST_F(NemoIntegrationTest, DISABLED_testNemoPlacementFourLevelsDenseTopology) {
    uint64_t childThreshold = 0;
    uint64_t combinerThreshold = 0;
    uint64_t expectedNoBuffers = 9;
    auto content = testTopology(*restPort, *rpcCoordinatorPort, 4, 3, 3, childThreshold, combinerThreshold, expectedNoBuffers);

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "0,10000,1,153\n"
                             "10000,20000,1,435\n"
                             "0,10000,4,3\n"
                             "0,10000,11,15\n"
                             "0,10000,12,3\n"
                             "0,10000,16,6\n"
                             "0,10000,1,153\n"
                             "10000,20000,1,435\n"
                             "0,10000,4,3\n"
                             "0,10000,11,15\n"
                             "0,10000,12,3\n"
                             "0,10000,16,6\n"
                             "0,10000,1,153\n"
                             "10000,20000,1,435\n"
                             "0,10000,4,3\n"
                             "0,10000,11,15\n"
                             "0,10000,12,3\n"
                             "0,10000,16,6\n"
                             "0,10000,1,153\n"
                             "10000,20000,1,435\n"
                             "0,10000,4,3\n"
                             "0,10000,11,15\n"
                             "0,10000,12,3\n"
                             "0,10000,16,6\n"
                             "0,10000,1,153\n"
                             "10000,20000,1,435\n"
                             "0,10000,4,3\n"
                             "0,10000,11,15\n"
                             "0,10000,12,3\n"
                             "0,10000,16,6\n"
                             "0,10000,1,153\n"
                             "10000,20000,1,435\n"
                             "0,10000,4,3\n"
                             "0,10000,11,15\n"
                             "0,10000,12,3\n"
                             "0,10000,16,6\n"
                             "0,10000,1,153\n"
                             "10000,20000,1,435\n"
                             "0,10000,4,3\n"
                             "0,10000,11,15\n"
                             "0,10000,12,3\n"
                             "0,10000,16,6\n"
                             "0,10000,1,153\n"
                             "10000,20000,1,435\n"
                             "0,10000,4,3\n"
                             "0,10000,11,15\n"
                             "0,10000,12,3\n"
                             "0,10000,16,6\n"
                             "0,10000,1,153\n"
                             "10000,20000,1,435\n"
                             "0,10000,4,3\n"
                             "0,10000,11,15\n"
                             "0,10000,12,3\n"
                             "0,10000,16,6\n";

    NES_INFO("content=" << content);
    NES_INFO("expContent=" << expectedContent);
    ASSERT_EQ(content.size(), expectedContent.size());
    ASSERT_EQ(content, expectedContent);
}

}// namespace NES