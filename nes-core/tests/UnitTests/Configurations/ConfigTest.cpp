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

#include "Monitoring/Metrics/Gauge/DiskMetrics.hpp"
#include "Monitoring/Metrics/Gauge/MemoryMetrics.hpp"
#include "Monitoring/Metrics/Gauge/CpuMetrics.hpp"
#include "Monitoring/Metrics/Gauge/NetworkMetrics.hpp"

#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/KafkaSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/MQTTSourceType.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceFactory.hpp>
#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <string>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <cpprest/json.h>
#include <Monitoring/MonitoringAgent.hpp>
#include <Monitoring/MonitoringCatalog.hpp>

namespace NES {

using namespace Configurations;

class ConfigTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("Config.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup Configuration test class.");
    }

    void SetUp() override {}

    static void TearDownTestCase() { NES_INFO("Tear down ActorCoordinatorWorkerTest test class."); }
};

/**
 * @brief This reads an coordinator yaml and checks the configuration
 */
TEST_F(ConfigTest, testEmptyParamsAndMissingParamsCoordinatorYAMLFile) {

    CoordinatorConfigurationPtr coordinatorConfigPtr = std::make_shared<CoordinatorConfiguration>();
    coordinatorConfigPtr->overwriteConfigWithYAMLFileInput(std::string(TEST_DATA_DIRECTORY) + "emptyCoordinator.yaml");
    EXPECT_EQ(coordinatorConfigPtr->restPort.getValue(), coordinatorConfigPtr->restPort.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->rpcPort.getValue(), coordinatorConfigPtr->rpcPort.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->dataPort.getValue(), coordinatorConfigPtr->dataPort.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->restIp.getValue(), coordinatorConfigPtr->restIp.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->coordinatorIp.getValue(), coordinatorConfigPtr->coordinatorIp.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->numberOfSlots.getValue(), coordinatorConfigPtr->numberOfSlots.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->logLevel.getValue(), coordinatorConfigPtr->logLevel.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->numberOfBuffersInGlobalBufferManager.getValue(),
              coordinatorConfigPtr->numberOfBuffersInGlobalBufferManager.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->numberOfBuffersPerWorker.getValue(),
              coordinatorConfigPtr->numberOfBuffersPerWorker.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->numberOfBuffersInSourceLocalBufferPool.getValue(),
              coordinatorConfigPtr->numberOfBuffersInSourceLocalBufferPool.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->bufferSizeInBytes.getValue(), coordinatorConfigPtr->bufferSizeInBytes.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->numWorkerThreads.getValue(), coordinatorConfigPtr->numWorkerThreads.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->optimizer.queryBatchSize.getValue(),
              coordinatorConfigPtr->optimizer.queryBatchSize.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->optimizer.queryMergerRule.getValue(),
              coordinatorConfigPtr->optimizer.queryMergerRule.getDefaultValue());
}

TEST_F(ConfigTest, testLogicalSourceAndSchemaParamsCoordinatorYAMLFile) {
    CoordinatorConfigurationPtr coordinatorConfigPtr = std::make_shared<CoordinatorConfiguration>();
    coordinatorConfigPtr->overwriteConfigWithYAMLFileInput(std::string(TEST_DATA_DIRECTORY)
                                                           + "coordinatorLogicalSourceAndSchema.yaml");
    EXPECT_FALSE(coordinatorConfigPtr->logicalSources.empty());
    EXPECT_EQ(coordinatorConfigPtr->logicalSources.size(), 2);
    auto logicalSources = coordinatorConfigPtr->logicalSources.getValues();
    EXPECT_EQ(logicalSources[0].getValue()->getLogicalSourceName(), "lsn1");
    EXPECT_EQ(logicalSources[1].getValue()->getLogicalSourceName(), "lsn2");
    auto firstSourceSchema = logicalSources[0].getValue()->getSchema();
    auto secondSourceSchema = logicalSources[1].getValue()->getSchema();
    EXPECT_EQ(firstSourceSchema->getSize(), 3);
    EXPECT_EQ(secondSourceSchema->getSize(), 1);
    EXPECT_TRUE(firstSourceSchema->contains("csv_id"));
    EXPECT_TRUE(firstSourceSchema->contains("csv_id_2"));
    EXPECT_TRUE(firstSourceSchema->contains("csv_id_3"));
    EXPECT_TRUE(secondSourceSchema->contains("csv_id_4"));
}

TEST_F(ConfigTest, testWorkerConfigLennart) {
    // schema zum Vergleich
    std::list<std::string> configuredDisk = {"F_BSIZE", "F_BLOCKS", "F_FRSIZE"};
    int sampleDisk = 50;
    SchemaPtr schemaDisk = DiskMetrics::createSchema("", configuredDisk);
    std::list<std::string> configuredCpu = {"coreNum", "user", "system"};
    int sampleCpu = 60;
    SchemaPtr schemaCpu = CpuMetrics::createSchema("", configuredCpu);
    std::list<std::string> configuredMem = {"FREE_RAM", "FREE_SWAP", "TOTAL_RAM"};
    int sampleMem = 40;
    SchemaPtr schemaMem = MemoryMetrics::createSchema("", configuredMem);
    std::list<std::string> configuredNetwork = {"rBytes", "rFifo", "tPackets"};
    int sampleNetwork = 30;
    SchemaPtr schemaNetwork = NetworkMetrics::createSchema("", configuredNetwork);

    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
//    workerConfigPtr->overwriteConfigWithYAMLFileInput(std::string(TEST_DATA_DIRECTORY) + "workerConfigLennart.yaml");
   workerConfigPtr->overwriteConfigWithYAMLFileInput("/home/loell/CLionProjects/nebulastream/nes-core/tests/test_data/workerConfigLennart.yaml");
//    workerConfigPtr->overwriteConfigWithYAMLFileInput("/home/lenson/CLionProjects/nebulastream/nes-core/tests/test_data/workerConfigLennart.yaml");

    // Json Kram
    web::json::value configurationMonitoringJson =
        MetricUtils::parseMonitoringConfigStringToJson(workerConfigPtr->monitoringConfiguration.getValue());
    MonitoringPlanPtr monitoringPlanJson = MonitoringPlan::setSchemaJson(configurationMonitoringJson);
    MonitoringCatalogPtr monitoringCatalog = MonitoringCatalog::createCatalog(monitoringPlanJson);

    ASSERT_TRUE(monitoringPlanJson->getSchema(CpuMetric)->equals(schemaCpu, false));
    ASSERT_TRUE(monitoringPlanJson->getSchema(DiskMetric)->equals(schemaDisk, false));
    ASSERT_TRUE(monitoringPlanJson->getSchema(MemoryMetric)->equals(schemaMem, false));
    ASSERT_TRUE(monitoringPlanJson->getSchema(NetworkMetric)->equals(schemaNetwork, false));

    // TODO: check if Catalog is init right; check the schema for each MetricType


    MonitoringAgentPtr monitoringAgent = MonitoringAgent::create(monitoringPlanJson, monitoringCatalog, true);

    std::cout << "Well done!";
}

TEST_F(ConfigTest, testCoordinatorEPERATPRmptyParamsConsoleInput) {

    CoordinatorConfigurationPtr coordinatorConfigPtr = std::make_shared<CoordinatorConfiguration>();
    std::string argv[] = {"--restIp=localhost",
                          "--numberOfSlots=10",
                          "--numberOfBuffersInSourceLocalBufferPool=128",
                          "--bufferSizeInBytes=1024"};
    int argc = 4;

    std::map<string, string> commandLineParams;

    for (int i = 0; i < argc; ++i) {
        commandLineParams.insert(
            std::pair<string, string>(string(argv[i]).substr(0, string(argv[i]).find('=')),
                                      string(argv[i]).substr(string(argv[i]).find('=') + 1, string(argv[i]).length() - 1)));
    }

    coordinatorConfigPtr->overwriteConfigWithCommandLineInput(commandLineParams);

    EXPECT_EQ(coordinatorConfigPtr->restPort.getValue(), coordinatorConfigPtr->restPort.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->rpcPort.getValue(), coordinatorConfigPtr->rpcPort.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->dataPort.getValue(), coordinatorConfigPtr->dataPort.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->restIp.getValue(), coordinatorConfigPtr->restIp.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->coordinatorIp.getValue(), coordinatorConfigPtr->coordinatorIp.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->numberOfSlots.getValue(), coordinatorConfigPtr->numberOfSlots.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->logLevel.getValue(), coordinatorConfigPtr->logLevel.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->numberOfBuffersInGlobalBufferManager.getValue(),
              coordinatorConfigPtr->numberOfBuffersInGlobalBufferManager.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->numberOfBuffersPerWorker.getValue(),
              coordinatorConfigPtr->numberOfBuffersPerWorker.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->numberOfBuffersInSourceLocalBufferPool.getValue(),
              coordinatorConfigPtr->numberOfBuffersInSourceLocalBufferPool.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->bufferSizeInBytes.getValue(), coordinatorConfigPtr->bufferSizeInBytes.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->numWorkerThreads.getValue(), coordinatorConfigPtr->numWorkerThreads.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->optimizer.queryBatchSize.getValue(),
              coordinatorConfigPtr->optimizer.queryBatchSize.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->optimizer.queryMergerRule.getValue(),
              coordinatorConfigPtr->optimizer.queryMergerRule.getDefaultValue());
}

TEST_F(ConfigTest, testEmptyParamsAndMissingParamsWorkerYAMLFile) {

    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    workerConfigPtr->overwriteConfigWithYAMLFileInput(std::string(TEST_DATA_DIRECTORY) + "emptyWorker.yaml");

    EXPECT_NE(workerConfigPtr->localWorkerIp.getValue(), workerConfigPtr->localWorkerIp.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->rpcPort.getValue(), workerConfigPtr->rpcPort.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->dataPort.getValue(), workerConfigPtr->dataPort.getDefaultValue());
    EXPECT_NE(workerConfigPtr->coordinatorPort.getValue(), workerConfigPtr->coordinatorPort.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->coordinatorIp.getValue(), workerConfigPtr->coordinatorIp.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->numberOfSlots.getValue(), workerConfigPtr->numberOfSlots.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->logLevel.getValue(), workerConfigPtr->logLevel.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numberOfBuffersInGlobalBufferManager.getValue(),
              workerConfigPtr->numberOfBuffersInGlobalBufferManager.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->numberOfBuffersPerWorker.getValue(), workerConfigPtr->numberOfBuffersPerWorker.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getValue(),
              workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->bufferSizeInBytes.getValue(), workerConfigPtr->bufferSizeInBytes.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numWorkerThreads.getValue(), workerConfigPtr->numWorkerThreads.getDefaultValue());
    EXPECT_TRUE(workerConfigPtr->physicalSources.empty());
}

TEST_F(ConfigTest, testWorkerYAMLFileWithMultiplePhysicalSource) {

    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    workerConfigPtr->overwriteConfigWithYAMLFileInput(std::string(TEST_DATA_DIRECTORY) + "workerWithPhysicalSources.yaml");

    EXPECT_NE(workerConfigPtr->localWorkerIp.getValue(), workerConfigPtr->localWorkerIp.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->rpcPort.getValue(), workerConfigPtr->rpcPort.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->dataPort.getValue(), workerConfigPtr->dataPort.getDefaultValue());
    EXPECT_NE(workerConfigPtr->coordinatorPort.getValue(), workerConfigPtr->coordinatorPort.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->coordinatorIp.getValue(), workerConfigPtr->coordinatorIp.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->numberOfSlots.getValue(), workerConfigPtr->numberOfSlots.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->logLevel.getValue(), workerConfigPtr->logLevel.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numberOfBuffersInGlobalBufferManager.getValue(),
              workerConfigPtr->numberOfBuffersInGlobalBufferManager.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->numberOfBuffersPerWorker.getValue(), workerConfigPtr->numberOfBuffersPerWorker.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getValue(),
              workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->bufferSizeInBytes.getValue(), workerConfigPtr->bufferSizeInBytes.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numWorkerThreads.getValue(), workerConfigPtr->numWorkerThreads.getDefaultValue());
    EXPECT_TRUE(!workerConfigPtr->physicalSources.empty());
    EXPECT_TRUE(workerConfigPtr->physicalSources.size() == 2);
    for (const auto& physicalSource : workerConfigPtr->physicalSources.getValues()) {
        EXPECT_TRUE(physicalSource.getValue()->getPhysicalSourceType()->instanceOf<DefaultSourceType>()
                    || physicalSource.getValue()->getPhysicalSourceType()->instanceOf<MQTTSourceType>());
    }
    EXPECT_EQ(workerConfigPtr->locationCoordinates.getValue(), workerConfigPtr->locationCoordinates.getDefaultValue());
}

TEST_F(ConfigTest, testWorkerEmptyParamsConsoleInput) {

    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    std::string argv[] = {"--localWorkerIp=localhost",
                          "--coordinatorPort=5000",
                          "--numWorkerThreads=5",
                          "--numberOfBuffersInGlobalBufferManager=2048",
                          "--numberOfBuffersInSourceLocalBufferPool=128",
                          "--queryCompiler.compilationStrategy=FAST",
                          "--queryCompiler.pipeliningStrategy=OPERATOR_AT_A_TIME",
                          "--queryCompiler.outputBufferOptimizationLevel=ONLY_INPLACE_OPERATIONS_NO_FALLBACK",
                          "--physicalSources.type=DefaultSource",
                          "--physicalSources.numberOfBuffersToProduce=5",
                          "--physicalSources.rowLayout=false",
                          "--physicalSources.physicalSourceName=x",
                          "--physicalSources.logicalSourceName=default",
                          "--fieldNodeLocationCoordinates=23.88,-3.4"};
    int argc = 14;

    std::map<string, string> commandLineParams;

    for (int i = 0; i < argc; ++i) {
        commandLineParams.insert(
            std::pair<string, string>(string(argv[i]).substr(0, string(argv[i]).find('=')),
                                      string(argv[i]).substr(string(argv[i]).find('=') + 1, string(argv[i]).length() - 1)));
    }

    workerConfigPtr->overwriteConfigWithCommandLineInput(commandLineParams);

    EXPECT_NE(workerConfigPtr->localWorkerIp.getValue(), workerConfigPtr->localWorkerIp.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->rpcPort.getValue(), workerConfigPtr->rpcPort.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->dataPort.getValue(), workerConfigPtr->dataPort.getDefaultValue());
    EXPECT_NE(workerConfigPtr->coordinatorPort.getValue(), workerConfigPtr->coordinatorPort.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->coordinatorIp.getValue(), workerConfigPtr->coordinatorIp.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->numberOfSlots.getValue(), workerConfigPtr->numberOfSlots.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->logLevel.getValue(), workerConfigPtr->logLevel.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numberOfBuffersInGlobalBufferManager.getValue(),
              workerConfigPtr->numberOfBuffersInGlobalBufferManager.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->numberOfBuffersPerWorker.getValue(), workerConfigPtr->numberOfBuffersPerWorker.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getValue(),
              workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->bufferSizeInBytes.getValue(), workerConfigPtr->bufferSizeInBytes.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numWorkerThreads.getValue(), workerConfigPtr->numWorkerThreads.getDefaultValue());
    EXPECT_NE(workerConfigPtr->queryCompiler.compilationStrategy.getValue(),
              workerConfigPtr->queryCompiler.compilationStrategy.getDefaultValue());
    EXPECT_NE(workerConfigPtr->queryCompiler.pipeliningStrategy.getValue(),
              workerConfigPtr->queryCompiler.pipeliningStrategy.getDefaultValue());
    EXPECT_NE(workerConfigPtr->queryCompiler.outputBufferOptimizationLevel.getValue(),
              workerConfigPtr->queryCompiler.outputBufferOptimizationLevel.getDefaultValue());
    EXPECT_NE(workerConfigPtr->locationCoordinates.getValue(), workerConfigPtr->locationCoordinates.getDefaultValue());
}

TEST_F(ConfigTest, testWorkerCSCVSourceConsoleInput) {

    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    std::string argv[] = {"--localWorkerIp=localhost",
                          "--coordinatorPort=5000",
                          "--numWorkerThreads=5",
                          "--numberOfBuffersInGlobalBufferManager=2048",
                          "--numberOfBuffersInSourceLocalBufferPool=128",
                          "--queryCompiler.compilationStrategy=FAST",
                          "--queryCompiler.pipeliningStrategy=OPERATOR_AT_A_TIME",
                          "--queryCompiler.outputBufferOptimizationLevel=ONLY_INPLACE_OPERATIONS_NO_FALLBACK",
                          "--physicalSources.type=CSVSource",
                          "--physicalSources.filePath=fileLoc",
                          "--physicalSources.rowLayout=false",
                          "--physicalSources.physicalSourceName=x",
                          "--physicalSources.logicalSourceName=default"};
    int argc = 13;

    std::map<string, string> commandLineParams;

    for (int i = 0; i < argc; ++i) {
        commandLineParams.insert(
            std::pair<string, string>(string(argv[i]).substr(0, string(argv[i]).find('=')),
                                      string(argv[i]).substr(string(argv[i]).find('=') + 1, string(argv[i]).length() - 1)));
    }

    workerConfigPtr->overwriteConfigWithCommandLineInput(commandLineParams);

    EXPECT_NE(workerConfigPtr->localWorkerIp.getValue(), workerConfigPtr->localWorkerIp.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->rpcPort.getValue(), workerConfigPtr->rpcPort.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->dataPort.getValue(), workerConfigPtr->dataPort.getDefaultValue());
    EXPECT_NE(workerConfigPtr->coordinatorPort.getValue(), workerConfigPtr->coordinatorPort.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->coordinatorIp.getValue(), workerConfigPtr->coordinatorIp.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->numberOfSlots.getValue(), workerConfigPtr->numberOfSlots.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->logLevel.getValue(), workerConfigPtr->logLevel.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numberOfBuffersInGlobalBufferManager.getValue(),
              workerConfigPtr->numberOfBuffersInGlobalBufferManager.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->numberOfBuffersPerWorker.getValue(), workerConfigPtr->numberOfBuffersPerWorker.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getValue(),
              workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->bufferSizeInBytes.getValue(), workerConfigPtr->bufferSizeInBytes.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numWorkerThreads.getValue(), workerConfigPtr->numWorkerThreads.getDefaultValue());
    EXPECT_NE(workerConfigPtr->queryCompiler.compilationStrategy.getValue(),
              workerConfigPtr->queryCompiler.compilationStrategy.getDefaultValue());
    EXPECT_NE(workerConfigPtr->queryCompiler.pipeliningStrategy.getValue(),
              workerConfigPtr->queryCompiler.pipeliningStrategy.getDefaultValue());
    EXPECT_NE(workerConfigPtr->queryCompiler.outputBufferOptimizationLevel.getValue(),
              workerConfigPtr->queryCompiler.outputBufferOptimizationLevel.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->locationCoordinates.getValue(), workerConfigPtr->locationCoordinates.getDefaultValue());
}

TEST_F(ConfigTest, testSourceEmptyParamsConsoleInput) {

    std::string argv[] = {"type=DefaultSource",
                          "numberOfBuffersToProduce=5",
                          "rowLayout=false",
                          "physicalSourceName=x",
                          "logicalSourceName=default"};
    int argc = 5;

    std::map<string, string> commandLineParams;

    for (int i = 0; i < argc; ++i) {
        commandLineParams.insert(
            std::pair<string, string>(string(argv[i]).substr(0, string(argv[i]).find('=')),
                                      string(argv[i]).substr(string(argv[i]).find('=') + 1, string(argv[i]).length() - 1)));
    }

    PhysicalSourcePtr physicalSource1 = PhysicalSourceFactory::createFromString("", commandLineParams);
    EXPECT_EQ(physicalSource1->getLogicalSourceName(), "default");
    EXPECT_EQ(physicalSource1->getPhysicalSourceName(), "x");
    EXPECT_TRUE(physicalSource1->getPhysicalSourceType()->instanceOf<DefaultSourceType>());

    DefaultSourceTypePtr physicalSourceType1 = physicalSource1->getPhysicalSourceType()->as<DefaultSourceType>();
    EXPECT_EQ(physicalSourceType1->getSourceGatheringInterval()->getValue(),
              physicalSourceType1->getSourceGatheringInterval()->getDefaultValue());
    EXPECT_NE(physicalSourceType1->getNumberOfBuffersToProduce()->getValue(), 5u);

    std::string argv1[] = {"type=KafkaSource",
                           "physicalSourceName=x",
                           "logicalSourceName=default",
                           "topic=newTopic",
                           "connectionTimeout=100",
                           "brokers=testBroker",
                           "groupId=testId"};

    argc = 7;

    std::map<string, string> commandLineParams1;

    for (int i = 0; i < argc; ++i) {
        commandLineParams1.insert(
            std::pair<string, string>(string(argv1[i]).substr(0, string(argv1[i]).find('=')),
                                      string(argv1[i]).substr(string(argv1[i]).find('=') + 1, string(argv1[i]).length() - 1)));
    }

    PhysicalSourcePtr physicalSource2 = PhysicalSourceFactory::createFromString("", commandLineParams1);
    EXPECT_EQ(physicalSource2->getLogicalSourceName(), "default");
    EXPECT_EQ(physicalSource2->getPhysicalSourceName(), "x");
    EXPECT_TRUE(physicalSource2->getPhysicalSourceType()->instanceOf<KafkaSourceType>());

    KafkaSourceTypePtr physicalSourceType2 = physicalSource2->getPhysicalSourceType()->as<KafkaSourceType>();
    EXPECT_NE(physicalSourceType2->getBrokers()->getValue(), physicalSourceType2->getBrokers()->getDefaultValue());
    EXPECT_EQ(physicalSourceType2->getAutoCommit()->getValue(), physicalSourceType2->getAutoCommit()->getDefaultValue());
    EXPECT_NE(physicalSourceType2->getGroupId()->getValue(), physicalSourceType2->getGroupId()->getDefaultValue());
    EXPECT_NE(physicalSourceType2->getTopic()->getValue(), physicalSourceType2->getTopic()->getDefaultValue());
    EXPECT_NE(physicalSourceType2->getConnectionTimeout()->getValue(),
              physicalSourceType2->getConnectionTimeout()->getDefaultValue());
}

}// namespace NES