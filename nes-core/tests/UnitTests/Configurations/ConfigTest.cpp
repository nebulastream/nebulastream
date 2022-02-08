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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/KafkaSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/MQTTSourceType.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceFactory.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <string>

namespace NES {

using namespace Configurations;

class ConfigTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("Config.log", NES::LOG_DEBUG);
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
    EXPECT_EQ(coordinatorConfigPtr->optimizer.queryBatchSize.getValue(), coordinatorConfigPtr->optimizer.queryBatchSize.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->optimizer.queryMergerRule.getValue(), coordinatorConfigPtr->optimizer.queryMergerRule.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->optimizer.enableSemanticQueryValidation.getValue(),
              coordinatorConfigPtr->optimizer.enableSemanticQueryValidation.getDefaultValue());
}

TEST_F(ConfigTest, testCoordinatorEmptyParamsConsoleInput) {

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
    EXPECT_EQ(coordinatorConfigPtr->optimizer.queryBatchSize.getValue(), coordinatorConfigPtr->optimizer.queryBatchSize.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->optimizer.queryMergerRule.getValue(), coordinatorConfigPtr->optimizer.queryMergerRule.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->optimizer.enableSemanticQueryValidation.getValue(),
              coordinatorConfigPtr->optimizer.enableSemanticQueryValidation.getDefaultValue());
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
}

TEST_F(ConfigTest, testWorkerEmptyParamsConsoleInput) {

    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    std::string argv[] = {
        "--localWorkerIp=localhost",
        "--coordinatorPort=5000",
        "--numWorkerThreads=5",
        "--numberOfBuffersInGlobalBufferManager=2048",
        "--numberOfBuffersInSourceLocalBufferPool=128",
        "--queryCompilerCompilationStrategy=FAST",
        "--queryCompilerPipeliningStrategy=OPERATPR_AT_A_TIME",
        "--queryCompilerOutputBufferOptimizationLevel=ONLY_INPLACE_OPERATIONS_NO_FALLBACK",
    };
    int argc = 8;

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
}

TEST_F(ConfigTest, testSourceEmptyParamsConsoleInput) {

    std::string argv[] = {"--type=DefaultSource",
                          "--numberOfBuffersToProduce=5",
                          "--rowLayout=false",
                          "--physicalSourceName=x",
                          "--logicalSourceName=default"};
    int argc = 5;

    std::map<string, string> commandLineParams;

    for (int i = 0; i < argc; ++i) {
        commandLineParams.insert(
            std::pair<string, string>(string(argv[i]).substr(0, string(argv[i]).find('=')),
                                      string(argv[i]).substr(string(argv[i]).find('=') + 1, string(argv[i]).length() - 1)));
    }

    PhysicalSourcePtr physicalSource1 = PhysicalSourceFactory::createPhysicalSource(commandLineParams);
    EXPECT_EQ(physicalSource1->getLogicalSourceName(), "default");
    EXPECT_EQ(physicalSource1->getPhysicalSourceName(), "x");
    EXPECT_TRUE(physicalSource1->getPhysicalSourceType()->instanceOf<DefaultSourceType>());

    DefaultSourceTypePtr physicalSourceType1 = physicalSource1->getPhysicalSourceType()->as<DefaultSourceType>();
    EXPECT_EQ(physicalSourceType1->getSourceFrequency()->getValue(),
              physicalSourceType1->getSourceFrequency()->getDefaultValue());
    EXPECT_NE(physicalSourceType1->getNumberOfBuffersToProduce()->getValue(), 5u);

    std::string argv1[] = {"--type=KafkaSource",
                           "--physicalSourceName=x",
                           "--logicalSourceName=default",
                           "--topic=newTopic",
                           "--connectionTimeout=100",
                           "--brokers=testBroker",
                           "--groupId=testId"};

    argc = 7;

    std::map<string, string> commandLineParams1;

    for (int i = 0; i < argc; ++i) {
        commandLineParams1.insert(
            std::pair<string, string>(string(argv1[i]).substr(0, string(argv1[i]).find('=')),
                                      string(argv1[i]).substr(string(argv1[i]).find('=') + 1, string(argv1[i]).length() - 1)));
    }

    PhysicalSourcePtr physicalSource2 = PhysicalSourceFactory::createPhysicalSource(commandLineParams1);
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