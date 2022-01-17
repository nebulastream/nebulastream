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

#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Sources/KafkaSourceConfig.hpp>
#include <Configurations/Sources/MQTTSourceConfig.hpp>
#include <Configurations/Sources/PhysicalStreamConfigFactory.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <filesystem>
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
 * @brief This test starts two workers and a coordinator and submit the same query but will output the results in different files
 */
TEST_F(ConfigTest, testEmptyParamsAndMissingParamsCoordinatorYAMLFile) {

    CoordinatorConfigPtr coordinatorConfigPtr = CoordinatorConfiguration::create();
    coordinatorConfigPtr->overwriteConfigWithYAMLFileInput(std::string(TEST_DATA_DIRECTORY) + "emptyCoordinator.yaml");

    EXPECT_EQ(coordinatorConfigPtr->getRestPort()->getValue(), coordinatorConfigPtr->getRestPort()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getRpcPort()->getValue(), coordinatorConfigPtr->getRpcPort()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getDataPort()->getValue(), coordinatorConfigPtr->getDataPort()->getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->getRestIp()->getValue(), coordinatorConfigPtr->getRestIp()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getCoordinatorIp()->getValue(), coordinatorConfigPtr->getCoordinatorIp()->getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->getNumberOfSlots()->getValue(), coordinatorConfigPtr->getNumberOfSlots()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getLogLevel()->getValue(), coordinatorConfigPtr->getLogLevel()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getNumberOfBuffersInGlobalBufferManager()->getValue(),
              coordinatorConfigPtr->getNumberOfBuffersInGlobalBufferManager()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getNumberOfBuffersPerWorker()->getValue(),
              coordinatorConfigPtr->getNumberOfBuffersPerWorker()->getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->getNumberOfBuffersInSourceLocalBufferPool()->getValue(),
              coordinatorConfigPtr->getNumberOfBuffersInSourceLocalBufferPool()->getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->getBufferSizeInBytes()->getValue(),
              coordinatorConfigPtr->getBufferSizeInBytes()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getNumWorkerThreads()->getValue(),
              coordinatorConfigPtr->getNumWorkerThreads()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getQueryBatchSize()->getValue(),
              coordinatorConfigPtr->getQueryBatchSize()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getQueryMergerRule()->getValue(),
              coordinatorConfigPtr->getQueryMergerRule()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getEnableSemanticQueryValidation()->getValue(),
              coordinatorConfigPtr->getEnableSemanticQueryValidation()->getDefaultValue());
}

TEST_F(ConfigTest, testCoordinatorEmptyParamsConsoleInput) {

    CoordinatorConfigPtr coordinatorConfigPtr = CoordinatorConfiguration::create();
    std::string argv[] = {"--restIp=localhost",
                          "--coordinatorIp=",
                          "--dataPort=",
                          "--numberOfSlots=10",
                          "--enableQueryMerging=",
                          "--numberOfBuffersInGlobalBufferManager=",
                          "--numberOfBuffersPerWorker=",
                          "--numberOfBuffersInSourceLocalBufferPool=128",
                          "--bufferSizeInBytes=1024"};
    int argc = 9;

    std::map<string, string> commandLineParams;

    for (int i = 0; i < argc; ++i) {
        commandLineParams.insert(
            std::pair<string, string>(string(argv[i]).substr(0, string(argv[i]).find('=')),
                                      string(argv[i]).substr(string(argv[i]).find('=') + 1, string(argv[i]).length() - 1)));
    }

    coordinatorConfigPtr->overwriteConfigWithCommandLineInput(commandLineParams);

    EXPECT_EQ(coordinatorConfigPtr->getRestPort()->getValue(), coordinatorConfigPtr->getRestPort()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getRpcPort()->getValue(), coordinatorConfigPtr->getRpcPort()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getDataPort()->getValue(), coordinatorConfigPtr->getDataPort()->getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->getRestIp()->getValue(), coordinatorConfigPtr->getRestIp()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getCoordinatorIp()->getValue(), coordinatorConfigPtr->getCoordinatorIp()->getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->getNumberOfSlots()->getValue(), coordinatorConfigPtr->getNumberOfSlots()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getLogLevel()->getValue(), coordinatorConfigPtr->getLogLevel()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getNumberOfBuffersInGlobalBufferManager()->getValue(),
              coordinatorConfigPtr->getNumberOfBuffersInGlobalBufferManager()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getNumberOfBuffersPerWorker()->getValue(),
              coordinatorConfigPtr->getNumberOfBuffersPerWorker()->getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->getNumberOfBuffersInSourceLocalBufferPool()->getValue(),
              coordinatorConfigPtr->getNumberOfBuffersInSourceLocalBufferPool()->getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->getBufferSizeInBytes()->getValue(),
              coordinatorConfigPtr->getBufferSizeInBytes()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getNumWorkerThreads()->getValue(),
              coordinatorConfigPtr->getNumWorkerThreads()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getQueryBatchSize()->getValue(),
              coordinatorConfigPtr->getQueryBatchSize()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getQueryMergerRule()->getValue(),
              coordinatorConfigPtr->getQueryMergerRule()->getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->getEnableSemanticQueryValidation()->getValue(),
              coordinatorConfigPtr->getEnableSemanticQueryValidation()->getDefaultValue());
}

TEST_F(ConfigTest, testEmptyParamsAndMissingParamsWorkerYAMLFile) {

    WorkerConfigPtr workerConfigPtr = WorkerConfiguration::create();
    workerConfigPtr->overwriteConfigWithYAMLFileInput(std::string(TEST_DATA_DIRECTORY) + "emptyWorker.yaml");

    EXPECT_NE(workerConfigPtr->getLocalWorkerIp()->getValue(), workerConfigPtr->getLocalWorkerIp()->getDefaultValue());
    EXPECT_EQ(workerConfigPtr->getRpcPort()->getValue(), workerConfigPtr->getRpcPort()->getDefaultValue());
    EXPECT_EQ(workerConfigPtr->getDataPort()->getValue(), workerConfigPtr->getDataPort()->getDefaultValue());
    EXPECT_NE(workerConfigPtr->getCoordinatorPort()->getValue(), workerConfigPtr->getCoordinatorPort()->getDefaultValue());
    EXPECT_EQ(workerConfigPtr->getCoordinatorIp()->getValue(), workerConfigPtr->getCoordinatorIp()->getDefaultValue());
    EXPECT_EQ(workerConfigPtr->getNumberOfSlots()->getValue(), workerConfigPtr->getNumberOfSlots()->getDefaultValue());
    EXPECT_EQ(workerConfigPtr->getLogLevel()->getValue(), workerConfigPtr->getLogLevel()->getDefaultValue());
    EXPECT_NE(workerConfigPtr->getNumberOfBuffersInGlobalBufferManager()->getValue(),
              workerConfigPtr->getNumberOfBuffersInGlobalBufferManager()->getDefaultValue());
    EXPECT_EQ(workerConfigPtr->getNumberOfBuffersPerWorker()->getValue(),
              workerConfigPtr->getNumberOfBuffersPerWorker()->getDefaultValue());
    EXPECT_NE(workerConfigPtr->getNumberOfBuffersInSourceLocalBufferPool()->getValue(),
              workerConfigPtr->getNumberOfBuffersInSourceLocalBufferPool()->getDefaultValue());
    EXPECT_EQ(workerConfigPtr->getBufferSizeInBytes()->getValue(), workerConfigPtr->getBufferSizeInBytes()->getDefaultValue());
    EXPECT_NE(workerConfigPtr->getNumWorkerThreads()->getValue(), workerConfigPtr->getNumWorkerThreads()->getDefaultValue());
}

TEST_F(ConfigTest, testWorkerEmptyParamsConsoleInput) {

    WorkerConfigurationPtr workerConfigPtr = WorkerConfiguration::create();
    std::string argv[] = {
        "--localWorkerIp=localhost",
        "--coordinatorIp=",
        "--coordinatorPort=5000",
        "--numberOfSlots=",
        "--numWorkerThreads=5",
        "--numberOfBuffersInGlobalBufferManager=2048",
        "--numberOfBuffersPerWorker=",
        "--numberOfBuffersInSourceLocalBufferPool=128",
        "--queryCompilerCompilationStrategy=FAST",
        "--queryCompilerPipeliningStrategy=OPERATPR_AT_A_TIME",
        "--queryCompilerOutputBufferOptimizationLevel=ONLY_INPLACE_OPERATIONS_NO_FALLBACK",

    };
    int argc = 11;

    std::map<string, string> commandLineParams;

    for (int i = 0; i < argc; ++i) {
        commandLineParams.insert(
            std::pair<string, string>(string(argv[i]).substr(0, string(argv[i]).find('=')),
                                      string(argv[i]).substr(string(argv[i]).find('=') + 1, string(argv[i]).length() - 1)));
    }

    workerConfigPtr->overwriteConfigWithCommandLineInput(commandLineParams);

    EXPECT_NE(workerConfigPtr->getLocalWorkerIp()->getValue(), workerConfigPtr->getLocalWorkerIp()->getDefaultValue());
    EXPECT_EQ(workerConfigPtr->getRpcPort()->getValue(), workerConfigPtr->getRpcPort()->getDefaultValue());
    EXPECT_EQ(workerConfigPtr->getDataPort()->getValue(), workerConfigPtr->getDataPort()->getDefaultValue());
    EXPECT_NE(workerConfigPtr->getCoordinatorPort()->getValue(), workerConfigPtr->getCoordinatorPort()->getDefaultValue());
    EXPECT_EQ(workerConfigPtr->getCoordinatorIp()->getValue(), workerConfigPtr->getCoordinatorIp()->getDefaultValue());
    EXPECT_EQ(workerConfigPtr->getNumberOfSlots()->getValue(), workerConfigPtr->getNumberOfSlots()->getDefaultValue());
    EXPECT_EQ(workerConfigPtr->getLogLevel()->getValue(), workerConfigPtr->getLogLevel()->getDefaultValue());
    EXPECT_NE(workerConfigPtr->getNumberOfBuffersInGlobalBufferManager()->getValue(),
              workerConfigPtr->getNumberOfBuffersInGlobalBufferManager()->getDefaultValue());
    EXPECT_EQ(workerConfigPtr->getNumberOfBuffersPerWorker()->getValue(),
              workerConfigPtr->getNumberOfBuffersPerWorker()->getDefaultValue());
    EXPECT_NE(workerConfigPtr->getNumberOfBuffersInSourceLocalBufferPool()->getValue(),
              workerConfigPtr->getNumberOfBuffersInSourceLocalBufferPool()->getDefaultValue());
    EXPECT_EQ(workerConfigPtr->getBufferSizeInBytes()->getValue(), workerConfigPtr->getBufferSizeInBytes()->getDefaultValue());
    EXPECT_NE(workerConfigPtr->getNumWorkerThreads()->getValue(), workerConfigPtr->getNumWorkerThreads()->getDefaultValue());
    EXPECT_NE(workerConfigPtr->getQueryCompilerCompilationStrategy()->getValue(),
              workerConfigPtr->getQueryCompilerCompilationStrategy()->getDefaultValue());
    EXPECT_NE(workerConfigPtr->getQueryCompilerPipeliningStrategy()->getValue(),
              workerConfigPtr->getQueryCompilerPipeliningStrategy()->getDefaultValue());
    EXPECT_NE(workerConfigPtr->getQueryCompilerOutputBufferAllocationStrategy()->getValue(),
              workerConfigPtr->getQueryCompilerOutputBufferAllocationStrategy()->getDefaultValue());
}

TEST_F(ConfigTest, testEmptyParamsAndMissingParamsSourceYAMLFile) {

    std::map<string, string> commandLineParams;

    commandLineParams.insert(std::pair<string, string>("--sourceConfigPath",  std::string(TEST_DATA_DIRECTORY) +"emptySource.yaml"));

    SourceConfigPtr sourceConfigPtr =
        PhysicalStreamConfigFactory::createSourceConfig(commandLineParams, commandLineParams.size());

    EXPECT_EQ(sourceConfigPtr->getSourceType()->getValue(), sourceConfigPtr->getSourceType()->getDefaultValue());
    EXPECT_EQ(sourceConfigPtr->getSourceFrequency()->getValue(), sourceConfigPtr->getSourceFrequency()->getDefaultValue());
    EXPECT_EQ(sourceConfigPtr->getNumberOfBuffersToProduce()->getValue(),
              sourceConfigPtr->getNumberOfBuffersToProduce()->getDefaultValue());
    EXPECT_EQ(sourceConfigPtr->getNumberOfTuplesToProducePerBuffer()->getValue(),
              sourceConfigPtr->getNumberOfTuplesToProducePerBuffer()->getDefaultValue());
    EXPECT_EQ(sourceConfigPtr->getPhysicalStreamName()->getValue(), sourceConfigPtr->getPhysicalStreamName()->getDefaultValue());
    EXPECT_NE(sourceConfigPtr->getLogicalStreamName()->getValue(), sourceConfigPtr->getLogicalStreamName()->getDefaultValue());

    commandLineParams.insert_or_assign("--sourceConfigPath", std::string(TEST_DATA_DIRECTORY) + "emptyMQTTSource.yaml");

    SourceConfigPtr sourceConfigPtr1 =
        PhysicalStreamConfigFactory::createSourceConfig(commandLineParams, commandLineParams.size());

    EXPECT_EQ(sourceConfigPtr1->getSourceType()->getValue(), sourceConfigPtr1->getSourceType()->getDefaultValue());
    EXPECT_EQ(sourceConfigPtr1->getSourceFrequency()->getValue(), sourceConfigPtr1->getSourceFrequency()->getDefaultValue());
    EXPECT_EQ(sourceConfigPtr1->getNumberOfBuffersToProduce()->getValue(),
              sourceConfigPtr1->getNumberOfBuffersToProduce()->getDefaultValue());
    EXPECT_EQ(sourceConfigPtr1->getNumberOfTuplesToProducePerBuffer()->getValue(),
              sourceConfigPtr1->getNumberOfTuplesToProducePerBuffer()->getDefaultValue());
    EXPECT_EQ(sourceConfigPtr1->getPhysicalStreamName()->getValue(),
              sourceConfigPtr1->getPhysicalStreamName()->getDefaultValue());
    EXPECT_NE(sourceConfigPtr1->getLogicalStreamName()->getValue(), sourceConfigPtr1->getLogicalStreamName()->getDefaultValue());

    EXPECT_NE(sourceConfigPtr1->as<MQTTSourceConfig>()->getUrl()->getValue(),
              sourceConfigPtr1->as<MQTTSourceConfig>()->getUrl()->getDefaultValue());
    EXPECT_NE(sourceConfigPtr1->as<MQTTSourceConfig>()->getClientId()->getValue(),
              sourceConfigPtr1->as<MQTTSourceConfig>()->getClientId()->getDefaultValue());
    EXPECT_NE(sourceConfigPtr1->as<MQTTSourceConfig>()->getTopic()->getValue(),
              sourceConfigPtr1->as<MQTTSourceConfig>()->getTopic()->getDefaultValue());
    EXPECT_EQ(sourceConfigPtr1->as<MQTTSourceConfig>()->getQos()->getValue(),
              sourceConfigPtr1->as<MQTTSourceConfig>()->getQos()->getDefaultValue());
    EXPECT_EQ(sourceConfigPtr1->as<MQTTSourceConfig>()->getCleanSession()->getValue(),
              sourceConfigPtr1->as<MQTTSourceConfig>()->getCleanSession()->getDefaultValue());
    EXPECT_NE(sourceConfigPtr1->as<MQTTSourceConfig>()->getUserName()->getValue(),
              sourceConfigPtr1->as<MQTTSourceConfig>()->getUserName()->getDefaultValue());
}

TEST_F(ConfigTest, testSourceEmptyParamsConsoleInput) {

    std::string argv[] = {"--sourceType=NoSource",
                          "--numberOfBuffersToProduce=5",
                          "--rowLayout=false",
                          "--physicalStreamName=",
                          "--logicalStreamName=default"};
    int argc = 5;

    std::map<string, string> commandLineParams;

    for (int i = 0; i < argc; ++i) {
        commandLineParams.insert(
            std::pair<string, string>(string(argv[i]).substr(0, string(argv[i]).find('=')),
                                      string(argv[i]).substr(string(argv[i]).find('=') + 1, string(argv[i]).length() - 1)));
    }

    SourceConfigPtr sourceConfigPtr =
        PhysicalStreamConfigFactory::createSourceConfig(commandLineParams, commandLineParams.size());

    EXPECT_NE(sourceConfigPtr->getSourceType()->getValue(), sourceConfigPtr->getSourceType()->getDefaultValue());
    EXPECT_EQ(sourceConfigPtr->getSourceFrequency()->getValue(), sourceConfigPtr->getSourceFrequency()->getDefaultValue());
    EXPECT_NE(sourceConfigPtr->getNumberOfBuffersToProduce()->getValue(),
              sourceConfigPtr->getNumberOfBuffersToProduce()->getDefaultValue());
    EXPECT_EQ(sourceConfigPtr->getNumberOfTuplesToProducePerBuffer()->getValue(),
              sourceConfigPtr->getNumberOfTuplesToProducePerBuffer()->getDefaultValue());
    EXPECT_EQ(sourceConfigPtr->getPhysicalStreamName()->getValue(), sourceConfigPtr->getPhysicalStreamName()->getDefaultValue());
    EXPECT_NE(sourceConfigPtr->getLogicalStreamName()->getValue(), sourceConfigPtr->getLogicalStreamName()->getDefaultValue());

    std::string argv1[] = {"--sourceType=KafkaSource",
                           "--physicalStreamName=",
                           "--logicalStreamName=default",
                           "--KafkaSourceTopic=newTopic",
                           "--KafkaSourceConnectionTimeout=100",
                           "--KafkaSourceBrokers=testBroker",
                           "--KafkaSourceGroupId=testId"};

    argc = 7;

    std::map<string, string> commandLineParams1;

    for (int i = 0; i < argc; ++i) {
        commandLineParams1.insert(
            std::pair<string, string>(string(argv1[i]).substr(0, string(argv1[i]).find('=')),
                                      string(argv1[i]).substr(string(argv1[i]).find('=') + 1, string(argv1[i]).length() - 1)));
    }

    SourceConfigPtr sourceConfigPtr1 =
        PhysicalStreamConfigFactory::createSourceConfig(commandLineParams1, commandLineParams1.size());

    EXPECT_EQ(sourceConfigPtr1->as<KafkaSourceConfig>()->getSourceType()->getValue(),
              sourceConfigPtr1->as<KafkaSourceConfig>()->getSourceType()->getDefaultValue());
    EXPECT_EQ(sourceConfigPtr1->as<KafkaSourceConfig>()->getSourceFrequency()->getValue(),
              sourceConfigPtr1->as<KafkaSourceConfig>()->getSourceFrequency()->getDefaultValue());
    EXPECT_EQ(sourceConfigPtr1->as<KafkaSourceConfig>()->getNumberOfBuffersToProduce()->getValue(),
              sourceConfigPtr1->as<KafkaSourceConfig>()->getNumberOfBuffersToProduce()->getDefaultValue());
    EXPECT_EQ(sourceConfigPtr1->as<KafkaSourceConfig>()->getNumberOfTuplesToProducePerBuffer()->getValue(),
              sourceConfigPtr1->as<KafkaSourceConfig>()->getNumberOfTuplesToProducePerBuffer()->getDefaultValue());
    EXPECT_EQ(sourceConfigPtr1->as<KafkaSourceConfig>()->getPhysicalStreamName()->getValue(),
              sourceConfigPtr1->as<KafkaSourceConfig>()->getPhysicalStreamName()->getDefaultValue());
    EXPECT_NE(sourceConfigPtr1->as<KafkaSourceConfig>()->getLogicalStreamName()->getValue(),
              sourceConfigPtr1->as<KafkaSourceConfig>()->getLogicalStreamName()->getDefaultValue());
    EXPECT_NE(sourceConfigPtr1->as<KafkaSourceConfig>()->getBrokers()->getValue(),
              sourceConfigPtr1->as<KafkaSourceConfig>()->getBrokers()->getDefaultValue());
    EXPECT_EQ(sourceConfigPtr1->as<KafkaSourceConfig>()->getAutoCommit()->getValue(),
              sourceConfigPtr1->as<KafkaSourceConfig>()->getAutoCommit()->getDefaultValue());
    EXPECT_NE(sourceConfigPtr1->as<KafkaSourceConfig>()->getGroupId()->getValue(),
              sourceConfigPtr1->as<KafkaSourceConfig>()->getGroupId()->getDefaultValue());
    EXPECT_NE(sourceConfigPtr1->as<KafkaSourceConfig>()->getTopic()->getValue(),
              sourceConfigPtr1->as<KafkaSourceConfig>()->getTopic()->getDefaultValue());
    EXPECT_NE(sourceConfigPtr1->as<KafkaSourceConfig>()->getConnectionTimeout()->getValue(),
              sourceConfigPtr1->as<KafkaSourceConfig>()->getConnectionTimeout()->getDefaultValue());
}

}// namespace NES