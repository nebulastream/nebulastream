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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/KafkaSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/MQTTSourceType.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceFactory.hpp>
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

    CoordinatorConfigurationPtr coordinatorConfigPtr = CoordinatorConfiguration::create();
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

    CoordinatorConfigurationPtr coordinatorConfigPtr = CoordinatorConfiguration::create();
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

    WorkerConfigurationPtr workerConfigPtr = WorkerConfiguration::create();
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
    EXPECT_TRUE(workerConfigPtr->getPhysicalSources().empty());
}

TEST_F(ConfigTest, testWorkerYAMLFileWithMultiplePhysicalSource) {

    WorkerConfigurationPtr workerConfigPtr = WorkerConfiguration::create();
    workerConfigPtr->overwriteConfigWithYAMLFileInput(std::string(TEST_DATA_DIRECTORY) + "workerWithPhysicalSources.yaml");

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
    EXPECT_TRUE(!workerConfigPtr->getPhysicalSources().empty());
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

    auto filePath = std::string(TEST_DATA_DIRECTORY) + "emptySource.yaml";
    auto contents = Util::detail::file_get_contents<std::string>(filePath.c_str());
    ryml::Tree tree = ryml::parse(ryml::to_csubstr(contents));
    ryml::NodeRef root = tree.rootref();

    NES_INFO(tree.num_children(0));
    auto physicalStreams = tree[ryml::to_csubstr(PHYSICAL_STREAMS_CONFIG)];
    NES_INFO(physicalStreams.num_children());



    NES_INFO(physicalStreams.has_child(ryml::to_csubstr(LOGICAL_SOURCE_NAME_CONFIG)));
    NES_INFO(physicalStreams.find_child(ryml::to_csubstr(LOGICAL_SOURCE_NAME_CONFIG)).val().str);


    NES_INFO(root.is_map());
    NES_INFO(root.val().str);
//    NES_INFO(root[PHYSICAL_STREAMS_CONFIG]);


    auto physicalSourceConfigs = root.find_child(ryml::to_csubstr(PHYSICAL_STREAMS_CONFIG));
    NES_INFO(physicalSourceConfigs.is_map());
    NES_INFO(physicalSourceConfigs.is_doc());
    ASSERT_TRUE(physicalSourceConfigs.is_seq());

    NES_INFO(physicalSourceConfigs.val().str);

    std::vector<PhysicalSourcePtr> physicalSources = PhysicalSourceFactory::createPhysicalSources(physicalSourceConfigs);
    EXPECT_TRUE(physicalSources.size() == 1);
    EXPECT_TRUE(physicalSources[0]->getPhysicalSourceType()->instanceOf<DefaultSourceType>());
    DefaultSourceTypePtr physicalSourceType = physicalSources[0]->getPhysicalSourceType()->as<DefaultSourceType>();
    EXPECT_EQ(physicalSourceType->getSourceFrequency()->getValue(), physicalSourceType->getSourceFrequency()->getDefaultValue());
    EXPECT_EQ(physicalSourceType->getNumberOfBuffersToProduce()->getValue(),
              physicalSourceType->getNumberOfBuffersToProduce()->getDefaultValue());

    filePath = std::string(TEST_DATA_DIRECTORY) + "emptyMQTTSource.yaml";
    contents = Util::detail::file_get_contents<std::string>(filePath.c_str());
    tree = ryml::parse(ryml::to_csubstr(contents));
    root = tree.rootref();
    const c4::yml::NodeRef& physicalSourceConfigsMQTT = root.find_child(ryml::to_csubstr(PHYSICAL_STREAMS_CONFIG));

    physicalSources = PhysicalSourceFactory::createPhysicalSources(physicalSourceConfigsMQTT);
    EXPECT_TRUE(physicalSources[0]->getPhysicalSourceType()->instanceOf<DefaultSourceType>());
    DefaultSourceTypePtr physicalSourceType2 = physicalSources[0]->getPhysicalSourceType()->as<DefaultSourceType>();
    EXPECT_EQ(physicalSourceType2->getSourceFrequency()->getValue(),
              physicalSourceType2->getSourceFrequency()->getDefaultValue());
    EXPECT_EQ(physicalSourceType2->getNumberOfBuffersToProduce()->getValue(),
              physicalSourceType2->getNumberOfBuffersToProduce()->getDefaultValue());
    //    EXPECT_EQ(sourceConfigPtr1->getNumberOfTuplesToProducePerBuffer()->getValue(),
    //              sourceConfigPtr1->getNumberOfTuplesToProducePerBuffer()->getDefaultValue());
    //    EXPECT_EQ(sourceConfigPtr1->getPhysicalStreamName()->getValue(),
    //              sourceConfigPtr1->getPhysicalStreamName()->getDefaultValue());
    //    EXPECT_NE(sourceConfigPtr1->getLogicalStreamName()->getValue(), sourceConfigPtr1->getLogicalStreamName()->getDefaultValue());
    //
    //    EXPECT_NE(sourceConfigPtr1->as<MQTTSourceConfig>()->getUrl()->getValue(),
    //              sourceConfigPtr1->as<MQTTSourceConfig>()->getUrl()->getDefaultValue());
    //    EXPECT_NE(sourceConfigPtr1->as<MQTTSourceConfig>()->getClientId()->getValue(),
    //              sourceConfigPtr1->as<MQTTSourceConfig>()->getClientId()->getDefaultValue());
    //    EXPECT_NE(sourceConfigPtr1->as<MQTTSourceConfig>()->getTopic()->getValue(),
    //              sourceConfigPtr1->as<MQTTSourceConfig>()->getTopic()->getDefaultValue());
    //    EXPECT_EQ(sourceConfigPtr1->as<MQTTSourceConfig>()->getQos()->getValue(),
    //              sourceConfigPtr1->as<MQTTSourceConfig>()->getQos()->getDefaultValue());
    //    EXPECT_EQ(sourceConfigPtr1->as<MQTTSourceConfig>()->getCleanSession()->getValue(),
    //              sourceConfigPtr1->as<MQTTSourceConfig>()->getCleanSession()->getDefaultValue());
    //    EXPECT_NE(sourceConfigPtr1->as<MQTTSourceConfig>()->getUserName()->getValue(),
    //              sourceConfigPtr1->as<MQTTSourceConfig>()->getUserName()->getDefaultValue());
}

TEST_F(ConfigTest, testSourceEmptyParamsConsoleInput) {

    std::string argv[] = {"--sourceType=DefaultSource",
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

    std::string argv1[] = {"--sourceType=KafkaSource",
                           "--physicalSourceName=x",
                           "--logicalSourceName=default",
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

    PhysicalSourcePtr physicalSource2 = PhysicalSourceFactory::createPhysicalSource(commandLineParams);
    EXPECT_EQ(physicalSource2->getLogicalSourceName(), "default");
    EXPECT_EQ(physicalSource2->getPhysicalSourceName(), "x");
    EXPECT_TRUE(physicalSource2->getPhysicalSourceType()->instanceOf<KafkaSourceType>());

    KafkaSourceTypePtr physicalSourceType2 = physicalSource1->getPhysicalSourceType()->as<KafkaSourceType>();
    EXPECT_NE(physicalSourceType2->getBrokers()->getValue(), physicalSourceType2->getBrokers()->getDefaultValue());
    EXPECT_EQ(physicalSourceType2->getAutoCommit()->getValue(), physicalSourceType2->getAutoCommit()->getDefaultValue());
    EXPECT_NE(physicalSourceType2->getGroupId()->getValue(), physicalSourceType2->getGroupId()->getDefaultValue());
    EXPECT_NE(physicalSourceType2->getTopic()->getValue(), physicalSourceType2->getTopic()->getDefaultValue());
    EXPECT_NE(physicalSourceType2->getConnectionTimeout()->getValue(),
              physicalSourceType2->getConnectionTimeout()->getDefaultValue());
}

}// namespace NES