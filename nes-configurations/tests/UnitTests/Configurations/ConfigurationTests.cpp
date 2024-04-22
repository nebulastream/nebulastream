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

#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Coordinator/LogicalSourceType.hpp>
#include <Configurations/Coordinator/SchemaType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/KafkaSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/MQTTSourceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/GeoLocation.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <vector>

namespace NES {

using namespace Configurations;

class ConfigTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("Config.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup Configuration test class.");
    }

    static std::vector<const char*> makePosixArgs(const std::vector<std::string>& args) {
        static const char* empty = "";
        std::vector<const char*> argv(args.size() + 1);
        argv[0] = empty;
        std::transform(args.begin(), args.end(), argv.begin() + 1, [](const std::string& arg) {
            return arg.c_str();
        });
        return argv;
    }

    static std::map<std::string, std::string> makeCommandLineArgs(const std::vector<std::string>& args) {
        std::map<std::string, std::string> result;
        for (auto arg : args) {
            auto pos = arg.find('=');
            result.insert({arg.substr(0, pos), arg.substr(pos + 1, arg.length() - 1)});
        }
        return result;
    }

    static void TearDownTestCase() { NES_INFO("Tear down Configuration test class."); }
};

/**
* @brief This reads an coordinator yaml and checks the configuration
*/
TEST_F(ConfigTest, testEmptyParamsAndMissingParamsCoordinatorYAMLFile) {

    CoordinatorConfigurationPtr coordinatorConfigPtr = std::make_shared<CoordinatorConfiguration>();
    coordinatorConfigPtr->overwriteConfigWithYAMLFileInput(std::filesystem::path(TEST_DATA_DIRECTORY) / "emptyCoordinator.yaml");
    EXPECT_EQ(coordinatorConfigPtr->restPort.getValue(), coordinatorConfigPtr->restPort.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->rpcPort.getValue(), coordinatorConfigPtr->rpcPort.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->restIp.getValue(), coordinatorConfigPtr->restIp.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->coordinatorIp.getValue(), coordinatorConfigPtr->coordinatorIp.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->worker.numberOfSlots.getValue(),
              coordinatorConfigPtr->worker.numberOfSlots.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->logLevel.getValue(), coordinatorConfigPtr->logLevel.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->worker.numberOfBuffersInGlobalBufferManager.getValue(),
              coordinatorConfigPtr->worker.numberOfBuffersInGlobalBufferManager.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->worker.numberOfBuffersPerWorker.getValue(),
              coordinatorConfigPtr->worker.numberOfBuffersPerWorker.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->worker.numberOfBuffersInSourceLocalBufferPool.getValue(),
              coordinatorConfigPtr->worker.numberOfBuffersInSourceLocalBufferPool.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->worker.bufferSizeInBytes.getValue(),
              coordinatorConfigPtr->worker.bufferSizeInBytes.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->worker.numWorkerThreads.getValue(),
              coordinatorConfigPtr->worker.numWorkerThreads.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->optimizer.queryMergerRule.getValue(),
              coordinatorConfigPtr->optimizer.queryMergerRule.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->worker.numberOfBuffersPerEpoch.getValue(),
              coordinatorConfigPtr->worker.numberOfBuffersPerEpoch.getDefaultValue());
}

TEST_F(ConfigTest, testLogicalSourceAndSchemaParamsCoordinatorYAMLFile) {
    CoordinatorConfigurationPtr coordinatorConfigPtr = std::make_shared<CoordinatorConfiguration>();
    coordinatorConfigPtr->overwriteConfigWithYAMLFileInput(std::string(TEST_DATA_DIRECTORY)
                                                           + "coordinatorLogicalSourceAndSchema.yaml");
    EXPECT_FALSE(coordinatorConfigPtr->logicalSourceTypes.empty());
    EXPECT_EQ(coordinatorConfigPtr->logicalSourceTypes.size(), 3);
    auto logicalSources = coordinatorConfigPtr->logicalSourceTypes.getValues();
    EXPECT_EQ(logicalSources[0].getValue()->getLogicalSourceName(), "lsn1");
    EXPECT_EQ(logicalSources[1].getValue()->getLogicalSourceName(), "lsn2");
    EXPECT_EQ(logicalSources[2].getValue()->getLogicalSourceName(), "lsn3");
    auto firstSourceSchema = logicalSources[0].getValue()->getSchemaType();
    auto secondSourceSchema = logicalSources[1].getValue()->getSchemaType();
    auto thirdSourceSchema = logicalSources[2].getValue()->getSchemaType();
    EXPECT_EQ(firstSourceSchema->getSchemaFieldDetails().size(), 3);
    EXPECT_EQ(secondSourceSchema->getSchemaFieldDetails().size(), 1);
    EXPECT_TRUE(firstSourceSchema->contains("csv_id"));
    EXPECT_TRUE(firstSourceSchema->contains("csv_id_2"));
    EXPECT_TRUE(firstSourceSchema->contains("csv_id_3"));
    EXPECT_TRUE(secondSourceSchema->contains("csv_id_4"));
    EXPECT_TRUE(thirdSourceSchema->contains("csv_id_5"));
}

TEST_F(ConfigTest, testCoordinatorEPERATPRmptyParamsConsoleInput) {
    // given
    CoordinatorConfigurationPtr coordinatorConfigPtr = std::make_shared<CoordinatorConfiguration>();
    auto commandLineParams = makeCommandLineArgs({"--restIp=localhost",
                                                  "--worker.numberOfSlots=10",
                                                  "--worker.numberOfBuffersInSourceLocalBufferPool=128",
                                                  "--worker.bufferSizeInBytes=1024"});
    // when
    coordinatorConfigPtr->overwriteConfigWithCommandLineInput(commandLineParams);
    // then
    EXPECT_EQ(coordinatorConfigPtr->restPort.getValue(), coordinatorConfigPtr->restPort.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->rpcPort.getValue(), coordinatorConfigPtr->rpcPort.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->restIp.getValue(), coordinatorConfigPtr->restIp.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->coordinatorIp.getValue(), coordinatorConfigPtr->coordinatorIp.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->worker.numberOfSlots.getValue(),
              coordinatorConfigPtr->worker.numberOfSlots.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->logLevel.getValue(), coordinatorConfigPtr->logLevel.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->worker.numberOfBuffersInGlobalBufferManager.getValue(),
              coordinatorConfigPtr->worker.numberOfBuffersInGlobalBufferManager.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->worker.numberOfBuffersPerWorker.getValue(),
              coordinatorConfigPtr->worker.numberOfBuffersPerWorker.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->worker.numberOfBuffersInSourceLocalBufferPool.getValue(),
              coordinatorConfigPtr->worker.numberOfBuffersInSourceLocalBufferPool.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->worker.bufferSizeInBytes.getValue(),
              coordinatorConfigPtr->worker.bufferSizeInBytes.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->worker.numWorkerThreads.getValue(),
              coordinatorConfigPtr->worker.numWorkerThreads.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->optimizer.queryMergerRule.getValue(),
              coordinatorConfigPtr->optimizer.queryMergerRule.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->worker.numWorkerThreads.getValue(),
              coordinatorConfigPtr->worker.numWorkerThreads.getDefaultValue());
}

TEST_F(ConfigTest, testEmptyParamsAndMissingParamsWorkerYAMLFile) {

    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    workerConfigPtr->overwriteConfigWithYAMLFileInput(std::filesystem::path(TEST_DATA_DIRECTORY) / "emptyWorker.yaml");

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
    EXPECT_EQ(workerConfigPtr->numberOfBuffersPerEpoch.getValue(), workerConfigPtr->numberOfBuffersPerEpoch.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numWorkerThreads.getValue(), workerConfigPtr->numWorkerThreads.getDefaultValue());
    EXPECT_TRUE(workerConfigPtr->physicalSourceTypes.empty());
}

TEST_F(ConfigTest, testWorkerYAMLFileWithMultiplePhysicalSource) {

    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    workerConfigPtr->overwriteConfigWithYAMLFileInput(std::filesystem::path(TEST_DATA_DIRECTORY)
                                                      / "workerWithPhysicalSources.yaml");

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
    EXPECT_TRUE(!workerConfigPtr->physicalSourceTypes.empty());
    EXPECT_TRUE(workerConfigPtr->physicalSourceTypes.size() == 2);
    for (const auto& physicalSource : workerConfigPtr->physicalSourceTypes.getValues()) {
        EXPECT_TRUE(physicalSource.getValue()->instanceOf<DefaultSourceType>()
                    || physicalSource.getValue()->instanceOf<MQTTSourceType>());
    }
    EXPECT_EQ(workerConfigPtr->locationCoordinates.getValue(), workerConfigPtr->locationCoordinates.getDefaultValue());
}

TEST_F(ConfigTest, testWorkerYAMLFileFixedLocationNode) {
    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    auto yamlPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "fixedLocationNode.yaml";
    std::ofstream outFile(yamlPath);
    outFile << "fieldNodeLocationCoordinates: \"23.88,-3.4\"" << std::endl << "nodeSpatialType: FIXED_LOCATION" << std::endl;
    workerConfigPtr->overwriteConfigWithYAMLFileInput(yamlPath);

    EXPECT_EQ(workerConfigPtr->locationCoordinates.getValue(), NES::Spatial::DataTypes::Experimental::GeoLocation(23.88, -3.4));
    EXPECT_EQ(workerConfigPtr->nodeSpatialType.getValue(), NES::Spatial::Experimental::SpatialType::FIXED_LOCATION);
}

TEST_F(ConfigTest, testWorkerEmptyParamsConsoleInput) {
    // given
    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    auto commandLineParams =
        makeCommandLineArgs({"--localWorkerIp=localhost",
                             "--coordinatorPort=5000",
                             "--numWorkerThreads=5",
                             "--numberOfBuffersInGlobalBufferManager=2048",
                             "--numberOfBuffersInSourceLocalBufferPool=128",
                             "--queryCompiler.compilationStrategy=FAST",
                             "--queryCompiler.pipeliningStrategy=OPERATOR_AT_A_TIME",
                             "--queryCompiler.outputBufferOptimizationLevel=ONLY_INPLACE_OPERATIONS_NO_FALLBACK",
                             "--physicalSources.type=DEFAULT_SOURCE",
                             "--physicalSources.numberOfBuffersToProduce=5",
                             "--physicalSources.rowLayout=false",
                             "--physicalSources.physicalSourceName=x",
                             "--physicalSources.logicalSourceName=default",
                             "--fieldNodeLocationCoordinates=23.88,-3.4"});
    // when
    workerConfigPtr->overwriteConfigWithCommandLineInput(commandLineParams);
    // then
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
    // given
    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    auto commandLineParams =
        makeCommandLineArgs({"--localWorkerIp=localhost",
                             "--coordinatorPort=5000",
                             "--numWorkerThreads=5",
                             "--numberOfBuffersInGlobalBufferManager=2048",
                             "--numberOfBuffersInSourceLocalBufferPool=128",
                             "--queryCompiler.compilationStrategy=FAST",
                             "--queryCompiler.pipeliningStrategy=OPERATOR_AT_A_TIME",
                             "--queryCompiler.outputBufferOptimizationLevel=ONLY_INPLACE_OPERATIONS_NO_FALLBACK",
                             "--physicalSources.type=CSV_SOURCE",
                             "--physicalSources.filePath=fileLoc",
                             "--physicalSources.rowLayout=false",
                             "--physicalSources.physicalSourceName=x",
                             "--physicalSources.logicalSourceName=default"});
    // when
    workerConfigPtr->overwriteConfigWithCommandLineInput(commandLineParams);
    // then
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
    auto commandLineParams = makeCommandLineArgs({"type=DEFAULT_SOURCE",
                                                  "numberOfBuffersToProduce=5",
                                                  "rowLayout=false",
                                                  "physicalSourceName=x",
                                                  "logicalSourceName=default",
                                                  "offsetMode=earliest"});

    PhysicalSourceTypePtr physicalSourceType1 = PhysicalSourceTypeFactory::createFromString("", commandLineParams);
    auto physicalSource1 = PhysicalSource::create(physicalSourceType1);
    EXPECT_EQ(physicalSource1->getLogicalSourceName(), "default");
    EXPECT_EQ(physicalSource1->getPhysicalSourceName(), "x");
    EXPECT_TRUE(physicalSource1->getPhysicalSourceType()->instanceOf<DefaultSourceType>());

    EXPECT_EQ(physicalSourceType1->as<DefaultSourceType>()->getSourceGatheringInterval()->getValue(),
              physicalSourceType1->as<DefaultSourceType>()->getSourceGatheringInterval()->getDefaultValue());
    EXPECT_NE(physicalSourceType1->as<DefaultSourceType>()->getNumberOfBuffersToProduce()->getValue(), 5u);

    auto commandLineParams1 = makeCommandLineArgs({"type=KAFKA_SOURCE",
                                                   "physicalSourceName=x",
                                                   "logicalSourceName=default",
                                                   "topic=newTopic",
                                                   "connectionTimeout=100",
                                                   "brokers=testBroker",
                                                   "groupId=testId",
                                                   "offsetMode=earliest"});

    PhysicalSourceTypePtr physicalSourceType2 = PhysicalSourceTypeFactory::createFromString("", commandLineParams1);
    auto physicalSource2 = PhysicalSource::create(physicalSourceType2);
    EXPECT_EQ(physicalSource2->getLogicalSourceName(), "default");
    EXPECT_EQ(physicalSource2->getPhysicalSourceName(), "x");
    EXPECT_TRUE(physicalSource2->getPhysicalSourceType()->instanceOf<KafkaSourceType>());

    EXPECT_NE(physicalSourceType2->as<KafkaSourceType>()->getBrokers()->getValue(),
              physicalSourceType2->as<KafkaSourceType>()->getBrokers()->getDefaultValue());
    EXPECT_EQ(physicalSourceType2->as<KafkaSourceType>()->getAutoCommit()->getValue(),
              physicalSourceType2->as<KafkaSourceType>()->getAutoCommit()->getDefaultValue());
    EXPECT_NE(physicalSourceType2->as<KafkaSourceType>()->getGroupId()->getValue(),
              physicalSourceType2->as<KafkaSourceType>()->getGroupId()->getDefaultValue());
    EXPECT_NE(physicalSourceType2->as<KafkaSourceType>()->getTopic()->getValue(),
              physicalSourceType2->as<KafkaSourceType>()->getTopic()->getDefaultValue());
    EXPECT_NE(physicalSourceType2->as<KafkaSourceType>()->getConnectionTimeout()->getValue(),
              physicalSourceType2->as<KafkaSourceType>()->getConnectionTimeout()->getDefaultValue());
}

TEST_F(ConfigTest, testPhysicalSourceAndGatheringModeWorkerConsoleInput) {
    // given
    auto commandLineParams = makeCommandLineArgs({"type=DEFAULT_SOURCE",
                                                  "numberOfBuffersToProduce=5",
                                                  "rowLayout=false",
                                                  "physicalSourceName=x",
                                                  "logicalSourceName=default"});
    // when
    auto physicalSourceType1 = PhysicalSourceTypeFactory::createFromString("", commandLineParams);
    // then
    EXPECT_EQ(physicalSourceType1->as<DefaultSourceType>()->getGatheringMode()->getValue(),
              physicalSourceType1->as<DefaultSourceType>()->getGatheringMode()->getDefaultValue());
    EXPECT_EQ(physicalSourceType1->as<DefaultSourceType>()->getGatheringMode()->getValue(), GatheringMode::INTERVAL_MODE);
}

TEST_F(ConfigTest, testCSVPhysicalSourceAndDefaultGatheringModeWorkerConsoleInput) {
    // given
    auto commandLineParams = makeCommandLineArgs({"type=CSV_SOURCE",
                                                  "numberOfBuffersToProduce=5",
                                                  "rowLayout=false",
                                                  "physicalSourceName=x",
                                                  "logicalSourceName=default",
                                                  "filePath=fileLoc"});
    // when
    auto physicalSourceType = PhysicalSourceTypeFactory::createFromString("", commandLineParams);
    // then
    EXPECT_EQ(physicalSourceType->as<CSVSourceType>()->getGatheringMode()->getValue(),
              physicalSourceType->as<CSVSourceType>()->getGatheringMode()->getDefaultValue());
    EXPECT_EQ(physicalSourceType->as<CSVSourceType>()->getGatheringMode()->getValue(), GatheringMode::INTERVAL_MODE);
}

TEST_F(ConfigTest, testCSVPhysicalSourceAndAdaptiveGatheringModeWorkerConsoleInput) {
    // given
    auto commandLineParams = makeCommandLineArgs({"type=CSV_SOURCE",
                                                  "numberOfBuffersToProduce=5",
                                                  "rowLayout=false",
                                                  "physicalSourceName=x",
                                                  "logicalSourceName=default",
                                                  "filePath=fileLoc",
                                                  "sourceGatheringMode=ADAPTIVE_MODE"});
    // when
    auto physicalSourceType = PhysicalSourceTypeFactory::createFromString("", commandLineParams);
    // then
    EXPECT_NE(physicalSourceType->as<CSVSourceType>()->getGatheringMode()->getValue(),
              physicalSourceType->as<CSVSourceType>()->getGatheringMode()->getDefaultValue());
    EXPECT_EQ(physicalSourceType->as<CSVSourceType>()->getGatheringMode()->getValue(), GatheringMode::ADAPTIVE_MODE);
}

TEST_F(ConfigTest, testWorkerYAMLFileWithCSVPhysicalSourceAdaptiveGatheringMode) {

    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    workerConfigPtr->overwriteConfigWithYAMLFileInput(std::filesystem::path(TEST_DATA_DIRECTORY)
                                                      / "workerWithAdaptiveCSVSource.yaml");
    EXPECT_TRUE(workerConfigPtr->physicalSourceTypes.size() == 1);

    auto csvSourceType = workerConfigPtr->physicalSourceTypes.getValues()[0].getValue()->as<CSVSourceType>();
    EXPECT_NE(csvSourceType->getGatheringMode()->getValue(), csvSourceType->getGatheringMode()->getDefaultValue());
    EXPECT_EQ(csvSourceType->getGatheringMode()->getValue(), magic_enum::enum_cast<GatheringMode>("ADAPTIVE_MODE").value());
}

TEST_F(ConfigTest, invalidCommandLineInputForBoolOptions) {
    std::vector<std::pair<std::string, std::vector<std::string>>> commandLineArgs = {
        {"--numaAwareness", {"not_a_bool", "2", "-1"}},
        {"--enableMonitoring", {"not_a_bool", "2", "-1"}},
        {"--enableSourceSharing", {"not_a_bool", "2", "-1"}},
        {"--enableStatisticOuput", {"not_a_bool", "2", "-1"}},
        {"--isJavaUDFSupported", {"not_a_bool", "2", "-1"}},
        {"--connectSinksAsync", {"not_a_bool", "2", "-1"}},
        {"--connectSourceEventChannelsAsync", {"not_a_bool", "2", "-1"}}};

    for (const auto& optionPair : commandLineArgs) {
        for (const auto& value : optionPair.second) {
            EXPECT_ANY_THROW({
                WorkerConfigurationPtr config = std::make_shared<WorkerConfiguration>();
                std::vector<std::string> args = {optionPair.first + "=" + value};
                config->overwriteConfigWithCommandLineInput(makeCommandLineArgs(args));
            });
        }
    }
}

TEST_F(ConfigTest, invalidCommandLineInputForIntOptions) {
    std::vector<std::pair<std::string, std::vector<std::string>>> commandLineArgs = {
        {"--workerId", {"not_an_int", "1.5", "-1"}},
        {"--rpcPort", {"not_an_int", "1.5", "-1"}},
        {"--dataPort", {"not_an_int", "1.5", "-1"}},
        {"--coordinatorPort", {"not_an_int", "1.5", "-1"}},
        {"--numWorkerThreads", {"not_an_int", "1.5", "-1"}},
        {"--numberOfBuffersInGlobalBufferManager", {"not_an_int", "1.5", "-1"}},
        {"--numberOfBuffersPerWorker", {"not_an_int", "1.5", "-1"}},
        {"--numberOfBuffersInSourceLocalBufferPool", {"not_an_int", "1.5", "-1"}},
        {"--bufferSizeInBytes", {"not_an_int", "1.5", "-1"}},
        {"--numberOfQueues", {"not_an_int", "1.5", "-1"}}};

    for (const auto& optionPair : commandLineArgs) {
        for (const auto& value : optionPair.second) {
            EXPECT_ANY_THROW({
                WorkerConfigurationPtr config = std::make_shared<WorkerConfiguration>();
                std::vector<std::string> args = {optionPair.first + "=" + value};
                config->overwriteConfigWithCommandLineInput(makeCommandLineArgs(args));
            });
        }
    }
}

TEST_F(ConfigTest, invalidCommandLineInputForFloatOptions) {
    std::vector<std::pair<std::string, std::vector<std::string>>> commandLineArgs = {
        {"--transferRate", {"not_a_float", "2..5", "-1.0"}}};

    for (const auto& optionPair : commandLineArgs) {
        for (const auto& value : optionPair.second) {
            EXPECT_ANY_THROW({
                auto config = std::make_shared<ElegantConfigurations>();
                std::vector<std::string> args = {optionPair.first + "=" + value};
                config->overwriteConfigWithCommandLineInput(makeCommandLineArgs(args));
            });
        }
    }
}

TEST_F(ConfigTest, invalidCommandLineInputForIpOptions) {
    std::vector<std::pair<std::string, std::vector<std::string>>> commandLineArgs = {
        {"--localWorkerIp", {"300.300.300.300", "192.168.1", "localhost", "127.0.0.-1", "192.168..1"}},
        {"--coordinatorIp", {"300.300.300.300", "192.168.1", "localhost", "127.0.0.-1", "192.168..1"}},
    };

    for (const auto& optionPair : commandLineArgs) {
        for (const auto& value : optionPair.second) {
            EXPECT_ANY_THROW({
                WorkerConfigurationPtr config = std::make_shared<WorkerConfiguration>();
                std::vector<std::string> args = {optionPair.first + "=" + value};
                config->overwriteConfigWithCommandLineInput(makeCommandLineArgs(args));
            });
        }
    }
}

TEST_F(ConfigTest, invalidBooleanYamlInputs) {
    std::vector<std::pair<std::string, std::vector<std::string>>> invalidBooleanConfigs = {
        {"numaAwareness", {"not_a_bool", "2", "-1"}},
        {"enableMonitoring", {"not_a_bool", "2", "-1"}},
        {"enableSourceSharing", {"not_a_bool", "2", "-1"}},
        {"enableStatisticOutput", {"not_a_bool", "2", "-1"}},
        {"isJavaUDFSupported", {"not_a_bool", "2", "-1"}},
        {"connectSinksAsync", {"not_a_bool", "2", "-1"}},
        {"connectSourceEventChannelsAsync", {"not_a_bool", "2", "-1"}}};

    for (const auto& [optionName, invalidValues] : invalidBooleanConfigs) {
        for (const auto& value : invalidValues) {
            std::string fileName = optionName + "_" + value + ".yaml";
            auto filePath = std::filesystem::path(TEST_DATA_DIRECTORY) / fileName;

            std::ofstream file(filePath);
            ASSERT_TRUE(file.is_open());
            file << optionName << ": " << value << std::endl;
            file.close();

            EXPECT_ANY_THROW({
                WorkerConfigurationPtr config = std::make_shared<WorkerConfiguration>();
                config->overwriteConfigWithYAMLFileInput(filePath);
            });

            std::filesystem::remove(filePath);
        }
    }
}

TEST_F(ConfigTest, invalidIntYamlInputs) {
    std::vector<std::pair<std::string, std::vector<std::string>>> invalidIntConfigs = {
        {"--workerId", {"not_an_int", "1.5", "-1"}},
        {"--rpcPort", {"not_an_int", "1.5", "-1"}},
        {"--dataPort", {"not_an_int", "1.5", "-1"}},
        {"--coordinatorPort", {"not_an_int", "1.5", "-1"}},
        {"--numWorkerThreads", {"not_an_int", "1.5", "-1"}},
        {"--numberOfBuffersInGlobalBufferManager", {"not_an_int", "1.5", "-1"}},
        {"--numberOfBuffersPerWorker", {"not_an_int", "1.5", "-1"}},
        {"--numberOfBuffersInSourceLocalBufferPool", {"not_an_int", "1.5", "-1"}},
        {"--bufferSizeInBytes", {"not_an_int", "1.5", "-1"}},
        {"--numberOfQueues", {"not_an_int", "1.5", "-1"}}};

    for (const auto& [optionName, invalidValues] : invalidIntConfigs) {
        for (const auto& value : invalidValues) {
            std::string fileName = optionName + "_" + value + ".yaml";
            auto filePath = std::filesystem::path(TEST_DATA_DIRECTORY) / fileName;

            std::ofstream file(filePath);
            ASSERT_TRUE(file.is_open());
            file << optionName << ": " << value << std::endl;
            file.close();

            EXPECT_ANY_THROW({
                WorkerConfigurationPtr config = std::make_shared<WorkerConfiguration>();
                config->overwriteConfigWithYAMLFileInput(filePath);
            });

            std::filesystem::remove(filePath);
        }
    }
}

TEST_F(ConfigTest, invalidFloatYamlInputs) {
    std::vector<std::pair<std::string, std::vector<std::string>>> invalidFloatConfigs = {
        {"--transferRate", {"not_a_float", "2..5", "-1.0"}}};

    for (const auto& [optionName, invalidValues] : invalidFloatConfigs) {
        for (const auto& value : invalidValues) {
            std::string fileName = optionName + "_" + value + ".yaml";
            auto filePath = std::filesystem::path(TEST_DATA_DIRECTORY) / fileName;

            std::ofstream file(filePath);
            ASSERT_TRUE(file.is_open());
            file << optionName << ": " << value << std::endl;
            file.close();

            EXPECT_ANY_THROW({
                auto config = std::make_shared<ElegantConfigurations>();
                config->overwriteConfigWithYAMLFileInput(filePath);
            });

            std::filesystem::remove(filePath);
        }
    }
}

TEST_F(ConfigTest, invalidIpYamlInputs) {
    std::vector<std::pair<std::string, std::vector<std::string>>> invalidIpConfigs = {
        {"--localWorkerIp", {"300.300.300.300", "192.168.1", "127.0.0.-1", "192.168..1"}},
        {"--coordinatorIp", {"300.300.300.300", "192.168.1", "127.0.0.-1", "192.168..1"}},
    };

    for (const auto& [optionName, invalidValues] : invalidIpConfigs) {
        for (const auto& value : invalidValues) {
            std::string fileName = optionName + "_" + value + ".yaml";
            auto filePath = std::filesystem::path(TEST_DATA_DIRECTORY) / fileName;

            std::ofstream file(filePath);
            ASSERT_TRUE(file.is_open());
            file << optionName << ": " << value << std::endl;
            file.close();

            EXPECT_ANY_THROW({
                WorkerConfigurationPtr config = std::make_shared<WorkerConfiguration>();
                config->overwriteConfigWithYAMLFileInput(filePath);
            });

            std::filesystem::remove(filePath);
        }
    }
}
}// namespace NES