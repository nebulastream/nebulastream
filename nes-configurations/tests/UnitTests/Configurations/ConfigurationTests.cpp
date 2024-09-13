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

#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Coordinator/LogicalSourceType.hpp>
#include <Configurations/Coordinator/SchemaType.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>

namespace NES
{

using namespace Configurations;

class ConfigTest : public Testing::BaseIntegrationTest
{
public:
    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("Config.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup Configuration test class.");
    }

    static std::vector<std::string> makePosixArgs(const std::vector<std::string>& args)
    {
        static const auto empty = "";
        std::vector<std::string> argv(args.size() + 1);
        argv[0] = empty;
        std::ranges::transform(args.begin(), args.end(), argv.begin() + 1, [](const std::string& arg) { return arg.c_str(); });
        return argv;
    }

    static std::map<std::string, std::string> makeCommandLineArgs(const std::vector<std::string>& args)
    {
        std::map<std::string, std::string> result;
        for (auto arg : args)
        {
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
TEST_F(ConfigTest, testEmptyParamsAndMissingParamsCoordinatorYAMLFile)
{
    CoordinatorConfigurationPtr coordinatorConfigPtr = std::make_shared<CoordinatorConfiguration>();
    coordinatorConfigPtr->overwriteConfigWithYAMLFileInput(std::filesystem::path(TEST_DATA_DIRECTORY) / "emptyCoordinator.yaml");
    EXPECT_EQ(coordinatorConfigPtr->restPort.getValue(), coordinatorConfigPtr->restPort.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->rpcPort.getValue(), coordinatorConfigPtr->rpcPort.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->restIp.getValue(), coordinatorConfigPtr->restIp.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->coordinatorHost.getValue(), coordinatorConfigPtr->coordinatorHost.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->logLevel.getValue(), coordinatorConfigPtr->logLevel.getDefaultValue());
    EXPECT_EQ(
        coordinatorConfigPtr->worker.numberOfBuffersInGlobalBufferManager.getValue(),
        coordinatorConfigPtr->worker.numberOfBuffersInGlobalBufferManager.getDefaultValue());
    EXPECT_EQ(
        coordinatorConfigPtr->worker.numberOfBuffersPerWorker.getValue(),
        coordinatorConfigPtr->worker.numberOfBuffersPerWorker.getDefaultValue());
    EXPECT_NE(
        coordinatorConfigPtr->worker.numberOfBuffersInSourceLocalBufferPool.getValue(),
        coordinatorConfigPtr->worker.numberOfBuffersInSourceLocalBufferPool.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->worker.bufferSizeInBytes.getValue(), coordinatorConfigPtr->worker.bufferSizeInBytes.getDefaultValue());
    EXPECT_EQ(
        coordinatorConfigPtr->worker.numberOfWorkerThreads.getValue(),
        coordinatorConfigPtr->worker.numberOfWorkerThreads.getDefaultValue());
    EXPECT_EQ(
        coordinatorConfigPtr->optimizer.queryMergerRule.getValue(), coordinatorConfigPtr->optimizer.queryMergerRule.getDefaultValue());
}

TEST_F(ConfigTest, testLogicalSourceAndSchemaParamsCoordinatorYAMLFile)
{
    CoordinatorConfigurationPtr coordinatorConfigPtr = std::make_shared<CoordinatorConfiguration>();
    coordinatorConfigPtr->overwriteConfigWithYAMLFileInput(std::string(TEST_DATA_DIRECTORY) + "coordinatorLogicalSourceAndSchema.yaml");
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

TEST_F(ConfigTest, testCoordinatorEPERATPRmptyParamsConsoleInput)
{
    /// given
    CoordinatorConfigurationPtr coordinatorConfigPtr = std::make_shared<CoordinatorConfiguration>();
    auto commandLineParams = makeCommandLineArgs(
        {"--restIp=localhost", "--worker.numberOfBuffersInSourceLocalBufferPool=128", "--worker.bufferSizeInBytes=1024"});
    /// when
    coordinatorConfigPtr->overwriteConfigWithCommandLineInput(commandLineParams);
    /// then
    EXPECT_EQ(coordinatorConfigPtr->restPort.getValue(), coordinatorConfigPtr->restPort.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->rpcPort.getValue(), coordinatorConfigPtr->rpcPort.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->restIp.getValue(), coordinatorConfigPtr->restIp.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->coordinatorHost.getValue(), coordinatorConfigPtr->coordinatorHost.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->logLevel.getValue(), coordinatorConfigPtr->logLevel.getDefaultValue());
    EXPECT_EQ(
        coordinatorConfigPtr->worker.numberOfBuffersInGlobalBufferManager.getValue(),
        coordinatorConfigPtr->worker.numberOfBuffersInGlobalBufferManager.getDefaultValue());
    EXPECT_EQ(
        coordinatorConfigPtr->worker.numberOfBuffersPerWorker.getValue(),
        coordinatorConfigPtr->worker.numberOfBuffersPerWorker.getDefaultValue());
    EXPECT_NE(
        coordinatorConfigPtr->worker.numberOfBuffersInSourceLocalBufferPool.getValue(),
        coordinatorConfigPtr->worker.numberOfBuffersInSourceLocalBufferPool.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->worker.bufferSizeInBytes.getValue(), coordinatorConfigPtr->worker.bufferSizeInBytes.getDefaultValue());
    EXPECT_EQ(
        coordinatorConfigPtr->worker.numberOfWorkerThreads.getValue(),
        coordinatorConfigPtr->worker.numberOfWorkerThreads.getDefaultValue());
    EXPECT_EQ(
        coordinatorConfigPtr->optimizer.queryMergerRule.getValue(), coordinatorConfigPtr->optimizer.queryMergerRule.getDefaultValue());
    EXPECT_EQ(
        coordinatorConfigPtr->worker.numberOfWorkerThreads.getValue(),
        coordinatorConfigPtr->worker.numberOfWorkerThreads.getDefaultValue());
}

TEST_F(ConfigTest, testEmptyParamsAndMissingParamsWorkerYAMLFile)
{
    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    workerConfigPtr->overwriteConfigWithYAMLFileInput(std::filesystem::path(TEST_DATA_DIRECTORY) / "emptyWorker.yaml");

    EXPECT_NE(workerConfigPtr->localWorkerHost.getValue(), workerConfigPtr->localWorkerHost.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->rpcPort.getValue(), workerConfigPtr->rpcPort.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->logLevel.getValue(), workerConfigPtr->logLevel.getDefaultValue());
    EXPECT_NE(
        workerConfigPtr->numberOfBuffersInGlobalBufferManager.getValue(),
        workerConfigPtr->numberOfBuffersInGlobalBufferManager.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->numberOfBuffersPerWorker.getValue(), workerConfigPtr->numberOfBuffersPerWorker.getDefaultValue());
    EXPECT_NE(
        workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getValue(),
        workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->bufferSizeInBytes.getValue(), workerConfigPtr->bufferSizeInBytes.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numberOfWorkerThreads.getValue(), workerConfigPtr->numberOfWorkerThreads.getDefaultValue());
}

TEST_F(ConfigTest, testWorkerYAMLFileWithMultiplePhysicalSource)
{
    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    workerConfigPtr->overwriteConfigWithYAMLFileInput(std::filesystem::path(TEST_DATA_DIRECTORY) / "workerWithPhysicalSources.yaml");

    EXPECT_NE(workerConfigPtr->localWorkerHost.getValue(), workerConfigPtr->localWorkerHost.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->rpcPort.getValue(), workerConfigPtr->rpcPort.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->logLevel.getValue(), workerConfigPtr->logLevel.getDefaultValue());
    EXPECT_NE(
        workerConfigPtr->numberOfBuffersInGlobalBufferManager.getValue(),
        workerConfigPtr->numberOfBuffersInGlobalBufferManager.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->numberOfBuffersPerWorker.getValue(), workerConfigPtr->numberOfBuffersPerWorker.getDefaultValue());
    EXPECT_NE(
        workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getValue(),
        workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->bufferSizeInBytes.getValue(), workerConfigPtr->bufferSizeInBytes.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numberOfWorkerThreads.getValue(), workerConfigPtr->numberOfWorkerThreads.getDefaultValue());
}

TEST_F(ConfigTest, testWorkerSourceCSVConsoleInput)
{
    /// given
    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    auto commandLineParams = makeCommandLineArgs(
        {"--localWorkerHost=localhost",
         "--numberOfWorkerThreads=5",
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
    /// when
    workerConfigPtr->overwriteConfigWithCommandLineInput(commandLineParams);
    /// then
    EXPECT_NE(workerConfigPtr->localWorkerHost.getValue(), workerConfigPtr->localWorkerHost.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->rpcPort.getValue(), workerConfigPtr->rpcPort.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->logLevel.getValue(), workerConfigPtr->logLevel.getDefaultValue());
    EXPECT_NE(
        workerConfigPtr->numberOfBuffersInGlobalBufferManager.getValue(),
        workerConfigPtr->numberOfBuffersInGlobalBufferManager.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->numberOfBuffersPerWorker.getValue(), workerConfigPtr->numberOfBuffersPerWorker.getDefaultValue());
    EXPECT_NE(
        workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getValue(),
        workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->bufferSizeInBytes.getValue(), workerConfigPtr->bufferSizeInBytes.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numberOfWorkerThreads.getValue(), workerConfigPtr->numberOfWorkerThreads.getDefaultValue());
    EXPECT_NE(
        workerConfigPtr->queryCompiler.compilationStrategy.getValue(),
        workerConfigPtr->queryCompiler.compilationStrategy.getDefaultValue());
    EXPECT_NE(
        workerConfigPtr->queryCompiler.pipeliningStrategy.getValue(), workerConfigPtr->queryCompiler.pipeliningStrategy.getDefaultValue());
    EXPECT_NE(
        workerConfigPtr->queryCompiler.outputBufferOptimizationLevel.getValue(),
        workerConfigPtr->queryCompiler.outputBufferOptimizationLevel.getDefaultValue());
}

TEST_F(ConfigTest, invalidCommandLineInputForBoolOptions)
{
    std::vector<std::pair<std::string, std::vector<std::string>>> commandLineArgs = {{"--enableStatisticOuput", {"not_a_bool", "2", "-1"}}};
    for (const auto& optionPair : commandLineArgs)
    {
        for (const auto& value : optionPair.second)
        {
            EXPECT_ANY_THROW({
                WorkerConfigurationPtr config = std::make_shared<WorkerConfiguration>();
                std::vector<std::string> args = {optionPair.first + "=" + value};
                config->overwriteConfigWithCommandLineInput(makeCommandLineArgs(args));
            });
        }
    }
}

TEST_F(ConfigTest, invalidCommandLineInputForIntOptions)
{
    std::vector<std::pair<std::string, std::vector<std::string>>> commandLineArgs
        = {{"--rpcPort", {"not_an_int", "1.5", "-1"}},
           {"--numberOfWorkerThreads", {"not_an_int", "1.5", "-1"}},
           {"--numberOfBuffersInGlobalBufferManager", {"not_an_int", "1.5", "-1"}},
           {"--numberOfBuffersPerWorker", {"not_an_int", "1.5", "-1"}},
           {"--numberOfBuffersInSourceLocalBufferPool", {"not_an_int", "1.5", "-1"}},
           {"--bufferSizeInBytes", {"not_an_int", "1.5", "-1"}}};

    for (const auto& optionPair : commandLineArgs)
    {
        for (const auto& value : optionPair.second)
        {
            EXPECT_ANY_THROW({
                WorkerConfigurationPtr config = std::make_shared<WorkerConfiguration>();
                std::vector<std::string> args = {optionPair.first + "=" + value};
                config->overwriteConfigWithCommandLineInput(makeCommandLineArgs(args));
            });
        }
    }
}

TEST_F(ConfigTest, invalidCommandLineInputForIpOptions)
{
    std::vector<std::pair<std::string, std::vector<std::string>>> commandLineArgs
        = {{"--restIp", {"300.300.300.300", "192.168.1", "not_an_ip", "127.0.0.-1", "192.168..1"}}};

    for (const auto& optionPair : commandLineArgs)
    {
        for (const auto& value : optionPair.second)
        {
            EXPECT_ANY_THROW({
                CoordinatorConfigurationPtr config = std::make_shared<CoordinatorConfiguration>();
                std::vector<std::string> args = {optionPair.first + "=" + value};
                config->overwriteConfigWithCommandLineInput(makeCommandLineArgs(args));
            });
        }
    }
}

TEST_F(ConfigTest, invalidBooleanYamlInputs)
{
    std::vector<std::pair<std::string, std::vector<std::string>>> invalidBooleanConfigs
        = {{"enableStatisticOutput", {"not_a_bool", "2", "-1"}}};
    for (const auto& [optionName, invalidValues] : invalidBooleanConfigs)
    {
        for (const auto& value : invalidValues)
        {
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

TEST_F(ConfigTest, invalidIntYamlInputs)
{
    std::vector<std::pair<std::string, std::vector<std::string>>> invalidIntConfigs
        = {{"--rpcPort", {"not_an_int", "1.5", "-1"}},
           {"--numberOfWorkerThreads", {"not_an_int", "1.5", "-1"}},
           {"--numberOfBuffersInGlobalBufferManager", {"not_an_int", "1.5", "-1"}},
           {"--numberOfBuffersPerWorker", {"not_an_int", "1.5", "-1"}},
           {"--numberOfBuffersInSourceLocalBufferPool", {"not_an_int", "1.5", "-1"}},
           {"--bufferSizeInBytes", {"not_an_int", "1.5", "-1"}}};
    for (const auto& [optionName, invalidValues] : invalidIntConfigs)
    {
        for (const auto& value : invalidValues)
        {
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

TEST_F(ConfigTest, invalidIpYamlInputs)
{
    std::vector<std::pair<std::string, std::vector<std::string>>> invalidIpConfigs
        = {{"--restIp", {"300.300.300.300", "192.168.1", "not_an_ip", "127.0.0.-1", "192.168..1"}}};

    for (const auto& [optionName, invalidValues] : invalidIpConfigs)
    {
        for (const auto& value : invalidValues)
        {
            std::string fileName = optionName + "_" + value + ".yaml";
            auto filePath = std::filesystem::path(TEST_DATA_DIRECTORY) / fileName;

            std::ofstream file(filePath);
            ASSERT_TRUE(file.is_open());
            file << optionName << ": " << value << std::endl;
            file.close();

            EXPECT_ANY_THROW({
                CoordinatorConfigurationPtr config = std::make_shared<CoordinatorConfiguration>();
                config->overwriteConfigWithYAMLFileInput(filePath);
            });

            std::filesystem::remove(filePath);
        }
    }
}
} /// namespace NES
