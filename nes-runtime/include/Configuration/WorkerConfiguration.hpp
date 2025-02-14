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

#pragma once

#include <string>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Validation/NonZeroValidation.hpp>
#include <Configurations/WrapOption.hpp>
#include <QueryCompiler/Configurations/QueryCompilerConfiguration.hpp>
#include <QueryEngineConfiguration.hpp>

namespace NES
{


namespace Configurations
{


class WorkerConfiguration : public BaseConfiguration
{
public:
    WorkerConfiguration() : BaseConfiguration() {};
    WorkerConfiguration(std::string name, std::string description) : BaseConfiguration(name, description) {};


    StringOption localWorkerHost = {LOCAL_WORKER_HOST_CONFIG, "127.0.0.1", "Worker IP or hostname."};

    /**
     * @brief Port for the RPC server of the Worker.
     * This is used to receive control messages from the coordinator or other workers .
     */
    UIntOption rpcPort = {RPC_PORT_CONFIG, "0", "RPC server port of the NES Worker.", {std::make_shared<NumberValidation>()}};

    EnumOption<LogLevel> logLevel
        = {LOG_LEVEL_CONFIG, LogLevel::LOG_INFO, "The log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE)"};

    Runtime::QueryEngineConfiguration queryEngineConfiguration = {"queryEngine", "Query Engine Configuration"};

    /**
     * @brief The number of buffers in the global buffer manager.
     * Controls how much memory is consumed by the system.
     */
    UIntOption numberOfBuffersInGlobalBufferManager
        = {NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG,
           "1024",
           "Number buffers in global buffer pool.",
           {std::make_shared<NumberValidation>()}};

    UIntOption numberOfBuffersPerWorker
        = {NUMBER_OF_BUFFERS_PER_WORKER_CONFIG, "128", "Number buffers in task local buffer pool.", {std::make_shared<NumberValidation>()}};

    /**
     * @brief Indicates how many buffers a single data source can allocate.
     * This property controls the backpressure mechanism as a data source that can't allocate new records can't ingest more data.
     */
    UIntOption numberOfBuffersInSourceLocalBufferPool
        = {NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG,
           "64",
           "Number buffers in source local buffer pool.",
           {std::make_shared<NumberValidation>()}};

    /**
     * @brief Configures the buffer size of individual TupleBuffers in bytes.
     * This property has to be the same over a whole deployment.
     */
    UIntOption bufferSizeInBytes = {BUFFERS_SIZE_IN_BYTES_CONFIG, "4096", "BufferSizeInBytes.", {std::make_shared<NumberValidation>()}};

    BoolOption enableStatisticOuput
        = {ENABLE_STATISTIC_OUTPUT_CONFIG, "false", "Enable statistic output", {std::make_shared<BooleanValidation>()}};

    QueryCompilation::Configurations::QueryCompilerConfiguration queryCompiler
        = {QUERY_COMPILER_CONFIG, "Configuration for the query compiler"};

    /**
     * @brief Configuration yaml path.
     * @warning this is just a placeholder configuration
     */
    StringOption configPath = {CONFIG_PATH, "", "Path to configuration file."};

    UIntOption senderHighwatermark
        = {SENDER_HIGH_WATERMARK,
           "8",
           "Number of tuple buffers allowed in one network channel before blocking transfer.",
           {std::make_shared<NumberValidation>()}};


private:
    std::vector<Configurations::BaseOption*> getOptions() override
    {
        return {
            &localWorkerHost,
            &rpcPort,
            &queryEngineConfiguration,
            &numberOfBuffersInGlobalBufferManager,
            &numberOfBuffersPerWorker,
            &numberOfBuffersInSourceLocalBufferPool,
            &bufferSizeInBytes,
            &logLevel,
            &queryCompiler,
            &configPath,
        };
    }
};
}
}
