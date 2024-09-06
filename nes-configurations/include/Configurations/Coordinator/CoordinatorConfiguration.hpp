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
#include <Configurations/Coordinator/LogicalSourceTypeFactory.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Configurations/Enums/StorageHandlerType.hpp>
#include <Configurations/Validation/IpValidation.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>

namespace NES::Configurations
{

using CoordinatorConfigurationPtr = std::shared_ptr<class CoordinatorConfiguration>;

class CoordinatorConfiguration : public BaseConfiguration
{
public:
    StringOption restIp = {REST_IP_CONFIG, "127.0.0.1", "NES ip of the REST server.", {std::make_shared<IpValidation>()}};

    UIntOption restPort = {REST_PORT_CONFIG, "8081", "Port exposed for rest endpoints", {std::make_shared<NumberValidation>()}};

    StringOption coordinatorHost = {COORDINATOR_HOST_CONFIG, "127.0.0.1", "RPC IP address or hostname of NES Coordinator."};

    UIntOption rpcPort = {RPC_PORT_CONFIG, "4000", "RPC server port of the Coordinator", {std::make_shared<NumberValidation>()}};

    EnumOption<LogLevel> logLevel
        = {LOG_LEVEL_CONFIG, LogLevel::LOG_INFO, "The log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE)"};

    UIntOption requestExecutorThreads
        = {REQUEST_EXECUTOR_THREAD_CONFIG, "1", "Number of request executor thread", {std::make_shared<NumberValidation>()}};

    EnumOption<RequestProcessor::StorageHandlerType> storageHandlerType
        = {STORAGE_HANDLER_TYPE_CONFIG,
           RequestProcessor::StorageHandlerType::TwoPhaseLocking,
           "The Storage Handler Type (TwoPhaseLocking, SerialHandler)"};

    OptimizerConfiguration optimizer = {OPTIMIZER_CONFIG, "Defines the configuration for the optimizer."};

    /// @deprecated This is currently only used for testing and will be removed.
    SequenceOption<WrapOption<LogicalSourceTypePtr, LogicalSourceTypeFactory>> logicalSourceTypes = {LOGICAL_SOURCES, "Logical Sources"};

    StringOption configPath = {CONFIG_PATH, "", "Path to configuration file."};

    WorkerConfiguration worker = {WORKER_CONFIG, "Defines the configuration for the worker."};

    StringOption workerConfigPath = {WORKER_CONFIG_PATH, "", "Path to a configuration file for the internal worker."};

    UIntOption coordinatorHealthCheckWaitTime
        = {HEALTH_CHECK_WAIT_TIME, "1", "Number of seconds to wait between health checks", {std::make_shared<NumberValidation>()}};

    StringOption restServerCorsAllowedOrigin
        = {REST_SERVER_CORS_ORIGIN,
           "*",
           "The allowed origins to be set in the header of the responses to rest requests\n "
           "The default value '*' allows all CORS requests per default. Setting the value to 'false' disables CORS requests."};

    /// Create a CoordinatorConfiguration object and set values from the POSIX command line parameters stored in argv.
    static CoordinatorConfigurationPtr create(const int argc, const char** argv);
    static std::shared_ptr<CoordinatorConfiguration> createDefault() { return std::make_shared<CoordinatorConfiguration>(); }

private:
    std::vector<BaseOption*> getOptions() override
    {
        return {
            &restIp,
            &coordinatorHost,
            &rpcPort,
            &restPort,
            &logLevel,
            &configPath,
            &worker,
            &workerConfigPath,
            &optimizer,
            &logicalSourceTypes,
            &coordinatorHealthCheckWaitTime,
            &restServerCorsAllowedOrigin,
        };
    }
};
}
