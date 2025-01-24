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

#ifndef NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_COORDINATOR_COORDINATORCONFIGURATION_HPP_
#define NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_COORDINATOR_COORDINATORCONFIGURATION_HPP_

#include "Configurations/BaseConfiguration.hpp"
#include "Configurations/Coordinator/ElegantConfigurations.hpp"
#include "Configurations/Coordinator/LogicalSourceTypeFactory.hpp"
#include "Configurations/Coordinator/OptimizerConfiguration.hpp"
#include "Configurations/Enums/StorageHandlerType.hpp"
#include "Configurations/Validation/IpValidation.hpp"
#include "Configurations/Worker/WorkerConfiguration.hpp"
#include <string>

namespace NES::Configurations {

class CoordinatorConfiguration;
using CoordinatorConfigurationPtr = std::shared_ptr<CoordinatorConfiguration>;

/**
 * @brief Configuration options for the Coordinator.
 */
class CoordinatorConfiguration : public BaseConfiguration {
  public:
    /**
     * @brief IP of the REST server.
     */
    StringOption restIp = {REST_IP_CONFIG, "127.0.0.1", "NES ip of the REST server.", {std::make_shared<IpValidation>()}};

    /**
     * @brief Port of the REST server.
     */
    UIntOption restPort = {REST_PORT_CONFIG, "8081", "Port exposed for rest endpoints", {std::make_shared<NumberValidation>()}};

    /**
     * @brief IP or hostname of the Coordinator.
     */
    StringOption coordinatorHost = {COORDINATOR_HOST_CONFIG, "127.0.0.1", "RPC IP address or hostname of NES Coordinator."};

    /**
     * @brief log path.
     */
    StringOption logPath = {LOG_PATH_CONFIG, "nesCoordinatorStarter.log", "Path to the log file."};

    /**
     * @brief Port for the RPC server of the Coordinator.
     * This is used to receive control messages.
     */
    UIntOption rpcPort = {RPC_PORT_CONFIG, "4000", "RPC server port of the Coordinator", {std::make_shared<NumberValidation>()}};

    /**
     * @brief The current log level. Controls the detail of log messages.
     */
    EnumOption<LogLevel> logLevel = {LOG_LEVEL_CONFIG,
                                     LogLevel::LOG_INFO,
                                     "The log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE)"};

    /**
     * @brief Indicates if the monitoring stack is enables.
     */
    BoolOption enableMonitoring = {ENABLE_MONITORING_CONFIG,
                                   "false",
                                   "Enable monitoring",
                                   {std::make_shared<BooleanValidation>()}};

    /**
     * @brief Indicates the number of request executor threads
     */
    UIntOption requestExecutorThreads = {REQUEST_EXECUTOR_THREAD_CONFIG,
                                         "1",
                                         "Number of request executor thread",
                                         {std::make_shared<NumberValidation>()}};

    /**
     * @brief Storage handler for request executor
     */
    EnumOption<RequestProcessor::StorageHandlerType> storageHandlerType = {
        STORAGE_HANDLER_TYPE_CONFIG,
        RequestProcessor::StorageHandlerType::TwoPhaseLocking,
        "The Storage Handler Type (TwoPhaseLocking, SerialHandler)"};

    /**
     * @brief Configures different properties for the query optimizer.
     */
    OptimizerConfiguration optimizer = {OPTIMIZER_CONFIG, "Defines the configuration for the optimizer."};

    /**
     * @brief Allows the configuration of logical sources at the coordinator.
     * @deprecated This is currently only used for testing and will be removed.
     */
    SequenceOption<WrapOption<LogicalSourceTypePtr, LogicalSourceTypeFactory>> logicalSourceTypes = {LOGICAL_SOURCES,
                                                                                                     "Logical Sources"};

    /**
     * @brief Configuration yaml path.
     * @warning this is just a placeholder configuration
     */
    StringOption configPath = {CONFIG_PATH, "", "Path to configuration file."};

    /**
     * @brief Configures different properties of the internal worker in the coordinator configuration file and on the command line.
     */
    WorkerConfiguration worker = {WORKER_CONFIG, "Defines the configuration for the worker."};

    /**
     * @brief Path to a dedicated configuration file for the internal worker.
     */
    StringOption workerConfigPath = {WORKER_CONFIG_PATH, "", "Path to a configuration file for the internal worker."};

    /**
     * @brief Configuration of waiting time of the coordinator health check.
     * Set the number of seconds waiting to perform health checks
     */
    UIntOption coordinatorHealthCheckWaitTime = {HEALTH_CHECK_WAIT_TIME,
                                                 "1",
                                                 "Number of seconds to wait between health checks",
                                                 {std::make_shared<NumberValidation>()}};

    /**
     * @brief The allowed origin for CORS requests which will be sent as part of the header of the http responses of the rest server.
     *        The default value '*' allows all CORS requests per default. Setting the value to 'false' disables CORS requests.
     */
    StringOption restServerCorsAllowedOrigin = {REST_SERVER_CORS_ORIGIN,
                                                "*",
                                                "The allowed origins to be set in the header of the responses to rest requests"};

    /**
     * @brief ELEGANT related configuration parameters
     */
    ElegantConfigurations elegant = {ELEGANT, "Define ELEGANT configuration"};

    /**
     * @brief Create a default CoordinatorConfiguration object with default values.
     * @return A CoordinatorConfiguration object with default values.
     */
    static std::shared_ptr<CoordinatorConfiguration> createDefault() { return std::make_shared<CoordinatorConfiguration>(); }

    /**
     * Create a CoordinatorConfiguration object and set values from the POSIX command line parameters stored in argv.
     * @param argc The argc parameter given to the main function.
     * @param argv The argv parameter given to the main function.
     * @return A configured configuration object.
     */
    static CoordinatorConfigurationPtr create(const int argc, const char** argv);

  private:
    std::vector<Configurations::BaseOption*> getOptions() override {
        return {&restIp,
                &coordinatorHost,
                &logPath,
                &rpcPort,
                &restPort,
                &logLevel,
                &enableMonitoring,
                &configPath,
                &worker,
                &workerConfigPath,
                &optimizer,
                &logicalSourceTypes,
                &coordinatorHealthCheckWaitTime,
                &restServerCorsAllowedOrigin,
                &elegant};
    }
};

}// namespace NES::Configurations

#endif// NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_COORDINATOR_COORDINATORCONFIGURATION_HPP_
