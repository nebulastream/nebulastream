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
#ifndef NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_WORKERLATENCYCONFIGURATION_HPP_
#define NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_WORKERLATENCYCONFIGURATION_HPP_

#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Util/Latency/NetworkCoordinateProviderType.hpp>
#include <memory>

namespace NES::Configurations::Synthetic::Latency::Experimental {

class WorkerLatencyConfiguration;
using WorkerLatencyConfigurationPtr = std::shared_ptr<WorkerLatencyConfiguration>;

/**
* @brief this class stores the configuration options necessary for network coordinate enabled devices
*/

class WorkerLatencyConfiguration : public BaseConfiguration {
  public:
    WorkerLatencyConfiguration() : BaseConfiguration(){};
    WorkerLatencyConfiguration(std::string name, std::string description) : BaseConfiguration(name, description){};
    /**
     * @brief Factory function for a worker latency config
     */
    static std::shared_ptr<WorkerLatencyConfiguration> create() { return std::make_shared<WorkerLatencyConfiguration>(); }

    /**
     * @brief defines how many locations should be saved in the buffer which is used to calculate the predicted path
     */
    UIntOption networkCoordinateBufferSize = {NETWORK_COORDINATE_BUFFER_SIZE_CONFIG,
                                     "30",
                                     "The amount of past locations to be recorded in order to predict the future trajectory", std::make_shared<NumberValidation>()};

    /**
     * @brief the network distance in milliseconds from the network distance of a field node within which we assume the connection
     * between the nodes to be reasonably fast
     */
    UIntOption defaultCoverageRadius = {DEFAULT_COVERAGE_RADIUS_CONFIG,
                                        "50",
                                        "The coverage in milliseconds each node is assumed to have", std::make_shared<NumberValidation>()};

    /**
     * @brief the network distance in milliseconds which a device has to move in the synthetic coordinate space before it informs the coordinator about the coordinate change
     */
    UIntOption sendDeviceNetworkCoordinateUpdateThreshold = {
        SEND_DEVICE_NETWORK_COORDINATE_UPDATE_THRESHOLD_CONFIG,
        "100",
        "The distance in milliseconds after which the device will report it's new coordinates", std::make_shared<NumberValidation>()};

    /**
     * @brief the time for which the thread running at the worker latency handler will sleep after each iteration
     */
     //TODO: Thread must sleep until it receives a latency measurement
    UIntOption latencyHandlerUpdateInterval = {
        SEND_LATENCY_UPDATE_INTERVAL_CONFIG,
        "10000",
        "the time which the thread running at the worker LATENCY handler will sleep after each iteration",
        std::make_shared<NumberValidation>()};


    /**
     * @brief a boolean to define if the worker should inform the coordinator about a change in network coordinate which is larger than a certain threshold
     */
    BoolOption pushDeviceNetworkCoordinateUpdates = {PUSH_DEVICE_NETWORK_COORDINATE_UPDATES_CONFIG,
                                            "true",
                                            "determines if network coordinate updates should be sent to the coordinator", std::make_shared<BooleanValidation>()};

    /**
     * @brief specify from which kind of interface a worker can obtain its current network coordinates. This can for example be a simulation or
     * a calculation in the synthetic space.
     */
    EnumOption<NES::Synthetic::Latency::Experimental::NetworkCoordinateProviderType> networkCoordinateProviderType = {
        NETWORK_COORDINATE_PROVIDER_TYPE_CONFIG,
        NES::Synthetic::Latency::Experimental::NetworkCoordinateProviderType::BASE,
        "the kind of interface which the worker gets its networkCoordinate info from"};

    /**
     * @brief specify the config data specific to the source of location data which was specified in the networkCoordinateProviderType option
     */
    StringOption networkCoordinateProviderConfig = {NETWORK_COORDINATE_PROVIDER_CONFIG, "", "the configuration data for the network coordinate interface"};

    /**
     * @brief if the locationprovider simulates device movement, setting this option to a non zero value will result in that
     * value being used as the start time to which offsets will be added to obtain absolute timestamps
     */
    UIntOption networkCoordinateProviderSimulatedStartTime = {NETWORK_COORDINATE_SIMULATED_START_TIME_CONFIG,
                                                     "0",
                                                     "The start time to be simulated if device network coordinates are simulated", std::make_shared<NumberValidation>()};

  private:
    std::vector<Configurations::BaseOption*> getOptions() override {
        return {&networkCoordinateBufferSize,
                &defaultCoverageRadius,
                &sendDeviceNetworkCoordinateUpdateThreshold,
                &pushDeviceNetworkCoordinateUpdates,
                &networkCoordinateProviderType,
                &networkCoordinateProviderConfig,
                &networkCoordinateProviderSimulatedStartTime};
    }
};
}// namespace NES::Configurations::Synthetic::Latency::Experimental

#endif // NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_WORKERLATENCYCONFIGURATION_HPP_
