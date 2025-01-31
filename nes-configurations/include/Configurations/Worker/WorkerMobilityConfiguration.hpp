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
#ifndef NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_WORKERMOBILITYCONFIGURATION_HPP_
#define NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_WORKERMOBILITYCONFIGURATION_HPP_

#include "Configurations/BaseConfiguration.hpp"
#include "Configurations/ConfigurationOption.hpp"
#include "Configurations/Validation/BooleanValidation.hpp"
#include "Configurations/Validation/FloatValidation.hpp"
#include "Configurations/Validation/NumberValidation.hpp"
#include "Util/Mobility/LocationProviderType.hpp"
#include <Util/Mobility/ReconnectPredictorType.hpp>
#include <memory>

namespace NES::Configurations::Spatial::Mobility::Experimental {

class WorkerMobilityConfiguration;
using WorkerMobilityConfigurationPtr = std::shared_ptr<WorkerMobilityConfiguration>;

/**
 * @brief this class stores the configuration options necessary for mobile devices
 */

class WorkerMobilityConfiguration : public BaseConfiguration {
  public:
    WorkerMobilityConfiguration() : BaseConfiguration(){};
    WorkerMobilityConfiguration(std::string name, std::string description) : BaseConfiguration(name, description){};
    /**
     * @brief Factory function for a worker mobility config
     */
    static std::shared_ptr<WorkerMobilityConfiguration> create() { return std::make_shared<WorkerMobilityConfiguration>(); }

    /**
     * @brief defines how many locations should be saved in the buffer which is used to calculate the predicted path
     */
    UIntOption locationBufferSize = {LOCATION_BUFFER_SIZE_CONFIG,
                                     "30",
                                     "The amount of past locations to be recorded in order to predict the future trajectory",
                                     {std::make_shared<NumberValidation>()}};

    /**
     * @brief defines after how many path prediction update steps a new location should be saved to the buffer
     */
    UIntOption locationBufferSaveRate = {
        LOCATION_BUFFER_SAVE_RATE_CONFIG,
        "4",
        "Determines after how many location updates a new location will be inserted in the location buffer",
        {std::make_shared<NumberValidation>()}};

    /**
     * @brief defines the minimum distance in meters between the current predicted path and the device position that will lead to a recalculation of the prediction
     */
    UIntOption pathDistanceDelta = {PATH_DISTANCE_DELTA_CONFIG,
                                    "20",
                                    "when deviating further than delta meters from the current predicted path, an update of the "
                                    "prediction will be triggered",
                                    {std::make_shared<NumberValidation>()}};

    /**
     * @brief defines the radius of the circle used to determine the area within which all field node data will be downloaded during an update of the local index
     */
    UIntOption nodeInfoDownloadRadius = {NODE_INFO_DOWNLOAD_RADIUS_CONFIG,
                                         "10000",
                                         "The radius in meters in which nodes will be downloaded",
                                         {std::make_shared<NumberValidation>()}};

    /**
     * @brief defines the distance from the edge of the covered by the current node index which when reached will trigger an update.
     * Needs to be less than nodeInfoDownloadRadius to avoid cconstant redownloading. Needs to be more than coverage to avoid
     * scheduling suboptimal reconnects
     */
    UIntOption nodeIndexUpdateThreshold = {NODE_INDEX_UPDATE_THRESHOLD_CONFIG,
                                           "2000",
                                           "Trigger download of new node info when the device is less than threshold away from "
                                           "the boundary of the area covered by the current info",
                                           {std::make_shared<NumberValidation>()}};

    /**
     * @brief the distance in meters from the geographical position of a field node within which we assume the connection
     * between the mobile devices and the field node to be reasonably fast
     */
    UIntOption defaultCoverageRadius = {DEFAULT_COVERAGE_RADIUS_CONFIG,
                                        "1000",
                                        "The coverage in meters each field node is assumed to have",
                                        {std::make_shared<NumberValidation>()}};

    /**
     * @brief the length of the path to be predicted
     */
    UIntOption pathPredictionLength = {PATH_PREDICTION_LENGTH_CONFIG,
                                       "10000",
                                       "The Length of the predicted path to be computed",
                                       {std::make_shared<NumberValidation>()}};

    /**
     * @brief the allowed factor for speed changes before a recalculation of the predictions is triggered
     */
    FloatOption speedDifferenceThresholdFactor = {
        SPEED_DIFFERENCE_THRESHOLD_FACTOR_CONFIG,
        "0.00001",
        "The factor by which the speed needs to change to trigger a recalculation of reconnect predictions",
        {std::make_shared<FloatValidation>()}};

    /**
     * @brief the distance in meters which a device has to move before it informs the coordinator about the location change
     */
    UIntOption sendDevicePositionUpdateThreshold = {
        SEND_DEVICE_LOCATION_UPDATE_THRESHOLD_CONFIG,
        "100",
        "The distance in meters after which the device will report it's new position in meters",
        {std::make_shared<NumberValidation>()}};

    /**
     * @brief a boolean to define if the worker should inform the coordinator about a change in position which is larger than a certain threshold
     */
    BoolOption pushDeviceLocationUpdates = {PUSH_DEVICE_LOCATION_UPDATES_CONFIG,
                                            "true",
                                            "determines if position updates should be sent to the coordinator",
                                            {std::make_shared<BooleanValidation>()}};

    /**
     * @brief the time which the thread running at the worker mobility handler will sleep after each iteration
     */
    UIntOption mobilityHandlerUpdateInterval = {
        SEND_LOCATION_UPDATE_INTERVAL_CONFIG,
        "10000",
        "the time which the thread running at the worker mobility handler will sleep after each iteration",
        {std::make_shared<NumberValidation>()}};

    /**
     * @brief specify from which kind of interface a mobile worker can obtain its current location. This can for example be a GPS device or
     * a simulation
     */
    EnumOption<NES::Spatial::Mobility::Experimental::LocationProviderType> locationProviderType = {
        LOCATION_PROVIDER_TYPE_CONFIG,
        NES::Spatial::Mobility::Experimental::LocationProviderType::BASE,
        "the kind of interface which the  mobile worker gets its geolocation info from"};

    EnumOption<NES::Spatial::Mobility::Experimental::ReconnectPredictorType> reconnectPredictorType = {
        RECONNECT_PREDICTOR_TYPE_CONFIG,
        NES::Spatial::Mobility::Experimental::ReconnectPredictorType::LIVE,
        "The method how the worker obtains the next reconnects"};

    /**
     * @brief specify the config data specific to the source of location data which was specified in the locationProviderType option
     */
    StringOption locationProviderConfig = {LOCATION_PROVIDER_CONFIG, "", "the configuration data for the location interface"};

    StringOption precalculatedReconnectPath = {PRECALCULATED_RECONNECTS_PATH,
                                               "",
                                               "the configuration data for the location interface"};

    /**
     * @brief if the locationprovider simulates device movement, setting this option to a non zero value will result in that
     * value being used as the start time to which offsets will be added to obtain absolute timestamps
     */
    UIntOption locationProviderSimulatedStartTime = {LOCATION_SIMULATED_START_TIME_CONFIG,
                                                     "0",
                                                     "The start time to be simulated if device movement is simulated",
                                                     {std::make_shared<NumberValidation>()}};

  private:
    std::vector<Configurations::BaseOption*> getOptions() override {
        return {&locationBufferSize,
                &locationBufferSaveRate,
                &pathDistanceDelta,
                &nodeInfoDownloadRadius,
                &nodeIndexUpdateThreshold,
                &defaultCoverageRadius,
                &pathPredictionLength,
                &speedDifferenceThresholdFactor,
                &sendDevicePositionUpdateThreshold,
                &pushDeviceLocationUpdates,
                &mobilityHandlerUpdateInterval,
                &locationProviderType,
                &locationProviderConfig,
                &locationProviderSimulatedStartTime,
                &precalculatedReconnectPath,
                &reconnectPredictorType
        };
    }
};
}// namespace NES::Configurations::Spatial::Mobility::Experimental

#endif// NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_WORKERMOBILITYCONFIGURATION_HPP_
