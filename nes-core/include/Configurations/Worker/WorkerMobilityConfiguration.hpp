#ifndef NES_WORKERMOBILITYCONFIGURATION_H
#define NES_WORKERMOBILITYCONFIGURATION_H


#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <memory>
#include <Util/Experimental/LocationProviderType.hpp>

namespace NES {

namespace Configurations::Spatial::Mobility::Experimental {

class WorkerMobilityConfiguration;
using WorkerMobilityConfigurationPtr = std::shared_ptr<WorkerMobilityConfiguration>;

class WorkerMobilityConfiguration : public BaseConfiguration {
  public:
    /**
     * @brief Factory function for a worker config
     */
    static std::shared_ptr<WorkerMobilityConfiguration> create() { return std::make_shared<WorkerMobilityConfiguration>(); }

    /**
     * @brief defines how many milliseconds to wait, between recalculations of the devices predicted path
     */
    UIntOption pathPredictionUpdateInterval = {PATH_PREDICTION_UPDATE_INTERVAL_CONFIG,
                                               1000,
                                               "Sleep time after the last update of the predicted path"};

    /**
     * @brief defines how many locations should be saved in the buffer which is used to calculate the predicted path
     */
    UIntOption locationBufferSize = {LOCATION_BUFFER_SIZE_CONFIG,
                                     30,
                                     "The amount of past locations to be recorded in order to predict the future trajectory"};

    UIntOption locationBufferSaveRate = {LOCATION_BUFFER_SAVE_RATE_CONFIG,
                           4,
                           "Determines after how many location updates a new location will be inserted in the location buffer"};

    UIntOption pathDistanceDelta = {PATH_DISTANCE_DELTA_CONFIG,
                                    20,
                                    "when deviating further than delta meters from the current predicted path, an update of the prediction will be triggered"};

    UIntOption nodeInfoDownloadRadius = {NODE_INFO_DOWNLOAD_RADIUS_CONFIG,
                                         10000,
                                         "The radius in meters in which nodes will be downloaded"};

    UIntOption nodeIndexUpdateThreshold = {NODE_INDEX_UPDATE_THRESHOLD_CONFIG,
                                           2000,
                                           "Trigger download of new node info when the device is less than threshold away from the boundary of the area covered by the current info"};//todo: make a check, that this is not less than the coverage and not more then the save radius

    UIntOption defaultCoverageRadius = {DEFAULT_COVERAGE_RADIUS_CONFIG,
                                        1000,
                                        "The coverage in meters each field node is assumed to have"};

    UIntOption pathPredictionLength = {PATH_PREDICTION_LENGTH_CONFIG,
                                       10000,
                                       "The Length of the predicted path to be computed"};

    UIntOption sendDevicePositionUpdateThreshold = {SEND_DEVICE_LOCATION_UPDATE_THRESHOLD_CONFIG,
                                                    10,
                                                    "The distance in meters after which the device will report it's new position in meters"};

    BoolOption pushDeviceLocationUpdates = {PUSH_DEVICE_LOCATION_UPDATES_CONFIG,
                                            true,
                                            "determines if position updates should be sent to the coordinator"};

    UIntOption sendLocationUpdateInterval = {SEND_LOCATION_UPDATE_INTERVAL_CONFIG,
                                             10000,
                                             "the sleep amount between 2 checks if a locatin update should be sent to the coordinator"};

    /**
     * @brief specify from which kind of interface a mobile worker can obtain its current location. This can for example be a GPS device or
     * a simulation
     */
    EnumOption<NES::Spatial::Mobility::Experimental::LocationProviderType> locationProviderType = {
        LOCATION_PROVIDER_TYPE_CONFIG,
        NES::Spatial::Mobility::Experimental::LocationProviderType::BASE,
        "the kind of interface which the  mobile worker gets its geolocation info from"};

    /**
     * @brief specify the config data specific to the source of location data which was specified in the locationProviderType option
     */
    StringOption locationProviderConfig = {LOCATION_PROVIDER_CONFIG, "", "the configuration data for the location interface"};

  private:
    std::vector<Configurations::BaseOption*> getOptions() override {
        return {&pathPredictionUpdateInterval,
                &locationBufferSize,
                &locationBufferSaveRate,
                &pathDistanceDelta,
                &nodeInfoDownloadRadius,
                &nodeIndexUpdateThreshold,
                &defaultCoverageRadius,
                &pathPredictionLength,
                &sendDevicePositionUpdateThreshold,
                &pushDeviceLocationUpdates,
                &sendLocationUpdateInterval,
                &locationProviderType,
                &locationProviderConfig};
    }
};
}
}
#endif//NES_WORKERMOBILITYCONFIGURATION_H
