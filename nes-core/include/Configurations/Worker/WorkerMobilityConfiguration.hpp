#ifndef NES_WORKERMOBILITYCONFIGURATION_H
#define NES_WORKERMOBILITYCONFIGURATION_H


#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <memory>

namespace NES {

namespace Configurations::Spatial::Mobility::Experimental {

//todo: make the naming consistent
class WorkerMobilityConfiguration;
using WorkerMobilityConfigurationPtr = std::shared_ptr<WorkerMobilityConfiguration>;

class WorkerMobilityConfiguration : public BaseConfiguration {
  public:
    //todo: check if these defaults are sane
    UIntOption pathPredictionUpdateInterval = {PATH_PREDICTION_UPDATE_INTERVAL,
                                               1000,
                                               "Sleep time after the last update of the predicted path"};
    UIntOption locationBufferSize = {LOCATION_BUFFER_SIZE,
                                     30,
                                     "The amount of past locations to be recorded in order to predict the future trajectory"};
    UIntOption saveRate = {LOCATION_BUFFER_SAVE_RATE,
                           4,
                           "Determines after how many location updates a new location will be inserted in the location buffer"};
    UIntOption pathDistanceDelta = {PATH_DISTANCE_DELTA_METERS,
                                    20,
                                    "when deviating further than delta meters from the current predicted path, an update of the prediction will be triggered"};
    UIntOption nodeDownloadRadius = {NODE_DOWNLOAD_RADIUS, 10000, "The radius in meters in which nodes will be downloaded"};
    UIntOption nodeIndexUpdateThreshold = {
        NODE_INDEX_UPDATE_THRESHOLD,
        2000,
        "Trigger download of new node info when the device is less than threshold away from the boundary of the area covered by the current info"};//todo: make a check, that this is not less than the coverage and not more then the save radius
    UIntOption defaultCoverageRadius = {DEFAULT_COVERAGE_RADIUS,
                                        1000,
                                        "The coverage in meters each field node is assumed to have"};
    UIntOption pathPredictionLength = {PATH_PREDICTION_LENGTH, 10000, "The Length of the predicted path to be computed"};

  private:
    std::vector<Configurations::BaseOption*> getOptions() override {
        return {&pathPredictionUpdateInterval,
                &locationBufferSize,
                &saveRate,
                &pathDistanceDelta,
                &nodeDownloadRadius,
                &nodeIndexUpdateThreshold,
                &defaultCoverageRadius,
                &pathPredictionLength};
    }
};
}
}
#endif//NES_WORKERMOBILITYCONFIGURATION_H
