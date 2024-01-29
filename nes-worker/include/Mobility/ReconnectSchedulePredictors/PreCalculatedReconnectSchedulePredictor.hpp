#ifndef NES_PRECALCULATEDRECONNECTSCHEDULEPREDICTOR_HPP
#define NES_PRECALCULATEDRECONNECTSCHEDULEPREDICTOR_HPP

#include <Mobility/ReconnectSchedulePredictors/ReconnectSchedulePredictor.hpp>
#include <Identifiers.hpp>

namespace NES::Spatial::Mobility::Experimental {
class PreCalculatedReconnectSchedulePredictor : public ReconnectSchedulePredictor {
  public:
    PreCalculatedReconnectSchedulePredictor(
        const Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& configuration);

    static ReconnectSchedulePredictorPtr create(const Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& configuration);
    /**
     * @brief calculate a new reconnect schedule based on the location of other workers (potential parents to reconnect
     * to), this devices own position and the position of the current parent. A new reconnect schedule will only be calculated,
     * if the workers trajectory or speed has changed enough to pass the thresholds which were set at object construction
     * since the last schedule was calculated or if the spatial index containing the neighbouring worker locations has been updated in the meantime.
     * If none of the above conditions are true, this function will return nullopt because the previously calculated
     * schedule is assumed to be still correct.
     * @param currentLocation the mobile workers current location
     * @param parentLocation the location of the mobile workers current parent
     * @param FieldNodeIndex a spatial index containing data about other workers in the area
     * @param isIndexUpdted indicates if the index was updated or has remained the same since the last schedule was calculated
     * @return the new schedule if one was calculated or nullopt else
     */
    std::optional<ReconnectSchedule> getReconnectSchedule(const DataTypes::Experimental::Waypoint& currentLocation,
                                                          const DataTypes::Experimental::GeoLocation& parentLocation,
                                                          const S2PointIndex<uint64_t>& FieldNodeIndex,
                                                          bool isIndexUpdted);

    virtual ~PreCalculatedReconnectSchedulePredictor();
    std::optional<std::pair<NES::WorkerId, NES::Timestamp>> getReconnect(WorkerId currentParent);
  private:
    void loadReconnectSimulationDataFromFile();
    Timestamp startTime;
    std::string csvPath;
    std::vector<std::pair<WorkerId, Timestamp>> reconnects;
    uint64_t nextReconnectIndex = 0;
};
}

#endif//NES_PRECALCULATEDRECONNECTSCHEDULEPREDICTOR_HPP
