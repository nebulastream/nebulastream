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

#ifndef NES_WORKER_INCLUDE_MOBILITY_RECONNECTSCHEDULEPREDICTORS_RECONNECTSCHEDULEPREDICTOR_HPP_
#define NES_WORKER_INCLUDE_MOBILITY_RECONNECTSCHEDULEPREDICTORS_RECONNECTSCHEDULEPREDICTOR_HPP_

#include <Util/Mobility/ReconnectPoint.hpp>
#include <Util/TimeMeasurement.hpp>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

#ifdef S2DEF
#include <s2/s1chord_angle.h>
#include <s2/s2closest_point_query.h>
#include <s2/s2earth.h>
#include <s2/s2point.h>
#include <s2/s2point_index.h>
#include <s2/s2polyline.h>
#endif

namespace NES {

namespace Configurations::Spatial::Mobility::Experimental {
class WorkerMobilityConfiguration;
using WorkerMobilityConfigurationPtr = std::shared_ptr<WorkerMobilityConfiguration>;
}// namespace Configurations::Spatial::Mobility::Experimental

namespace Spatial::DataTypes::Experimental {
using NodeIdToGeoLocationMap = std::unordered_map<uint64_t, GeoLocation>;
}// namespace Spatial::DataTypes::Experimental

namespace Spatial::Mobility::Experimental {

class ReconnectSchedule;
using ReconnectSchedulePtr = std::unique_ptr<ReconnectSchedule>;
class ReconnectSchedulePredictor;
using ReconnectSchedulePredictorPtr = std::shared_ptr<ReconnectSchedulePredictor>;

/**
 * @brief this class uses mobile device location data in order to make a prediction about the devices future trajectory and creates a schedule
 * of planned reconnects to new field nodes. It also triggers the reconnect process when the device is sufficiently close to the new parent
 * This class is not thread safe!
 */
class ReconnectSchedulePredictor {
  public:
    ReconnectSchedulePredictor(
        const Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& configuration);

    /**
     * Creates a new reconnect schedule predictor if NES was compiles with NES_USE_S2. Otherwise returns a nullptr
     * @param configuration The configuration for the reconnect schedulue predictor to be created
     * @return a pointer to the created reconnect schedule creator or nullptr is s2 is not activated
     */
    static ReconnectSchedulePredictorPtr
    create(const Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& configuration);

#ifdef S2DEF
    /**
     * @brief calculate the distance between the projected point on the path which is closest to coveringNode and the a point on the path
     * which is at a distance of exactly the coverage. This distance equals half of the entire distance on the line covered by a
     * circle with a radius equal to coverage. The function also returns a Location object for the point which is at exactly a distance
     * of coverage and is further away from the beginning of the path (from the vertex with index 0) than the point which is
     * closest to coveringNode. We can call this function on multiple coveringNodes within range of the device or the end of the
     * previous coverage to compare which field nodes coverage ends at the point which is closest to the end of the path and
     * therefore represents a good reconnect decision.
     * @param path : a polyline representing the predicted device trajectory
     * @param coveringNode : the position of the field node
     * @param coverage : the coverage distance of the field node
     * @return a pair containing an S2Point marking the end of the coverage and an S1Angle representing the distance between the
     * projected point closest to covering node and the end of coverage (half of the entire covered distance)
     */
    static std::pair<S2Point, S1Angle> findPathCoverage(const S2Polyline& path, S2Point coveringNode, S1Angle coverage);

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
#endif
  private:
    /**
     * check if the device deviated further than the defined distance threshold from the predicted path. If so, interpolate a new
     * path by drawing a line from an old position through the current position
     * @param newPathStart : a previous device position (obtained from the location buffer)
     * @param currentLocation : the current device position
     * @return true if the trajectory was recalculated, false if the device did not deviate further than the threshold
     */
    bool updatePredictedPath(const NES::Spatial::DataTypes::Experimental::GeoLocation& newPathStart,
                             const NES::Spatial::DataTypes::Experimental::GeoLocation& currentLocation);

#ifdef S2DEF
    /**
     * @brief find the minimal covering set of field nodes covering the predicted path. This represents the reconnect schedule
     * with the least possible reconnects along the predicted trajectory. Use the average movement speed to estimate the time
     * at which each reconnect will happen.
     * @param currentParendLocation : The location of the workers current parent
     * @param fieldNodeIndex : a spatial index containing ids of fixed location nodes
     */
    bool scheduleReconnects(const S2Point& currentParentLocation, const S2PointIndex<uint64_t>& fieldNodeIndex);
#endif

    /**
     * @brief use positions and timestamps in the location buffer to calculate the devices average  movement speed during the
     * time interval covered by the location buffer and compare it to the previous movements speed. update the saved movement speed
     * if the new one differs more than the threshold.
     * @return true if the movement speed has changed more than threshold and the variable was therefore updated
     */
    bool updateAverageMovementSpeed();

    //todo #3365: implement a reset function to lose all non-config state
#ifdef S2DEF
    //configuration
    size_t locationBufferSize;
    size_t locationBufferSaveRate;
    double nodeInfoDownloadRadius;
    S1Angle predictedPathLengthAngle;
    S1Angle pathDistanceDeltaAngle;
    S1Angle reconnectSearchRadius;
    S1Angle defaultCoverageRadiusAngle;
    //prediction data
    std::optional<S2Polyline> trajectoryLine;
    std::deque<DataTypes::Experimental::Waypoint> locationBuffer;
    std::vector<NES::Spatial::Mobility::Experimental::ReconnectPoint> reconnectPoints;
    double bufferAverageMovementSpeed;
    double speedDifferenceThresholdFactor;
    size_t stepsSinceLastLocationSave;
#endif
};
}// namespace Spatial::Mobility::Experimental
}// namespace NES

#endif // NES_WORKER_INCLUDE_MOBILITY_RECONNECTSCHEDULEPREDICTORS_RECONNECTSCHEDULEPREDICTOR_HPP_
