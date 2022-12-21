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

#ifndef NES_CORE_INCLUDE_SPATIAL_MOBILITY_TRAJECTORYPREDICTOR_HPP_
#define NES_CORE_INCLUDE_SPATIAL_MOBILITY_TRAJECTORYPREDICTOR_HPP_

#include <Util/TimeMeasurement.hpp>
#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#ifdef S2DEF
#include "ReconnectPoint.hpp"
#include <s2/s1chord_angle.h>
#include <s2/s2closest_point_query.h>
#include <s2/s2earth.h>
#include <s2/s2point.h>
#include <s2/s2point_index.h>
#include <s2/s2polyline.h>

using S2PolylinePtr = std::shared_ptr<S2Polyline>;
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

struct ReconnectPrediction;

/**
 * @brief this class uses mobile device location data in order to make a prediction about the devices future trajectory and creates a schedule
 * of planned reconnects to new field nodes. It also triggers the reconnect process when the device is sufficiently close to the new parent
 * This class is not thread safe!
 */
class ReconnectSchedulePredictor {
  public:
    ReconnectSchedulePredictor(const Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& configuration);

    /**
     * Experimental
     * @brief construct a reconnect schedule object containing beggining and end of the current predicted path, the position of the last
     * update of the local spatial index and a vector containing the scheduled reconnects
     * @return a smart pointer to the reconnect schedule object
     */
    Mobility::Experimental::ReconnectSchedulePtr getReconnectSchedule();

    /**
     * @brief calculate the distance between the projected point on the path which is closest to coveringNode and the a point on the path
     * which is at a distance of exactly the coverage. This distance equals half of the entire distance on the line covered by a
     * circle with a radius equal to coverage. The function also returns a Location object for the point which is at exactly a distance
     * of coverage and is further away from the beginning of the path (from the vertex with index 0) than the point which is
     * closest to coveringNode. We can call this function on multiple coverngNodes within range of the device or the end of the
     * previous coverage to compare which field nodes coverage ends at the point which is closest to the end of the path and
     * therefore represents a good reconnect decision.
     * @param path : a polyline representing the predicted device trajectory
     * @param coveringNode : the position of the field node
     * @param coverage : the coverage distance of the field node
     * @return a pair containing an S2Point marking the end of the coverage and an S1Angle representing the distance between the
     * projected point closest to covering node and the end of coverage (half of the entire covered distance)
     */
#ifdef S2DEF
    static std::pair<S2Point, S1Angle> findPathCoverage(const S2PolylinePtr& path, S2Point coveringNode, S1Angle coverage);
#endif

    ReconnectSchedulePtr getReconnectSchedule(const DataTypes::Experimental::Waypoint& currentLocation,
                                              const DataTypes::Experimental::GeoLocation& parentLocation,
                                              const S2PointIndex<uint64_t>& fieldNodeIndex,
                                              bool indexUpdated);
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

    /**
     * @brief find the minimal covering set of field nodes covering the predicted path. This represents the reconnect schedule
     * with the least possible reconnects along the predicted trajectory. Use the average movement speed to estimate the time
     * at which each reconnect will happen.
     * @param currentParendLocation : The location of the workers current parent
     * @param fieldNodeIndex : a spatial index containing ids of fixed location nodes
     */
    void scheduleReconnects(const S2Point &currentParentLocation, const S2PointIndex<uint64_t> &fieldNodeIndex );

    /**
     * @brief check if the device has moved closer than the threshold to the edge of the area covered by the current local
     * spatial index. If so download new node data around the current location
     * @param currentLocation : the current location of the mobile device
     * @return true if the index was updated, false if the device is still further than the threshold away from the edge.
     */
    bool updateDownloadedNodeIndex(const DataTypes::Experimental::GeoLocation& currentLocation);

    /**
     * @brief use positions and timestamps in the location buffer to calculate the devices average  movement speed during the
     * time interval covered by the location buffer and compare it to the previous movements speed. update the saved movement speed
     * if the new one differs more than the threshold.
     * @return true if the movement speed has changed more than threshold and the variable was therefore updated
     */
    bool updateAverageMovementSpeed();

    /**
     * @brief: Perform a reconnect to change this workers parent in the topology and update devicePositionTuplesAtLastReconnect,
     * ParentId and currentParentLocation.
     * @param newParentId: The id of the new parent to connect to
     * @param ownLocation: This workers current location
     */
    void reconnect(uint64_t newParentId);

    //configuration
    //uint64_t pathPredictionUpdateInterval;
    size_t locationBufferSize;
    size_t locationBufferSaveRate;
    double nodeInfoDownloadRadius;
#ifdef S2DEF
    S1Angle predictedPathLengthAngle;
    S1Angle pathDistanceDeltaAngle;
    S1Angle reconnectSearchRadius;
    S1Angle defaultCoverageRadiusAngle;

    //prediction data
    S2PolylinePtr trajectoryLine;
#endif
    std::deque<DataTypes::Experimental::Waypoint> locationBuffer;
    std::shared_ptr<std::vector<NES::Spatial::Mobility::Experimental::ReconnectPoint>> reconnectVector;
    double bufferAverageMovementSpeed;
    double speedDifferenceThresholdFactor;
    size_t stepsSinceLastLocationSave;
};
}// namespace Spatial::Mobility::Experimental
}// namespace NES

#endif// NES_CORE_INCLUDE_SPATIAL_MOBILITY_TRAJECTORYPREDICTOR_HPP_
