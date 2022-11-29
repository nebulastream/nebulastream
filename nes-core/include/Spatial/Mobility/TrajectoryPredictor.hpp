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

#include <Spatial/Index/Location.hpp>
#include <Spatial/Mobility/ReconnectPrediction.hpp>
#include <Util/TimeMeasurement.hpp>
#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#ifdef S2DEF
#include <s2/s1chord_angle.h>
#include <s2/s2closest_point_query.h>
#include <s2/s2earth.h>
#include <s2/s2point.h>
#include <s2/s2point_index.h>
#include <s2/s2polyline.h>
#endif

namespace NES::Configurations::Spatial::Mobility::Experimental {
class WorkerMobilityConfiguration;
using WorkerMobilityConfigurationPtr = std::shared_ptr<WorkerMobilityConfiguration>;
}// namespace NES::Configurations::Spatial::Mobility::Experimental

#ifdef S2DEF
using S2PolylinePtr = std::shared_ptr<S2Polyline>;
#endif
namespace NES::Spatial::Index::Experimental {
class Location;
using LocationPtr = std::shared_ptr<Location>;
}// namespace NES::Spatial::Index::Experimental

namespace NES::Spatial::Mobility::Experimental {
class LocationProvider;
using LocationProviderPtr = std::shared_ptr<LocationProvider>;

class ReconnectSchedule;
using ReconnectSchedulePtr = std::shared_ptr<ReconnectSchedule>;

class ReconnectConfigurator;
using ReconnectConfiguratorPtr = std::shared_ptr<ReconnectConfigurator>;

struct ReconnectPrediction;
using ReconnectPredictionPtr = std::shared_ptr<ReconnectPrediction>;
struct ReconnectPoint;
using ReconnectPointPtr = std::shared_ptr<ReconnectPoint>;

/**
 * @brief this class uses mobile device location data in order to make a prediction about the devices future trajectory and creates a schedule
 * of planned reconnects to new field nodes. It also triggers the reconnect process when the device is sufficiently close to the new parent
 */
class TrajectoryPredictor {
  public:
    TrajectoryPredictor(LocationProviderPtr locationProvider,
                        const Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& configuration,
                        uint64_t parentId);

    /**
     * Experimental
     * @brief construct a reconnect schedule object containing beggining and end of the current predicted path, the position of the last
     * update of the local spatial index and a vector containing the scheduled reconnects
     * @return a smart pointer to the reconnect schedule object
     */
    Mobility::Experimental::ReconnectSchedulePtr getReconnectSchedule();

    /**
     * Experimental
     * @brief Download the field node locations in the vicinity into the local spatial index and start the reconnect planning thread
     */
    void setUpReconnectPlanning(ReconnectConfiguratorPtr reconnectConfigurator);

    /**
     * Experimental
     * @brief enter loop to periodically query for the current device location and update path prediction and reconnect schedule
     * accordingly as well as instructing the ReconnectConfigurator to perform a reconnect if the device is close enough to the new parent
     */
    void startReconnectPlanning();

    /**
     * Experimental
     * @brief Stop the reconnect planner thread
     * @return true if a thread was running, false otherwise
     */
    bool stopReconnectPlanning();

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

    /**
     * @brief returns the location of a field node if it exists in the local index
     * @param id : the id of the field node
     * @return the location of the node of an invalid Location if the node does not exist in the local index
     */
    NES::Spatial::Index::Experimental::Location getNodeLocationById(uint64_t id);

    /**
     * @return the amount of field node locations saved locally
     */
    size_t getSizeOfSpatialIndex();

    /**
     * @brief return the predicted next reconnect of the device
     * @return an optional containing a tuple consisting of the id of the expected new parent and reconnect location and time or
     * nullopt if no reconnect has been calculated
     */
    std::shared_ptr<ReconnectPoint> getNextPredictedReconnect();

    /**
     * @brief return position and time at which the last reconnect happened
     * @return a tuple containing the a Location and a time with the location being invalid if no reconnect has been recorded yet
     */
    //todo 2951: change return type to struct
    Index::Experimental::WaypointPtr getLastReconnectLocationAndTime();

  private:
    /**
     * check if the device deviated further than the defined distance threshold from the predicted path. If so, interpolate a new
     * path by drawing a line from an old position through the current position
     * @param newPathStart : a previous device position (obtained from the location buffer)
     * @param currentLocation : the current device position
     * @return true if the trajectory was recalculated, false if the device did not deviate further than the threshold
     */
    bool updatePredictedPath(const NES::Spatial::Index::Experimental::LocationPtr& newPathStart,
                             const NES::Spatial::Index::Experimental::LocationPtr& currentLocation);

    /**
     * @brief find the minimal covering set of field nodes covering the predicted path. This represents the reconnect schedule
     * with the least possible reconnects along the predicted trajectory. Use the average movement speed to estimate the time
     * at which each reconnect will happen.
     */
    void scheduleReconnects();

    /**
     * @brief check if the device has moved closer than the threshold to the edge of the area covered by the current local
     * spatial index. If so download new node data around the current location
     * @param currentLocation : the current location of the mobile device
     * @return true if the index was updated, false if the device is still further than the threshold away from the edge.
     */
    bool updateDownloadedNodeIndex(Index::Experimental::LocationPtr currentLocation);

    /**
     * @brief download the the field node locations within the configured distance around the devices position. If the list of the
     * downloaded positions is non empty, delete the old spatial index and replace it with the new data.
     * @param currentLocation : the device position
     * @return true if the received list of node positions was not empty
     */
    bool downloadFieldNodes(Index::Experimental::LocationPtr currentLocation);

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
    void reconnect(uint64_t newParentId, const Index::Experimental::WaypointPtr& ownLocation);

    /**
     * @brief: Perform a reconnect to change this workers parent in the topology to the closest node in the local node index and
     * update devicePositionTuplesAtLastReconnect, ParentId and currentParentLocation.
     * @param ownLocation: This workers current location
     */
    bool reconnectToClosestNode(const Index::Experimental::WaypointPtr& ownLocation);

    LocationProviderPtr locationProvider;
    ReconnectConfiguratorPtr reconnectConfigurator;

    std::shared_ptr<std::thread> locationUpdateThread;

    //mutexes
    std::recursive_mutex trajectoryLineMutex;
    std::recursive_mutex indexUpdatePositionMutex;
    std::recursive_mutex nodeIndexMutex;
    std::recursive_mutex reconnectVectorMutex;
    std::recursive_mutex lastReconnectTupleMutex;
    std::atomic<bool> updatePrediction;

    //configuration
    uint64_t pathPredictionUpdateInterval;
    size_t locationBufferSize;
    size_t locationBufferSaveRate;
    double nodeInfoDownloadRadius;
#ifdef S2DEF
    S1Angle predictedPathLengthAngle;
    S1Angle pathDistanceDeltaAngle;
    S1Angle reconnectSearchRadius;
    S1Angle coveredRadiusWithoutThreshold;
    S1Angle defaultCoverageRadiusAngle;

    //prediction data
    std::optional<S2Point> currentParentLocation;
    S2PolylinePtr trajectoryLine;
    std::unordered_map<uint64_t, S2Point> fieldNodeMap;
    std::optional<S2Point> positionOfLastNodeIndexUpdate;
    S2PointIndex<uint64_t> fieldNodeIndex;
#endif
    uint64_t parentId;
    std::deque<Index::Experimental::WaypointPtr> locationBuffer;
    std::shared_ptr<std::vector<std::shared_ptr<NES::Spatial::Mobility::Experimental::ReconnectPoint>>> reconnectVector;
    double bufferAverageMovementSpeed;
    double speedDifferenceThresholdFactor;
    Index::Experimental::WaypointPtr lastReconnectWaypoint;
};
}// namespace NES::Spatial::Mobility::Experimental
#endif// NES_CORE_INCLUDE_SPATIAL_MOBILITY_TRAJECTORYPREDICTOR_HPP_
