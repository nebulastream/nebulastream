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

#ifndef NES_TRAJECTORYPREDICTOR_HPP
#define NES_TRAJECTORYPREDICTOR_HPP

#include <memory>
#include <thread>
#include <vector>
#include <Util/TimeMeasurement.hpp>
#include <mutex>

#ifdef S2DEF
#include <s2/s1chord_angle.h>
#include <s2/s2point.h>
#include <s2/s2point_index.h>
#include <s2/s2closest_point_query.h>
#include <s2/s2earth.h>
#include <s2/s2point.h>
#include <s2/s2polyline.h>
#endif

namespace NES::Configurations::Spatial::Mobility::Experimental {
class WorkerMobilityConfiguration;
using WorkerMobilityConfigurationPtr = std::shared_ptr<WorkerMobilityConfiguration>;
}

using S2PolylinePtr = std::shared_ptr<S2Polyline>;
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

class TrajectoryPredictor {
  public:
    TrajectoryPredictor(LocationProviderPtr locationProvider, Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr configuration, uint64_t parentId);

    Mobility::Experimental::ReconnectSchedulePtr getReconnectSchedule();

    /**
     * Experimental
     */
    void setUpReconnectPlanning(ReconnectConfiguratorPtr reconnectConfigurator);

    /**
     * Experimental
     */
    void startReconnectPlanning();

    bool stopReconnectPlanning();

    static std::pair<S2Point, S1Angle> findPathCoverage(const S2PolylinePtr& path, S2Point coveringNode, S1Angle coverage);

    NES::Spatial::Index::Experimental::Location getNodeLocationById(uint64_t id);

    size_t getSizeOfSpatialIndex();

  private:
    bool updatePredictedPath(const NES::Spatial::Index::Experimental::LocationPtr& oldLocation, const NES::Spatial::Index::Experimental::LocationPtr& currentLocation);

    void scheduleReconnects();

    std::recursive_mutex trajectoryLineMutex;
    std::recursive_mutex indexUpdatePositionMutex;
    std::recursive_mutex nodeIndexMutex;
    std::recursive_mutex reconnectVectorMutex;
    std::atomic<bool> updatePrediction;

    LocationProviderPtr locationProvider;

    std::deque<std::pair<NES::Spatial::Index::Experimental::LocationPtr, Timestamp>> locationBuffer;

    std::shared_ptr<std::thread> locationUpdateThread;

    uint64_t pathPredictionUpdateInterval;
    size_t locationBufferSize;
    size_t locationBufferSaveRate;
    //todo: make these pointers inf necessary. would allow forward declarations
    NES::Timestamp endOfCoverageETA;
    S2PolylinePtr trajectoryLine;
    //todo: check which angles need to be s1angle and which ones can be chorda angles
    S1Angle predictedPathLengthAngle;
    S1Angle pathDistanceDeltaAngle;
    S2PointIndex<uint64_t> fieldNodeIndex;
    //todo: we can remove this on later, once we add it as a part of the tuple in th ereconnect vector
    std::unordered_map<uint64_t, S2Point> fieldNodeMap;
    double nodeInfoDownloadRadius;
    S1Angle reconnectSearchRadius;
    //uint64_t nodeIndexUpdateThreshold;
    S1Angle coveredRadiusWithoutThreshold;
    //todo: maybe use a cap for this instead?
    S2Point currentParentLocation;
    uint64_t parentId;

    S1Angle defaultCoverageRadiusAngle;
    std::optional<S2Point> positionOfLastNodeIndexUpdate;
    std::vector<std::tuple<uint64_t, NES::Spatial::Index::Experimental::LocationPtr, Timestamp>> reconnectVector;
    bool updateDownloadedNodeIndex(Index::Experimental::Location currentLocation);
    bool downloadFieldNodes();

    ReconnectConfiguratorPtr reconnectConfigurator;
    std::optional<std::tuple<uint64_t, Index::Experimental::LocationPtr, Timestamp>> getNextReconnect();
};

}

#endif//NES_TRAJECTORYPREDICTOR_HPP
