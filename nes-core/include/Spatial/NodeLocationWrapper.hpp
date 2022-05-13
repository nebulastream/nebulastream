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
#ifndef NES_GEOLOCATION_LOCATIONSERVICE_HPP
#define NES_GEOLOCATION_LOCATIONSERVICE_HPP

#include <Util/Experimental/LocationProviderType.hpp>
#include <memory>
#include <s2/s2point.h>
#include <s2/s2point_index.h>
#include <thread>
#include <vector>
#include <deque>
#include <Util/TimeMeasurement.hpp>

class S2Polyline;
using S2PolylinePtr = std::shared_ptr<S2Polyline>;

namespace NES {
class CoordinatorRPCClient;
using CoordinatorRPCClientPtr = std::shared_ptr<CoordinatorRPCClient>;
}// namespace NES

namespace NES::Spatial::Index::Experimental {
class Location;
using LocationPtr = std::shared_ptr<Location>;
}// namespace NES::Spatial::Index::Experimental
namespace NES::Spatial::Mobility::Experimental {
class LocationProvider;
using LocationProviderPtr = std::shared_ptr<LocationProvider>;

class ReconnectSchedule;
using ReconnectSchedulePtr = std::shared_ptr<ReconnectSchedule>;

//todo: make this a member variale and set the default via config
//todo: check what alternatives there are to sleep() in order to use more finegrained intervals
const uint64_t kDefaultUpdateInterval = 10;
const size_t kDefaultLocBufferSize = 10;
const size_t kDefaultSaveRate = 4;
//todo: do we want this in absolute degrees or relative to the speed?
const double kPathDistanceDelta = 1;
const double kPathExtensionFactor = 10;
const double kSaveNodesRange = 50;
const double kDefaultCoverage = 3;
const double kPathLength = 200;

class NodeLocationWrapper {
  public:
    explicit NodeLocationWrapper(bool isMobile, Index::Experimental::Location fieldNodeLoc);

    /**
     * Experimental
     * @brief construct a mobile worker location provider. This function is experimental.
     * @param type defines the the subclass of locationProvider to be used
     * @param config the config parameters for the location provider
     * @return
     */
    bool createLocationProvider(LocationProviderType type, std::string config);

    /**
     * Experimental
     * Set the rpcClient which this LocationWrapper can use to communicate with the coordinator
     * @param rpcClientPtr
     */
    void setUpReconnectPlanning(CoordinatorRPCClientPtr rpcClientPtr);
    /**
     * Experimental
     * @brief checks if this Worker runs on a non-mobile device with a known location (Field Node)
     */
    bool isFieldNode();

    /**
     * Experimental
     * @brief check if this worker runs on a mobile device
     */
    [[nodiscard]] bool isMobileNode() const;

    /**
     * Experimental
     * @brief returns an optional containing a Location object if the node has a fixed location or
     * containing a nullopt_t if the node does not have a location
     * @return optional containing the Location
     */
    Index::Experimental::LocationPtr getLocation();

    Mobility::Experimental::ReconnectSchedulePtr getReconnectSchedule();
    /**
     * Experimental
     * @brief Method to get all field nodes within a certain range around a geographical point
     * @param coord: Location representing the center of the query area
     * @param radius: radius in km to define query area
     * @return list of node IDs and their corresponding GeographicalLocations
     */
    std::vector<std::pair<uint64_t, Index::Experimental::Location>> getNodeIdsInRange(Index::Experimental::Location coord,
                                                                                      double radius);

    /**
     * Experimental
     * @brief Method to get all field nodes within a certain range around the location of this node
     * @param radius = radius in km to define query area
     * @return list of node IDs and their corresponding GeographicalLocations
     */
    std::vector<std::pair<uint64_t, Index::Experimental::Location>> getNodeIdsInRange(double radius);

    /**
     * Experimental
     */
    [[noreturn]] void startReconnectPlanning();

    /**
     * @brief method to set the Nodes Location. it does not update the topology and is meant for initialization
     * @param geoLoc: The new fixed Location to be set
     * @return success of operation
     */
    bool setFixedLocationCoordinates(const Index::Experimental::Location& geoLoc);

    /**
     * @brief getter function for the location provider used to get the devices current location
     * @return a pointer to the provider object
     */
    LocationProviderPtr getLocationProvider();

    void setCoordinatorRPCCLient(CoordinatorRPCClientPtr coordinatorClient);

    static std::pair<S2Point, S1Angle> findPathCoverage(S2PolylinePtr path, S2Point coveringNode, S1Angle coverage);

  private:
    void updatePredictedPath(NES::Spatial::Index::Experimental::LocationPtr oldLoc, NES::Spatial::Index::Experimental::LocationPtr newLoc);
    void scheduleReconnects(const NES::Spatial::Index::Experimental::LocationPtr& currentLoc);
    CoordinatorRPCClientPtr coordinatorRpcClient;
    Index::Experimental::LocationPtr fixedLocationCoordinates;
    bool isMobile;
    LocationProviderPtr locationProvider;
    std::deque<std::pair<NES::Spatial::Index::Experimental::LocationPtr, Timestamp>> locationBuffer;

    std::shared_ptr<std::thread> locationUpdateThread;

    uint64_t updateInterval;
    size_t locBufferSize;
    size_t saveRate;
    //todo: make these pointers inf necessary. would allow forward declarations
    S2Point pathBeginning;
    S2Point pathEnd;
    S2PolylinePtr trajectoryLine;
    S2PointIndex<uint64_t> fieldNodeIndex;
    //todo: maybe use a cap for this instead?
    NES::Spatial::Index::Experimental::LocationPtr positionOfLastNodeIndexUpdate;
    std::vector<std::tuple<uint64_t, NES::Spatial::Index::Experimental::LocationPtr, Timestamp>> reconnectVector;
};

}//namespace NES::Spatial::Mobility::Experimental
#endif//NES_GEOLOCATION_LOCATIONSERVICE_HPP
