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

#include <iostream>

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerMobilityConfiguration.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Exceptions/CoordinatesOutOfRangeException.hpp>
#include <Exceptions/LocationProviderException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <NesBaseTest.hpp>
#include <Services/QueryService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Spatial/DataTypes/GeoLocation.hpp>
#include <Spatial/DataTypes/Waypoint.hpp>
#include <Spatial/Index/LocationIndex.hpp>
#include <Spatial/Mobility/LocationProviders/LocationProvider.hpp>
#include <Spatial/Mobility/LocationProviders/LocationProviderCSV.hpp>
#include <Spatial/Mobility/ReconnectSchedulePredictors/ReconnectPoint.hpp>
#include <Spatial/Mobility/ReconnectSchedulePredictors/ReconnectSchedule.hpp>
#include <Spatial/Mobility/ReconnectSchedulePredictors/ReconnectSchedulePredictor.hpp>
#include <Spatial/Mobility/WorkerMobilityHandler.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/S2Utilities.hpp>
#include <Util/Experimental/SpatialType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/TimeMeasurement.hpp>
#include <gtest/gtest.h>

using std::map;
using std::string;
uint16_t timeout = 5;
namespace NES::Spatial {

class SimulationDublin : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LocationIntegrationTests.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LocationIntegrationTests test class.");

        std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> waypoints;
        waypoints.push_back({{52.55227464714949, 13.351743136322877}, 0});
        waypoints.push_back({{2.574709862890394, 13.419206057808077}, 1000000000});
        waypoints.push_back({{2.61756571840606, 13.505980882863446}, 2000000000});
        waypoints.push_back({{2.67219559419452, 13.591124924963108}, 3000000000});
        auto csvPath = std::string(TEST_DATA_DIRECTORY) + "testLocations.csv";
        remove(csvPath.c_str());
        writeWaypointsToCsv(csvPath, waypoints);

        std::string singleLocationPath = std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv";
        remove(singleLocationPath.c_str());
        writeWaypointsToCsv(singleLocationPath, {{{52.55227464714949, 13.351743136322877}, 0}});

#ifdef S2DEF
        auto interpolatedCsv = std::string(TEST_DATA_DIRECTORY) + "path1.csv";
        remove(interpolatedCsv.c_str());
        std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> waypointsToInterpolate;
        waypointsToInterpolate.push_back({{52.58210307572243, 12.987507417206261}, 0});
        waypointsToInterpolate.push_back({{52.5225665088927, 13.198478869225813}, 1000000000});
        waypointsToInterpolate.push_back({{52.5824815034542, 13.280594641984383}, 2000000000});
        waypointsToInterpolate.push_back({{52.5251960754162, 13.400310793986574}, 3000000000});
        waypointsToInterpolate.push_back({{52.51309876750171, 13.57837236374691}, 5000000000});
        auto interpolatedPath = interpolatePath(waypointsToInterpolate, 1000);
        writeWaypointsToCsv(interpolatedCsv, interpolatedPath);
#endif

        auto inputSequence = std::string(TEST_DATA_DIRECTORY) + "sequence_long.csv";
        std::ofstream inputSequenceStream(inputSequence);
        for (int i = 1; i < 100000; ++i) {
            inputSequenceStream << std::to_string(i) << std::endl;
        }
        inputSequenceStream.close();
        ASSERT_FALSE(inputSequenceStream.fail());
    }

    std::string location2 = "52.53736960143897, 13.299134894776092";
    std::string location3 = "52.52025049345923, 13.327886280405611";
    std::string location4 = "52.49846981391786, 13.514464421192917";
    std::chrono::duration<int64_t, std::milli> defaultTimeoutInSec = std::chrono::seconds(TestUtils::defaultTimeout);

    struct getGeolocationParameters {
        getGeolocationParameters(TopologyNodePtr node, TopologyManagerServicePtr service) : node(node), service(service) {}
        TopologyNodePtr node;
        TopologyManagerServicePtr service;
    };

    //wrapper function so allow the util function to call the member function of LocationProvider
    static std::optional<NES::Spatial::DataTypes::Experimental::GeoLocation>
    getLocationFromTopologyNode(const std::shared_ptr<void>& structParams) {
        auto casted = std::static_pointer_cast<getGeolocationParameters>(structParams);
        return casted->service->getGeoLocationForNode(casted->node->getId());
    }

    /**
     * @brief wait until the topology contains the expected number of nodes so we can rely on these nodes being present for
     * the rest of the test
     * @param timeoutSeconds time to wait before aborting
     * @param nodes expected number of nodes
     * @param topology  the topology object to query
     * @return true if expected number of nodes was reached. false in case of timeout before number was reached
     */
    static bool waitForNodes(int timeoutSeconds, size_t nodes, TopologyPtr topology) {
        size_t numberOfNodes = 0;
        for (int i = 0; i < timeoutSeconds; ++i) {
            auto topoString = topology->toString();
            numberOfNodes = std::count(topoString.begin(), topoString.end(), '\n');
            numberOfNodes -= 1;
            if (numberOfNodes == nodes) {
                break;
            }
        }
        return numberOfNodes == nodes;
    }

#ifdef S2DEF
    /**
     * @brief check if two location objects latitudes and longitudes do not differ more than the specified error
     * @param location1
     * @param location2
     * @param error the tolerated difference in latitute or longitude specified in degrees
     * @return true if location1 and location2 do not differ more then the specified degrees in latitiude of logintude
     */
    static bool isClose(NES::Spatial::DataTypes::Experimental::GeoLocation location1,
                        NES::Spatial::DataTypes::Experimental::GeoLocation location2,
                        double error) {
        if (std::abs(location1.getLatitude() - location2.getLatitude()) > error) {
            return false;
        }
        if (std::abs(location1.getLongitude() - location2.getLongitude()) > error) {
            return false;
        }
        return true;
    }

    static std::vector<NES::Spatial::DataTypes::Experimental::Waypoint>
    interpolatePath(std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> waypoints, Timestamp amount) {
        std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> interpolatedPath;
        Timestamp step = waypoints.back().getTimestamp().value() / amount;
        for (auto waypointsItr = waypoints.cbegin(); waypointsItr != waypoints.cend(); ++waypointsItr) {
            auto next = waypointsItr + 1;
            if (next == waypoints.cend()) {
                interpolatedPath.emplace_back(*waypointsItr);
                break;
            }
            auto prevLocation = waypointsItr->getLocation();
            auto prevTime = waypointsItr->getTimestamp().value();
            auto nextLocation = next->getLocation();
            auto nextTime = next->getTimestamp().value();
            Timestamp timerange = nextTime - prevTime;
            S2Point prevPoint = NES::Spatial::Util::S2Utilities::geoLocationToS2Point(prevLocation);
            S2Point nextPoint = NES::Spatial::Util::S2Utilities::geoLocationToS2Point(nextLocation);
            S2Polyline path({prevPoint, nextPoint});
            for (Timestamp i = 0; i < timerange; i += step) {
                double frac = (double) i / (double) timerange;
                S2LatLng interpolated(path.Interpolate(frac));
                interpolatedPath.emplace_back(NES::Spatial::DataTypes::Experimental::GeoLocation(interpolated.lat().degrees(),
                                                                                                 interpolated.lng().degrees()),
                                              prevTime + i);
            }
        }
        return interpolatedPath;
    }

    struct WorkerGeoData {
        std::string radioType; //todo: make this an enum
        NES::Spatial::DataTypes::Experimental::GeoLocation location;
        uint64_t id;
    };

    std::vector<std::pair<TopologyNodeId, NES::Spatial::DataTypes::Experimental::GeoLocation>>
    getNodeIdsInRange(const NES::Spatial::DataTypes::Experimental::GeoLocation& center, double radius, const std::vector<WorkerGeoData>& geoData) const {
#ifdef S2DEF
        S2PointIndex<TopologyNodeId> workerPointIndex;
        for (const auto& data : geoData) {
            //NES_DEBUG2("inserting node with lat = {}, lon = {}", data.location.getLatitude(), data.location.getLongitude());

            auto s2point = NES::Spatial::Util::S2Utilities::geoLocationToS2Point(data.location);
            workerPointIndex.Add(s2point, data.id);
        }
        NES_DEBUG2("size of location index = {}", workerPointIndex.num_points());
        S2ClosestPointQuery<TopologyNodeId> query(&workerPointIndex);
        query.mutable_options()->set_max_distance(S1Angle::Radians(S2Earth::KmToRadians(radius)));

        S2ClosestPointQuery<TopologyNodeId>::PointTarget target(
            S2Point(S2LatLng::FromDegrees(center.getLatitude(), center.getLongitude())));
        auto result = query.FindClosestPoints(&target);
        std::vector<std::pair<TopologyNodeId, NES::Spatial::DataTypes::Experimental::GeoLocation>> closestNodeList;
        for (auto r : result) {
            auto latLng = S2LatLng(r.point());
            closestNodeList.emplace_back(
                r.data(),
                NES::Spatial::DataTypes::Experimental::GeoLocation(latLng.lat().degrees(), latLng.lng().degrees()));
        }
        return closestNodeList;
#else
        NES_WARNING2("Files were compiled without s2, cannot find closest nodes");
        (void) center;
        (void) radius;
        return {};
#endif
    }

    std::vector<WorkerGeoData> loadFixedNodesFromCSV(std::string csvPath) {
        std::string csvLine;
        std::ifstream inputStream(csvPath);
        std::string latitudeString;
        std::string longitudeString;
        std::string idString;
        std::basic_string<char> delimiter = {','};
        std::vector<WorkerGeoData> geoDataList;

        NES_DEBUG2("loading fixed node locations from {}", csvPath)

        //read locations and time offsets from csv, calculate absolute timestamps from offsets by adding start time
        while (std::getline(inputStream, csvLine)) {
            std::stringstream stringStream(csvLine);
            std::vector<std::string> values;
            try {
                values = NES::Util::splitWithStringDelimiter<std::string>(csvLine, delimiter);
            } catch (std::exception& e) {
                std::string errorString =
                    std::string("An error occurred while splitting delimiter of workerGeoData CSV. ERROR: ") + strerror(errno);
                NES_ERROR2("LocationProviderCSV:  {}", errorString);
                throw NES::Spatial::Exception::LocationProviderException(errorString);
            }
            if (values.size() != 14) {
                std::string errorString =
                    std::string("LoationProviderCSV: could not read waypoints from csv, expected 3 columns but input file has ")
                    + std::to_string(values.size()) + std::string(" columns");
                NES_ERROR2("LocationProviderCSV:  {}", errorString);
                throw NES::Spatial::Exception::LocationProviderException(errorString);
            }
            //todo: why are these backwards?
            latitudeString = values[7];
            longitudeString = values[6];
            idString = values[4];

            uint64_t id;
            double latitude;
            double longitude;
            try {
                id = std::stoul(idString);
                latitude = std::stod(latitudeString);
                longitude = std::stod(longitudeString);
            } catch (std::exception& e) {
                std::string errorString = std::string("An error occurred while creating the workerGeoData. ERROR: ") + strerror(errno);
                NES_ERROR2("LocationProviderCSV: {}", errorString);
                throw NES::Spatial::Exception::LocationProviderException(errorString);
            }
            NES_TRACE2("Read from csv: {}, {}, {}", latitudeString, longitudeString, id);

            //construct a pair containing a location and the id at which the device is at exactly that point
            // and add it to a vector containing all waypoints
            auto workerGeoData = WorkerGeoData(values[0], DataTypes::Experimental::GeoLocation(latitude, longitude), id);
            geoDataList.push_back(workerGeoData);
        }
        if (geoDataList.empty()) {
            std::string errorString = std::string("No data in CSV, cannot start location provider");
            NES_ERROR2("LocationProviderCSV: {}", errorString);
            throw NES::Spatial::Exception::LocationProviderException(errorString);
        }
        NES_DEBUG2("read {} fixed node locations from csv", geoDataList.size());
        //set first csv entry as the next waypoint
        return geoDataList;
    }
#endif

    static void TearDownTestCase() {
        NES_INFO("Tear down LocationIntegrationTests class.");
        std::string singleLocationPath = std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv";
        remove(singleLocationPath.c_str());
        std::string testLocationsPath = std::string(TEST_DATA_DIRECTORY) + "testLocations.csv";
        remove(testLocationsPath.c_str());
        auto interpolatedCsv = std::string(TEST_DATA_DIRECTORY) + "path1.csv";
        remove(interpolatedCsv.c_str());
        auto inputSequence = std::string(TEST_DATA_DIRECTORY) + "sequence_long.csv";
        remove(inputSequence.c_str());
    }
};

#ifdef S2DEF
TEST_F(SimulationDublin, simulation1) {
    NES_INFO(" start coordinator");
    /*
    std::string testFile = getTestResourceFolder() / "sequence_with_reconnecting_out.csv";

    std::string compareString;
    std::ostringstream oss;
    oss << "seq$value:INTEGER" << std::endl;
    for (int i = 1; i <= 10000; ++i) {
        oss << std::to_string(i) << std::endl;
        compareString = oss.str();
    }

    NES_INFO("rest port = " << *restPort);
     */
    std::vector<NES::Spatial::DataTypes::Experimental::GeoLocation> lteInRange;
    {
        auto geoDatalist =
            loadFixedNodesFromCSV("/home/x/uni/ba/clion/nebulastream/nes-core/tests/test_data/ireland_celltowers.csv");
        std::vector<WorkerGeoData> lteCellsOnly;
        for (const auto& geoData : geoDatalist) {
            if (geoData.radioType == "LTE") {
            //if (geoData.radioType == "GSM") {
                lteCellsOnly.push_back(geoData);
            }
        }
        NES_DEBUG2("length of lte only list {}", lteCellsOnly.size());
        auto ltePairsInRange = getNodeIdsInRange({53.34575018230248, -6.264844767252219}, 0.3, lteCellsOnly);
        NES_DEBUG2("length of lte in range list {}", lteInRange.size());
        for (auto pair : ltePairsInRange) {
            lteInRange.push_back(pair.second);
        }
    }

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restServerCorsAllowedOrigin.setValue("http://localhost:3000/");
    //coordinatorConfig->restPort.setValue(*restPort);
    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully")

    TopologyPtr topology = crd->getTopology();
    ASSERT_TRUE(waitForNodes(5, 1, topology));

    TopologyNodePtr node = topology->getRoot();
    /*
    std::vector<NES::Spatial::DataTypes::Experimental::GeoLocation> locVec = {
        {52.53024925374664, 13.440408001670573},  {52.44959193751221, 12.994693532702838},
        {52.58394737653231, 13.404557656002641},  {52.48534029037908, 12.984138457171484},
        {52.37433823627218, 13.558651957244951},  {52.51533875315059, 13.241771507925069},
        {52.55973107205912, 13.015653271890772},  {52.63119966549814, 13.441159505328082},
        {52.52554704888443, 13.140415389311752},  {52.482596286130494, 13.292443465145574},
        {52.54298642356826, 13.73191525503437},   {52.42678133005856, 13.253118169911525},
        {52.49621174869779, 13.660943763979146},  {52.45590365225229, 13.683553731893118},
        {52.62859441558, 13.135969230535936},     {52.49564618880393, 13.333672868668472},
        {52.58790396655713, 13.283405589901832},  {52.43730546215479, 13.288472865017477},
        {52.452625895558846, 13.609715377620118}, {52.604381034747234, 13.236153100778251},
        {52.52406858008703, 13.202905224067974},  {52.48532771063918, 13.248322218507269},
        {52.50023010173765, 13.35516100143647},   {52.5655774963026, 13.416236069617133},
        {52.56839177666675, 13.311990021109548},  {52.42881523569258, 13.539510531504995},
        {52.55745803205775, 13.521177091034348},  {52.378590211721814, 13.39387224077735},
        {52.45968932886132, 13.466172426273232},  {52.60131778672673, 13.6759151640276},
        {52.59382248148305, 13.17751716953493},   {52.51690603363213, 13.627430091500505},
        {52.40035318355461, 13.386405495784041},  {52.49369404130713, 13.503477002208028},
        {52.52102316662499, 13.231109595273479},  {52.6264057419334, 13.239482930461145},
        {52.45997462557177, 13.038370380285766},  {52.405581430754694, 12.994506535621692},
        {52.5165220102255, 13.287867202522792},   {52.61937748717004, 13.607622490869543},
        {52.620153404197254, 13.236774758123099}, {52.53095039302521, 13.150218024942914},
        {52.60042748492653, 13.591960614892749},  {52.44688258081577, 13.091132219453291},
        {52.44810624782493, 13.189186365976528},  {52.631904019035325, 13.099599387131189},
        {52.51607843891218, 13.361003233097668},  {52.63920358795863, 13.365640690678045},
        {52.51050545031392, 13.687455299147123},  {52.42516226249599, 13.597154340475155},
        {52.585620728658185, 13.177440252255762}, {52.54251642039891, 13.270687079693818},
        {52.62589583837628, 13.58922212327232},   {52.63840628658707, 13.336777486335386},
        {52.382935034604074, 13.54689828854007},  {52.46173261319607, 13.637993027984113},
        {52.45558349451082, 13.774558360650097},  {52.50660545385822, 13.171564805090318},
        {52.38586011054127, 13.772290920473052},  {52.4010561708298, 13.426889487526187}};
        */


    //these variables are needed if we only create field nodes and no workers
    std::shared_ptr<TopologyManagerService> topologyManagerService = crd->getTopologyManagerService();
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;

    size_t idCount = 20001;

    std::vector<NesWorkerPtr> fieldNodes;
    //for (auto elem : locVec) {
    for (auto elem : lteInRange) {
        /*
        TopologyNodePtr currNode = TopologyNode::create(idCount, "127.0.0.1", 1, 0, 0, properties);
        topology->addNewTopologyNodeAsChild(node, currNode);
        topologyManagerService->addGeoLocation(currNode->getId(), NES::Spatial::DataTypes::Experimental::GeoLocation(elem));
        idCount++;
         */
        //nodeIndex.Add(NES::Spatial::Util::S2Utilities::geoLocationToS2Point(elem), currNode->getId());

        WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
        wrkConf->coordinatorPort.setValue(*rpcCoordinatorPort);
        wrkConf->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::FIXED_LOCATION);
        wrkConf->locationCoordinates.setValue(elem);
        wrkConf->enableMonitoring.setValue(false);
        NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(wrkConf));
        fieldNodes.push_back(wrk);
        bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
        ASSERT_TRUE(retStart);
    }
    //ASSERT_TRUE(waitForNodes(5, lteInRange.size() + 1, topology));
    //string singleLocStart = "52.55227464714949, 13.351743136322877";
    //crd->getSourceCatalog()->addLogicalSource("seq", "Schema::create()->addField(createField(\"value\", BasicType::UINT64));");

    /*
    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
     */

    /*
    auto stype = CSVSourceType::create();
    stype->setFilePath(std::string(TEST_DATA_DIRECTORY) + "sequence_long.csv");
    stype->setNumberOfBuffersToProduce(1000);
    stype->setNumberOfTuplesToProducePerBuffer(10);
    stype->setGatheringInterval(1);
    auto sequenceSource = PhysicalSource::create("seq", "test_stream", stype);
    wrkConf1->physicalSources.add(sequenceSource);
     */

    /*
    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf1->mobilityConfiguration.nodeInfoDownloadRadius.setValue(20000);
    wrkConf1->mobilityConfiguration.nodeIndexUpdateThreshold.setValue(5000);
    wrkConf1->mobilityConfiguration.mobilityHandlerUpdateInterval.setValue(10);
    wrkConf1->mobilityConfiguration.locationBufferSaveRate.setValue(1);
    wrkConf1->mobilityConfiguration.pathPredictionLength.setValue(40000);
    wrkConf1->mobilityConfiguration.defaultCoverageRadius.setValue(5000);
    wrkConf1->mobilityConfiguration.mobilityHandlerUpdateInterval.setValue(1000);
    wrkConf1->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "path1.csv");
     */

    //NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    //bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    //ASSERT_TRUE(retStart1);
    //ASSERT_TRUE(waitForNodes(5, lteInRange.size() + 2, topology));

    /*
    QueryId queryId = crd->getQueryService()->validateAndQueueAddQueryRequest(
        R"(Query::from("seq").sink(FileSinkDescriptor::create(")" + testFile + R"(", "CSV_FORMAT", "APPEND"));)",
        "BottomUp",
        FaultToleranceType::NONE,
        LineageType::NONE);

    NES_INFO("Query ID: " << queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);
    size_t recv_tuples = 0;
    auto startTimestamp = std::chrono::system_clock::now();
    while (recv_tuples < 10000 && std::chrono::system_clock::now() < startTimestamp + defaultTimeoutInSec) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile), std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("received: " << recv_tuples)
        sleep(1);
    }

    ASSERT_EQ(recv_tuples, 10001);
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareString, testFile));

    int response = remove(testFile.c_str());
    ASSERT_TRUE(response == 0);
     */
    getchar();

    cout << "stopping worker" << endl;
    //bool retStopWrk = wrk1->stop(false);
    //ASSERT_TRUE(retStopWrk);

    for (const auto& w : fieldNodes) {
        bool stop = w->stop(false);
        ASSERT_TRUE(stop);
    }

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}
#endif
}// namespace NES::Spatial
