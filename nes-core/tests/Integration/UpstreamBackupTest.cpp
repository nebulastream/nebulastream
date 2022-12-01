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
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <NesBaseTest.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <chrono>
#include <cpprest/filestream.h>
#include <cpprest/http_client.h>
#include <gtest/gtest.h>
#include <thread>
#include <Services/TopologyManagerService.hpp>

using namespace utility;
using namespace std;
using namespace web;
// Common features like URIs.
using namespace web::http;
// Common HTTP functionality
using namespace web::http::client;
// HTTP client features
using namespace concurrency::streams;

namespace NES {

using namespace Configurations;
const int timestamp = 1644426604;
const uint64_t numberOfTupleBuffers = 4;

class UpstreamBackupTest : public Testing::NESBaseTest {
  public:
    std::string ipAddress = "127.0.0.1";
    CoordinatorConfigurationPtr coordinatorConfig;
    WorkerConfigurationPtr workerConfig;
    CSVSourceTypePtr csvSourceTypeInfinite;
    CSVSourceTypePtr csvSourceTypeFinite;
    SchemaPtr inputSchema;
    SchemaPtr inputSchema2;
    Runtime::BufferManagerPtr bufferManager;
    std::vector<std::string> placementQueries = {};
    std::vector<std::string> purchaseQueries = {};
    std::vector<std::string> equivalentQueries = {};
    std::string testString = "";

    static void SetUpTestCase() {
        NES::Logger::setupLogging("UpstreamBackupTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup UpstreamBackupTest test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();

        bufferManager = std::make_shared<Runtime::BufferManager>(1024, 1);

        coordinatorConfig = CoordinatorConfiguration::create();
        coordinatorConfig = CoordinatorConfiguration::create();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;

        workerConfig = WorkerConfiguration::create();
        workerConfig->coordinatorPort = *rpcCoordinatorPort;
        //workerConfig->enableStatisticOutput = true;
        workerConfig->numberOfSlots = 15;

        csvSourceTypeInfinite = CSVSourceType::create();
        csvSourceTypeInfinite->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window-out-of-order.csv");
        csvSourceTypeInfinite->setNumberOfTuplesToProducePerBuffer(numberOfTupleBuffers);
        csvSourceTypeInfinite->setNumberOfBuffersToProduce(0);

        csvSourceTypeFinite = CSVSourceType::create();
        csvSourceTypeFinite->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window-out-of-order.csv");
        csvSourceTypeFinite->setNumberOfTuplesToProducePerBuffer(numberOfTupleBuffers - 1);
        csvSourceTypeFinite->setNumberOfBuffersToProduce(numberOfTupleBuffers);

        inputSchema = Schema::create()
                          ->addField("timestamp", DataTypeFactory::createUInt64())
                          ->addField("clientID", DataTypeFactory::createUInt32())
                          ->addField("objectID", DataTypeFactory::createUInt32())
                          ->addField("size", DataTypeFactory::createUInt32())
                          ->addField("method", DataTypeFactory::createUInt8())
                          ->addField("status", DataTypeFactory::createUInt8())
                          ->addField("type", DataTypeFactory::createUInt8())
                          ->addField("server", DataTypeFactory::createUInt8());

        inputSchema2 = Schema::create()
                          ->addField("userID", DataTypeFactory::createUInt64())
                          ->addField("itemID", DataTypeFactory::createUInt64())
                          ->addField("price", DataTypeFactory::createUInt32())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

        std::ofstream logFile;
        logFile.open("/home/noah/placements.csv");//3 Nodes
        logFile << "topologyId, queryId, executionNodes, approach" << "\n";
        logFile.close();

        /*std::ofstream timingsLogFile;
        timingsLogFile.open("/home/noah/timings.csv");//3 Nodes
        timingsLogFile << "activity, queryId, timeInMs" << "\n";
        timingsLogFile.close();*/

        placementQueries = {"Query::from(\"worldCup\").map(Attribute(\"method\")=8147 * Attribute(\"status\")).filter(Attribute(\"method\")>6075).project(Attribute(\"clientID\").as(\"ccudm\"),Attribute(\"objectID\").as(\"aesfy\"),Attribute(\"size\").as(\"dkqos\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"status\")>=5637).map(Attribute(\"server\")=5460 - Attribute(\"server\")).project(Attribute(\"clientID\").as(\"dolix\"),Attribute(\"objectID\").as(\"brpcc\"),Attribute(\"size\").as(\"eckkb\"),Attribute(\"method\").as(\"aonak\"),Attribute(\"status\").as(\"fpbvl\"),Attribute(\"type\").as(\"vibdr\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"objectID\")=1531 + Attribute(\"objectID\")).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"type\"),Attribute(\"timestamp\")).filter(Attribute(\"objectID\")>2412).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"clientID\")=6807 + Attribute(\"type\")).filter(Attribute(\"objectID\")>=8200).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"type\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"type\"),Attribute(\"timestamp\")).map(Attribute(\"objectID\")=8132 + Attribute(\"clientID\")).filter(Attribute(\"clientID\")<3049).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"server\")=6772 * Attribute(\"objectID\")).project(Attribute(\"clientID\").as(\"xlfnq\"),Attribute(\"objectID\").as(\"oxyfj\"),Attribute(\"size\").as(\"mdiug\"),Attribute(\"method\").as(\"oytdq\"),Attribute(\"status\").as(\"lxphp\"),Attribute(\"timestamp\")).filter(Attribute(\"oytdq\")<=659).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"status\")=4694 - Attribute(\"server\")).filter(Attribute(\"size\")>=9090).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"type\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"status\")>=3557).map(Attribute(\"objectID\")=3533 * Attribute(\"clientID\")).project(Attribute(\"clientID\").as(\"wonsj\"),Attribute(\"objectID\").as(\"pqywm\"),Attribute(\"size\").as(\"auuvi\"),Attribute(\"method\").as(\"wikte\"),Attribute(\"status\").as(\"ngljs\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"type\")=5334 - Attribute(\"size\")).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"timestamp\")).filter(Attribute(\"objectID\")<2423).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\").as(\"xrnwd\"),Attribute(\"objectID\").as(\"tprcy\"),Attribute(\"timestamp\")).filter(Attribute(\"xrnwd\")<=9466).map(Attribute(\"tprcy\")=4794 - Attribute(\"xrnwd\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\").as(\"ebmis\"),Attribute(\"objectID\").as(\"aphmg\"),Attribute(\"size\").as(\"gkjcx\"),Attribute(\"timestamp\")).map(Attribute(\"gkjcx\")=306 * Attribute(\"gkjcx\")).filter(Attribute(\"gkjcx\")>=2872).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"server\")>=9904).project(Attribute(\"clientID\").as(\"yhkcw\"),Attribute(\"objectID\").as(\"opffn\"),Attribute(\"size\").as(\"ycgdn\"),Attribute(\"method\").as(\"osytn\"),Attribute(\"status\").as(\"qnhsw\"),Attribute(\"type\").as(\"ujiic\"),Attribute(\"timestamp\")).map(Attribute(\"ujiic\")=9481 - Attribute(\"yhkcw\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"clientID\")=7984 + Attribute(\"server\")).filter(Attribute(\"clientID\")>8412).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"timestamp\")).window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")), Seconds(22))).byKey(Attribute(\"clientID\")).apply(Sum(Attribute(\"size\"))->as(Attribute(\"ncnqc\"))).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"timestamp\")).map(Attribute(\"size\")=9886 + Attribute(\"clientID\")).filter(Attribute(\"clientID\")<=2793).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\").as(\"gbzoe\"),Attribute(\"objectID\").as(\"qhlgg\"),Attribute(\"size\").as(\"gacih\"),Attribute(\"method\").as(\"bvlsz\"),Attribute(\"status\").as(\"bgbyn\"),Attribute(\"type\").as(\"gwacw\"),Attribute(\"server\").as(\"hgrpv\"),Attribute(\"timestamp\")).filter(Attribute(\"qhlgg\")>9303).map(Attribute(\"bgbyn\")=9561 + Attribute(\"qhlgg\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"status\")=8023 * Attribute(\"type\")).filter(Attribute(\"status\")<=9717).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\").as(\"dkyzl\"),Attribute(\"objectID\").as(\"sqczt\"),Attribute(\"size\").as(\"dgxmv\"),Attribute(\"timestamp\")).map(Attribute(\"dgxmv\")=541 + Attribute(\"dkyzl\")).filter(Attribute(\"sqczt\")<9764).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"server\")>2567).project(Attribute(\"clientID\").as(\"hvjbo\"),Attribute(\"objectID\").as(\"xadns\"),Attribute(\"size\").as(\"fwnpz\"),Attribute(\"timestamp\")).map(Attribute(\"xadns\")=636 - Attribute(\"fwnpz\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"objectID\")<2662).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"type\"),Attribute(\"timestamp\")).map(Attribute(\"objectID\")=6101 - Attribute(\"clientID\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"method\")=5215 + Attribute(\"type\")).filter(Attribute(\"type\")<=7486).project(Attribute(\"clientID\").as(\"yjpsk\"),Attribute(\"objectID\").as(\"exfvn\"),Attribute(\"size\").as(\"plagv\"),Attribute(\"method\").as(\"kcggv\"),Attribute(\"status\").as(\"vmiuc\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\").as(\"nhbha\"),Attribute(\"objectID\").as(\"tsqtz\"),Attribute(\"size\").as(\"kgglb\"),Attribute(\"method\").as(\"duzpy\"),Attribute(\"status\").as(\"cnbkv\"),Attribute(\"timestamp\")).filter(Attribute(\"tsqtz\")<=1095).map(Attribute(\"nhbha\")=1691 - Attribute(\"tsqtz\")).window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")), Minutes(86))).apply(Max(Attribute(\"kgglb\"))->as(Attribute(\"kshkx\"))).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\").as(\"hsvny\"),Attribute(\"objectID\").as(\"rjnvm\"),Attribute(\"size\").as(\"xrgbs\"),Attribute(\"timestamp\")).filter(Attribute(\"xrgbs\")<=5137).map(Attribute(\"xrgbs\")=4695 + Attribute(\"hsvny\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"clientID\")>4601).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"type\"),Attribute(\"server\"),Attribute(\"timestamp\")).map(Attribute(\"type\")=6825 * Attribute(\"clientID\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"type\")>=37).map(Attribute(\"status\")=3113 + Attribute(\"method\")).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"server\")=4640 - Attribute(\"type\")).filter(Attribute(\"server\")>1011).project(Attribute(\"clientID\").as(\"wkptf\"),Attribute(\"objectID\").as(\"tzkcz\"),Attribute(\"size\").as(\"blusy\"),Attribute(\"method\").as(\"fqlps\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"size\")=8502 - Attribute(\"type\")).project(Attribute(\"clientID\").as(\"sexkz\"),Attribute(\"objectID\").as(\"sebmo\"),Attribute(\"size\").as(\"xbtpg\"),Attribute(\"method\").as(\"fvdhe\"),Attribute(\"timestamp\")).filter(Attribute(\"sebmo\")<=2128).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\").as(\"qauym\"),Attribute(\"objectID\").as(\"xrtrf\"),Attribute(\"timestamp\")).filter(Attribute(\"qauym\")<9097).map(Attribute(\"xrtrf\")=737 * Attribute(\"xrtrf\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"timestamp\")).map(Attribute(\"objectID\")=7453 * Attribute(\"size\")).filter(Attribute(\"clientID\")<=9984).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"type\")=2106 * Attribute(\"objectID\")).filter(Attribute(\"size\")<741).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"objectID\")=2798 * Attribute(\"method\")).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"timestamp\")).filter(Attribute(\"objectID\")>5016).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"status\")=228 + Attribute(\"type\")).filter(Attribute(\"size\")<=481).project(Attribute(\"clientID\").as(\"eabsg\"),Attribute(\"objectID\").as(\"ywcar\"),Attribute(\"size\").as(\"ktczb\"),Attribute(\"method\").as(\"ozagw\"),Attribute(\"status\").as(\"lgbey\"),Attribute(\"type\").as(\"hnxvj\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"server\")>=6733).map(Attribute(\"method\")=8860 - Attribute(\"type\")).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"size\")=5632 + Attribute(\"server\")).project(Attribute(\"clientID\").as(\"iqdka\"),Attribute(\"objectID\").as(\"yhqfx\"),Attribute(\"size\").as(\"xvvph\"),Attribute(\"method\").as(\"qzlbg\"),Attribute(\"status\").as(\"rcyug\"),Attribute(\"timestamp\")).filter(Attribute(\"qzlbg\")<4753).window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")), Minutes(69))).byKey(Attribute(\"xvvph\")).apply(Avg(Attribute(\"yhqfx\"))->as(Attribute(\"madyj\"))).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"clientID\")>1367).map(Attribute(\"clientID\")=5379 - Attribute(\"type\")).project(Attribute(\"clientID\").as(\"tlpbf\"),Attribute(\"objectID\").as(\"satel\"),Attribute(\"size\").as(\"xwgoy\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"timestamp\")).filter(Attribute(\"size\")<=1932).map(Attribute(\"size\")=8886 + Attribute(\"objectID\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"server\")=173 - Attribute(\"server\")).project(Attribute(\"clientID\").as(\"wgkwb\"),Attribute(\"objectID\").as(\"uxjkn\"),Attribute(\"size\").as(\"mvmdg\"),Attribute(\"method\").as(\"bzqpu\"),Attribute(\"status\").as(\"wuskz\"),Attribute(\"type\").as(\"rykio\"),Attribute(\"server\").as(\"olygw\"),Attribute(\"timestamp\")).filter(Attribute(\"wuskz\")>=2219).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"size\")>=3213).map(Attribute(\"size\")=599 * Attribute(\"clientID\")).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"type\"),Attribute(\"server\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"type\")=6646 + Attribute(\"status\")).filter(Attribute(\"status\")>=6004).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"type\"),Attribute(\"server\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"status\")>=6127).map(Attribute(\"type\")=3540 + Attribute(\"method\")).project(Attribute(\"clientID\").as(\"hvuan\"),Attribute(\"objectID\").as(\"bynoq\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"clientID\")=4147 + Attribute(\"type\")).project(Attribute(\"clientID\").as(\"svqtu\"),Attribute(\"objectID\").as(\"unzhd\"),Attribute(\"size\").as(\"vfijx\"),Attribute(\"method\").as(\"atshd\"),Attribute(\"status\").as(\"lqvfy\"),Attribute(\"type\").as(\"faudw\"),Attribute(\"timestamp\")).filter(Attribute(\"svqtu\")>6097).window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")), Minutes(5))).apply(Max(Attribute(\"lqvfy\"))->as(Attribute(\"luhmg\"))).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"status\")<=9467).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"type\"),Attribute(\"timestamp\")).map(Attribute(\"objectID\")=4490 - Attribute(\"objectID\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"type\"),Attribute(\"server\"),Attribute(\"timestamp\")).map(Attribute(\"clientID\")=5093 * Attribute(\"objectID\")).filter(Attribute(\"size\")>1862).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\").as(\"cojva\"),Attribute(\"objectID\").as(\"utaru\"),Attribute(\"size\").as(\"lglpa\"),Attribute(\"method\").as(\"vkphj\"),Attribute(\"timestamp\")).filter(Attribute(\"cojva\")<282).map(Attribute(\"vkphj\")=2939 * Attribute(\"lglpa\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"timestamp\")).filter(Attribute(\"clientID\")<7013).map(Attribute(\"objectID\")=4198 - Attribute(\"method\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\").as(\"iffwg\"),Attribute(\"objectID\").as(\"ptkff\"),Attribute(\"size\").as(\"ixbrk\"),Attribute(\"method\").as(\"yqthc\"),Attribute(\"status\").as(\"yaavc\"),Attribute(\"timestamp\")).filter(Attribute(\"yaavc\")<7556).map(Attribute(\"iffwg\")=7534 - Attribute(\"iffwg\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"status\")=9903 - Attribute(\"type\")).filter(Attribute(\"objectID\")<7467).project(Attribute(\"clientID\").as(\"khnos\"),Attribute(\"objectID\").as(\"qjehq\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"timestamp\")).filter(Attribute(\"objectID\")<=7524).map(Attribute(\"size\")=8502 * Attribute(\"method\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"timestamp\")).filter(Attribute(\"clientID\")<=1169).map(Attribute(\"clientID\")=5520 * Attribute(\"objectID\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"objectID\")<=9305).project(Attribute(\"clientID\").as(\"gkllv\"),Attribute(\"objectID\").as(\"mabzw\"),Attribute(\"size\").as(\"cbbjj\"),Attribute(\"timestamp\")).map(Attribute(\"gkllv\")=5045 + Attribute(\"gkllv\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"size\")>5114).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"timestamp\")).map(Attribute(\"objectID\")=8933 - Attribute(\"objectID\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"type\"),Attribute(\"timestamp\")).filter(Attribute(\"size\")<7444).map(Attribute(\"method\")=5049 - Attribute(\"size\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"type\"),Attribute(\"server\"),Attribute(\"timestamp\")).filter(Attribute(\"server\")>=3707).map(Attribute(\"method\")=3327 * Attribute(\"size\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"status\")=3936 - Attribute(\"method\")).filter(Attribute(\"method\")>=6879).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\").as(\"lsnmf\"),Attribute(\"objectID\").as(\"vjacc\"),Attribute(\"size\").as(\"nivli\"),Attribute(\"method\").as(\"mypjd\"),Attribute(\"timestamp\")).filter(Attribute(\"mypjd\")>=4333).map(Attribute(\"vjacc\")=7960 - Attribute(\"mypjd\")).window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")), Minutes(68))).byKey(Attribute(\"lsnmf\")).apply(Min(Attribute(\"vjacc\"))->as(Attribute(\"fliza\"))).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\").as(\"lnfyy\"),Attribute(\"objectID\").as(\"hlsll\"),Attribute(\"timestamp\")).filter(Attribute(\"lnfyy\")>212).map(Attribute(\"lnfyy\")=5568 - Attribute(\"hlsll\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"objectID\")<3511).map(Attribute(\"objectID\")=6918 * Attribute(\"method\")).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"objectID\")=5279 * Attribute(\"size\")).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"timestamp\")).filter(Attribute(\"clientID\")>6345).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"status\")<2279).map(Attribute(\"clientID\")=1649 - Attribute(\"type\")).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"type\"),Attribute(\"server\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"timestamp\")).filter(Attribute(\"objectID\")<8975).map(Attribute(\"objectID\")=8830 + Attribute(\"clientID\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"clientID\")=2476 * Attribute(\"server\")).filter(Attribute(\"status\")>=3578).project(Attribute(\"clientID\").as(\"jeeea\"),Attribute(\"objectID\").as(\"kzidh\"),Attribute(\"size\").as(\"ckleh\"),Attribute(\"method\").as(\"gkzoo\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"status\")=2740 + Attribute(\"size\")).filter(Attribute(\"server\")>7840).project(Attribute(\"clientID\").as(\"qeqau\"),Attribute(\"objectID\").as(\"fcxwm\"),Attribute(\"size\").as(\"aatuv\"),Attribute(\"method\").as(\"swtvp\"),Attribute(\"status\").as(\"kaccf\"),Attribute(\"type\").as(\"ssger\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"timestamp\")).filter(Attribute(\"objectID\")>324).map(Attribute(\"objectID\")=4787 * Attribute(\"objectID\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"size\")>5519).map(Attribute(\"clientID\")=4047 * Attribute(\"type\")).project(Attribute(\"clientID\").as(\"azzvs\"),Attribute(\"objectID\").as(\"tmuzx\"),Attribute(\"size\").as(\"xfsum\"),Attribute(\"method\").as(\"okete\"),Attribute(\"status\").as(\"rroxp\"),Attribute(\"type\").as(\"qpwhr\"),Attribute(\"server\").as(\"gokeh\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"clientID\")=7279 - Attribute(\"clientID\")).project(Attribute(\"clientID\").as(\"kbtpu\"),Attribute(\"objectID\").as(\"gstwi\"),Attribute(\"size\").as(\"fqihn\"),Attribute(\"method\").as(\"hdxon\"),Attribute(\"status\").as(\"ujrov\"),Attribute(\"timestamp\")).filter(Attribute(\"ujrov\")<5757).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"size\")<=3146).project(Attribute(\"clientID\").as(\"kpsxi\"),Attribute(\"objectID\").as(\"astji\"),Attribute(\"size\").as(\"pehtp\"),Attribute(\"method\").as(\"ptjbx\"),Attribute(\"timestamp\")).map(Attribute(\"ptjbx\")=2398 + Attribute(\"astji\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\").as(\"gymhd\"),Attribute(\"objectID\").as(\"cidec\"),Attribute(\"size\").as(\"kbtbz\"),Attribute(\"method\").as(\"rqnra\"),Attribute(\"timestamp\")).filter(Attribute(\"cidec\")<1270).map(Attribute(\"cidec\")=5885 + Attribute(\"kbtbz\")).window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")), Seconds(21))).byKey(Attribute(\"gymhd\")).apply(Sum(Attribute(\"cidec\"))).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"clientID\")>2008).map(Attribute(\"clientID\")=7265 * Attribute(\"status\")).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"server\")=7871 - Attribute(\"method\")).filter(Attribute(\"objectID\")>=7101).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"type\"),Attribute(\"server\"),Attribute(\"timestamp\")).window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")), Minutes(91))).byKey(Attribute(\"status\")).apply(Avg(Attribute(\"clientID\"))).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"size\")=7096 * Attribute(\"server\")).filter(Attribute(\"size\")<=345).project(Attribute(\"clientID\").as(\"xuazj\"),Attribute(\"objectID\").as(\"vfwlk\"),Attribute(\"size\").as(\"uczzb\"),Attribute(\"method\").as(\"zhtcm\"),Attribute(\"status\").as(\"egjzh\"),Attribute(\"type\").as(\"ndijt\"),Attribute(\"server\").as(\"jkyio\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"objectID\")>8258).project(Attribute(\"clientID\").as(\"kiivo\"),Attribute(\"objectID\").as(\"sgfzv\"),Attribute(\"size\").as(\"uwnxj\"),Attribute(\"timestamp\")).map(Attribute(\"uwnxj\")=8731 * Attribute(\"sgfzv\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"clientID\")=8145 - Attribute(\"type\")).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"type\"),Attribute(\"server\"),Attribute(\"timestamp\")).filter(Attribute(\"status\")<=5345).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"server\")=1647 * Attribute(\"server\")).filter(Attribute(\"clientID\")<=6664).project(Attribute(\"clientID\").as(\"uohnd\"),Attribute(\"objectID\").as(\"ykods\"),Attribute(\"size\").as(\"ajpxd\"),Attribute(\"method\").as(\"jwpvq\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"timestamp\")).filter(Attribute(\"method\")>5390).map(Attribute(\"size\")=5355 + Attribute(\"method\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\").as(\"rumwc\"),Attribute(\"objectID\").as(\"clgxl\"),Attribute(\"size\").as(\"juijt\"),Attribute(\"method\").as(\"fdaeq\"),Attribute(\"status\").as(\"tlgau\"),Attribute(\"type\").as(\"xijnu\"),Attribute(\"server\").as(\"vyest\"),Attribute(\"timestamp\")).filter(Attribute(\"clgxl\")>1999).map(Attribute(\"rumwc\")=5855 - Attribute(\"rumwc\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"server\")=4170 * Attribute(\"type\")).filter(Attribute(\"objectID\")>=8729).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"type\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\").as(\"lgmbj\"),Attribute(\"objectID\").as(\"wruid\"),Attribute(\"size\").as(\"jnugw\"),Attribute(\"method\").as(\"bfetf\"),Attribute(\"timestamp\")).map(Attribute(\"lgmbj\")=8118 - Attribute(\"wruid\")).filter(Attribute(\"jnugw\")>4201).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"method\")>=5603).map(Attribute(\"status\")=1452 * Attribute(\"type\")).project(Attribute(\"clientID\").as(\"beejl\"),Attribute(\"objectID\").as(\"snukv\"),Attribute(\"size\").as(\"udckh\"),Attribute(\"method\").as(\"enrjn\"),Attribute(\"status\").as(\"rjces\"),Attribute(\"type\").as(\"bbdim\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\").as(\"pulhu\"),Attribute(\"objectID\").as(\"pujxi\"),Attribute(\"timestamp\")).map(Attribute(\"pujxi\")=8189 + Attribute(\"pujxi\")).filter(Attribute(\"pulhu\")<7634).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"type\")=708 * Attribute(\"type\")).filter(Attribute(\"status\")>2488).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\").as(\"dgrdu\"),Attribute(\"objectID\").as(\"qmtpd\"),Attribute(\"size\").as(\"otztr\"),Attribute(\"method\").as(\"ivwst\"),Attribute(\"status\").as(\"wudqq\"),Attribute(\"timestamp\")).filter(Attribute(\"otztr\")<=4894).map(Attribute(\"qmtpd\")=8917 + Attribute(\"ivwst\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"status\")>=980).map(Attribute(\"server\")=5642 - Attribute(\"objectID\")).project(Attribute(\"clientID\").as(\"zwwgp\"),Attribute(\"objectID\").as(\"fvwez\"),Attribute(\"size\").as(\"uwaun\"),Attribute(\"method\").as(\"ntlrd\"),Attribute(\"status\").as(\"tcecx\"),Attribute(\"type\").as(\"strdo\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"method\")>=2127).map(Attribute(\"size\")=6460 * Attribute(\"type\")).project(Attribute(\"clientID\").as(\"arycv\"),Attribute(\"objectID\").as(\"pvxgi\"),Attribute(\"size\").as(\"hmvns\"),Attribute(\"method\").as(\"nbibj\"),Attribute(\"status\").as(\"zubng\"),Attribute(\"type\").as(\"kqmlo\"),Attribute(\"server\").as(\"wimpf\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\").as(\"gftxe\"),Attribute(\"objectID\").as(\"hlksb\"),Attribute(\"size\").as(\"ldhlr\"),Attribute(\"method\").as(\"tchur\"),Attribute(\"status\").as(\"ulcjp\"),Attribute(\"type\").as(\"hiwuq\"),Attribute(\"server\").as(\"bwvmg\"),Attribute(\"timestamp\")).filter(Attribute(\"hlksb\")>6220).map(Attribute(\"bwvmg\")=2717 + Attribute(\"ulcjp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"clientID\")=9376 * Attribute(\"clientID\")).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"type\"),Attribute(\"timestamp\")).filter(Attribute(\"clientID\")>=1535).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"size\")=4274 + Attribute(\"status\")).filter(Attribute(\"type\")<=9617).project(Attribute(\"clientID\").as(\"jkhxq\"),Attribute(\"objectID\").as(\"blsap\"),Attribute(\"size\").as(\"thwhv\"),Attribute(\"method\").as(\"ibypi\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"objectID\")=4888 * Attribute(\"clientID\")).filter(Attribute(\"size\")<4660).project(Attribute(\"clientID\").as(\"nzwug\"),Attribute(\"objectID\").as(\"aarsn\"),Attribute(\"size\").as(\"nonll\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\").as(\"vcuyv\"),Attribute(\"objectID\").as(\"iduzs\"),Attribute(\"size\").as(\"dmxvo\"),Attribute(\"method\").as(\"hmgjq\"),Attribute(\"status\").as(\"ajjai\"),Attribute(\"type\").as(\"femop\"),Attribute(\"timestamp\")).map(Attribute(\"iduzs\")=2859 - Attribute(\"vcuyv\")).filter(Attribute(\"femop\")>1688).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"status\")=1809 - Attribute(\"type\")).filter(Attribute(\"size\")>7106).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"type\")>=3878).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"timestamp\")).map(Attribute(\"clientID\")=3391 + Attribute(\"method\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"status\")=6799 * Attribute(\"objectID\")).project(Attribute(\"clientID\").as(\"nrcxy\"),Attribute(\"objectID\").as(\"bpujn\"),Attribute(\"size\").as(\"yiixe\"),Attribute(\"timestamp\")).filter(Attribute(\"bpujn\")<4877).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"method\")=115 + Attribute(\"objectID\")).filter(Attribute(\"type\")>=3795).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"type\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"objectID\")<4865).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"timestamp\")).map(Attribute(\"size\")=8374 - Attribute(\"clientID\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\").as(\"cyyfq\"),Attribute(\"objectID\").as(\"lwyzy\"),Attribute(\"timestamp\")).filter(Attribute(\"cyyfq\")>=4305).map(Attribute(\"cyyfq\")=4031 * Attribute(\"cyyfq\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\").as(\"dimpc\"),Attribute(\"objectID\").as(\"nfiop\"),Attribute(\"size\").as(\"udzdq\"),Attribute(\"method\").as(\"gzfgk\"),Attribute(\"status\").as(\"jbcjx\"),Attribute(\"type\").as(\"kwvzs\"),Attribute(\"server\").as(\"amhqq\"),Attribute(\"timestamp\")).filter(Attribute(\"nfiop\")<2093).map(Attribute(\"jbcjx\")=2263 + Attribute(\"jbcjx\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"method\")=3815 + Attribute(\"status\")).project(Attribute(\"clientID\").as(\"nzxpr\"),Attribute(\"objectID\").as(\"fxcot\"),Attribute(\"timestamp\")).filter(Attribute(\"fxcot\")<=6466).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"size\")=3398 - Attribute(\"objectID\")).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"method\"),Attribute(\"status\"),Attribute(\"timestamp\")).filter(Attribute(\"size\")>=3039).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"clientID\")=774 * Attribute(\"clientID\")).filter(Attribute(\"objectID\")<9087).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"clientID\")=5005 * Attribute(\"type\")).filter(Attribute(\"status\")<5284).project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").filter(Attribute(\"method\")<2278).project(Attribute(\"clientID\").as(\"exyii\"),Attribute(\"objectID\").as(\"qnuwv\"),Attribute(\"size\").as(\"hsajy\"),Attribute(\"method\").as(\"ravsz\"),Attribute(\"status\").as(\"rnlnx\"),Attribute(\"timestamp\")).map(Attribute(\"rnlnx\")=614 * Attribute(\"rnlnx\")).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").map(Attribute(\"size\")=6642 + Attribute(\"size\")).project(Attribute(\"clientID\").as(\"dzohu\"),Attribute(\"objectID\").as(\"pvzwq\"),Attribute(\"size\").as(\"fhfzd\"),Attribute(\"method\").as(\"szwdh\"),Attribute(\"timestamp\")).filter(Attribute(\"fhfzd\")>=4380).sink(NullOutputSinkDescriptor::create());"};

        equivalentQueries = {"Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"timestamp\")).map(Attribute(\"objectID\")=6).filter(Attribute(\"clientID\")>Attribute(\"clientID\") - Attribute(\"objectID\")).map(Attribute(\"clientID\")=Attribute(\"clientID\") + Attribute(\"objectID\")).filter(Attribute(\"clientID\")<Attribute(\"objectID\")).map(Attribute(\"clientID\")=10 * Attribute(\"clientID\")).filter(Attribute(\"clientID\")<32).window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")), Seconds(60))).apply(Sum(Attribute(\"clientID\"))).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"timestamp\")).map(Attribute(\"objectID\")=6).filter(Attribute(\"clientID\")>Attribute(\"clientID\") - 6).map(Attribute(\"clientID\")=Attribute(\"objectID\") + Attribute(\"clientID\")).filter(Attribute(\"objectID\")>Attribute(\"clientID\")).map(Attribute(\"clientID\")=10 * Attribute(\"clientID\")).filter(Attribute(\"clientID\")<33).window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")), Minutes(1))).apply(Sum(Attribute(\"clientID\"))).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"timestamp\")).map(Attribute(\"objectID\")=6).filter(Attribute(\"clientID\")>Attribute(\"clientID\") - Attribute(\"objectID\")).map(Attribute(\"clientID\")=Attribute(\"objectID\") + Attribute(\"clientID\")).filter(Attribute(\"objectID\")>Attribute(\"clientID\")).map(Attribute(\"clientID\")=10 * Attribute(\"clientID\")).filter(Attribute(\"clientID\")<35).window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")), Minutes(1))).apply(Sum(Attribute(\"clientID\"))).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"timestamp\")).map(Attribute(\"objectID\")=6).filter(Attribute(\"clientID\")>Attribute(\"clientID\") - Attribute(\"objectID\")).map(Attribute(\"clientID\")=Attribute(\"objectID\") + Attribute(\"clientID\")).filter(Attribute(\"clientID\")<Attribute(\"objectID\")).map(Attribute(\"clientID\")=10 * Attribute(\"clientID\")).filter(Attribute(\"clientID\")<34).window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")), Minutes(1))).apply(Sum(Attribute(\"clientID\"))).sink(NullOutputSinkDescriptor::create());"};

        purchaseQueries = {"Query::from(\"purchases\").project(Attribute(\"userID\"),Attribute(\"itemID\"),Attribute(\"price\"),Attribute(\"timestamp\")).map(Attribute(\"userID\")=2553 - Attribute(\"price\")).filter(Attribute(\"itemID\")<5630).sink(NullOutputSinkDescriptor::create());",
        "Query::from(\"purchases\").map(Attribute(\"itemID\")=4692 + Attribute(\"itemID\")).filter(Attribute(\"itemID\")<1462).project(Attribute(\"userID\").as(\"xlcfv\"),Attribute(\"itemID\").as(\"cljyw\"),Attribute(\"price\").as(\"vbfta\"),Attribute(\"timestamp\")).sink(NullOutputSinkDescriptor::create());",
        };


    }
};

/*
 * @brief test timestamp of watermark processor
 */
TEST_F(UpstreamBackupTest, testTimestampWatermarkProcessor) {
    NES_INFO("UpstreamBackupTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("window", inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UpstreamBackupTest: Coordinator started successfully");

    //Setup Worker
    NES_INFO("UpstreamBackupTest: Start worker 1");
    auto physicalSource1 = PhysicalSource::create("window", "x1", csvSourceTypeInfinite);
    workerConfig->physicalSources.add(physicalSource1);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UpstreamBackupTest: Worker1 started successfully");

    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UpstreamBackupTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testUpstreamBackup.out";
    remove(outputFilePath.c_str());

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("UpstreamBackupTest: Submit query");
    string query =
        "Query::from(\"window\").sink(FileSinkDescriptor::create(\"" + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    auto querySubPlanIds = crd->getNodeEngine()->getSubQueryIds(queryId);
    for (auto querySubPlanId : querySubPlanIds) {
        auto sinks = crd->getNodeEngine()->getExecutableQueryPlan(querySubPlanId)->getSinks();
        for (auto& sink : sinks) {
            auto buffer1 = bufferManager->getUnpooledBuffer(timestamp);
            buffer1->setWatermark(timestamp);
            buffer1->setSequenceNumber(1);
            sink->updateWatermark(buffer1.value());
            auto buffer2 = bufferManager->getUnpooledBuffer(timestamp);
            buffer2->setWatermark(timestamp);
            buffer2->setOriginId(1);
            buffer2->setSequenceNumber(1);
            sink->updateWatermark(buffer2.value());
            auto currentTimestamp = sink->getCurrentEpochBarrier();
            while (currentTimestamp == 0) {
                NES_INFO("UpstreamBackupTest: current timestamp: " << currentTimestamp);
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                currentTimestamp = sink->getCurrentEpochBarrier();
            }
            EXPECT_TRUE(currentTimestamp == timestamp);
        }
    }

    NES_INFO("UpstreamBackupTest: Remove query");
    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("UpstreamBackupTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UpstreamBackupTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("UpstreamBackupTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UpstreamBackupTest: Test finished");
}
/*
 * @brief test message passing between sink-coordinator-sources
 */
TEST_F(UpstreamBackupTest, testMessagePassingSinkCoordinatorSources) {
    NES_INFO("UpstreamBackupTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("window", inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UpstreamBackupTest: Coordinator started successfully");

    //Setup Worker
    NES_INFO("UpstreamBackupTest: Start worker 1");
    auto physicalSource1 = PhysicalSource::create("window", "x1", csvSourceTypeInfinite);
    workerConfig->physicalSources.add(physicalSource1);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UpstreamBackupTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testUpstreamBackup.out";
    remove(outputFilePath.c_str());

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("UpstreamBackupTest: Submit query");
    string query =
        "Query::from(\"window\").sink(FileSinkDescriptor::create(\"" + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    //get sink
    auto querySubPlanIds = crd->getNodeEngine()->getSubQueryIds(queryId);
    for (auto querySubPlanId : querySubPlanIds) {
        auto sinks = crd->getNodeEngine()->getExecutableQueryPlan(querySubPlanId)->getSinks();
        for (auto& sink : sinks) {
            sink->notifyEpochTermination(timestamp);
        }
    }

    auto currentTimestamp = crd->getReplicationService()->getCurrentEpochBarrier(queryId);
    while (currentTimestamp == -1) {
        NES_INFO("UpstreamBackupTest: current timestamp: " << currentTimestamp);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        currentTimestamp = crd->getReplicationService()->getCurrentEpochBarrier(queryId);
    }

    //check if the method was called
    EXPECT_TRUE(currentTimestamp == timestamp);

    NES_INFO("UpstreamBackupTest: Remove query");
    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("UpstreamBackupTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UpstreamBackupTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UpstreamBackupTest: Test finished");
}

/*
 * @brief test if upstream backup doesn't fail
 */
TEST_F(UpstreamBackupTest, testUpstreamBackupTest) {
    NES_INFO("UpstreamBackupTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("window", inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UpstreamBackupTest: Coordinator started successfully");

    //Setup Worker
    NES_INFO("UpstreamBackupTest: Start worker 1");
    auto physicalSource1 = PhysicalSource::create("window", "x1", csvSourceTypeFinite);
    workerConfig->physicalSources.add(physicalSource1);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UpstreamBackupTest: Worker1 started successfully");

    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UpstreamBackupTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testUpstreamBackup.out";
    remove(outputFilePath.c_str());

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("UpstreamBackupTest: Submit query");
    string query =
        "Query::from(\"window\").sink(FileSinkDescriptor::create(\"" + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("UpstreamBackupTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("UpstreamBackupTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UpstreamBackupTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("UpstreamBackupTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UpstreamBackupTest: Test finished");
}

TEST_F(UpstreamBackupTest, faultToleranceTest) {
    NES_INFO("UpstreamBackupTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("window", inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UpstreamBackupTest: Coordinator started successfully");

    //Setup Worker
    NES_INFO("UpstreamBackupTest: Start worker 1");
    auto physicalSource1 = PhysicalSource::create("window", "x1", csvSourceTypeInfinite);
    workerConfig->physicalSources.add(physicalSource1);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UpstreamBackupTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testUpstreamBackup.out";
    remove(outputFilePath.c_str());

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("UpstreamBackupTest: Submit query");
    string query =
        "Query::from(\"window\").sink(FileSinkDescriptor::create(\"" + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);



    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    //get sink
    /*auto querySubPlanIds = crd->getNodeEngine()->getSubQueryIds(queryId);
    for (auto querySubPlanId : querySubPlanIds) {
        auto sinks = crd->getNodeEngine()->getExecutableQueryPlan(querySubPlanId)->getSinks();
        for (auto& sink : sinks) {
            sink->notifyEpochTermination(timestamp);
        }
    }*/

    /*auto currentTimestamp = crd->getReplicationService()->getCurrentEpochBarrier(queryId);
    while (currentTimestamp == -1) {
        NES_INFO("UpstreamBackupTest: current timestamp: " << currentTimestamp);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        currentTimestamp = crd->getReplicationService()->getCurrentEpochBarrier(queryId);
    }*/

    //check if the method was called
    /*EXPECT_TRUE(currentTimestamp == timestamp);

    NES_INFO("UpstreamBackupTest: Remove query");
    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("UpstreamBackupTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UpstreamBackupTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UpstreamBackupTest: Test finished");*/
}

TEST_F(UpstreamBackupTest, topology1) {
    NES_INFO("UpstreamBackupTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("worldCup", inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UpstreamBackupTest: Coordinator started successfully");

    //Setup Worker
    NES_INFO("UpstreamBackupTest: Start worker 1");
    auto physicalSource1 = PhysicalSource::create("worldCup", "x1", csvSourceTypeInfinite);
    workerConfig->physicalSources.add(physicalSource1);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UpstreamBackupTest: Worker1 started successfully");

    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UpstreamBackupTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    crd->getTopologyManagerService()->removeParent(3,1);
    crd->getTopologyManagerService()->addParent(3,2);


    std::string outputFilePath = getTestResourceFolder() / "testUpstreamBackup.out";
    remove(outputFilePath.c_str());

    //ADD QUERIES
    NES_INFO("UpstreamBackupTest: Submit query");
    /*string query1 =
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"timestamp\")).map(Attribute(\"size\")=9317 + Attribute(\"clientID\")).filter(Attribute(\"clientID\")<=2491).sink(FileSinkDescriptor::create(\"" + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId1 =
        queryService->validateAndQueueAddQueryRequest(query1, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    string query2 =
        "Query::from(\"worldCup\").map(Attribute(\"clientID\")=8285 * Attribute(\"server\")).project(Attribute(\"clientID\").as(\"qowhd\"),Attribute(\"objectID\").as(\"ogfpg\"),Attribute(\"timestamp\")).filter(Attribute(\"ogfpg\")>6787).sink(FileSinkDescriptor::create(\"" + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId2 =
        queryService->validateAndQueueAddQueryRequest(query2, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);*/

    for(auto& q : placementQueries){
        queryService->validateAndQueueAddQueryRequest(q, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    }

    /*for(auto& queryId : queryIds){
        queryService->validateAndQueueStopQueryRequest(queryId);
    }*/
    /*GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId1, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId1, globalQueryPlan, 1));

    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId2, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId2, globalQueryPlan, 1));

    //REMOVE QUERIES
    NES_INFO("UpstreamBackupTest: Remove query 1");
    queryService->validateAndQueueStopQueryRequest(queryId1);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalogService));

    NES_INFO("UpstreamBackupTest: Remove query 2");
    queryService->validateAndQueueStopQueryRequest(queryId2);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalogService));



    NES_INFO("UpstreamBackupTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UpstreamBackupTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("UpstreamBackupTest: Stop worker 2");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_INFO("UpstreamBackupTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UpstreamBackupTest: Test finished");*/
}

TEST_F(UpstreamBackupTest, topology2) {
    NES_INFO("UpstreamBackupTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("worldCup", inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UpstreamBackupTest: Coordinator started successfully");

    //Setup Worker
    NES_INFO("UpstreamBackupTest: Start worker 1");
    auto physicalSource1 = PhysicalSource::create("worldCup", "x1", csvSourceTypeInfinite);
    workerConfig->physicalSources.add(physicalSource1);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UpstreamBackupTest: Worker1 started successfully");

    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UpstreamBackupTest: Worker2 started successfully");

    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("UpstreamBackupTest: Worker3 started successfully");

    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_INFO("UpstreamBackupTest: Worker4 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    //crd->getTopologyManagerService()->removeParent(2,1);
    crd->getTopologyManagerService()->removeParent(3,1);
    crd->getTopologyManagerService()->removeParent(4,1);
    crd->getTopologyManagerService()->removeParent(5,1);
    crd->getTopologyManagerService()->addParent(3,2);
    crd->getTopologyManagerService()->addParent(4,3);
    crd->getTopologyManagerService()->addParent(5,4);

    std::string outputFilePath = getTestResourceFolder() / "testUpstreamBackup.out";
    remove(outputFilePath.c_str());

    //ADD QUERIES
    NES_INFO("UpstreamBackupTest: Submit query");
    /*string query1 =
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"timestamp\")).map(Attribute(\"size\")=9317 + Attribute(\"clientID\")).filter(Attribute(\"clientID\")<=2491).sink(FileSinkDescriptor::create(\"" + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId1 =
        queryService->validateAndQueueAddQueryRequest(query1, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    string query2 =
        "Query::from(\"worldCup\").map(Attribute(\"clientID\")=8285 * Attribute(\"server\")).project(Attribute(\"clientID\").as(\"qowhd\"),Attribute(\"objectID\").as(\"ogfpg\"),Attribute(\"timestamp\")).filter(Attribute(\"ogfpg\")>6787).sink(FileSinkDescriptor::create(\"" + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId2 =
        queryService->validateAndQueueAddQueryRequest(query2, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);*/

    /*for(auto& q : placementQueries){
        queryService->validateAndQueueAddQueryRequest(q, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    }*/

    for (int i = 0; i < 100; ++i) {
        queryService->validateAndQueueAddQueryRequest(placementQueries[i], "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    }
    for (int i = 0; i < 50; ++i) {
        queryService->validateAndQueueAddQueryRequest(placementQueries[i], "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    }

    //queryService->validateAndQueueAddQueryRequest(placementQueries[0], "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //queryService->validateAndQueueAddQueryRequest(placementQueries[14], "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);


    /*for(auto& q : equivalentQueries){
        queryService->validateAndQueueAddQueryRequest(q, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    }*/

    /*for(auto& queryId : queryIds){
        queryService->validateAndQueueStopQueryRequest(queryId);
    }*/
    /*GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId1, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId1, globalQueryPlan, 1));

    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId2, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId2, globalQueryPlan, 1));

    //REMOVE QUERIES
    NES_INFO("UpstreamBackupTest: Remove query 1");
    queryService->validateAndQueueStopQueryRequest(queryId1);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalogService));

    NES_INFO("UpstreamBackupTest: Remove query 2");
    queryService->validateAndQueueStopQueryRequest(queryId2);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalogService));



    NES_INFO("UpstreamBackupTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UpstreamBackupTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("UpstreamBackupTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_INFO("UpstreamBackupTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_INFO("UpstreamBackupTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UpstreamBackupTest: Test finished");*/
}

TEST_F(UpstreamBackupTest, topology3) {
    NES_INFO("UpstreamBackupTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("worldCup", inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UpstreamBackupTest: Coordinator started successfully");

    //Setup Worker
    NES_INFO("UpstreamBackupTest: Start worker 1");
    auto physicalSource1 = PhysicalSource::create("worldCup", "x1", csvSourceTypeInfinite);
    workerConfig->physicalSources.add(physicalSource1);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UpstreamBackupTest: Worker1 started successfully");

    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UpstreamBackupTest: Worker2 started successfully");

    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("UpstreamBackupTest: Worker3 started successfully");

    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_INFO("UpstreamBackupTest: Worker4 started successfully");

    NesWorkerPtr wrk5 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart5);
    NES_INFO("UpstreamBackupTest: Worker5 started successfully");

    NesWorkerPtr wrk6 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart6 = wrk6->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart6);
    NES_INFO("UpstreamBackupTest: Worker6 started successfully");

    NesWorkerPtr wrk7 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart7 = wrk7->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart7);
    NES_INFO("UpstreamBackupTest: Worker6 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    crd->getTopologyManagerService()->removeParent(3,1);
    crd->getTopologyManagerService()->removeParent(4,1);
    crd->getTopologyManagerService()->removeParent(5,1);
    crd->getTopologyManagerService()->removeParent(6,1);
    crd->getTopologyManagerService()->removeParent(7,1);
    crd->getTopologyManagerService()->removeParent(8,1);
    crd->getTopologyManagerService()->addParent(3,2);
    crd->getTopologyManagerService()->addParent(4,3);
    crd->getTopologyManagerService()->addParent(5,4);
    crd->getTopologyManagerService()->addParent(6,5);
    crd->getTopologyManagerService()->addParent(7,6);
    crd->getTopologyManagerService()->addParent(8,7);

    std::string outputFilePath = getTestResourceFolder() / "testUpstreamBackup.out";
    remove(outputFilePath.c_str());

    //ADD QUERIES
    NES_INFO("UpstreamBackupTest: Submit query");
    /*string query1 =
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"timestamp\")).map(Attribute(\"size\")=9317 + Attribute(\"clientID\")).filter(Attribute(\"clientID\")<=2491).sink(FileSinkDescriptor::create(\"" + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId1 =
        queryService->validateAndQueueAddQueryRequest(query1, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    string query2 =
        "Query::from(\"worldCup\").map(Attribute(\"clientID\")=8285 * Attribute(\"server\")).project(Attribute(\"clientID\").as(\"qowhd\"),Attribute(\"objectID\").as(\"ogfpg\"),Attribute(\"timestamp\")).filter(Attribute(\"ogfpg\")>6787).sink(FileSinkDescriptor::create(\"" + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId2 =
        queryService->validateAndQueueAddQueryRequest(query2, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);*/

    for(auto& q : placementQueries){
        queryService->validateAndQueueAddQueryRequest(q, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    }

    /*for(auto& queryId : queryIds){
        queryService->validateAndQueueStopQueryRequest(queryId);
    }*/
    /*GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId1, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId1, globalQueryPlan, 1));

    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId2, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId2, globalQueryPlan, 1));

    //REMOVE QUERIES
    NES_INFO("UpstreamBackupTest: Remove query 1");
    queryService->validateAndQueueStopQueryRequest(queryId1);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalogService));

    NES_INFO("UpstreamBackupTest: Remove query 2");
    queryService->validateAndQueueStopQueryRequest(queryId2);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalogService));*/



    /*NES_INFO("UpstreamBackupTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UpstreamBackupTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("UpstreamBackupTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_INFO("UpstreamBackupTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_INFO("UpstreamBackupTest: Stop worker 5");
    bool retStopWrk5 = wrk5->stop(true);
    EXPECT_TRUE(retStopWrk5);

    NES_INFO("UpstreamBackupTest: Stop worker 6");
    bool retStopWrk6 = wrk6->stop(true);
    EXPECT_TRUE(retStopWrk6);

    NES_INFO("UpstreamBackupTest: Stop worker 7");
    bool retStopWrk7 = wrk7->stop(true);
    EXPECT_TRUE(retStopWrk7);

    NES_INFO("UpstreamBackupTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UpstreamBackupTest: Test finished");*/
}

TEST_F(UpstreamBackupTest, topology4) {
    NES_INFO("UpstreamBackupTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("worldCup", inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UpstreamBackupTest: Coordinator started successfully");

    //Setup Worker
    NES_INFO("UpstreamBackupTest: Start worker 1");
    auto physicalSource1 = PhysicalSource::create("worldCup", "x1", csvSourceTypeInfinite);
    workerConfig->physicalSources.add(physicalSource1);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UpstreamBackupTest: Worker1 started successfully");

    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UpstreamBackupTest: Worker2 started successfully");

    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("UpstreamBackupTest: Worker3 started successfully");

    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_INFO("UpstreamBackupTest: Worker4 started successfully");

    NesWorkerPtr wrk5 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart5);
    NES_INFO("UpstreamBackupTest: Worker5 started successfully");

    NesWorkerPtr wrk6 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart6 = wrk6->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart6);
    NES_INFO("UpstreamBackupTest: Worker6 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    crd->getTopologyManagerService()->removeParent(4,1);
    crd->getTopologyManagerService()->removeParent(3,1);
    crd->getTopologyManagerService()->removeParent(6,1);
    crd->getTopologyManagerService()->removeParent(7,1);
    crd->getTopologyManagerService()->addParent(3,2);
    crd->getTopologyManagerService()->addParent(4,3);
    crd->getTopologyManagerService()->addParent(6,5);
    crd->getTopologyManagerService()->addParent(7,6);

    std::string outputFilePath = getTestResourceFolder() / "testUpstreamBackup.out";
    remove(outputFilePath.c_str());

    //ADD QUERIES
    NES_INFO("UpstreamBackupTest: Submit query");
    /*string query1 =
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"timestamp\")).map(Attribute(\"size\")=9317 + Attribute(\"clientID\")).filter(Attribute(\"clientID\")<=2491).sink(FileSinkDescriptor::create(\"" + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId1 =
        queryService->validateAndQueueAddQueryRequest(query1, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    string query2 =
        "Query::from(\"worldCup\").map(Attribute(\"clientID\")=8285 * Attribute(\"server\")).project(Attribute(\"clientID\").as(\"qowhd\"),Attribute(\"objectID\").as(\"ogfpg\"),Attribute(\"timestamp\")).filter(Attribute(\"ogfpg\")>6787).sink(FileSinkDescriptor::create(\"" + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId2 =
        queryService->validateAndQueueAddQueryRequest(query2, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);*/

    for (int i = 0; i < 5; ++i) {
        queryService->validateAndQueueAddQueryRequest(placementQueries[i], "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    }


    /*for(auto& queryId : queryIds){
        queryService->validateAndQueueStopQueryRequest(queryId);
    }*/
    /*GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId1, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId1, globalQueryPlan, 1));

    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId2, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId2, globalQueryPlan, 1));

    //REMOVE QUERIES
    NES_INFO("UpstreamBackupTest: Remove query 1");
    queryService->validateAndQueueStopQueryRequest(queryId1);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalogService));

    NES_INFO("UpstreamBackupTest: Remove query 2");
    queryService->validateAndQueueStopQueryRequest(queryId2);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalogService));



    NES_INFO("UpstreamBackupTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UpstreamBackupTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("UpstreamBackupTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_INFO("UpstreamBackupTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_INFO("UpstreamBackupTest: Stop worker 5");
    bool retStopWrk5 = wrk5->stop(true);
    EXPECT_TRUE(retStopWrk5);

    NES_INFO("UpstreamBackupTest: Stop worker 6");
    bool retStopWrk6 = wrk6->stop(true);
    EXPECT_TRUE(retStopWrk6);

    NES_INFO("UpstreamBackupTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UpstreamBackupTest: Test finished");*/
}

TEST_F(UpstreamBackupTest, topology5) {
    NES_INFO("UpstreamBackupTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("worldCup", inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UpstreamBackupTest: Coordinator started successfully");

    //Setup Worker
    NES_INFO("UpstreamBackupTest: Start worker 1");
    auto physicalSource1 = PhysicalSource::create("worldCup", "x1", csvSourceTypeInfinite);
    workerConfig->physicalSources.add(physicalSource1);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UpstreamBackupTest: Worker1 started successfully");

    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UpstreamBackupTest: Worker2 started successfully");

    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("UpstreamBackupTest: Worker3 started successfully");

    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_INFO("UpstreamBackupTest: Worker4 started successfully");

    NesWorkerPtr wrk5 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart5);
    NES_INFO("UpstreamBackupTest: Worker5 started successfully");

    NesWorkerPtr wrk6 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart6 = wrk6->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart6);
    NES_INFO("UpstreamBackupTest: Worker6 started successfully");

    NesWorkerPtr wrk7 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart7 = wrk7->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart7);
    NES_INFO("UpstreamBackupTest: Worker7 started successfully");

    NesWorkerPtr wrk8 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart8 = wrk8->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart8);
    NES_INFO("UpstreamBackupTest: Worker6 started successfully");

    NesWorkerPtr wrk9 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart9 = wrk9->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart9);
    NES_INFO("UpstreamBackupTest: Worker6 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    crd->getTopologyManagerService()->removeParent(4,1);
    crd->getTopologyManagerService()->removeParent(3,1);
    crd->getTopologyManagerService()->removeParent(6,1);
    crd->getTopologyManagerService()->removeParent(7,1);
    crd->getTopologyManagerService()->removeParent(9,1);
    crd->getTopologyManagerService()->removeParent(10,1);
    crd->getTopologyManagerService()->addParent(3,2);
    crd->getTopologyManagerService()->addParent(4,3);
    crd->getTopologyManagerService()->addParent(6,5);
    crd->getTopologyManagerService()->addParent(7,6);
    crd->getTopologyManagerService()->addParent(9,8);
    crd->getTopologyManagerService()->addParent(10,9);

    std::string outputFilePath = getTestResourceFolder() / "testUpstreamBackup.out";
    remove(outputFilePath.c_str());

    //ADD QUERIES
    NES_INFO("UpstreamBackupTest: Submit query");
    /*string query1 =
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"timestamp\")).map(Attribute(\"size\")=9317 + Attribute(\"clientID\")).filter(Attribute(\"clientID\")<=2491).sink(FileSinkDescriptor::create(\"" + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId1 =
        queryService->validateAndQueueAddQueryRequest(query1, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    string query2 =
        "Query::from(\"worldCup\").map(Attribute(\"clientID\")=8285 * Attribute(\"server\")).project(Attribute(\"clientID\").as(\"qowhd\"),Attribute(\"objectID\").as(\"ogfpg\"),Attribute(\"timestamp\")).filter(Attribute(\"ogfpg\")>6787).sink(FileSinkDescriptor::create(\"" + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId2 =
        queryService->validateAndQueueAddQueryRequest(query2, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);*/

    for(auto& q : placementQueries){
        queryService->validateAndQueueAddQueryRequest(q, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    }

    /*for(auto& queryId : queryIds){
        queryService->validateAndQueueStopQueryRequest(queryId);
    }*/
    /*GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId1, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId1, globalQueryPlan, 1));

    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId2, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId2, globalQueryPlan, 1));

    //REMOVE QUERIES
    NES_INFO("UpstreamBackupTest: Remove query 1");
    queryService->validateAndQueueStopQueryRequest(queryId1);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalogService));

    NES_INFO("UpstreamBackupTest: Remove query 2");
    queryService->validateAndQueueStopQueryRequest(queryId2);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalogService));



    NES_INFO("UpstreamBackupTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UpstreamBackupTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("UpstreamBackupTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_INFO("UpstreamBackupTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_INFO("UpstreamBackupTest: Stop worker 5");
    bool retStopWrk5 = wrk5->stop(true);
    EXPECT_TRUE(retStopWrk5);

    NES_INFO("UpstreamBackupTest: Stop worker 6");
    bool retStopWrk6 = wrk6->stop(true);
    EXPECT_TRUE(retStopWrk6);

    NES_INFO("UpstreamBackupTest: Stop worker 7");
    bool retStopWrk7 = wrk7->stop(true);
    EXPECT_TRUE(retStopWrk7);

    NES_INFO("UpstreamBackupTest: Stop worker 8");
    bool retStopWrk8 = wrk8->stop(true);
    EXPECT_TRUE(retStopWrk8);

    NES_INFO("UpstreamBackupTest: Stop worker 9");
    bool retStopWrk9 = wrk9->stop(true);
    EXPECT_TRUE(retStopWrk9);

    NES_INFO("UpstreamBackupTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UpstreamBackupTest: Test finished");*/
}

TEST_F(UpstreamBackupTest, topology6) {
    NES_INFO("UpstreamBackupTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("worldCup", inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UpstreamBackupTest: Coordinator started successfully");

    //Setup Worker
    NES_INFO("UpstreamBackupTest: Start worker 1");
    auto physicalSource1 = PhysicalSource::create("worldCup", "x1", csvSourceTypeInfinite);
    workerConfig->physicalSources.add(physicalSource1);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UpstreamBackupTest: Worker1 started successfully");

    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UpstreamBackupTest: Worker2 started successfully");

    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("UpstreamBackupTest: Worker3 started successfully");

    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_INFO("UpstreamBackupTest: Worker4 started successfully");

    NesWorkerPtr wrk5 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart5);
    NES_INFO("UpstreamBackupTest: Worker5 started successfully");

    NesWorkerPtr wrk6 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart6 = wrk6->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart6);
    NES_INFO("UpstreamBackupTest: Worker6 started successfully");

    NesWorkerPtr wrk7 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart7 = wrk7->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart7);
    NES_INFO("UpstreamBackupTest: Worker7 started successfully");

    NesWorkerPtr wrk8 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart8 = wrk8->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart8);
    NES_INFO("UpstreamBackupTest: Worker6 started successfully");

    NesWorkerPtr wrk9 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart9 = wrk9->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart9);
    NES_INFO("UpstreamBackupTest: Worker6 started successfully");

    NesWorkerPtr wrk10 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart10 = wrk10->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart10);
    NES_INFO("UpstreamBackupTest: Worker6 started successfully");

    NesWorkerPtr wrk11 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart11 = wrk11->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart11);
    NES_INFO("UpstreamBackupTest: Worker6 started successfully");

    NesWorkerPtr wrk12 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart12 = wrk12->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart12);
    NES_INFO("UpstreamBackupTest: Worker6 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    crd->getTopologyManagerService()->removeParent(4,1);
    crd->getTopologyManagerService()->removeParent(3,1);
    crd->getTopologyManagerService()->removeParent(6,1);
    crd->getTopologyManagerService()->removeParent(7,1);
    crd->getTopologyManagerService()->removeParent(9,1);
    crd->getTopologyManagerService()->removeParent(10,1);
    crd->getTopologyManagerService()->removeParent(12,1);
    crd->getTopologyManagerService()->removeParent(13,1);
    crd->getTopologyManagerService()->addParent(3,2);
    crd->getTopologyManagerService()->addParent(4,3);
    crd->getTopologyManagerService()->addParent(6,5);
    crd->getTopologyManagerService()->addParent(7,6);
    crd->getTopologyManagerService()->addParent(9,8);
    crd->getTopologyManagerService()->addParent(10,9);
    crd->getTopologyManagerService()->addParent(12,11);
    crd->getTopologyManagerService()->addParent(13,12);

    std::string outputFilePath = getTestResourceFolder() / "testUpstreamBackup.out";
    remove(outputFilePath.c_str());

    //ADD QUERIES
    NES_INFO("UpstreamBackupTest: Submit query");
    /*string query1 =
        "Query::from(\"worldCup\").project(Attribute(\"clientID\"),Attribute(\"objectID\"),Attribute(\"size\"),Attribute(\"timestamp\")).map(Attribute(\"size\")=9317 + Attribute(\"clientID\")).filter(Attribute(\"clientID\")<=2491).sink(FileSinkDescriptor::create(\"" + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId1 =
        queryService->validateAndQueueAddQueryRequest(query1, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    string query2 =
        "Query::from(\"worldCup\").map(Attribute(\"clientID\")=8285 * Attribute(\"server\")).project(Attribute(\"clientID\").as(\"qowhd\"),Attribute(\"objectID\").as(\"ogfpg\"),Attribute(\"timestamp\")).filter(Attribute(\"ogfpg\")>6787).sink(FileSinkDescriptor::create(\"" + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId2 =
        queryService->validateAndQueueAddQueryRequest(query2, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);*/

    for(auto& q : placementQueries){
        queryService->validateAndQueueAddQueryRequest(q, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    }

    /*for(auto& queryId : queryIds){
        queryService->validateAndQueueStopQueryRequest(queryId);
    }*/
    /*GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId1, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId1, globalQueryPlan, 1));

    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId2, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId2, globalQueryPlan, 1));

    //REMOVE QUERIES
    NES_INFO("UpstreamBackupTest: Remove query 1");
    queryService->validateAndQueueStopQueryRequest(queryId1);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalogService));

    NES_INFO("UpstreamBackupTest: Remove query 2");
    queryService->validateAndQueueStopQueryRequest(queryId2);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalogService));*/



    /*NES_INFO("UpstreamBackupTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UpstreamBackupTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("UpstreamBackupTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_INFO("UpstreamBackupTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_INFO("UpstreamBackupTest: Stop worker 5");
    bool retStopWrk5 = wrk5->stop(true);
    EXPECT_TRUE(retStopWrk5);

    NES_INFO("UpstreamBackupTest: Stop worker 6");
    bool retStopWrk6 = wrk6->stop(true);
    EXPECT_TRUE(retStopWrk6);

    NES_INFO("UpstreamBackupTest: Stop worker 7");
    bool retStopWrk7 = wrk7->stop(true);
    EXPECT_TRUE(retStopWrk7);

    NES_INFO("UpstreamBackupTest: Stop worker 8");
    bool retStopWrk8 = wrk8->stop(true);
    EXPECT_TRUE(retStopWrk8);

    NES_INFO("UpstreamBackupTest: Stop worker 9");
    bool retStopWrk9 = wrk9->stop(true);
    EXPECT_TRUE(retStopWrk9);

    NES_INFO("UpstreamBackupTest: Stop worker 9");
    bool retStopWrk10 = wrk10->stop(true);
    EXPECT_TRUE(retStopWrk10);

    NES_INFO("UpstreamBackupTest: Stop worker 9");
    bool retStopWrk11 = wrk11->stop(true);
    EXPECT_TRUE(retStopWrk11);

    NES_INFO("UpstreamBackupTest: Stop worker 9");
    bool retStopWrk12 = wrk12->stop(true);
    EXPECT_TRUE(retStopWrk12);

    NES_INFO("UpstreamBackupTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UpstreamBackupTest: Test finished");*/
}
}// namespace NES