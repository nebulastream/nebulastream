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

#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Sources/Arrow/ArrowSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>

#include <iostream>

namespace NES {
class ArrowSourceIntegrationTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ArrowSourceIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ArrowSourceIntegrationTest test class.");
    }
};

/// This test reads from a arrow source which contains all data types compatible with NES and writes
/// them to a csv sink
TEST_F(ArrowSourceIntegrationTest, testArrowSourceWithMultipleDatatypes) {
    std::string inputFileName = "arrow_test.arrow";
    std::string outputFileName = "testArrowSource.csv";
    std::string inputFilePath = std::filesystem::path(TEST_DATA_DIRECTORY) / inputFileName;
    std::string outputFilePath = getTestResourceFolder() / outputFileName;
    remove(outputFilePath.c_str());

    // test schema with all data types
    auto testSchema = Schema::create()
                          ->addField("field_boolean", BasicType::BOOLEAN)
                          ->addField("field_int8", BasicType::INT8)
                          ->addField("field_int16", BasicType::INT16)
                          ->addField("field_int32", BasicType::INT32)
                          ->addField("field_int64", BasicType::INT64)
                          ->addField("field_uint8", BasicType::UINT8)
                          ->addField("field_uint16", BasicType::UINT16)
                          ->addField("field_uint32", BasicType::UINT32)
                          ->addField("field_uint64", BasicType::UINT64)
                          ->addField("field_float", BasicType::FLOAT32)
                          ->addField("field_double", BasicType::FLOAT64)
                          ->addField("field_string", BasicType::TEXT);

    // setup coordinator
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    NES_INFO("ArrowIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    crd->getSourceCatalogService()->registerLogicalSource("arrow_data", testSchema);

    EXPECT_NE(port, 0UL);
    NES_DEBUG("ArrowIntegrationTest: Coordinator started successfully");
    NES_DEBUG("ArrowIntegrationTest: Start worker 1");

    // setup Arrow source
    ArrowSourceTypePtr arrowSourceTypePtr = ArrowSourceType::create();
    arrowSourceTypePtr->setFilePath(inputFilePath);
    arrowSourceTypePtr->setNumberOfTuplesToProducePerBuffer(4);
    arrowSourceTypePtr->setNumberOfBuffersToProduce(8);
    auto physicalSource = PhysicalSource::create("arrow_data", "test_stream", arrowSourceTypePtr);

    // setup worker
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    workerConfig1->physicalSources.add(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);

    ASSERT_TRUE(retStart1);
    NES_INFO("ArrowIntegrationTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    // register query
    auto query = Query::from("arrow_data").sink(FileSinkDescriptor::create(outputFilePath, "CSV_FORMAT", "APPEND"));

    QueryId queryId = queryService->addQueryRequest(query.getQueryPlan()->toString(),
                                                    query.getQueryPlan(),
                                                    Optimizer::PlacementStrategy::BottomUp,
                                                    FaultToleranceType::NONE,
                                                    LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();

    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("ArrowSourceIntegrationTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    std::string const expectedContent =
        "arrow_data$field_boolean:Boolean,arrow_data$field_int8:INTEGER(8 bits),arrow_data$field_int16:INTEGER(16 bits),"
        "arrow_data$field_int32:INTEGER(32 bits),arrow_data$field_int64:INTEGER(64 bits),arrow_data$field_uint8:INTEGER(8 bits),"
        "arrow_data$field_uint16:INTEGER(16 bits),arrow_data$field_uint32:INTEGER(32 bits),arrow_data$field_uint64:INTEGER(64 "
        "bits),"
        "arrow_data$field_float:Float(32 bits),arrow_data$field_double:Float(64 bits),arrow_data$field_string:Text\n"
        "1,50,-27811,-323626163,-1122580231,252,8365,2025898209,416703305,896356.125000,293428.052303,"
        "HpPsbQhWyDtd57iLLasBW0YpnMIEvKXO76GqQE7A0BD2PcvtdtSE7ZzZTdgbRxNpkkxYA5Y5frEVKoRi5a8QNXx0kwcq8iGBtZAI8Ye05rRoDmaTsK5yaWAN"
        "t9\n"
        "1,114,-24130,-465012861,2068161396,209,36688,1185091338,253862470,-962888.500000,-1038564.601238,"
        "N7tvunJzWBZwpZH0xVB2OaV9iMepUsQ4ApGoMIBMjIpeIWTmWs9qUiXYs\n"
        "0,69,-19965,-1286479984,650481802,190,313,1391773716,1903082104,-351749.625000,481495.329274,"
        "dheBIudI4kzG5BzJRqkX3P0jklTiURJqPy7ZbYM70qTtCUoEpaRCtKf5ZXyt5d70RMitgUn0Gw6NxCslMuYwzuEC7VHTS3laxxu0SRFlRCqNjmAWCDhuBW5F"
        "B\n"
        "0,116,-19095,58280767,1481907277,22,59094,2094272558,1170484018,-373966.718750,171906.292570,"
        "f510AlNlMminNxwNiPcusq0LLvsFRDhXkvsf\n"
        "0,-64,-6348,2107871825,-1669099722,94,51332,613474514,1171471976,-208961.640625,174151.230542,"
        "XefwHD6q1nyI9a3PoTZyMEA5iVHsYPa7sybNqL\n"
        "1,-9,-26156,1563917870,442194576,84,51706,2109002843,862892708,374212.875000,-302923.938047,26gBhBNVaIUm\n"
        "0,-50,-12995,1066513350,-1946200578,71,63644,849986537,1030180466,-317624.781250,959148.574271,"
        "z0EA5217g8UyRIAyJbfRUV5wl4PND8dkmddIKjVOkuEVIf3hfbXoiv3aJ\n"
        "0,-119,-28937,810413059,823515396,149,26520,1517959399,169815053,-603446.937500,-370286.150750,"
        "9R8m16IBc5HeRI7DknxpEUgWJBm4x20PxC59CMnItOtk8vJrC6A4CKZ8VKWpRcXF9FCiNeOYnk9eQpNC1H53atYgJakFlsMBw\n"
        "1,-45,-31338,-36954419,-664692289,184,63195,1219562073,17002319,-1173596.000000,824824.817642,l4jig0WOpg\n"
        "0,26,-27793,-1526335162,-1214586578,198,59743,924277691,874011040,83242.539062,926950.248806,"
        "9PSdhuF3wSGWbSYBlQ1TG1AY4W8dP9XYdkF8xEhBBjfMgAkRvMh1ikPi2K0yLd76VRSRxkyR0b5XmiR1mSZov18DMss3ZZFmL74UbKbaO8\n"
        "1,82,29185,521671067,-837017136,49,40723,165292647,947936979,841361.062500,-844738.573421,"
        "zTHJ4OqIFHHwtA1g8CQJ7TKHim\n"
        "0,87,5615,-1051269906,-1604385370,146,11441,1630198967,1249713761,-388103.843750,1076189.950762,"
        "YuEPEBIxj7ObXh72Hs0tfXuSMAiulmM\n"
        "0,14,-15735,-601665975,465282690,112,38313,2110954487,220676856,-59186.222656,-388313.026369,"
        "Q1TaMCjhwBehLpr6nZWSw7AiIniRlZLQLGpb5o6Jw0wkkPCm9Trlwx0JunKBdb71z5XZ6EO4nGwrYymokKj2Tgd69hJ3zDfXtW91pk8LJoogZ5k4eBY6AO4f"
        "5foOXx8Y\n"
        "1,12,-16776,1180521872,223443156,17,9129,79731701,1224093411,1081219.500000,827164.125278,"
        "QIBfBZ5IJ3cUrflMnXhbwUR11G7Kio7usbjBOYpcWUS7x0HO1KxoVuTtdVaoRlZA1rvPD97TA2dYmssqLN6xIRZeONtDLWAQJ\n"
        "0,-107,31727,-1591867183,-1996825670,149,36982,212142779,261612501,-1145179.125000,300664.524855,"
        "m3jQ5F9KK5GvNuzu4vNA3HXOiUnuwa9EBk4ujjNAYgB4Sbc\n"
        "0,-30,-18706,397209663,-1749250070,218,23433,947213670,1214747362,764643.875000,-1067754.642536,"
        "TDnTXGH7rQrQtLqzzSBvsvqDndOw20f63OhsNvdKO7KUa3tBqUYesnzXGJcAvQwTlC\n"
        "1,5,-4739,1473399672,-1380497726,42,34907,1185509519,1703735839,-1109720.875000,282228.585866,"
        "kCHt9YWDWXXKCWhKQjF6WiUEPKavByhM99Wg4zfQ8yHTcWQQLn8gLmOYaWhLUDTsb8K\n"
        "0,11,-11118,1585403252,-1972677775,138,10132,1857810184,1743755518,715141.625000,662444.853267,ZDWlsTAOiA\n"
        "0,88,-19408,1723735039,1260887765,63,45245,1562115657,881121328,941628.375000,-1040018.300530,"
        "za9igKwKTot2eSLeGmwUxLeetxIVVgoWLl1Wepc7O\n"
        "0,-104,-18440,-723827727,-1661682992,6,52302,1207399417,1847229768,-972030.125000,134324.879264,"
        "Vt7AMWEiP9enBY484ZZiMdZmkYYksb1aRbis34iAVichqrbM0Be1No78sedOjkrDuPbZZdw9nEuZxJ7TXDLH8EvArlP8X\n"
        "1,-80,-206,96628487,156208353,68,12243,130572485,2044844152,-764714.062500,270513.514219,"
        "P1z5hXPLEXHpLdmRj61fC8V1nheoXFm1KwCLGaH9HbWLNU5TlBB4TcGeeyEKBKaWX6RpwvcKiJcIFKwrRX0icI10JEHAx9TOnXCEcajVC1w1L\n"
        "0,-57,-21184,845909599,787358012,44,12598,2047340542,89679215,757567.375000,350092.517722,gDzrbOv3JI3XMABuPJ3h7YjGaGh\n"
        "0,-75,11284,-1675622729,79772755,113,42959,1862822314,1379179351,752514.062500,558004.136503,"
        "9WUijQ0zYrBH2LgcuJb8Q8hBr9QrOIF7di7IFi8Lrf2\n"
        "0,111,-12141,-1860157389,683151660,158,46134,1180195859,1905553416,-321015.562500,1134796.195329,b84HKebLJog4DZBxBitL4\n"
        "1,31,20403,364408560,1504358421,119,59333,1199439758,1250930994,787195.750000,821429.563308,"
        "KXOPTymuzFCHvPsaw7S6eWu0u2SrjrgcE5HCPCmgIUZip\n"
        "0,-16,18362,868018649,16290422,25,21331,1910424797,1778389107,-933677.562500,550810.816085,"
        "ihMG1s6UDvt0UfOYUxVaQN3usaHlEHpOiYrNAfpR5r5bkbTn5KO3saiIiyuO\n"
        "1,-57,-8778,-2029860170,648390872,126,9314,578281003,1809629262,-1041320.187500,-229250.552097,"
        "rKUjD7zhcwjO8m5rfB6hOEbefuQM0lTAg9vxEFixSetUdWhxtlVyfiPpXbtwVtI3QJSjczoxiONGoY2DjVumRWhqCqMO\n"
        "0,-104,3083,1139859351,-1642508208,146,53389,1361027944,633457793,796547.687500,522759.663071,oLklMI5y\n"
        "1,-61,1149,229770258,-1361838366,139,5251,983998010,446556328,966855.437500,-1167761.135882,"
        "x0wR0HHdAzCLblJ6bIffCV4I5nMEOMBqgr6WVyVSwQ9Xs4A6XfeBxEofmLgnvAzdRQ7xOOxrzdAhlSfA\n"
        "1,125,10838,-188633009,1918923526,238,34201,566408884,1028368880,-391306.468750,973974.297202,"
        "4huXNKM6jYGY20yljuDMKwcb52wb2fkD7IK2X31ceCyFaUbKqG3SDGZREd45pA5\n"
        "0,-13,-11558,1214675152,1121128416,124,7391,682503757,1288930014,798041.250000,-414278.181314,"
        "Q94u5n7CR4aE9XT1WAuAXMDJ91Ts3xqQ4XblNtEkHE2ASN8k7KUmJVSDkMp5vlaJOZKgqxJfNwqgmHZVmUuARRL2FarfkFdwn8UnwHfuR08HYwWmipNpnLZe"
        "GKBQ\n"
        "0,56,-27511,-1775846754,789020451,148,1172,1560164030,641828670,149693.234375,-753146.007309,"
        "rfXefrKSQZvtvcg478wbymKu9pnGaj0glOug2LN3M\n";

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_INFO("ArrowSourceIntegrationTest: content={}", content);
    NES_INFO("ArrowSourceIntegrationTest: expContent={}", expectedContent);

    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

}// namespace NES