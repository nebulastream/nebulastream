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
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/MemorySourceType.hpp>
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
#include <string.h>

namespace NES {

class VariableLengthIntegrationTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("VariableLengthIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup VariableLengthIntegrationTest test class.");
    }

    std::string generateRandomString(size_t size) {
        auto generateRandomCharacter = []() -> char {
            const char charset[] = "0123456789"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "abcdefghijklmnopqrstuvwxyz";
            return charset[rand() % (sizeof(charset) - 1)];
        };
        std::string str(size, 0);
        std::generate_n(str.begin(), size, generateRandomCharacter);
        return str;
    }
};

/// This test reads from a csv sink which contains variable-length fields and writes
/// the same to a csv sink
TEST_F(VariableLengthIntegrationTest, testCsvSourceWithVariableLengthFields) {
    // TODO check if using TestUtils is a better idea here?
    std::string inputFileName = "variable-length.csv";
    std::string outputFileName = "testCsvSourceWithVariableLengthFields.csv";
    std::string inputFilePath = std::filesystem::path(TEST_DATA_DIRECTORY) / inputFileName;
    std::string outputFilePath = getTestResourceFolder() / outputFileName;
    remove(outputFilePath.c_str());

    // elegant project test schema
    auto testSchema = Schema::create()
                          ->addField("camera_id", BasicType::UINT64)
                          ->addField("timestamp", BasicType::UINT64)
                          ->addField("rows", BasicType::UINT64)
                          ->addField("cols", BasicType::UINT64)
                          ->addField("type", BasicType::UINT64)
                          ->addField("data", BasicType::TEXT);// TEXT is the variable length field

    // setup coordinator
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    NES_INFO("VariableLengthIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    crd->getSourceCatalogService()->registerLogicalSource("variable_length", testSchema);

    EXPECT_NE(port, 0UL);
    NES_DEBUG("VariableLengthIntegrationTest: Coordinator started successfully");
    NES_DEBUG("VariableLengthIntegrationTest: Start worker 1");

    // setup csv sources
    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(inputFilePath);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(10);
    csvSourceType->setNumberOfBuffersToProduce(1);
    auto physicalSource = PhysicalSource::create("variable_length", "test_stream", csvSourceType);

    // setup worker
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    workerConfig1->physicalSources.add(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);

    ASSERT_TRUE(retStart1);
    NES_INFO("VariableLengthIntegrationTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    // register query
    auto query = Query::from("variable_length").sink(FileSinkDescriptor::create(outputFilePath, "CSV_FORMAT", "APPEND"));
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

    NES_INFO("VariableLengthIntegrationTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    string const expectedContent =
        "variable_length$camera_id:INTEGER(64 bits),variable_length$timestamp:INTEGER(64 bits),variable_length$rows:INTEGER(64 "
        "bits),"
        "variable_length$cols:INTEGER(64 bits),variable_length$type:INTEGER(64 bits),variable_length$data:Text\n"
        "50,45,198,209,14,"
        "fa37JncCHryDsbzayy4cBWDxS22JjzhMaiRrV41mtzxlYvKWrO72tK0LK0e1zLOZ2nOXpPIhMFSv8kP07U20o0J90xA0GWXIIwo7J4ogHFZQxwQ2RQ0D"
        "RJKR"
        "ETPVzxlFrXL8b7mtKLHIGhIh5JuWcFwrgJKdE3t5bECALy3eKIwYxEF3V7Z8KTx0nFe1IX5tjH22F5gXOa5LnIMIQuOiNJj\n"
        "19,178,31,54,134,"
        "8YL8rqDiZSkZfoEDAmGTXXqqvkCd5WKE2fMtVXa2zKae6opGY4i6bYuUG67LaSXd5tUbO4bNPB0TxnkWrSaQyUuEa0X9Q5mVwG4JLgeipeBlQtFFJpgH"
        "JYTr"
        "Wz0w2kQw1UFK8u2yWBjw3yCMlqc4M3tt2un4cDzdiEvq8vmf7TZAPjUAZ6Cu86nAyYDamCCSQ7GX33A8W\n"
        "46,119,171,110,140,hGwRk40pHuxNf5JEItyS3QrBgOChWKCDa6eIAd7RV4mBA5NQxJt0jk9N6L5cdFnDLSWV3bvYghhol4EgN5e4poSt7V\n"
        "55,153,184,11,12,VlkJw5jSYm4TKi92Ws4iYQoCSbysV6Nyp5Fl8wCfiE81uF1O7\n"
        "32,121,189,87,183,36dRsouSmmxq8tfB7PK3Zzmn5lhLm5Qn92F2q9UatPR1G4DNRVR0SBlXwQqgTFRdHgd5n5ffS4gi9\n"
        "246,250,214,64,35,"
        "r6YKVZmgIIaj8ECLfncKQh5TLkvPPcYEg5ZBeJpubNdiZq3CbeW2JcTeKP4j1ayffXqHqdCQ0n8Xb9jDnEF7oij85ls4MqjzLXF9A\n"
        "40,31,23,205,194,"
        "PZ8CffopP1adEfRuPX0AP2UDmSWHhgS6DaIrE4eb5EEJudCHACPYCulwMIE1wg57ENyQSc1VpFnjqz019PZLHIIbYWaSAfaM3WnT7oyw2jdsibrryODE"
        "hTpF"
        "zQi73GT6kGXr5Ul7DOxwxplwDyAuRx8OLoVP2zTmDzeITNNekLYh8KbLIjEihK408aNAXrwko\n"
        "207,192,48,64,218,"
        "Y1HwMtgfSLnmx72gLiLfnKlLhtsWpaKMZZGwTubvFNhAUhppQASDSBYA4OetwzDWYTQzNzubMZlqHadfj3sBEOJIkyAevNATpYRAYLlutVj85MnoOfyc"
        "1Hvl"
        "F3N8QYaD41OcK7VDcELgY8SwlQXmiQVvTt4rPe5RdR4xYXB9lUpHdHCMgj7O7aHaRJRovWGYvKUUrfba7Qpif15LiChpkxNCGp0AJGgFYAh\n"
        "161,77,146,35,31,"
        "PnIxvgndJmgfTqKGbHenWRlgk2KxaVeyGuv9YinsTRVwIpCt7qedHPH0Pbx04awLSrS1YFr1fMvx97oGwQrBp89Di5Bmf757yY6UlvTQHOLRU9fQZXZN"
        "dhYL"
        "mj6RqBWmhbHRWkrm9BBbIqzqLYDzFjK1SQQIav2HWJi22Ym9jxkzojp7F06TjRUBptRPoUfKlLKnr7uY2eYqLNwbO247RWHHNieBAHTwdohUtc3vEbkY"
        "yg9K"
        "iBS\n"
        "241,110,30,47,135,"
        "8fjP3P1EYJiUwU9ONjRGw00UxgbHNmjVRQsUotjMAPo4txTEfsUbrT3o9e5UQnxpBnIzfzLpO9uF5LTiDvH4OKqWywyMhw9sjRsOQBCmL61ORS6cONfm"
        "hVGd"
        "PFx6B\n";

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_INFO("VariableLengthIntegrationTest: content={}", content);
    NES_INFO("VariableLengthIntegrationTest: expContent={}", expectedContent);

    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

}// namespace NES