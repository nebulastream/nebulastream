/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <benchmark/benchmark.h>
#include <iostream>
#include <util/BenchmarkUtils.hpp>

NesCoordinatorPtr crd;
NesWorkerPtr wrk1;
std::vector<NodeEngine::QueryStatistics*>* statisticsVec;
QueryServicePtr queryService;
QueryId queryId;
QueryCatalogPtr queryCatalog;
SchemaPtr schema;

void setup() {
    NES::setupLogging("RunSelectionBenchmark.log", NES::LOG_DEBUG);
    NES_INFO("Setup RunSelectionBenchmark test class.");

    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(4000);
    crdConf->setRestPort(8081);
    wrkConf->setCoordinatorPort(4000);

    NES_INFO("RunSelectionBenchmark: Start coordinator");
    crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    NES_INFO("RunSelectionBenchmark: Coordinator started successfully");

    NES_INFO("RunSelectionBenchmark: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    wrk1 = std::make_shared<NesWorker>(wrkConf, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    assert(retStart1);
    NES_INFO("RunSelectionBenchmark: Worker1 started successfully");

    schema = Schema::create()
                      ->addField(createField("value", UINT64))
                      ->addField(createField("id", UINT64))
                      ->addField(createField("timestamp", UINT64));


    //register logical stream qnv
    std::string input =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "input.hpp";
    std::ofstream out(testSchemaFileName);
    out << input;
    out.close();
    wrk1->registerLogicalStream("input", testSchemaFileName);

    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/window.csv");
    //    srcConf->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->setNumberOfBuffersToProduce(10);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("input");
    srcConf->setSkipHeader(true);
    //register physical stream
    PhysicalStreamConfigPtr inputStream = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(inputStream);

    queryService = crd->getQueryService();
    queryCatalog = crd->getQueryCatalog();

    NES_INFO("RunSelectionBenchmark: Submit query");
    string query = "Query::from(\"input\").project(Attribute(\"id\")).sink(FileSinkDescriptor::create(\"test.out\"));";
    queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    assert(TestUtils::waitForQueryToStart(queryId, queryCatalog));
}

void tearDown() {
    NES_INFO("RunSelectionBenchmark: Remove query");
    assert(queryService->validateAndQueueStopRequest(queryId));
    assert(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("RunSelectionBenchmark: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    assert(retStopWrk1);

    NES_INFO("RunSelectionBenchmark: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    assert(retStopCord);
    NES_INFO("RunSelectionBenchmark: Test finished");
}

int main() {
    std::cout << "setup" << std::endl;
    setup();
    statisticsVec = new std::vector<NodeEngine::QueryStatistics*>();
    NES::Benchmarking::BenchmarkUtils::runSingleExperimentSeconds = 10;
    NES::Benchmarking::BenchmarkUtils::periodLengthInSeconds = 1;

    NES::Benchmarking::BenchmarkUtils::recordStatistics(*statisticsVec, wrk1->getNodeEngine());

    NES::Benchmarking::BenchmarkUtils::computeDifferenceOfStatistics(*statisticsVec);
    std::cout << "Benchmark results" << std::endl;
    for (auto statistic : *statisticsVec) {
        std::cout << NES::Benchmarking::BenchmarkUtils::getStatisticsAsCSV(statistic, schema) << std::endl;
    }

    //    sleep(5)
    //    std::cout << "wakeup" << std::endl;

    tearDown();
    std::cout << "tear down" << std::endl;

    return 0;
}