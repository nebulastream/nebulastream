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

#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <filesystem>
#include <util/BenchmarkUtils.hpp>
#include <util/E2EBase.hpp>

using namespace Benchmarking;

const NES::DebugLevel LOG_LEVEL = NES::LOG_INFO;

static uint64_t portOffset = 13;
uint64_t sourceCnt;

std::chrono::nanoseconds runtime;
NES::NesCoordinatorPtr coordinator;

NES::QueryServicePtr queryService;
QueryId queryId;
NES::QueryCatalogPtr queryCatalog;
NES::StreamCatalogPtr streamCatalog;
NES::SchemaPtr schema;

void setupSources() {
    //register logical stream qnv
    schema =
        NES::Schema::create()->addField("id", NES::UINT64)->addField("value", NES::UINT64)->addField("timestamp", NES::UINT64);
    streamCatalog->addLogicalStream("benchmark", schema);

    for (int i = 0; i < 10; i++) {
        auto topoNode = TopologyNode::create(i, "", i, i, 2);
        auto streamCat = StreamCatalogEntry::create("CSV", "benchmark", "benchmark" + i, topoNode);
        streamCatalog->addPhysicalStream("benchmark", streamCat);
    }
}

void setUp() {
    std::cout << "setup" << std::endl;

    portOffset += 13;

    NES::CoordinatorConfigPtr crdConf = NES::CoordinatorConfig::create();
    crdConf->setEnableQueryMerging(true);
    std::cout << "E2EBase: Start coordinator" << std::endl;
    coordinator = std::make_shared<NES::NesCoordinator>(crdConf);
    coordinator->startCoordinator(/**blocking**/ false);

    queryService = coordinator->getQueryService();
    queryCatalog = coordinator->getQueryCatalog();
    streamCatalog = coordinator->getStreamCatalog();
    setupSources();
}

/**
 * @brief This benchmarks runs a projection query on one worker and one coordinator
 * @return
 */
int main() {
    NES::setupLogging("E2EMapBenchmark.log", LOG_LEVEL);
    std::cout << "Setup E2EMapBenchmark test class." << std::endl;

    setUp();

    string query = "Query::from(\"benchmark\").sink(NullOutputSinkDescriptor::create());";

    std::string benchmarkName = "E2EWindowBenchmark";
    std::string nesVersion = NES_VERSION;

    std::stringstream ss;
    ss << "Time,BM_Name,NES_Version,WorkerThreads,CoordinatorThreadCnt,SourceCnt,Mode,ProcessedBuffersTotal,ProcessedTasksTotal,"
          "ProcessedTuplesTotal,ProcessedBytesTotal,ThroughputInTupsPerSec,ThroughputInMBPerSec"
       << std::endl;

    std::cout << ss.str() << std::endl;
    auto startTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    for (int i = 1; i <= 1000; i++) {
        auto queryObj = std::make_shared<Query>(Query::from("benchmark").sink(NullOutputSinkDescriptor::create()));
        const QueryPlanPtr queryPlan = queryObj->getQueryPlan();
        queryPlan->setQueryId(i);

        queryService->addQueryRequest(query, queryObj, "TopDown");
    }

    auto lastQuery = queryCatalog->getQueryCatalogEntry(1000);
    while (lastQuery->getQueryStatus() != QueryStatus::Running) {
    }

    auto endTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    std::cout << queryCatalog->getAllQueryCatalogEntries().size();
    std::cout << "result=" << std::endl;
    ss << startTime << " , " << endTime;
    std::cout << ss.str() << std::endl;

    std::ofstream out("E2EWindowBenchmark.csv");
    out << ss.str();
    out.close();
    std::cout << "benchmark finish" << std::endl;

    return 0;
}