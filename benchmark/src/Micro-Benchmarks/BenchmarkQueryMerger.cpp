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

const NES::DebugLevel LOG_LEVEL = NES::LOG_NONE;
const uint64_t NO_OF_PHYSICAL_SOURCES = 10;
const uint64_t NO_OF_DISTINCT_SOURCES = 2;
const uint64_t NO_OF_QUERIES_TO_SEND = 1000;
//const std::string QUERY_MERGER_RULE = "SyntaxBasedCompleteQueryMergerRule";
const std::string QUERY_MERGER_RULE = "Z3SignatureBasedCompleteQueryMergerRule";
const uint64_t NO_OF_EXP_RUN = 1;
uint64_t sourceCnt;

std::chrono::nanoseconds runtime;
NES::NesCoordinatorPtr coordinator;

void setupSources(NesCoordinatorPtr nesCoordinator) {
    NES::StreamCatalogPtr streamCatalog = nesCoordinator->getStreamCatalog();
    //register logical stream qnv
    NES::SchemaPtr schema =
        NES::Schema::create()->addField("id", NES::UINT64)->addField("value", NES::UINT64)->addField("timestamp", NES::UINT64);
    for (int j = 0; j < NO_OF_DISTINCT_SOURCES; j++) {
        streamCatalog->addLogicalStream("benchmark" + std::to_string(j), schema);
    }

    for (int i = 1; i <= NO_OF_PHYSICAL_SOURCES; i++) {
        auto topoNode = TopologyNode::create(i, "", i, i, 2);
        for (int j = 0; j < NO_OF_DISTINCT_SOURCES; j++) {
            auto streamCat =
                StreamCatalogEntry::create("CSV", "benchmark" + std::to_string(j), "benchmark" + std::to_string(i), topoNode);
            streamCatalog->addPhysicalStream("benchmark" + std::to_string(j), streamCat);
        }
    }
}

void setUp() {
    std::cout << "setup and start coordinator" << std::endl;
    NES::CoordinatorConfigPtr crdConf = NES::CoordinatorConfig::create();
    crdConf->setEnableQueryMerging(true);
    crdConf->setQueryMergerRule(QUERY_MERGER_RULE);
    coordinator = std::make_shared<NES::NesCoordinator>(crdConf);
    coordinator->startCoordinator(/**blocking**/ false);
    setupSources(coordinator);
}

/**
 * @brief This benchmarks time taken in the preparation of Global Query Plan after merging @param{NO_OF_QUERIES_TO_SEND} number of queries.
 */
int main() {
    NES::setupLogging("E2EMapBenchmark.log", LOG_LEVEL);
    std::cout << "Setup E2EMapBenchmark test class." << std::endl;
    std::stringstream benchmarkOutput;
    benchmarkOutput
        << "Time,BM_Name,Merge_Rule,Num_of_Phy_Src,Num_Of_Queries,NES_Version,Run_Num,Start_Time,End_Time,Total_Run_Time"
        << std::endl;

    for (int expRun = 1; expRun <= NO_OF_EXP_RUN; expRun++) {
        setUp();
        NES::QueryServicePtr queryService = coordinator->getQueryService();
        NES::QueryCatalogPtr queryCatalog = coordinator->getQueryCatalog();
        string query = "Query::from(\"benchmark\").sink(NullOutputSinkDescriptor::create());";
        auto startTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        for (int i = 1; i <= NO_OF_QUERIES_TO_SEND; i++) {
            //            auto subQuery = Query::from("benchmark1");
            auto queryObj = std::make_shared<Query>(
                Query::from("benchmark0").filter(Attribute("value") > 10).sink(NullOutputSinkDescriptor::create()));
            const QueryPlanPtr queryPlan = queryObj->getQueryPlan();
            queryPlan->setQueryId(i);
            queryService->addQueryRequest(query, queryObj, "TopDown");
        }

        auto lastQuery = queryCatalog->getQueryCatalogEntry(NO_OF_QUERIES_TO_SEND);
        while (lastQuery->getQueryStatus() != QueryStatus::Running) {
            sleep(1);
        }

        auto endTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        benchmarkOutput << endTime << ",QueryMergerBenchmark," << QUERY_MERGER_RULE << "," << NO_OF_PHYSICAL_SOURCES << ","
                        << NO_OF_QUERIES_TO_SEND << "," << NES_VERSION << "," << expRun << "," << startTime << "," << endTime
                        << "," << endTime - startTime << std::endl;
        std::cout << "Finished Run " << expRun << "/" << NO_OF_EXP_RUN << std::endl;
    }

    std::cout << benchmarkOutput.str();
    std::ofstream out("E2EWindowBenchmark.csv");
    out << benchmarkOutput.str();
    out.close();
    std::cout << "benchmark finish" << std::endl;
    coordinator->stopCoordinator(true);
    return 0;
}