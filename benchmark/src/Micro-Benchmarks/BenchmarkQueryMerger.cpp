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

#include <E2EBenchmarks/E2EBase.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <filesystem>
#include <util/BenchmarkUtils.hpp>

using namespace NES;
using namespace NES::Benchmarking;

const NES::DebugLevel LOG_LEVEL = NES::LOG_NONE;
const uint64_t NO_OF_PHYSICAL_SOURCES = 10;
//const uint64_t NO_OF_DISTINCT_SOURCES = 2;
//const uint64_t NO_OF_QUERIES_TO_SEND = 1000;
//const std::string QUERY_MERGER_RULE = "SyntaxBasedCompleteQueryMergerRule";
//const std::string QUERY_MERGER_RULE = "StringSignatureBasedCompleteQueryMergerRule";
//const std::string QUERY_MERGER_RULE = "ImprovedStringSignatureBasedCompleteQueryMergerRule";
const std::string QUERY_MERGER_RULE = "Z3SignatureBasedCompleteQueryMergerRule";
const uint64_t NO_OF_EXP_RUN = 3;
uint64_t sourceCnt;

std::chrono::nanoseconds runtime;
NES::NesCoordinatorPtr coordinator;

void setupSources(NesCoordinatorPtr nesCoordinator) {
    NES::StreamCatalogPtr streamCatalog = nesCoordinator->getStreamCatalog();
    //register logical stream qnv
    NES::SchemaPtr schema = NES::Schema::create()
                                ->addField("id", NES::UINT64)
                                ->addField("val", NES::UINT64)
                                ->addField("X", NES::UINT64)
                                ->addField("Y", NES::UINT64);
    //FIXME: We need to revisit it when running different benchmarks
    //    for (int j = 0; j < NO_OF_DISTINCT_SOURCES; j++) {
    streamCatalog->addLogicalStream("example", schema);
    //    }

    for (int i = 1; i <= NO_OF_PHYSICAL_SOURCES; i++) {
        auto topoNode = TopologyNode::create(i, "", i, i, 2);
        //        for (int j = 0; j < NO_OF_DISTINCT_SOURCES; j++) {
        auto streamCat = StreamCatalogEntry::create("CSV", "example", "benchmark" + std::to_string(i), topoNode);
        streamCatalog->addPhysicalStream("example", streamCat);
        //        }
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

    NES::setupLogging("BenchmarkQueryMerger.log", LOG_LEVEL);
    std::cout << "Setup BenchmarkQueryMerger test class." << std::endl;
    std::stringstream benchmarkOutput;
    benchmarkOutput << "Time,BM_Name,Merge_Rule,Num_of_Phy_Src,Num_Of_Queries,Num_Of_SharedQueryPlans,NES_Version,Run_Num,Start_"
                       "Time,End_Time,Total_Run_Time"
                    << std::endl;

    std::ifstream infile("");
    std::vector<std::string> queries;
    std::string line;

    while (std::getline(infile, line)) {
        std::istringstream iss(line);
        queries.emplace_back(line);
    }
    QueryPtr queryObjects[queries.size()];

    for (auto i = 0; i < queries.size(); i++) {
        auto queryObj = UtilityFunctions::createQueryFromCodeString(queries[i]);
        queryObjects[i] = queryObj;
    }

    for (int expRun = 1; expRun <= NO_OF_EXP_RUN; expRun++) {
        setUp();
        NES::QueryServicePtr queryService = coordinator->getQueryService();
        NES::QueryCatalogPtr queryCatalog = coordinator->getQueryCatalog();
        auto globalQueryPlan = coordinator->getGlobalQueryPlan();

        auto startTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        for (int i = 1; i <= queries.size(); i++) {
            const QueryPlanPtr queryPlan = queryObjects[i - 1]->getQueryPlan();
            queryPlan->setQueryId(i);
            queryService->addQueryRequest(queries[i - 1], queryObjects[i - 1], "TopDown");
        }

        auto lastQuery = queryCatalog->getQueryCatalogEntry(queries.size());
        while (lastQuery->getQueryStatus() != QueryStatus::Running) {
            sleep(.1);
        }

        auto endTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        benchmarkOutput << endTime << ",QueryMergerBenchmark," << QUERY_MERGER_RULE << "," << NO_OF_PHYSICAL_SOURCES << ","
                        << queries.size() << "," << globalQueryPlan->getAllSharedQueryMetaData().size() << "," << NES_VERSION
                        << "," << expRun << "," << startTime << "," << endTime << "," << endTime - startTime << std::endl;
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