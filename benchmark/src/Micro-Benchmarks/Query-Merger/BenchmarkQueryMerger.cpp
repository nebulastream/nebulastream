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

#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/QueryCatalogEntry.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Catalogs/StreamCatalogEntry.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Components/NesCoordinator.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Services/QueryParsingService.hpp>
#include <Services/QueryService.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/yaml/Yaml.hpp>
#include <util/BenchmarkUtils.hpp>

using namespace NES;
using namespace NES::Benchmarking;
using std::filesystem::directory_iterator;

uint64_t sourceCnt;
std::vector<uint64_t> noOfPhysicalSources;
uint64_t noOfMeasurementsToCollect;
uint64_t numberOfDistinctSources;
uint64_t startupSleepIntervalInSeconds;
NES::DebugLevel loglevel;
std::vector<std::string> queryMergerRules;
std::vector<bool> enableQueryMerging;
std::vector<uint64_t> batchSizes;
std::string querySetLocation;
std::chrono::nanoseconds Runtime;
NES::NesCoordinatorPtr coordinator;

void setupSources(NesCoordinatorPtr nesCoordinator, uint64_t noOfPhysicalSource) {
    NES::StreamCatalogPtr streamCatalog = nesCoordinator->getStreamCatalog();
    //register logical stream qnv
    NES::SchemaPtr schema1 = NES::Schema::create()
                                 ->addField("a", NES::UINT64)
                                 ->addField("b", NES::UINT64)
                                 ->addField("c", NES::UINT64)
                                 ->addField("d", NES::UINT64)
                                 ->addField("e", NES::UINT64)
                                 ->addField("f", NES::UINT64)
                                 ->addField("time1", NES::UINT64)
                                 ->addField("time2", NES::UINT64);

    NES::SchemaPtr schema2 = NES::Schema::create()
                                 ->addField("g", NES::UINT64)
                                 ->addField("h", NES::UINT64)
                                 ->addField("i", NES::UINT64)
                                 ->addField("j", NES::UINT64)
                                 ->addField("k", NES::UINT64)
                                 ->addField("l", NES::UINT64)
                                 ->addField("time1", NES::UINT64)
                                 ->addField("time2", NES::UINT64);

    NES::SchemaPtr schema3 = NES::Schema::create()
                                 ->addField("m", NES::UINT64)
                                 ->addField("n", NES::UINT64)
                                 ->addField("o", NES::UINT64)
                                 ->addField("p", NES::UINT64)
                                 ->addField("q", NES::UINT64)
                                 ->addField("r", NES::UINT64)
                                 ->addField("time1", NES::UINT64)
                                 ->addField("time2", NES::UINT64);

    uint16_t counter = 1;
    for (uint64_t j = 0; j < numberOfDistinctSources; j++) {
        if (counter == 1) {
            streamCatalog->addLogicalStream("example" + std::to_string(j + 1), schema1);
        } else if (counter == 2) {
            streamCatalog->addLogicalStream("example" + std::to_string(j + 1), schema2);
        } else if (counter == 3) {
            streamCatalog->addLogicalStream("example" + std::to_string(j + 1), schema3);
            counter = 0;
        }
        counter++;

        for (uint64_t i = 1; i <= noOfPhysicalSource; i++) {
            auto topoNode = TopologyNode::create(i, "", i, i, 2);
            auto streamCat = StreamCatalogEntry::create("CSV",
                                                        "example" + std::to_string(j + 1) + std::to_string(i),
                                                        "example" + std::to_string(j + 1),
                                                        topoNode);
            streamCatalog->addPhysicalStream("example" + std::to_string(j + 1), streamCat);
        }
    }
}

void setUp(std::string queryMergerRule, uint64_t noOfPhysicalSources, uint64_t batchSize) {
    std::cout << "setup and start coordinator" << std::endl;
    NES::CoordinatorConfigPtr crdConf = NES::CoordinatorConfig::create();
    crdConf->setQueryMergerRule(queryMergerRule);
    crdConf->setQueryBatchSize(batchSize);
    coordinator = std::make_shared<NES::NesCoordinator>(crdConf);
    coordinator->startCoordinator(/**blocking**/ false);
    setupSources(coordinator, noOfPhysicalSources);
}

void loadConfigFromYAMLFile(const std::string& filePath) {

    if (!filePath.empty() && std::filesystem::exists(filePath)) {
        try {
            NES_INFO("NesE2EBenchmarkConfig: Using config file with path: " << filePath << " .");
            Yaml::Node config = *(new Yaml::Node());
            Yaml::Parse(config, filePath.c_str());
            noOfMeasurementsToCollect = config["numberOfMeasurementsToCollect"].As<uint64_t>();
            querySetLocation = config["querySetLocation"].As<std::string>();
            startupSleepIntervalInSeconds = config["startupSleepIntervalInSeconds"].As<uint64_t>();
            numberOfDistinctSources = config["numberOfDistinctSources"].As<uint64_t>();

            auto configuredQueryMergerRules = config["queryMergerRule"].As<std::string>();
            {
                std::stringstream ss(configuredQueryMergerRules);
                std::string str;
                while (getline(ss, str, ',')) {
                    queryMergerRules.emplace_back(str);
                }
            }

            auto configuredEnableQueryMerging = config["enableQueryMerging"].As<std::string>();
            {
                std::stringstream ss(configuredEnableQueryMerging);
                std::string str;
                while (getline(ss, str, ',')) {
                    bool b;
                    std::istringstream(str) >> std::boolalpha >> b;
                    enableQueryMerging.emplace_back(b);
                }
            }

            auto configuredNoOfPhysicalSources = config["noOfPhysicalSources"].As<std::string>();
            {
                std::stringstream ss(configuredNoOfPhysicalSources);
                std::string str;
                while (getline(ss, str, ',')) {
                    noOfPhysicalSources.emplace_back(std::stoi(str));
                }
            }

            loglevel = NES::getDebugLevelFromString(config["logLevel"].As<std::string>());

            auto configuredBatchSizes = config["batchSize"].As<std::string>();
            {
                std::stringstream ss(configuredBatchSizes);
                std::string str;
                while (getline(ss, str, ',')) {
                    batchSizes.emplace_back(std::stoi(str));
                }
            }
        } catch (std::exception& e) {
            NES_ERROR("NesE2EBenchmarkConfig: Error while initializing configuration parameters from YAML file." << e.what());
        }
        return;
    }
    NES_ERROR("NesE2EBenchmarkConfig: No file path was provided or file could not be found at " << filePath << ".");
    NES_WARNING("Keeping default values for Worker Config.");
}

/**
 * @brief This benchmarks time taken in the preparation of Global Query Plan after merging @param{NO_OF_QUERIES_TO_SEND} number of queries.
 */
int main(int argc, const char* argv[]) {

    NES::setupLogging("BenchmarkQueryMerger.log", NES::LOG_INFO);
    std::cout << "Setup BenchmarkQueryMerger test class." << std::endl;
    std::stringstream benchmarkOutput;
    benchmarkOutput << "Time,BM_Name,Merge_Rule,Num_of_Phy_Src,Num_Of_Queries,Num_Of_SharedQueryPlans,ActualOperator,"
                       "SharedOperators,OperatorEfficiency,NES_Version,Run_Num,Start_"
                       "Time,End_Time,Total_Run_Time"
                    << std::endl;

    std::map<std::string, std::string> commandLineParams;

    for (int i = 1; i < argc; ++i) {
        commandLineParams.insert(std::pair<std::string, std::string>(
            std::string(argv[i]).substr(0, std::string(argv[i]).find("=")),
            std::string(argv[i]).substr(std::string(argv[i]).find("=") + 1, std::string(argv[i]).length() - 1)));
    }

    auto configPath = commandLineParams.find("--configPath");

    if (configPath != commandLineParams.end()) {
        loadConfigFromYAMLFile(configPath->second.c_str());
    } else {
//                loadConfigFromYAMLFile(
//                    "/home/ankit/dima/nes/benchmark/src/Micro-Benchmarks/Query-Merger/Confs/QueryMergerBenchmarkConfig.yaml");
        loadConfigFromYAMLFile(
            "/home/ankit-ldap/tmp/nes/benchmark/src/Micro-Benchmarks/Query-Merger/Confs/QueryMergerBenchmarkConfig.yaml");
    }

    NES::setupLogging("BM.log", loglevel);
    for (const auto& file : directory_iterator(querySetLocation)) {
        std::ifstream infile(file.path());
        NES_BM("*****************************************************");
        NES_BM("Benchmark For : " << file.path().filename());
        NES_BM("*****************************************************");
        std::vector<std::string> queries;
        std::string line;

        while (std::getline(infile, line)) {
            std::istringstream iss(line);
            queries.emplace_back(line);
        }

        uint64_t noOfQueries = queries.size();
        QueryPtr queryObjects[noOfQueries];

#pragma omp parallel for
        for (uint64_t i = 0; i < queries.size(); i++) {
            auto cppCompiler = Compiler::CPPCompiler::create();
            auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
            auto queryParsingService = QueryParsingService::create(jitCompiler);
            auto queryObj = queryParsingService->createQueryFromCodeString(queries[i]);
            queryObjects[i] = queryObj;
        }

        uint64_t totalOperators = 0;
        for (auto queryObject : queryObjects) {
            totalOperators = totalOperators + QueryPlanIterator(queryObject->getQueryPlan()).snapshot().size();
        }

        for (size_t configNum = 0; configNum < queryMergerRules.size(); configNum++) {
            NES_BM("Query Merging Rule " << queryMergerRules[configNum]);

            for (uint64_t expRun = 1; expRun <= noOfMeasurementsToCollect; expRun++) {
                NES_BM("Experiment " << expRun);
                setUp(queryMergerRules[configNum], noOfPhysicalSources[configNum], batchSizes[configNum]);
                NES::QueryServicePtr queryService = coordinator->getQueryService();
                NES::QueryCatalogPtr queryCatalog = coordinator->getQueryCatalog();
                auto globalQueryPlan = coordinator->getGlobalQueryPlan();
                sleep(startupSleepIntervalInSeconds);

                auto startTime =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                for (uint64_t i = 1; i <= noOfQueries; i++) {
                    const QueryPlanPtr queryPlan = queryObjects[i - 1]->getQueryPlan();
                    queryPlan->setQueryId(i);
                    queryService->addQueryRequest(queries[i - 1], queryPlan, "TopDown");
                }

                auto lastQuery = queryCatalog->getQueryCatalogEntry(noOfQueries);
                while (lastQuery->getQueryStatus() != QueryStatus::Running) {
                    sleep(.1);
                }
                auto endTime =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();

                auto gqp = coordinator->getGlobalQueryPlan();
                auto allSQP = gqp->getAllSharedQueryPlans();
                std::cout << "Number of Sources : " << gqp->sourceNamesToSharedQueryPlanMap.size();
                uint64_t actualOperators = 0;
                for (auto sqp : allSQP) {
                    actualOperators = actualOperators + QueryPlanIterator(sqp->getQueryPlan()).snapshot().size();
                }

                auto efficiency = ((totalOperators - actualOperators) / totalOperators) * 100;

                benchmarkOutput << endTime << "," << file.path().filename() << "," << queryMergerRules[configNum] << ","
                                << noOfPhysicalSources[configNum] << "," << noOfQueries << ","
                                << globalQueryPlan->getAllSharedQueryPlans().size() << "," << totalOperators << ","
                                << actualOperators << "," << efficiency << "," << NES_VERSION << "," << expRun << "," << startTime
                                << "," << endTime << "," << endTime - startTime << std::endl;
                std::cout << "Finished Run " << expRun << "/" << noOfMeasurementsToCollect << std::endl;
                coordinator->stopCoordinator(true);
            }
            benchmarkOutput << std::endl << std::endl;
            std::cout << benchmarkOutput.str();
        }
    }
    std::cout << benchmarkOutput.str();
    std::ofstream out("BenchmarkQueryMerger.csv");
    out << benchmarkOutput.str();
    out.close();
    std::cout << "benchmark finish" << std::endl;
    return 0;
}