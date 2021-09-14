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
using std::filesystem::directory_iterator;

uint64_t sourceCnt;
std::vector<uint64_t> noOfPhysicalSources;
uint64_t noOfMeasurementsToCollect;
uint64_t startupSleepIntervalInSeconds;
std::vector<std::string> queryMergerRules;
std::vector<bool> enableQueryMerging;
std::string querySetLocation;
std::chrono::nanoseconds Runtime;
NES::NesCoordinatorPtr coordinator;

void setupSources(NesCoordinatorPtr nesCoordinator, uint64_t noOfPhysicalSource, uint64_t noOfDistinctSources = 1) {
    NES::StreamCatalogPtr streamCatalog = nesCoordinator->getStreamCatalog();
    //register logical stream qnv
    NES::SchemaPtr schema = NES::Schema::create()
                                ->addField("id", NES::UINT64)
                                ->addField("val", NES::UINT64)
                                ->addField("X", NES::UINT64)
                                ->addField("Y", NES::UINT64);

    for (int j = 0; j < noOfDistinctSources; j++) {
        streamCatalog->addLogicalStream("example", schema);
        for (int i = 1; i <= noOfPhysicalSource; i++) {
            auto topoNode = TopologyNode::create(i, "", i, i, 2);
            auto streamCat = StreamCatalogEntry::create("CSV", "example", "benchmark" + std::to_string(i), topoNode);
            streamCatalog->addPhysicalStream("example", streamCat);
        }
    }
}

void setUp(std::string queryMergerRule, bool enableQueryMerging, uint64_t noOfPhysicalSources) {
    std::cout << "setup and start coordinator" << std::endl;
    NES::CoordinatorConfigPtr crdConf = NES::CoordinatorConfig::create();
    crdConf->setEnableQueryMerging(enableQueryMerging);
    crdConf->setQueryMergerRule(queryMergerRule);
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

            NES::setLogLevel(NES::getDebugLevelFromString(config["logLevel"].As<std::string>()));
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
    benchmarkOutput << "Time,BM_Name,Merge_Rule,Num_of_Phy_Src,Num_Of_Queries,Num_Of_SharedQueryPlans,NES_Version,Run_Num,Start_"
                       "Time,End_Time,Total_Run_Time"
                    << std::endl;

    std::map<string, string> commandLineParams;

    for (int i = 1; i < argc; ++i) {
        commandLineParams.insert(
            std::pair<string, string>(string(argv[i]).substr(0, string(argv[i]).find("=")),
                                      string(argv[i]).substr(string(argv[i]).find("=") + 1, string(argv[i]).length() - 1)));
    }

    auto configPath = commandLineParams.find("--configPath");

    if (configPath != commandLineParams.end()) {
        loadConfigFromYAMLFile(configPath->second.c_str());
    } else {
        loadConfigFromYAMLFile("/home/ankit-ldap/tmp/nes/benchmark/src/Micro-Benchmarks/Query-Merger/Confs/Z3MergeConfig.yaml");
    }

    for (const auto& file : directory_iterator(querySetLocation)) {

        std::ifstream infile(file.path());
        NES_ERROR(file.path().filename());
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
            auto queryObj = Util::createQueryFromCodeString(queries[i]);
            queryObjects[i] = queryObj;
        }

        for (auto configNum = 0; configNum < queryMergerRules.size(); configNum++) {

            for (int expRun = 1; expRun <= noOfMeasurementsToCollect; expRun++) {
                setUp(queryMergerRules[configNum], enableQueryMerging[configNum], noOfPhysicalSources[configNum]);
                NES::QueryServicePtr queryService = coordinator->getQueryService();
                NES::QueryCatalogPtr queryCatalog = coordinator->getQueryCatalog();
                auto globalQueryPlan = coordinator->getGlobalQueryPlan();
                sleep(startupSleepIntervalInSeconds);

                auto startTime =
                    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                for (int i = 1; i <= noOfQueries; i++) {
                    const QueryPlanPtr queryPlan = queryObjects[i - 1]->getQueryPlan();
                    queryPlan->setQueryId(i);
                    queryService->addQueryRequest(queries[i - 1], queryObjects[i - 1], "TopDown");
                }

                auto lastQuery = queryCatalog->getQueryCatalogEntry(noOfQueries);
                while (lastQuery->getQueryStatus() != QueryStatus::Running) {
                    sleep(.1);
                }

                auto endTime =
                    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                benchmarkOutput << endTime << "," << file.path().filename() << ","
                                << (enableQueryMerging[configNum] ? queryMergerRules[configNum] : "NoMerging") << ","
                                << noOfPhysicalSources[configNum] << "," << noOfQueries << ","
                                << globalQueryPlan->getAllSharedQueryMetaData().size() << "," << NES_VERSION << "," << expRun
                                << "," << startTime << "," << endTime << "," << endTime - startTime << std::endl;
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