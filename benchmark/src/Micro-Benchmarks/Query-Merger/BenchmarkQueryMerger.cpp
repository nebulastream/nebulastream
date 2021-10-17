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

/**
 * @brief Set up the physical sources for the benchmark
 * @param nesCoordinator : the coordinator shared object
 * @param noOfPhysicalSource : number of physical sources
 */
void setupSources(NesCoordinatorPtr nesCoordinator, uint64_t noOfPhysicalSource) {
    NES::StreamCatalogPtr streamCatalog = nesCoordinator->getStreamCatalog();
    //register logical stream with different schema
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

    //Add the logical and physical stream to the stream catalog
    uint64_t counter = 1;
    for (uint64_t j = 0; j < numberOfDistinctSources; j++) {
        //We increment the counter till 3 and then reset it to 0
        //When the counter is 1 we add the logical stream with schema type 1
        //When the counter is 2 we add the logical stream with schema type 2
        //When the counter is 3 we add the logical stream with schema type 3
        if (counter == 1) {
            streamCatalog->addLogicalStream("example" + std::to_string(j + 1), schema1);
        } else if (counter == 2) {
            streamCatalog->addLogicalStream("example" + std::to_string(j + 1), schema2);
        } else if (counter == 3) {
            streamCatalog->addLogicalStream("example" + std::to_string(j + 1), schema3);
            counter = 0;
        }
        counter++;

        // Add Physical topology node and stream catalog entry
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

/**
 * @brief Setup coordinator configuration and sources to run the experiments
 * @param queryMergerRule : the query merger rule
 * @param noOfPhysicalSources : total number of physical sources
 * @param batchSize : the batch size for query processing
 */
void setUp(std::string queryMergerRule, uint64_t noOfPhysicalSources, uint64_t batchSize) {
    std::cout << "setup and start coordinator" << std::endl;
    NES::CoordinatorConfigPtr crdConf = NES::CoordinatorConfig::create();
    crdConf->setQueryMergerRule(queryMergerRule);
    crdConf->setQueryBatchSize(batchSize);
    coordinator = std::make_shared<NES::NesCoordinator>(crdConf);
    coordinator->startCoordinator(/**blocking**/ false);
    setupSources(coordinator, noOfPhysicalSources);
}

/**
 * @brief Split string by delimiter
 * @param input : string to split
 * @param delim : delimiter
 * @return  vector of split string
 */
std::vector<std::string> split(std::string input, char delim) {
    std::vector<std::string> result;
    std::stringstream ss(input);
    std::string item;
    while (getline(ss, item, delim)) {
        result.push_back(item);
    }
    return result;
}

/**
 * @brief Load provided configuration file
 * @param filePath : location of the configuration file
 */
void loadConfigFromYAMLFile(const std::string& filePath) {

    if (!filePath.empty() && std::filesystem::exists(filePath)) {
        try {
            NES_INFO("NesE2EBenchmarkConfig: Using config file with path: " << filePath << " .");
            Yaml::Node config = *(new Yaml::Node());
            Yaml::Parse(config, filePath.c_str());

            //Number of Measurements to collect
            noOfMeasurementsToCollect = config["numberOfMeasurementsToCollect"].As<uint64_t>();
            //Query set location
            querySetLocation = config["querySetLocation"].As<std::string>();
            //Startup sleep interval
            startupSleepIntervalInSeconds = config["startupSleepIntervalInSeconds"].As<uint64_t>();
            //Number of distinct sources
            numberOfDistinctSources = config["numberOfDistinctSources"].As<uint64_t>();
            //Query merger rules
            queryMergerRules = split(config["queryMergerRule"].As<std::string>(), ',');
            //Enable Query Merging
            auto enableQueryMergingOpts = split(config["enableQueryMerging"].As<std::string>(), ',');
            for (const auto& item : enableQueryMergingOpts) {
                bool booleanParm;
                std::istringstream(item) >> std::boolalpha >> booleanParm;
                enableQueryMerging.emplace_back(booleanParm);
            }

            //Load Number of Physical sources
            auto configuredNoOfPhysicalSources = split(config["noOfPhysicalSources"].As<std::string>(), ',');
            for (const auto& item : configuredNoOfPhysicalSources) {
                noOfPhysicalSources.emplace_back(std::stoi(item));
            }

            //Load batch size
            auto configuredBatchSizes = split(config["batchSize"].As<std::string>(), ',');
            for (const auto& item : configuredBatchSizes) {
                batchSizes.emplace_back(std::stoi(item));
            }

            //Log level
            loglevel = NES::getDebugLevelFromString(config["logLevel"].As<std::string>());
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

    //Load all command line arguments
    std::map<std::string, std::string> commandLineParams;
    for (int i = 1; i < argc; ++i) {
        commandLineParams.insert(std::pair<std::string, std::string>(
            std::string(argv[i]).substr(0, std::string(argv[i]).find("=")),
            std::string(argv[i]).substr(std::string(argv[i]).find("=") + 1, std::string(argv[i]).length() - 1)));
    }

    // Location of the configuration file
    auto configPath = commandLineParams.find("--configPath");

    //Load the configuration file
    if (configPath != commandLineParams.end()) {
        loadConfigFromYAMLFile(configPath->second.c_str());
    } else {
        NES_ERROR("Configuration file is not provided");
        return -1;
    }

    NES::setupLogging("BM.log", loglevel);

    //Load individual query set from the query set location and run the benchmark
    for (const auto& file : directory_iterator(querySetLocation)) {

        //Read the input query set and load the query string in the queries vector
        std::ifstream infile(file.path());
        std::vector<std::string> queries;
        std::string line;
        while (std::getline(infile, line)) {
            std::istringstream iss(line);
            queries.emplace_back(line);
        }

        uint64_t noOfQueries = queries.size();
        QueryPtr queryObjects[noOfQueries];

        //using Open MP to parallelize the compilation of string queries and string them in an array of query objects
#pragma omp parallel for
        for (uint64_t i = 0; i < queries.size(); i++) {
            auto cppCompiler = Compiler::CPPCompiler::create();
            auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
            auto queryParsingService = QueryParsingService::create(jitCompiler);
            auto queryObj = queryParsingService->createQueryFromCodeString(queries[i]);
            queryObjects[i] = queryObj;
        }

        //Compute total number of operators in the query set
        uint64_t totalOperators = 0;
        for (auto queryObject : queryObjects) {
            totalOperators = totalOperators + QueryPlanIterator(queryObject->getQueryPlan()).snapshot().size();
        }

        // For the input query set run the experiments with different type of query merger rule
        for (size_t configNum = 0; configNum < queryMergerRules.size(); configNum++) {
            //Number of time the experiments to run
            for (uint64_t expRun = 1; expRun <= noOfMeasurementsToCollect; expRun++) {

                //Setup coordinator for the experiment
                setUp(queryMergerRules[configNum], noOfPhysicalSources[configNum], batchSizes[configNum]);
                NES::QueryServicePtr queryService = coordinator->getQueryService();
                NES::QueryCatalogPtr queryCatalog = coordinator->getQueryCatalog();
                auto globalQueryPlan = coordinator->getGlobalQueryPlan();
                //Sleep for fixed time before starting the experiments
                sleep(startupSleepIntervalInSeconds);

                auto startTime =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                //Send queries to nebula stream for processing
                for (uint64_t i = 1; i <= noOfQueries; i++) {
                    const QueryPlanPtr queryPlan = queryObjects[i - 1]->getQueryPlan();
                    queryPlan->setQueryId(i);
                    queryService->addQueryRequest(queries[i - 1], queryPlan, "TopDown");
                }

                //Fetch the last query for the query catalog
                auto lastQuery = queryCatalog->getQueryCatalogEntry(noOfQueries);
                //Wait till the status of the last query is set as running
                while (lastQuery->getQueryStatus() != QueryStatus::Running) {
                    //Sleep for 100 milliseconds
                    sleep(.1);
                }
                auto endTime =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();

                //Fetch the global query plan and count the number of operators produced post merging the queries
                auto gqp = coordinator->getGlobalQueryPlan();
                auto allSQP = gqp->getAllSharedQueryPlans();
                std::cout << "Number of Sources : " << gqp->sourceNamesToSharedQueryPlanMap.size() << std::endl;
                uint64_t mergedOperators = 0;
                for (auto sqp : allSQP) {
                    unsigned long planSize = QueryPlanIterator(sqp->getQueryPlan()).snapshot().size();
                    mergedOperators = mergedOperators + planSize;
                }

                //Compute efficiency
                auto efficiency = ((totalOperators - mergedOperators) / totalOperators) * 100;

                //Add the information in the log
                benchmarkOutput << endTime << "," << file.path().filename() << "," << queryMergerRules[configNum] << ","
                                << noOfPhysicalSources[configNum] << "," << noOfQueries << ","
                                << globalQueryPlan->getAllSharedQueryPlans().size() << "," << totalOperators << ","
                                << mergedOperators << "," << efficiency << "," << NES_VERSION << "," << expRun << "," << startTime
                                << "," << endTime << "," << endTime - startTime << std::endl;
                std::cout << "Finished Run " << expRun << "/" << noOfMeasurementsToCollect << std::endl;
                //Stop NES coordinator
                coordinator->stopCoordinator(true);
            }
            benchmarkOutput << std::endl << std::endl;
            std::cout << benchmarkOutput.str();
        }
    }
    //Print the benchmark output and same it to the CSV file for further processing
    std::cout << benchmarkOutput.str();
    std::ofstream out("BenchmarkQueryMerger.csv");
    out << benchmarkOutput.str();
    out.close();
    std::cout << "benchmark finish" << std::endl;
    return 0;
}