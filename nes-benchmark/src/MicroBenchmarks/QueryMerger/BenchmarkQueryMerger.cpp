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

#include <BorrowedPort.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Components/NesCoordinator.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Services/QueryParsingService.hpp>
#include <Services/QueryService.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <Util/yaml/Yaml.hpp>
#include <Version/version.hpp>
#include <detail/PortDispatcher.hpp>
#include <fstream>
#include <unistd.h>

using namespace NES;
using std::filesystem::directory_iterator;

uint64_t sourceCnt;
std::vector<uint64_t> noOfPhysicalSources;
uint64_t noOfMeasurementsToCollect;
uint64_t numberOfDistinctSources;
uint64_t startupSleepIntervalInSeconds;
std::vector<std::string> queryMergerRules;
std::vector<bool> enableQueryMerging;
std::vector<uint64_t> batchSizes;
std::string querySetLocation;
std::chrono::nanoseconds Runtime;
NES::NesCoordinatorPtr coordinator;
std::string logLevel;

/**
 * @brief Set up the physical sources for the benchmark
 * @param nesCoordinator : the coordinator shared object
 * @param noOfPhysicalSource : number of physical sources
 */
void setupSources(NesCoordinatorPtr nesCoordinator, uint64_t noOfPhysicalSource) {
    Catalogs::Source::SourceCatalogPtr streamCatalog = nesCoordinator->getSourceCatalog();
    //register logical stream with different schema
    NES::SchemaPtr schema1 = NES::Schema::create()
                                 ->addField("a", BasicType::UINT64)
                                 ->addField("b", BasicType::UINT64)
                                 ->addField("c", BasicType::UINT64)
                                 ->addField("d", BasicType::UINT64)
                                 ->addField("e", BasicType::UINT64)
                                 ->addField("f", BasicType::UINT64)
                                 ->addField("time1", BasicType::UINT64)
                                 ->addField("time2", BasicType::UINT64);

    NES::SchemaPtr schema2 = NES::Schema::create()
                                 ->addField("g", BasicType::UINT64)
                                 ->addField("h", BasicType::UINT64)
                                 ->addField("i", BasicType::UINT64)
                                 ->addField("j", BasicType::UINT64)
                                 ->addField("k", BasicType::UINT64)
                                 ->addField("l", BasicType::UINT64)
                                 ->addField("time1", BasicType::UINT64)
                                 ->addField("time2", BasicType::UINT64);

    NES::SchemaPtr schema3 = NES::Schema::create()
                                 ->addField("m", BasicType::UINT64)
                                 ->addField("n", BasicType::UINT64)
                                 ->addField("o", BasicType::UINT64)
                                 ->addField("p", BasicType::UINT64)
                                 ->addField("q", BasicType::UINT64)
                                 ->addField("r", BasicType::UINT64)
                                 ->addField("time1", BasicType::UINT64)
                                 ->addField("time2", BasicType::UINT64);

    NES::SchemaPtr schema4 = NES::Schema::create()
                                 ->addField("s", BasicType::UINT64)
                                 ->addField("t", BasicType::UINT64)
                                 ->addField("u", BasicType::UINT64)
                                 ->addField("v", BasicType::UINT64)
                                 ->addField("w", BasicType::UINT64)
                                 ->addField("x", BasicType::UINT64)
                                 ->addField("time1", BasicType::UINT64)
                                 ->addField("time2", BasicType::UINT64);

    //Add the logical and physical stream to the stream catalog
    uint64_t counter = 1;
    for (uint64_t j = 0; j < numberOfDistinctSources; j++) {
        //We increment the counter till 3 and then reset it to 0
        //When the counter is 1 we add the logical stream with schema type 1
        //When the counter is 2 we add the logical stream with schema type 2
        //When the counter is 3 we add the logical stream with schema type 3

        if (counter == 1) {
            streamCatalog->addLogicalSource("example" + std::to_string(j + 1), schema1);
        } else if (counter == 2) {
            streamCatalog->addLogicalSource("example" + std::to_string(j + 1), schema2);
        } else if (counter == 3) {
            streamCatalog->addLogicalSource("example" + std::to_string(j + 1), schema3);
        } else if (counter == 4) {
            streamCatalog->addLogicalSource("example" + std::to_string(j + 1), schema4);
            counter = 0;
        }
        LogicalSourcePtr logicalSource = streamCatalog->getLogicalSource("example" + std::to_string(j + 1));
        counter++;

        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

        // Add Physical topology node and stream catalog entry
        for (uint64_t i = 1; i <= noOfPhysicalSource; i++) {
            //Create topology node
            auto topologyNode = TopologyNode::create(i, "", i, i, 2, properties);
            //Create physical source
            auto physicalSource =
                PhysicalSource::create("example" + std::to_string(j + 1), "example" + std::to_string(j + 1) + std::to_string(i));
            //Map physical source and topology node
            Catalogs::Source::SourceCatalogEntryPtr sce =
                std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, topologyNode);
            streamCatalog->addPhysicalSource("example" + std::to_string(j + 1), sce);
        }
    }
}

/**
 * @brief Setup coordinator configuration and sources to run the experiments
 * @param queryMergerRule : the query merger rule
 * @param noOfPhysicalSources : total number of physical sources
 * @param batchSize : the batch size for query processing
 */
void setUp(const std::string queryMergerRule, uint64_t noOfPhysicalSources, uint64_t batchSize) {
    std::cout << "setup and start coordinator" << std::endl;
    NES::CoordinatorConfigurationPtr coordinatorConfig = NES::CoordinatorConfiguration::createDefault();
    NES::Testing::BorrowedPortPtr restPort = NES::Testing::detail::getPortDispatcher().getNextPort();
    NES::Testing::BorrowedPortPtr rpcCoordinatorPort = NES::Testing::detail::getPortDispatcher().getNextPort();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    OptimizerConfiguration optimizerConfiguration;
    optimizerConfiguration.queryMergerRule = magic_enum::enum_cast<Optimizer::QueryMergerRule>(queryMergerRule).value();
    optimizerConfiguration.queryBatchSize = batchSize;
    coordinatorConfig->optimizer = optimizerConfiguration;
    coordinatorConfig->logLevel = magic_enum::enum_cast<LogLevel>(logLevel).value();
    coordinator = std::make_shared<NES::NesCoordinator>(coordinatorConfig);
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
            NES_INFO("NesE2EBenchmarkConfig: Using config file with path: {} .", filePath);
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

            logLevel = config["logLevel"].As<std::string>();
        } catch (std::exception& e) {
            NES_ERROR("NesE2EBenchmarkConfig: Error while initializing configuration parameters from YAML file. {}", e.what());
        }
        return;
    }
    NES_ERROR("NesE2EBenchmarkConfig: No file path was provided or file could not be found at {}.", filePath);
    NES_WARNING("Keeping default values for Worker Config.");
}

void compileQuery(const std::string& stringQuery,
                  uint64_t id,
                  const std::shared_ptr<QueryParsingService>& queryParsingService,
                  std::promise<QueryPlanPtr> promise) {
    auto queryplan = queryParsingService->createQueryFromCodeString(stringQuery);
    queryplan->setQueryId(id);
    promise.set_value(queryplan);
}

/**
 * @brief This benchmarks time taken in the preparation of Global Query Plan after merging @param{NO_OF_QUERIES_TO_SEND} number of queries.
 */
int main(int argc, const char* argv[]) {

    NES::Logger::setupLogging("BenchmarkQueryMerger.log", NES::LogLevel::LOG_INFO);
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
        loadConfigFromYAMLFile(configPath->second);
    } else {
        NES_ERROR("Configuration file is not provided");
        return -1;
    }

    NES::Logger::setupLogging("BM.log", magic_enum::enum_cast<LogLevel>(logLevel).value());

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

        //using thread pool to parallelize the compilation of string queries and string them in an array of query objects
        const uint32_t numOfQueries = queries.size();
        std::vector<QueryPlanPtr> queryObjects;

        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        auto queryParsingService = QueryParsingService::create(jitCompiler);

        //If no available thread then set number of threads to 1
        uint64_t numThreads = std::thread::hardware_concurrency();
        if (numThreads == 0) {
            NES_WARNING("No available threads. Going to use only 1 thread for parsing input queries.");
            numThreads = 1;
        }
        std::cout << "Using " << numThreads << " of threads for parallel parsing." << std::endl;

        uint64_t queryNum = 0;
        //Work till all queries are not parsed
        while (queryNum < numOfQueries) {
            std::vector<std::future<QueryPlanPtr>> futures;
            std::vector<std::thread> threadPool(numThreads);
            uint64_t threadNum;
            //Schedule queries to be parsed with #numThreads parallelism
            for (threadNum = 0; threadNum < numThreads; threadNum++) {
                //If no more query to parse
                if (queryNum >= numOfQueries) {
                    break;
                }
                //Schedule thread for execution and pass a promise
                std::promise<QueryPlanPtr> promise;
                //Store the future, schedule the thread, and increment the query count
                futures.emplace_back(promise.get_future());
                threadPool.emplace_back(
                    std::thread(compileQuery, queries[queryNum], queryNum + 1, queryParsingService, std::move(promise)));
                queryNum++;
            }

            //Wait for all unfinished threads
            for (auto& item : threadPool) {
                if (item.joinable()) {// if thread is not finished yet
                    item.join();
                }
            }
            std::cout << "Parsed " << queryNum << " queries." << std::endl;
            //Fetch the parsed query from all threads
            for (uint64_t futureNum = 0; futureNum < threadNum; futureNum++) {
                auto query = futures[futureNum].get();
                auto queryID = query->getQueryId();
                queryObjects.insert(queryObjects.begin() + queryID - 1, query);//Add the parsed query to the (queryID - 1)th index
            }
        }

        std::cout << "Parsed all queries." << std::endl;

        //Compute total number of operators in the query set
        uint64_t totalOperators = 0;
        for (auto queryObject : queryObjects) {
            totalOperators = totalOperators + QueryPlanIterator(queryObject).snapshot().size();
        }

        // For the input query set run the experiments with different type of query merger rule
        auto queryIter = 0;
        for (size_t configNum = 0; configNum < queryMergerRules.size(); configNum++) {
            //Number of time the experiments to run
            for (uint64_t expRun = 1; expRun <= noOfMeasurementsToCollect; expRun++) {

                //Setup coordinator for the experiment
                setUp(queryMergerRules[configNum], noOfPhysicalSources[configNum], batchSizes[configNum]);
                NES::QueryServicePtr queryService = coordinator->getQueryService();
                auto queryCatalogService = coordinator->getQueryCatalogService();
                auto globalQueryPlan = coordinator->getGlobalQueryPlan();
                //Sleep for fixed time before starting the experiments
                sleep(startupSleepIntervalInSeconds);

                auto startTime =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                //Send queries to nebula stream for processing
                for (uint64_t i = 1; i <= numOfQueries; i++) {
                    const QueryPlanPtr queryPlan = queryObjects[i - 1];
                    queryPlan->setQueryId(i);
                    queryService->addQueryRequest(queries[i - 1], queryPlan, Optimizer::PlacementStrategy::TopDown);
                }

                //Fetch the last query for the query catalog
                auto lastQuery = queryCatalogService->getEntryForQuery(numOfQueries + queryIter);
                //Wait till the status of the last query is set as running
                while (lastQuery->getQueryState() != QueryState::RUNNING) {
                    std::cout << "Query status " << lastQuery->getQueryState() << std::endl;
                    //Sleep for 100 milliseconds
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                auto endTime =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();

                //Fetch the global query plan and count the number of operators produced post merging the queries
                auto gqp = coordinator->getGlobalQueryPlan();
                auto allSQP = gqp->getAllSharedQueryPlans();
                uint64_t mergedOperators = 0;
                for (auto sqp : allSQP) {
                    unsigned long planSize = QueryPlanIterator(sqp->getQueryPlan()).snapshot().size();
                    mergedOperators = mergedOperators + planSize;
                }

                //Compute efficiency
                float efficiency = (((float) totalOperators - (float) mergedOperators) / (float) totalOperators) * 100;

                //Add the information in the log
                benchmarkOutput << endTime << "," << file.path().filename() << "," << queryMergerRules[configNum] << ","
                                << noOfPhysicalSources[configNum] << "," << numOfQueries << ","
                                << globalQueryPlan->getAllSharedQueryPlans().size() << "," << totalOperators << ","
                                << mergedOperators << "," << efficiency << "," << NES_VERSION << "," << expRun << "," << startTime
                                << "," << endTime << "," << endTime - startTime << std::endl;
                std::cout << "Finished Run " << expRun << "/" << noOfMeasurementsToCollect << std::endl;
                //Stop NES coordinator
                auto coordinatorSopped = coordinator->stopCoordinator(true);
                std::cout << "Coordinator stopped: " << coordinatorSopped << std::endl;
                queryIter += numOfQueries;
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