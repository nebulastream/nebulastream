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

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Index/LocationIndex.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Components/NesCoordinator.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Optimizer/Phases/PlacementAmendment/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddLinkEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddNodeEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddQueryEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPRemoveLinkEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPRemoveQueryEvent.hpp>
#include <Services/QueryParsingService.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/BenchmarkUtils.hpp>
#include <Util/Core.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <Util/yaml/Yaml.hpp>
#include <fstream>
#include <iostream>
#include <thread>
#include <z3++.h>

using namespace NES;
using namespace NES::Benchmark;
using std::filesystem::directory_iterator;

std::chrono::nanoseconds Runtime;

class ErrorHandler : public Exceptions::ErrorListener {
  public:
    virtual void onFatalError(int signalNumber, std::string callstack) override {
        std::cout << "onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] callstack " << callstack;
    }

    virtual void onFatalException(std::shared_ptr<std::exception> exception, std::string callstack) override {
        std::cout << "onFatalException: exception=[" << exception->what() << "] callstack=\n" << callstack;
    }
};

/**
 * @brief Set up the physical sources for the benchmark
 * @param leafWorkerIds: leaf worker ids
 */
void setupSources(std::vector<WorkerId> leafWorkerIds, SourceCatalogServicePtr sourceCatalogService) {

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

    //Add the logical and physical stream to the stream catalog such that each leaf has two distinct sources attached
    for (const auto& leafWorkerId : leafWorkerIds) {
        uint16_t noOfPhySourcePerWorker = 2;
        for (uint16_t counter = 1; counter <= noOfPhySourcePerWorker; counter++) {
            const auto& logicalSourceName = "example" + std::to_string(leafWorkerId) + "-" + std::to_string(counter);
            auto physicalSourceName = "phy_" + logicalSourceName;
            if (counter == 1) {
                sourceCatalogService->registerLogicalSource(logicalSourceName, schema3);
            } else if (counter == 2) {
                sourceCatalogService->registerLogicalSource(logicalSourceName, schema3);
            }
            sourceCatalogService->registerPhysicalSource(physicalSourceName, logicalSourceName, leafWorkerId);
        }
    }
}

void setupTopology(uint16_t rootNodes,
                   uint16_t intermediateNodes,
                   uint16_t sourceNodes,
                   RequestHandlerServicePtr requestHandlerService,
                   SourceCatalogServicePtr sourceCatalogService,
                   TopologyPtr topology) {

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    std::vector<RequestProcessor::ISQPEventPtr> rootISQPAddNodeEvents;
    for (uint16_t counter = 0; counter < rootNodes; counter++) {
        rootISQPAddNodeEvents.emplace_back(
            RequestProcessor::ISQPAddNodeEvent::create(INVALID_WORKER_NODE_ID, "localhost", 0, 0, UINT16_MAX, properties));
    }

    requestHandlerService->queueISQPRequest(rootISQPAddNodeEvents);

    std::vector<RequestProcessor::ISQPEventPtr> intermediateISQPAddNodeEvents;
    for (uint16_t counter = 0; counter < intermediateNodes; counter++) {
        intermediateISQPAddNodeEvents.emplace_back(
            RequestProcessor::ISQPAddNodeEvent::create(INVALID_WORKER_NODE_ID, "localhost", 0, 0, UINT16_MAX, properties));
    }
    requestHandlerService->queueISQPRequest(intermediateISQPAddNodeEvents);

    std::vector<RequestProcessor::ISQPEventPtr> leafISQPAddNodeEvents;
    for (uint16_t counter = 0; counter < sourceNodes; counter++) {
        leafISQPAddNodeEvents.emplace_back(
            RequestProcessor::ISQPAddNodeEvent::create(INVALID_WORKER_NODE_ID, "localhost", 0, 0, UINT16_MAX, properties));
    }
    requestHandlerService->queueISQPRequest(leafISQPAddNodeEvents);

    // Fetch worker Ids
    std::vector<WorkerId> rootWorkerIds;
    rootWorkerIds.emplace_back(topology->getRootTopologyNodeId());
    for (const auto& rootISQPAddNodeEvent : rootISQPAddNodeEvents) {
        const auto& response = rootISQPAddNodeEvent->as<RequestProcessor::ISQPAddNodeEvent>()->response.get_future().get();
        auto workerId = std::static_pointer_cast<RequestProcessor::ISQPAddNodeResponse>(response)->workerId;
        rootWorkerIds.emplace_back(workerId);
    }

    std::vector<WorkerId> intermediateWorkerIds;
    for (const auto& intermediateISQPAddNodeEvent : intermediateISQPAddNodeEvents) {
        const auto& response =
            intermediateISQPAddNodeEvent->as<RequestProcessor::ISQPAddNodeEvent>()->response.get_future().get();
        auto workerId = std::static_pointer_cast<RequestProcessor::ISQPAddNodeResponse>(response)->workerId;
        intermediateWorkerIds.emplace_back(workerId);
    }

    std::vector<WorkerId> leafWorkerIds;
    for (const auto& leafISQPAddNodeEvent : leafISQPAddNodeEvents) {
        const auto& response = leafISQPAddNodeEvent->as<RequestProcessor::ISQPAddNodeEvent>()->response.get_future().get();
        auto workerId = std::static_pointer_cast<RequestProcessor::ISQPAddNodeResponse>(response)->workerId;
        leafWorkerIds.emplace_back(workerId);
    }

    // Remove connection between leaf and root nodes
    std::vector<RequestProcessor::ISQPEventPtr> linkRemoveEvents;
    for (const auto& rootWorkerId : rootWorkerIds) {
        for (const auto& leafWorkerId : leafWorkerIds) {
            auto linkRemoveEvent = RequestProcessor::ISQPRemoveLinkEvent::create(rootWorkerId, leafWorkerId);
            linkRemoveEvents.emplace_back(linkRemoveEvent);
        }
    }
    requestHandlerService->queueISQPRequest(linkRemoveEvents);

    // Add connection between leaf and intermediate nodes
    std::vector<RequestProcessor::ISQPEventPtr> linkAddEvents;
    uint16_t intermediateNodeCounter = 0;
    for (const auto& intermediateWorkerId : intermediateWorkerIds) {
        uint16_t connectivityCounter = 0;
        for (; intermediateNodeCounter < leafWorkerIds.size(); intermediateNodeCounter++) {
            auto linkAddEvent =
                RequestProcessor::ISQPAddLinkEvent::create(intermediateWorkerId, leafWorkerIds[intermediateNodeCounter]);
            linkAddEvents.emplace_back(linkAddEvent);
            connectivityCounter++;
            if (connectivityCounter > leafWorkerIds.size() / intermediateWorkerIds.size()) {
                break;
            }
        }
    }
    requestHandlerService->queueISQPRequest(linkAddEvents);

    // create logical and physical sources
    setupSources(leafWorkerIds, sourceCatalogService);
};

/**
 * @brief Setup coordinator configuration and sources to run the experiments
 * @param queryMergerRule : the query merger rule
 * @param noOfPhysicalSources : total number of physical sources
 * @param batchSize : the batch size for query processing
 */
void setUp(uint16_t rootNodes,
           uint16_t intermediateNodes,
           uint16_t leafNodes,
           RequestHandlerServicePtr requestHandlerService,
           SourceCatalogServicePtr sourceCatalogService,
           TopologyPtr topology) {
    setupTopology(rootNodes, intermediateNodes, leafNodes, requestHandlerService, sourceCatalogService, topology);
}

/**
 * @brief Split string by delimiter
 * @param input : string to split
 * @param delim : delimiter
 * @return  vector of split string
 */
std::vector<std::string> split(const std::string input, char delim) {
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
Yaml::Node loadConfigFromYAMLFile(const std::string& filePath) {

    NES_INFO("BenchmarkIncrementalPlacement: Using config file with path: {} .", filePath);
    if (!filePath.empty() && std::filesystem::exists(filePath)) {
        try {
            Yaml::Node config = *(new Yaml::Node());
            Yaml::Parse(config, filePath.c_str());
            return config;
        } catch (std::exception& e) {
            NES_ERROR("BenchmarkIncrementalPlacement: Error while initializing configuration parameters from YAML file. {}",
                      e.what());
            throw e;
        }
    }
    NES_ERROR("BenchmarkIncrementalPlacement: No file path was provided or file could not be found at {}.", filePath);
    NES_THROW_RUNTIME_ERROR("Unable to find benchmark run configuration.");
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

    auto listener = std::make_shared<ErrorHandler>();
    Exceptions::installGlobalErrorListener(listener);

    NES::Logger::setupLogging("BenchmarkIncrementalPlacement.log", NES::LogLevel::LOG_INFO);
    std::cout << "Setup BenchmarkIncrementalPlacement test class." << std::endl;
    std::stringstream benchmarkOutput;
    benchmarkOutput << "Time,BM_Name,PlacementRule,IncrementalPlacement,Run_Num,Query_Num,Start_Time,End_Time,Total_Run_Time"
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

    Yaml::Node configs;
    //Load the configuration file
    if (configPath != commandLineParams.end()) {
        configs = loadConfigFromYAMLFile(configPath->second);
    } else {
        NES_ERROR("Configuration file is not provided");
        return -1;
    }

    //Fetch base benchmark configurations
    auto logLevel = configs["LogLevel"].As<std::string>();
    auto numberOfRun = configs["NumOfRuns"].As<uint16_t>();
    auto startupSleepInterval = configs["StartupSleepIntervalInSeconds"].As<uint16_t>();
    NES::Logger::setupLogging("BM.log", magic_enum::enum_cast<LogLevel>(logLevel).value());

    //Load queries from the query set location and run the benchmark
    auto querySetLocation = configs["QuerySetLocation"].As<std::string>();
    std::vector<std::string> queries;
    //Read the input query set and load the query string in the queries vector
    std::ifstream infile(querySetLocation);
    std::string line;
    while (std::getline(infile, line)) {
        std::istringstream iss(line);
        queries.emplace_back(line);
    }

    if (queries.empty()) {
        NES_THROW_RUNTIME_ERROR("Unable to find any query");
    }

    //using thread pool to parallelize the compilation of string queries and string them in an array of query objects
    const uint32_t numOfQueries = queries.size();
    QueryPlanPtr queryObjects[numOfQueries];

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

        //Fetch the parsed query from all threads
        for (uint64_t futureNum = 0; futureNum < threadNum; futureNum++) {
            auto query = futures[futureNum].get();
            auto queryID = query->getQueryId();
            queryObjects[queryID - 1] = query;//Add the parsed query to the (queryID - 1)th index
        }
    }

    std::cout << "Parsed all queries." << std::endl;

    //Perform benchmark for each run configuration
    auto runConfig = configs["RunConfig"];
    for (auto entry = runConfig.Begin(); entry != runConfig.End(); entry++) {
        auto node = (*entry).second;
        auto placementStrategy = node["QueryPlacementStrategy"].As<std::string>();
        auto incrementalPlacement = node["IncrementalPlacement"].As<bool>();
        auto coordinatorConfiguration = CoordinatorConfiguration::createDefault();
        //Set optimizer configuration
        OptimizerConfiguration optimizerConfiguration;
        optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
        optimizerConfiguration.enableIncrementalPlacement = incrementalPlacement;
        coordinatorConfiguration->optimizer = optimizerConfiguration;

        for (uint32_t run = 0; run < numberOfRun; run++) {
            std::this_thread::sleep_for(std::chrono::seconds(startupSleepInterval));

            auto nesCoordinator = std::make_shared<NesCoordinator>(coordinatorConfiguration);
            nesCoordinator->startCoordinator(false);
            auto requestHandlerService = nesCoordinator->getRequestHandlerService();
            auto sourceCatalogService = nesCoordinator->getSourceCatalogService();
            auto topology = nesCoordinator->getTopology();

            std::cout << "Setting up the topology." << std::endl;

            //Setup topology and source catalog
            setUp(1, 10, 100, requestHandlerService, sourceCatalogService, topology);

            auto placement = magic_enum::enum_cast<Optimizer::PlacementStrategy>(placementStrategy).value();
            //Perform steps to optimize queries
            for (uint64_t i = 0; i < numOfQueries; i++) {
                auto queryPlan = queryObjects[i];
                auto addQueryRequest = RequestProcessor::ISQPAddQueryEvent::create(queryPlan->copy(), placement);
                auto startTime =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                requestHandlerService->queueISQPRequest({addQueryRequest});
                auto endTime =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                benchmarkOutput << startTime << ",BM_Name," << placementStrategy << "," << incrementalPlacement << "," << run
                                << "," << queryPlan->getQueryId() << "," << startTime << "," << endTime << ","
                                << (endTime - startTime) << std::endl;
            }
            std::cout << "Finished Run " << run;
            nesCoordinator->stopCoordinator(true);
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