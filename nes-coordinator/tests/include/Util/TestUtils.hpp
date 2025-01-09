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

#ifndef NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTUTILS_HPP_
#define NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTUTILS_HPP_

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Runtime/QueryStatistics.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/Waypoint.hpp>
#include <Util/StdInt.hpp>
#include <Util/Subprocess/Subprocess.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <chrono>
#include <fstream>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <nlohmann/json.hpp>

using Clock = std::chrono::high_resolution_clock;
using std::cout;
using std::endl;
using std::string;
using namespace std::string_literals;

namespace NES {
static const char* BASE_URL = "http://127.0.0.1:";

namespace Runtime {
class NodeEngine;
using NodeEnginePtr = std::shared_ptr<NodeEngine>;
}// namespace Runtime

/**
 * @brief This define states all join strategies that will be tested in all join-specific tests
 */
#define ALL_JOIN_STRATEGIES                                                                                                      \
    ::testing::Values(QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN,                                                    \
                      QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCKING,                                            \
                      QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCK_FREE,                                          \
                      QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL,                                                     \
                      QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED)

/**
 * @brief This define states all window strategies that will be tested in all join-specific tests. Note that BUCKETING
 * is not supported for HASH_JOIN_VAR_SIZED
 */
#define ALL_WINDOW_STRATEGIES                                                                                                    \
    ::testing::Values(QueryCompilation::WindowingStrategy::SLICING, QueryCompilation::WindowingStrategy::BUCKETING)

/**
 * @brief This combines all join strategies and window strategies that will be tested in all join-specific test cases
 */
#define JOIN_STRATEGIES_WINDOW_STRATEGIES ::testing::Combine(ALL_JOIN_STRATEGIES, ALL_WINDOW_STRATEGIES)

/**
 * @brief this is a util class for the tests
 */
namespace TestUtils {

/**
 * @brief Struct for storing all csv file params for tests. It is solely a container for grouping csv files.
 */
struct CsvFileParams {
    CsvFileParams(const string& csvFileLeft, const string& csvFileRight, const string& expectedFile)
        : inputCsvFiles({csvFileLeft, csvFileRight}), expectedFile(expectedFile) {}

    explicit CsvFileParams(const std::vector<std::string>& inputCsvFiles, const string& expectedFile = "")
        : inputCsvFiles(inputCsvFiles), expectedFile(expectedFile) {}

    const std::vector<std::string> inputCsvFiles;
    const std::string expectedFile;
};

struct SourceTypeConfigCSV {
    const std::string logicalSourceName;
    const std::string physicalSourceName;
    const std::string fileName;
    uint64_t gatheringInterval = 0;
    uint64_t numberOfTuplesToProduce = 0;
    uint64_t numberOfBuffersToProduce = 0;
    bool isSkipHeader = false;
};

/**
 * @brief Struct for storing all parameter for the join
 */
struct JoinParams {
    JoinParams(const std::vector<SchemaPtr>& inputSchemas) : inputSchemas(inputSchemas) {
        NES_ASSERT(inputSchemas.size() >= 2, "JoinParams expect to have at least two input schemas");
        const std::shared_ptr<Schema>& inputSchema1 = inputSchemas[0];
        const std::shared_ptr<Schema>& inputSchema2 = inputSchemas[1];
        outputSchema = Runtime::Execution::Util::createJoinSchema(inputSchema1, inputSchema2);

        if (inputSchemas.size() > 2) {
            auto cnt = 0_u64;
            for (auto it = inputSchemas.begin() + 2; it != inputSchemas.end(); ++it) {
                const std::shared_ptr<Schema>& inputSchema = *it;
                outputSchema = Runtime::Execution::Util::createJoinSchema(outputSchema, inputSchema);
                cnt++;
            }
        }
    }

    const std::vector<SchemaPtr> inputSchemas;
    SchemaPtr outputSchema;
};

static constexpr auto defaultTimeout = std::chrono::seconds(60);
static constexpr auto defaultStartQueryTimeout = std::chrono::seconds(180);// starting a query requires time
static constexpr auto sleepDuration = std::chrono::milliseconds(250);
static constexpr auto defaultCooldown = std::chrono::seconds(3);// 3s after last processed task, the query should be done.

/**
 * Create a command line parameter for a configuration option for the coordinator or worker.
 * @param name The name of the command line option.
 * @param value The value of the command line option.
 * @param prefix If true, prefix the name of the option with "worker." to configure the internal worker of the coordinator.
 * @return A string representing the command line parameter.
 */
[[nodiscard]] const std::string configOption(const std::string& name, const std::string& value, bool prefix = false);

/**
 * Create a command line parameter for a configuration option for the coordinator or worker.
 * @param name The name of the command line option.
 * @param value The value of the command line option.
 * @param prefix If true, prefix the name of the option with "worker." to configure the internal worker of the coordinator.
* @return A string representing the command line parameter.
 */
template<typename T>
[[nodiscard]] std::string configOption(const std::string& name, T value, bool prefix = false) {
    return configOption(name, std::to_string(value), prefix);
}

/**
 * @brief Creates the command line argument with a buffer size
 * @param size
 * @param prefix
 * @return Command line argument
 */
[[nodiscard]] std::string bufferSizeInBytes(uint64_t size, bool prefix = false);

/**
 * @brief Creates the command line argument for the fileName
 * @param filename
* @return Command line argument
 */
[[nodiscard]] std::string configPath(const std::string& filename);

/**
 * @brief Creates the command line argument for the worker config path
 * @param filename
* @return Command line argument
 */
[[nodiscard]] std::string workerConfigPath(const std::string& filename);

/**
 * @brief Creates the command line argument for a coordinator port
 * @param coordinatorPort
* @return Command line argument
 */
[[nodiscard]] std::string coordinatorPort(uint64_t coordinatorPort);

/**
 * @brief Creates the command line argument for the parent id
 * @param parentId
* @return Command line argument
 */
[[nodiscard]] std::string parentId(uint64_t parentId);

/**
 * @brief Creates the command line argument for the numberOfSlots
 * @param coordinatorPort
 * @param prefix
* @return Command line argument
 */
[[nodiscard]] std::string numberOfSlots(uint64_t coordinatorPort, bool prefix = false);

/**
 * @brief Creates the command line argument for the number of local buffers
 * @param localBuffers
 * @param prefix
* @return Command line argument
 */
[[nodiscard]] std::string numLocalBuffers(uint64_t localBuffers, bool prefix = false);

/**
 * @brief Creates the command line argument for the number of global buffers
 * @param globalBuffers
 * @param prefix
* @return Command line argument
 */
[[nodiscard]] std::string numGlobalBuffers(uint64_t globalBuffers, bool prefix = false);

/**
 * @brief Creates the command line argument for the rpc port
 * @param rpcPort
* @return Command line argument
 */
[[nodiscard]] std::string rpcPort(uint64_t rpcPort);

/**
 * @brief Creates the command line argument for the source type
 * @param sourceType
* @return Command line argument
 */
[[nodiscard]] std::string sourceType(SourceType sourceType);

/**
 * @brief Creates the command line argument for the csv source file path
 * @param filePath
* @return Command line argument
 */
[[nodiscard]] std::string csvSourceFilePath(std::string filePath);

/**
 * @brief Creates the command line argument for the data port
 * @param dataPort
* @return Command line argument
 */
[[nodiscard]] std::string dataPort(uint64_t dataPort);

/**
 * @brief Creates the command line argument for the number of tuples of tuples to produce per buffer
 * @param numberOfTuplesToProducePerBuffer
 * @return Command line argument
 */
[[nodiscard]] std::string numberOfTuplesToProducePerBuffer(uint64_t numberOfTuplesToProducePerBuffer);

/**
 * @brief Creates the command line argument for the physical source name
 * @param physicalSourceName
 * @return Command line argument
 */
[[nodiscard]] std::string physicalSourceName(std::string physicalSourceName);

/**
 * @brief Creates the command line argument for setting the logical source name
 * @param logicalSourceName
 * @return Command line argument
 */
[[nodiscard]] std::string logicalSourceName(std::string logicalSourceName);

/**
 * @brief Creates the command line argument for setting the number of buffers to produce
 * @param numberOfBuffersToProduce
 * @return Command line argument
 */
[[nodiscard]] std::string numberOfBuffersToProduce(uint64_t numberOfBuffersToProduce);

/**
 * @brief Creates the command line argument for setting the source gathering interval
 * @param sourceGatheringInterval
 * @return Command line argument
 */
[[nodiscard]] std::string sourceGatheringInterval(uint64_t sourceGatheringInterval);

/**
 * @brief Enables the usage of tcp socket host
 * @return Command line argument
 */
[[nodiscard]] std::string tcpSocketHost(std::string host);

/**
 * @brief Enables the usage of tcp socket port
 * @return Command line argument
 */
[[nodiscard]] std::string tcpSocketPort(std::string port);

/**
 * @brief Enables the usage of tcp socket input format
 * @return Command line argument
 */
[[nodiscard]] std::string inputFormat(std::string format);

/**
 * @brief Enables the usage of tcp socket persistent source
 * @return Command line argument
 */
[[nodiscard]] std::string tcpSocketPersistentSource(std::string persistentSource);

/**
 * @brief Enables the usage of tcp socket decided message size
 * @return Command line argument
 */
[[nodiscard]] std::string tcpSocketDecidedMessageSize(std::string decidedSize);

/**
 * @brief Enables the usage of tcp socket decided message size
 * @return Command line argument
 */
[[nodiscard]] std::string tcpSocketBufferSize(std::string bufferSize);

/**
 * @brief Creates the command line argument for setting the rest port
 * @param restPort
 * @return Command line argument
 */
[[nodiscard]] std::string restPort(uint64_t restPort);

/**
 * @brief Creates the command line argument to enable debugging
 * @return Command line argument
 */
[[nodiscard]] std::string enableDebug();

/**
 * @brief Creates the command line argument for setting the health check wait time for the worker
 * @param workerWaitTime
 * @return Command line argument
 */
[[nodiscard]] std::string workerHealthCheckWaitTime(uint64_t workerWaitTime);

/**
 * @brief Creates the command line argument for setting the health check wait time for the coordinator
 * @param coordinatorWaitTime
 * @return Command line argument
 */
[[nodiscard]] std::string coordinatorHealthCheckWaitTime(uint64_t coordinatorWaitTime);

/**
 * @brief Creates the command line argument if to enable monitoring
 * @param prefix
 * @return Command line argument
 */
[[nodiscard]] std::string enableMonitoring(bool prefix = false);

/**
 * @brief Creates the command line argument if to set monitoring wait time
 * @param prefix
 * @return Command line argument
 */
[[nodiscard]] std::string monitoringWaitTime(uint64_t monitoringWaitTime);

// 2884: Fix configuration to disable distributed window rule
[[nodiscard]] std::string disableDistributedWindowingOptimization();

/**
 * @brief Creates the command line argument for enabling nemo placement
 * @return Command line argument
 */
[[nodiscard]] std::string enableNemoPlacement();

/**
 * @brief Creates the command line argument for enabling nemo join
 * @return Command line argument
 */
[[nodiscard]] std::string enableNemoJoin();

/**
 * @brief Creates the command line argument for enabling matrix join
 * @return Command line argument
 */
[[nodiscard]] std::string enableMatrixJoin();

/**
 * @brief Creates the command line argument for setting the threshold of the distributed window child
 * @param val
 * @return Command line argument
 */
[[nodiscard]] std::string setDistributedWindowChildThreshold(uint64_t val);

/**
 * @brief Creates the command line argument for setting the threshold of the distributed window combiner
 * @param val
 * @return Command line argument
 */
[[nodiscard]] std::string setDistributedWindowCombinerThreshold(uint64_t val);

/**
 * @brief Creates the command line argument if to enable slicing windowing
 * @param prefix
 * @return Command line argument
 */
[[nodiscard]] std::string enableSlicingWindowing(bool prefix = false);

/**
 * @brief Enables the usage of Nautilus
 * @return Command line argument
 */
[[nodiscard]] std::string enableNautilusWorker();

/**
 * @brief Enables the usage of Nautilus at the coordinator
 * @return Command line argument
 */
[[nodiscard]] std::string enableNautilusCoordinator();

/**
 * @brief start a new instance of a nes coordinator with a set of configuration flags
 * @param flags
* @return coordinator process, which terminates if it leaves the scope
 */
[[nodiscard]] Util::Subprocess startCoordinator(std::initializer_list<std::string> list);

/**
 * @brief start a new instance of a nes worker with a set of configuration flags
 * @param flags
 * @return worker process, which terminates if it leaves the scope
 */
[[nodiscard]] Util::Subprocess startWorker(std::initializer_list<std::string> flags);

/**
 * @brief start a new instance of a nes worker with a set of configuration flags
 * @param flags
 * @return worker process, which terminates if it leaves the scope
 */
[[nodiscard]] std::shared_ptr<Util::Subprocess> startWorkerPtr(std::initializer_list<std::string> flags);

/**
 * @brief method to check the produced buffers and tasks for n seconds and either return true or timeout
 * @param ptr to Runtime
 * @param queryId
 * @param expectedResult
 * @return bool indicating if the expected results are matched
 */
[[nodiscard]] bool checkCompleteOrTimeout(const Runtime::NodeEnginePtr& ptr, QueryId queryId, uint64_t expectedResult);

/**
 * @brief This method is used for waiting till the query gets into running status or a timeout occurs
 * @param queryId : the query id to check for
 * @param queryCatalog: the catalog to look into for status change
 * @param timeoutInSec: time to wait before stop checking
 * @return true if query gets into running status else false
 */
[[nodiscard]] bool waitForQueryToStart(QueryId queryId,
                                       const Catalogs::Query::QueryCatalogPtr& queryCatalog,
                                       std::chrono::seconds timeoutInSec = std::chrono::seconds(defaultStartQueryTimeout));

/**
 * @brief method to check the produced buffers and tasks for n seconds and either return true or timeout
 * @param nesWorker to NesWorker
 * @param queryId
 * @param queryCatalog
 * @param expectedResult
 * @return bool indicating if the expected results are matched
 */
template<typename Predicate = std::equal_to<uint64_t>>
[[nodiscard]] bool checkCompleteOrTimeout(const NesWorkerPtr& nesWorker,
                                          QueryId queryId,
                                          const GlobalQueryPlanPtr& globalQueryPlan,
                                          uint64_t expectedResult) {

    SharedQueryId sharedQueryId = globalQueryPlan->getSharedQueryId(queryId);
    if (sharedQueryId == INVALID_SHARED_QUERY_ID) {
        NES_ERROR("Unable to find global query Id for user query id {}", queryId);
        return false;
    }

    NES_INFO("Found global query id {} for user query {}", sharedQueryId, queryId);
    auto timeoutInSec = std::chrono::seconds(defaultTimeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_TRACE("checkCompleteOrTimeout: check result NesWorkerPtr");
        //FIXME: handle vector of statistics properly in #977
        auto statistics = nesWorker->getQueryStatistics(sharedQueryId);
        if (statistics.empty()) {
            NES_TRACE("checkCompleteOrTimeout: query={} stats size={}", sharedQueryId, statistics.size());
            std::this_thread::sleep_for(sleepDuration);
            continue;
        }
        uint64_t processed = statistics[0]->getProcessedBuffers();
        if (processed >= expectedResult) {
            NES_TRACE("checkCompleteOrTimeout: results are correct procBuffer={} procTasks={} procWatermarks={}",
                      statistics[0]->getProcessedBuffers(),
                      statistics[0]->getProcessedTasks(),
                      statistics[0]->getProcessedWatermarks());
            return true;
        }
        NES_TRACE("checkCompleteOrTimeout: NesWorkerPtr results are incomplete procBuffer={} procTasks={} procWatermarks={}",
                  statistics[0]->getProcessedBuffers(),
                  statistics[0]->getProcessedTasks(),
                  statistics[0]->getProcessedWatermarks());
        std::this_thread::sleep_for(sleepDuration);
    }
    auto statistics = nesWorker->getQueryStatistics(sharedQueryId);
    uint64_t processed = statistics[0]->getProcessedBuffers();
    NES_TRACE("checkCompleteOrTimeout: NesWorkerPtr expected results are not reached after timeout expected={} final result={}",
              expectedResult,
              processed);
    return false;
}

/**
 * @brief method to check the produced buffers and tasks for n seconds and either return true or timeout
 * @param nesCoordinator to NesCoordinator
 * @param queryId
 * @param queryCatalog
 * @param expectedResult
 * @return bool indicating if the expected results are matched
 */
template<typename Predicate = std::equal_to<uint64_t>>
[[nodiscard]] bool checkCompleteOrTimeout(const NesCoordinatorPtr& nesCoordinator,
                                          QueryId queryId,
                                          const GlobalQueryPlanPtr& globalQueryPlan,
                                          uint64_t expectedResult,
                                          bool minOneProcessedTask = false,
                                          std::chrono::seconds timeoutSeconds = defaultTimeout) {
    SharedQueryId sharedQueryId = globalQueryPlan->getSharedQueryId(queryId);
    if (sharedQueryId == INVALID_SHARED_QUERY_ID) {
        NES_ERROR("Unable to find global query Id for user query id {}", queryId);
        return false;
    }

    NES_INFO("Found global query id {} for user query {}", sharedQueryId, queryId);
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutSeconds) {
        NES_TRACE("checkCompleteOrTimeout: check result NesCoordinatorPtr");

        //FIXME: handle vector of statistics properly in #977
        auto statistics = nesCoordinator->getQueryStatistics(sharedQueryId);
        if (statistics.empty()) {
            continue;
        }

        uint64_t now =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch())
                .count();
        auto timeoutMillisec = std::chrono::milliseconds(defaultTimeout);

        // wait for another iteration if the last processed task was very recent.
        if (minOneProcessedTask
            && (statistics[0]->getTimestampLastProcessedTask() == 0 || statistics[0]->getTimestampFirstProcessedTask() == 0
                || statistics[0]->getTimestampLastProcessedTask() > now - defaultCooldown.count())) {
            NES_TRACE("checkCompleteOrTimeout: A task was processed within the last {}ms, the query may still be active. "
                      "Restart the timeout period.",
                      timeoutMillisec.count());
        }
        // return if enough buffer have been received
        else if (statistics[0]->getProcessedBuffers() >= expectedResult) {
            NES_TRACE("checkCompleteOrTimeout: NesCoordinatorPtr results are correct stats={} procTasks={} procWatermarks={}",
                      statistics[0]->getProcessedBuffers(),
                      statistics[0]->getProcessedTasks(),
                      statistics[0]->getProcessedWatermarks());
            return true;
        }
        NES_TRACE("checkCompleteOrTimeout: NesCoordinatorPtr results are incomplete procBuffer={} procTasks={} expected={}",
                  statistics[0]->getProcessedBuffers(),
                  statistics[0]->getProcessedTasks(),
                  expectedResult);

        std::this_thread::sleep_for(sleepDuration);
    }
    //FIXME: handle vector of statistics properly in #977
    NES_TRACE("checkCompleteOrTimeout: NesCoordinatorPtr expected results are not reached after timeout expected result={}"
              "processedBuffer={} processedTasks={} procWatermarks={}",
              expectedResult,
              nesCoordinator->getQueryStatistics(sharedQueryId)[0]->getProcessedBuffers(),
              nesCoordinator->getQueryStatistics(sharedQueryId)[0]->getProcessedTasks(),
              nesCoordinator->getQueryStatistics(sharedQueryId)[0]->getProcessedWatermarks());
    return false;
}

[[nodiscard]] bool checkRemovedDecomposedQueryOrTimeoutAtWorker(DecomposedQueryId decomposedQueryId,
                                                                DecomposedQueryPlanVersion decomposedQueryVersion,
                                                                NesWorkerPtr worker,
                                                                std::chrono::seconds timeout = defaultTimeout);

/**
 * @brief Check if the query is been stopped successfully within the timeout.
 * @param queryId: Id of the query to be stopped
 * @param queryCatalog: the catalog containig the queries in the system
 * @return true if successful
 */
[[nodiscard]] bool checkStoppedOrTimeout(QueryId queryId,
                                         const Catalogs::Query::QueryCatalogPtr& queryCatalog,
                                         std::chrono::seconds timeout = defaultTimeout);

/**
     * @brief Check if the query is been stopped successfully within the timeout.
     * @param sharedQueryId: Id of the query to be stopped
     * @param worker: the worker which the query runs on
     * @return true if successful
     */
[[nodiscard]] bool
checkStoppedOrTimeoutAtWorker(SharedQueryId sharedQueryId, NesWorkerPtr worker, std::chrono::seconds timeout = defaultTimeout);

/**
 * @brief Check if the query has failed within the timeout.
 * @param queryId: Id of the query to be stopped
 * @param queryCatalog: the catalog containig the queries in the system
 * @return true if successful
 */
[[nodiscard]] bool checkFailedOrTimeout(QueryId queryId,
                                        const Catalogs::Query::QueryCatalogPtr& queryCatalog,
                                        std::chrono::seconds timeout = defaultTimeout);

/**
 * @brief Check if the query result was produced
 * @param expectedContent
 * @param outputFilePath
 * @return true if successful
 */
[[nodiscard]] bool
checkOutputOrTimeout(string expectedContent, const string& outputFilePath, uint64_t customTimeoutInSeconds = 0);

/**
 * @brief Check if any query result was produced
 * @param outputFilePath
 * @return true if successful
 */
[[nodiscard]] bool
checkIfOutputFileIsNotEmtpy(uint64_t minNumberOfLines, const string& outputFilePath, uint64_t customTimeout = 0);

/**
  * @brief Check if the query result was produced
  * @param queryId
  * @param queryCatalogService
  * @param numberOfRecordsToExpect
  * @param outputFilePath
  * @param testTimeout
  * @return True if numberOfRecordsToExpect have been seen
  */
[[nodiscard]] bool checkOutputContentLengthOrTimeout(QueryId queryId,
                                                     Catalogs::Query::QueryCatalogPtr queryCatalog,
                                                     uint64_t numberOfRecordsToExpect,
                                                     const string& outputFilePath,
                                                     auto testTimeout = defaultTimeout) {
    auto timeoutInSec = std::chrono::seconds(testTimeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        std::this_thread::sleep_for(sleepDuration);
        NES_TRACE("TestUtil:checkBinaryOutputContentLengthOrTimeout: check content for file {}", outputFilePath);

        auto currentQueryState = queryCatalog->getQueryState(queryId);
        if (currentQueryState == QueryState::FAILED) {
            // the query failed, so we return true as a failure append during execution.
            NES_TRACE("checkStoppedOrTimeout: status reached failed");
            return false;
        }

        auto isQueryStopped = currentQueryState == QueryState::STOPPED;

        if (std::filesystem::exists(outputFilePath) && std::filesystem::is_regular_file(outputFilePath)) {

            // As we are writing a header in the output csv file of the query, we have to subtract one line
            std::ifstream outFile(outputFilePath);
            uint64_t currentContentSize = Util::countLines(outFile);
            if (currentContentSize > 0) {
                currentContentSize -= 1;
            }

            // File exists, now checking if we have seen the expected number of tuples
            if (currentContentSize > numberOfRecordsToExpect) {
                NES_DEBUG("content is larger than expected result: currentContentSize: {} - expectedNumberOfContent: {}",
                          currentContentSize,
                          numberOfRecordsToExpect);
                return false;
            } else if (currentContentSize < numberOfRecordsToExpect) {
                NES_DEBUG("number of expected lines {} not reached yet with {} lines",
                          numberOfRecordsToExpect,
                          currentContentSize);
            } else {
                NES_DEBUG("number of content in output file match expected number of content!");
                return true;
            }
        }

        if (isQueryStopped) {
            NES_DEBUG("query stopped but content not ready");
            return false;
        }
    }
    NES_DEBUG("expected result not reached within set timeout content");
    return false;
}

/**
 * @brief Check if a outputfile is created
 * @param expectedContent
 * @param outputFilePath
 * @return true if successful
 */
[[nodiscard]] bool checkFileCreationOrTimeout(const string& outputFilePath);

/**
 * @brief Check if Coordinator REST API is available or timeout
 * @param expectedContent
 * @param outputFilePath
 * @return true if successful
 */
[[nodiscard]] bool checkRESTServerStartedOrTimeout(uint64_t restPort, uint64_t customTimeout = 0);

/**
 * @brief This method is used for checking if the submitted query produced the expected result within the timeout
 * @param queryId: Id of the query
 * @param expectedNumberBuffers: The expected value
 * @return true if matched the expected result within the timeout
 */
[[nodiscard]] bool checkCompleteOrTimeout(QueryId queryId, uint64_t expectedNumberBuffers, const std::string& restPort = "8081");

/**
 * @brief This method is used for checking if the submitted query is running
 * @param queryId: Id of the query
 * @return true if is running within the timeout, else false
 */
[[nodiscard]] bool checkRunningOrTimeout(QueryId queryId, const std::string& restPort = "8081");

/**
 * @brief This method is used for stop a query
 * @param queryId: Id of the query
 * @return if stopped
 */
[[nodiscard]] bool stopQueryViaRest(QueryId queryId, const std::string& restPort = "8081");

/**
 * @brief This method is used for getting the execution plan via REST
 * @param rest port string
 * @return the execution plan
 */
[[nodiscard]] nlohmann::json getExecutionPlan(QueryId queryId, const std::string& restPort);

/**
 * @brief This method is used for executing a query
 * @param query string
 * @return if stopped
 */
[[nodiscard]] nlohmann::json startQueryViaRest(const string& queryString, const std::string& restPort = "8081");

/**
 * @brief This method is used for adding source statistics
 * @param query string
 * @return if stopped
 */
[[nodiscard]] nlohmann::json addSourceStatistics(const string& queryString, const std::string& restPort = "8081");

/**
 * @brief This method is used for making a monitoring rest call.
 * @param1 the rest call
 * @param2 the rest port
 * @return the json
 */
[[nodiscard]] nlohmann::json makeMonitoringRestCall(const string& restCall, const std::string& restPort = "8081");

/**
 * @brief This method is used adding a logical source
 * @param query string
 * @return
 */
[[nodiscard]] bool addLogicalSource(const string& schemaString, const std::string& restPort = "8081");

bool waitForWorkers(uint64_t restPort, uint16_t maxTimeout, uint16_t expectedWorkers);

/**
 * @brief This method is used for making a REST call to coordinator to get the topology as Json
 * @param1 the rest port
 * @return the json
 */
[[nodiscard]] nlohmann::json getTopology(uint64_t restPort);

/**
 * @brief Creates the expected buffers from the csv file
 * @param csvFileName
 * @param schema
 * @param bufferManager
 * @param numTuplesPerBuffer
 * @return Vector of TupleBuffers
 */
std::vector<Runtime::TupleBuffer> createExpectedBuffersFromCsv(const std::string& csvFileName,
                                                               const SchemaPtr& schema,
                                                               const Runtime::BufferManagerPtr& bufferManager,
                                                               uint64_t numTuplesPerBuffer);

/**
 * @brief Creates the expected buffers from the csv file
 * @param csvFileName
 * @param schema
 * @param bufferManager
 * @param skipHeader
 * @param numTuplesPerBuffer
 * @param delimiter
 * @return Vector of TupleBuffers
 */
std::vector<Runtime::TupleBuffer> createExpectedBuffersFromCsv(const std::string& csvFileName,
                                                               const SchemaPtr& schema,
                                                               const Runtime::BufferManagerPtr& bufferManager,
                                                               bool skipHeader = false,
                                                               uint64_t numTuplesPerBuffer = 0,
                                                               const std::string& delimiter = ",");

/**
 * @brief Fills the buffer from a stream
 * @param csvFileName
 * @param schema
 * @param bufferManager
 * @param skipHeader
 * @param numTuplesPerBuffer
 * @param delimiter
 * @return Vector of TupleBuffers
 */
std::vector<Runtime::TupleBuffer> createExpectedBufferFromStream(std::istream& istream,
                                                                 const SchemaPtr& schema,
                                                                 const Runtime::BufferManagerPtr& bufferManager,
                                                                 bool skipHeader = false,
                                                                 uint64_t numTuplesPerBuffer = 0,
                                                                 const std::string& delimiter = ",");

std::vector<Runtime::TupleBuffer> createExpectedBuffersFromCsvSpecificLines(const std::string& csvFileName,
                                                                            const SchemaPtr& schema,
                                                                            const Runtime::BufferManagerPtr& bufferManager,
                                                                            const int fromLine,
                                                                            const int toLine,
                                                                            bool skipHeader = false,
                                                                            uint64_t numTuplesPerBuffer = 0,
                                                                            const std::string& delimiter = ",");

std::vector<Runtime::TupleBuffer> createExpectedBufferFromStreamSpecificLines(std::istream& istream,
                                                                              const SchemaPtr& schema,
                                                                              const Runtime::BufferManagerPtr& bufferManager,
                                                                              const int fromLine,
                                                                              const int toLine,
                                                                              bool skipHeader = false,
                                                                              uint64_t numTuplesPerBuffer = 0,
                                                                              const std::string& delimiter = ",");

/**
 * @brief Fills the buffer from a stream
 * @param str
 * @param schema
 * @param bufferManager
 * @param skipHeader
 * @param numTuplesPerBuffer
 * @param delimiter
 * @return Vector of TupleBuffers
 */
std::vector<Runtime::TupleBuffer> createExpectedBufferFromCSVString(std::string str,
                                                                    const SchemaPtr& schema,
                                                                    const Runtime::BufferManagerPtr& bufferManager,
                                                                    bool skipHeader = false,
                                                                    uint64_t numTuplesPerBuffer = 0,
                                                                    const std::string& delimiter = ",");

/**
 * @brief Counts the tuple in all buffers
 * @param buffers
 * @return Tuplecount
 */
uint64_t countTuples(std::vector<Runtime::TupleBuffer>& buffers);

uint64_t countTuples(std::vector<Runtime::MemoryLayouts::TestTupleBuffer>& buffers);

/**
 * @brief Converts all of the tuple buffers to dynamic tuple buffers
 * @param buffers
 * @param schema
 * @return Vector of TestTupleBuffer
 */
std::vector<Runtime::MemoryLayouts::TestTupleBuffer> createTestTupleBuffers(std::vector<Runtime::TupleBuffer>& buffers,
                                                                            const SchemaPtr& schema);

/**
 * @brief Compares if leftBuffers contain the same tuples as rightBuffers
 * @param expectedBuffers
 * @param actualBuffers
 * @param orderSensitive: If set to true, the order is taken into account
 * @return True if the leftBuffers contain the same tuples in the rightBuffer
 */
bool buffersContainSameTuples(std::vector<Runtime::MemoryLayouts::TestTupleBuffer>& expectedBuffers,
                              std::vector<Runtime::MemoryLayouts::TestTupleBuffer>& actualBuffers,
                              bool orderSensitive = false);

/**
 * @brief Creates a vector for the memory [startPtr, endPtr]
 * @tparam T
 * @param startPtr
 * @param endPtr
 * @return Vector for the memory [startPtr, endPtr]
 */
template<typename T>
inline std::vector<T> createVecFromPointer(T* startPtr, T* endPtr) {
    return std::vector<T>(startPtr, endPtr);
}

/**
 * @brief Creates a vector for the memory [startPtr, startPtr + numItems]
 * @tparam T
 * @param startPtr
 * @param numItems
 * @return Vector for the memory [startPtr, startPtr + numItems]
 */
template<typename T>
inline std::vector<T> createVecFromPointer(T* startPtr, uint64_t numItems) {
    return createVecFromPointer<T>(startPtr, startPtr + numItems);
}

/**
 * @brief Creates a vector for the memory that this tupleBuffer is responsible for
 * @tparam T
 * @param startPtr
 * @param numItems
 * @return Vector
 */
template<typename T>
inline std::vector<T> createVecFromTupleBuffer(Runtime::TupleBuffer buffer) {
    return createVecFromPointer<T>(buffer.getBuffer<T>(), buffer.getBuffer<T>() + buffer.getNumberOfTuples());
}

/**
 * @brief Creates a csv source that produces as many buffers as the csv file contains
 * @param SourceTypeConfig: container for configuration parameters of a source type.
 * @return CSVSourceTypePtr
 */
CSVSourceTypePtr createSourceTypeCSV(const SourceTypeConfigCSV& sourceTypeConfigCSV);

std::vector<PhysicalTypePtr> getPhysicalTypes(const SchemaPtr& schema);
};// namespace TestUtils

class DummyQueryListener : public AbstractQueryStatusListener {
  public:
    virtual ~DummyQueryListener() {}

    bool canTriggerEndOfStream(SharedQueryId, DecomposedQueryId, OperatorId, Runtime::QueryTerminationType) override {
        return true;
    }
    bool notifySourceTermination(SharedQueryId, DecomposedQueryId, OperatorId, Runtime::QueryTerminationType) override {
        return true;
    }
    bool notifyQueryFailure(SharedQueryId, DecomposedQueryId, std::string) override { return true; }
    bool notifyQueryStatusChange(SharedQueryId, DecomposedQueryId, Runtime::Execution::ExecutableQueryPlanStatus) override {
        return true;
    }
 bool propagateNeighbourInformation(std::vector<std::pair<WorkerId, std::string>>) override {
     return true;
    }
    void offloadCheckpoint(uint64_t, std::vector<char>) override {
     return;
    }
    void rpcTrimCheckpoint(uint64_t, uint64_t) override {
     return;
    }
};

/**
 * @brief read mobile device path waypoints from csv
 * @param csvPath path to the csv with lines in the format <latitude, longitude, offsetFromStartTime>
 * @param startTime the real or simulated start time of the LocationProvider
 * @return a vector of waypoints with timestamps calculated by adding startTime to the offset obtained from csv
 */
std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> getWaypointsFromCsv(const std::string& csvPath, Timestamp startTime);

/**
 * @brief write mobile device path waypoints to a csv file to use as input for the LocationProviderCSV class
 * @param csvPath path to the output file
 * @param waypoints a vector of waypoints to be written to the file
 */
void writeWaypointsToCsv(const std::string& csvPath,
                         const std::vector<NES::Spatial::DataTypes::Experimental::Waypoint>& waypoints);

/**
 * This function counts the number of times the search string appears within the
 * target string. It uses the std::string::find() method to locate occurrences.
 *
 * @param searchString The string to search for.
 * @param targetString The string in which to search for occurrences.
 * @return The number of occurrences of the search string within the target string.
 */
uint64_t countOccurrences(const std::string& searchString, const std::string& targetString);

}// namespace NES
#endif// NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTUTILS_HPP_
