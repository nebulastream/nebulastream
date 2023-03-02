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

#ifndef NES_INCLUDE_UTIL_TESTUTILS_HPP_
#define NES_INCLUDE_UTIL_TESTUTILS_HPP_
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Common/Identifiers.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Runtime/QueryStatistics.hpp>
#include <Spatial/DataTypes/Waypoint.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Util/Subprocess/Subprocess.hpp>
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

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

namespace Runtime {
class NodeEngine;
using NodeEnginePtr = std::shared_ptr<NodeEngine>;
}// namespace Runtime

/**
 * @brief this is a util class for the tests
 */
namespace TestUtils {

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

[[nodiscard]] std::string bufferSizeInBytes(uint64_t size, bool prefix = false);
[[nodiscard]] std::string configPath(const std::string& filename);
[[nodiscard]] std::string workerConfigPath(const std::string& filename);
[[nodiscard]] std::string coordinatorPort(uint64_t coordinatorPort);
[[nodiscard]] std::string parentId(uint64_t parentId);
[[nodiscard]] std::string numberOfSlots(uint64_t coordinatorPort, bool prefix = false);

[[nodiscard]] std::string numLocalBuffers(uint64_t localBuffers, bool prefix = false);
[[nodiscard]] std::string numGlobalBuffers(uint64_t globalBuffers, bool prefix = false);

[[nodiscard]] std::string rpcPort(uint64_t rpcPort);
[[nodiscard]] std::string sourceType(std::string sourceType);

[[nodiscard]] std::string csvSourceFilePath(std::string filePath);

[[nodiscard]] std::string dataPort(uint64_t dataPort);
[[nodiscard]] std::string numberOfTuplesToProducePerBuffer(uint64_t numberOfTuplesToProducePerBuffer);

[[nodiscard]] std::string physicalSourceName(std::string physicalSourceName);

[[nodiscard]] std::string logicalSourceName(std::string logicalSourceName);

[[nodiscard]] std::string numberOfBuffersToProduce(uint64_t numberOfBuffersToProduce);

[[nodiscard]] std::string sourceGatheringInterval(uint64_t sourceGatheringInterval);

[[nodiscard]] std::string restPort(uint64_t restPort);
[[nodiscard]] std::string enableDebug();

[[nodiscard]] std::string workerHealthCheckWaitTime(uint64_t workerWaitTime);
[[nodiscard]] std::string coordinatorHealthCheckWaitTime(uint64_t coordinatorWaitTime);

[[nodiscard]] std::string enableMonitoring(bool prefix = false);
// 2884: Fix configuration to disable distributed window rule
[[nodiscard]] std::string disableDistributedWindowingOptimization();

[[nodiscard]] std::string enableNemoPlacement();

[[nodiscard]] std::string setDistributedWindowChildThreshold(uint64_t val);
[[nodiscard]] std::string setDistributedWindowCombinerThreshold(uint64_t val);

[[nodiscard]] std::string enableThreadLocalWindowing(bool prefix = false);

std::string enableNautilus() { return "--queryCompiler.queryCompilerType=NAUTILUS_QUERY_COMPILER"; }

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
     * @param queryCatalogService: the catalog to look into for status change
     * @param timeoutInSec: time to wait before stop checking
     * @return true if query gets into running status else false
     */
[[nodiscard]] bool waitForQueryToStart(QueryId queryId,
                                       const QueryCatalogServicePtr& queryCatalogService,
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
        NES_ERROR("Unable to find global query Id for user query id " << queryId);
        return false;
    }

    NES_INFO("Found global query id " << sharedQueryId << " for user query " << queryId);
    auto timeoutInSec = std::chrono::seconds(defaultTimeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_TRACE("checkCompleteOrTimeout: check result NesWorkerPtr");
        //FIXME: handle vector of statistics properly in #977
        auto statistics = nesWorker->getQueryStatistics(sharedQueryId);
        if (statistics.empty()) {
            NES_TRACE("checkCompleteOrTimeout: query=" << sharedQueryId << " stats size=" << statistics.size());
            std::this_thread::sleep_for(sleepDuration);
            continue;
        }
        uint64_t processed = statistics[0]->getProcessedBuffers();
        if (processed >= expectedResult) {
            NES_TRACE("checkCompleteOrTimeout: results are correct procBuffer="
                      << statistics[0]->getProcessedBuffers() << " procTasks=" << statistics[0]->getProcessedTasks()
                      << " procWatermarks=" << statistics[0]->getProcessedWatermarks());
            return true;
        }
        NES_TRACE("checkCompleteOrTimeout: NesWorkerPtr results are incomplete procBuffer="
                  << statistics[0]->getProcessedBuffers() << " procTasks=" << statistics[0]->getProcessedTasks()
                  << " procWatermarks=" << statistics[0]->getProcessedWatermarks());
        std::this_thread::sleep_for(sleepDuration);
    }
    auto statistics = nesWorker->getQueryStatistics(sharedQueryId);
    uint64_t processed = statistics[0]->getProcessedBuffers();
    NES_TRACE("checkCompleteOrTimeout: NesWorkerPtr expected results are not reached after timeout expected="
              << expectedResult << " final result=" << processed);
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
        NES_ERROR("Unable to find global query Id for user query id " << queryId);
        return false;
    }

    NES_INFO("Found global query id " << sharedQueryId << " for user query " << queryId);
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
            NES_TRACE("checkCompleteOrTimeout: A task was processed within the last "
                      << timeoutMillisec.count() << "ms, the query may still be active. Restart the timeout period.");
        }
        // return if enough buffer have been received
        else if (statistics[0]->getProcessedBuffers() >= expectedResult) {
            NES_TRACE("checkCompleteOrTimeout: NesCoordinatorPtr results are correct stats="
                      << statistics[0]->getProcessedBuffers() << " procTasks=" << statistics[0]->getProcessedTasks()
                      << " procWatermarks=" << statistics[0]->getProcessedWatermarks());
            return true;
        }
        NES_TRACE("checkCompleteOrTimeout: NesCoordinatorPtr results are incomplete procBuffer="
                  << statistics[0]->getProcessedBuffers() << " procTasks=" << statistics[0]->getProcessedTasks()
                  << " expected=" << expectedResult);

        std::this_thread::sleep_for(sleepDuration);
    }
    //FIXME: handle vector of statistics properly in #977
    NES_TRACE("checkCompleteOrTimeout: NesCoordinatorPtr expected results are not reached after timeout expected result="
              << expectedResult << " processedBuffer=" << nesCoordinator->getQueryStatistics(queryId)[0]->getProcessedBuffers()
              << " processedTasks=" << nesCoordinator->getQueryStatistics(queryId)[0]->getProcessedTasks()
              << " procWatermarks=" << nesCoordinator->getQueryStatistics(queryId)[0]->getProcessedWatermarks());
    return false;
}

/**
 * @brief Check if the query is been stopped successfully within the timeout.
 * @param queryId: Id of the query to be stopped
 * @param queryCatalogService: the catalog containig the queries in the system
 * @return true if successful
 */
[[nodiscard]] bool checkStoppedOrTimeout(QueryId queryId,
                                         const QueryCatalogServicePtr& queryCatalogService,
                                         std::chrono::seconds timeout = defaultTimeout);

/**
 * @brief Check if the query has failed within the timeout.
 * @param queryId: Id of the query to be stopped
 * @param queryCatalogService: the catalog containig the queries in the system
 * @return true if successful
 */
[[nodiscard]] bool checkFailedOrTimeout(QueryId queryId,
                                        const QueryCatalogServicePtr& queryCatalogService,
                                        std::chrono::seconds timeout = defaultTimeout);

/**
   * @brief Check if the query result was produced
   * @param expectedContent
   * @param outputFilePath
   * @return true if successful
   */
[[nodiscard]] bool checkOutputOrTimeout(string expectedContent, const string& outputFilePath, uint64_t customTimeout = 0);

/**
   * @brief Check if any query result was produced
   * @param outputFilePath
   * @return true if successful
   */
[[nodiscard]] bool
checkIfOutputFileIsNotEmtpy(uint64_t minNumberOfLines, const string& outputFilePath, uint64_t customTimeout = 0);

/**
  * @brief Check if the query result was produced
  * @param expectedContent
  * @param outputFilePath
  * @return true if successful
  */
template<typename T>
[[nodiscard]] bool checkBinaryOutputContentLengthOrTimeout(QueryId queryId,
                                                           QueryCatalogServicePtr queryCatalogService,
                                                           uint64_t expectedNumberOfContent,
                                                           const string& outputFilePath,
                                                           auto testTimeout = defaultTimeout) {
    auto timeoutInSec = std::chrono::seconds(testTimeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        std::this_thread::sleep_for(sleepDuration);
        NES_TRACE("TestUtil:checkBinaryOutputContentLengthOrTimeout: check content for file " << outputFilePath);

        auto entry = queryCatalogService->getEntryForQuery(queryId);
        if (entry->getQueryStatus() == QueryStatus::FAILED) {
            // the query failed, so we return true as a failure append during execution.
            NES_TRACE("checkStoppedOrTimeout: status reached failed");
            return false;
        }

        auto isQueryStopped = entry->getQueryStatus() == QueryStatus::STOPPED;

        // check if result is ready.
        std::ifstream ifs(outputFilePath);
        if (ifs.good() && ifs.is_open()) {
            NES_TRACE("TestUtil:checkBinaryOutputContentLengthOrTimeout:: file " << outputFilePath << " open and good");
            std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
            // check the length of the output file
            ifs.seekg(0, std::ifstream::end);
            auto length = ifs.tellg();
            ifs.seekg(0, std::ifstream::beg);

            // read the binary output as a vector of T
            auto* buff = reinterpret_cast<char*>(malloc(length));
            ifs.read(buff, length);
            std::vector<T> currentContent(reinterpret_cast<T*>(buff), reinterpret_cast<T*>(buff) + length / sizeof(T));
            uint64_t currentContentSize = currentContent.size();

            ifs.close();
            free(buff);

            if (expectedNumberOfContent != currentContentSize) {
                if (currentContentSize > expectedNumberOfContent) {
                    NES_DEBUG("TestUtil:checkBinaryOutputContentLengthOrTimeout:: content is larger than expected result: "
                              "currentContentSize: "
                              << currentContentSize << " - expectedNumberOfContent: " << expectedNumberOfContent);
                    return false;
                }

                NES_DEBUG("TestUtil:checkBinaryOutputContentLengthOrTimeout:: number of expected lines "
                          << expectedNumberOfContent << " not reached yet with " << currentContent.size()
                          << " lines content=" << content);

            } else {
                NES_DEBUG("TestUtil:checkBinaryOutputContentLengthOrTimeout: number of content in output file match expected "
                          "number of content");
                return true;
            }
        }
        if (isQueryStopped) {
            NES_DEBUG("TestUtil:checkBinaryOutputContentLengthOrTimeout: query stopped but content not ready");
            return false;
        }
    }
    NES_DEBUG("TestUtil:checkBinaryOutputContentLengthOrTimeout:: expected result not reached within set timeout content");
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
     * @brief This method is used for executing a query
     * @param query string
     * @return if stopped
     */
[[nodiscard]] nlohmann::json startQueryViaRest(const string& queryString, const std::string& restPort = "8081");

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

};// namespace TestUtils

class DummyQueryListener : public AbstractQueryStatusListener {
  public:
    virtual ~DummyQueryListener() {}

    bool canTriggerEndOfStream(QueryId, QuerySubPlanId, OperatorId, Runtime::QueryTerminationType) override { return true; }
    bool notifySourceTermination(QueryId, QuerySubPlanId, OperatorId, Runtime::QueryTerminationType) override { return true; }
    bool notifyQueryFailure(QueryId, QuerySubPlanId, std::string) override { return true; }
    bool notifyQueryStatusChange(QueryId, QuerySubPlanId, Runtime::Execution::ExecutableQueryPlanStatus) override { return true; }
    bool notifyEpochTermination(uint64_t, uint64_t) override { return false; }
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
void writeWaypointsToCsv(const std::string& csvPath, std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> waypoints);

}// namespace NES
#endif// NES_INCLUDE_UTIL_TESTUTILS_HPP_
