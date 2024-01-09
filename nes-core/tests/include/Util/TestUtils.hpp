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
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Common/Identifiers.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <REST/ServerTypes.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Spatial/DataTypes/Waypoint.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Subprocess/Subprocess.hpp>
#include <Util/TimeMeasurement.hpp>
#include <Util/UtilityFunctions.hpp>
#include <chrono>
#include <cpr/cpr.h>
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

/**
 * @brief this is a util class for the tests
 */
namespace TestUtils {

static constexpr auto defaultTimeout = std::chrono::seconds(600000);
static constexpr auto defaultStartQueryTimeout = std::chrono::seconds(1800000);// starting a query requires time
static constexpr auto sleepDuration = std::chrono::milliseconds(2500000);
static constexpr auto defaultCooldown = std::chrono::seconds(30000);// 3s after last processed task, the query should be done.

/**
 * Create a command line parameter for a configuration option for the coordinator or worker.
 * @param name The name of the command line option.
 * @param value The value of the command line option.
 * @param prefix If true, prefix the name of the option with "worker." to configure the internal worker of the coordinator.
 * @return A string representing the command line parameter.
 */
[[nodiscard]] const std::string configOption(const std::string& name, const std::string& value, bool prefix = false) {
    const std::string result = prefix ? "--worker." : "--";
    return result + name + "=" + value;
}

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

[[nodiscard]] std::string bufferSizeInBytes(uint64_t size, bool prefix = false) {
    return configOption(BUFFERS_SIZE_IN_BYTES_CONFIG, size, prefix);
}

[[nodiscard]] std::string configPath(const std::string& filename) { return "--" + CONFIG_PATH + "=" + filename; }

[[nodiscard]] std::string workerConfigPath(const std::string& filename) { return "--" + WORKER_CONFIG_PATH + "=" + filename; }

[[nodiscard]] std::string coordinatorPort(uint64_t coordinatorPort) {
    return "--" + COORDINATOR_PORT_CONFIG + "=" + std::to_string(coordinatorPort);
}

[[nodiscard]] std::string parentId(uint64_t parentId) { return "--" + PARENT_ID_CONFIG + "=" + std::to_string(parentId); }

[[nodiscard]] std::string numberOfSlots(uint64_t coordinatorPort, bool prefix = false) {
    return configOption(NUMBER_OF_SLOTS_CONFIG, coordinatorPort, prefix);
}

[[nodiscard]] std::string numLocalBuffers(uint64_t localBuffers, bool prefix = false) {
    return configOption(NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG, localBuffers, prefix);
}

[[nodiscard]] std::string numGlobalBuffers(uint64_t globalBuffers, bool prefix = false) {
    return configOption(NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG, globalBuffers, prefix);
}

[[nodiscard]] std::string rpcPort(uint64_t rpcPort) { return "--" + RPC_PORT_CONFIG + "=" + std::to_string(rpcPort); }

[[nodiscard]] std::string sourceType(std::string sourceType) {
    return "--physicalSources." + SOURCE_TYPE_CONFIG + "=" + sourceType;
}

[[nodiscard]] std::string csvSourceFilePath(std::string filePath) {
    return "--physicalSources." + FILE_PATH_CONFIG + "=" + filePath;
}

[[nodiscard]] std::string dataPort(uint64_t dataPort) { return "--" + DATA_PORT_CONFIG + "=" + std::to_string(dataPort); }

[[nodiscard]] std::string numberOfTuplesToProducePerBuffer(uint64_t numberOfTuplesToProducePerBuffer) {
    return "--physicalSources." + NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG + "="
        + std::to_string(numberOfTuplesToProducePerBuffer);
}

[[nodiscard]] std::string physicalSourceName(std::string physicalSourceName) {
    return "--physicalSources." + PHYSICAL_SOURCE_NAME_CONFIG + "=" + physicalSourceName;
}

[[nodiscard]] std::string logicalSourceName(std::string logicalSourceName) {
    return "--physicalSources." + LOGICAL_SOURCE_NAME_CONFIG + "=" + logicalSourceName;
}

[[nodiscard]] std::string numberOfBuffersToProduce(uint64_t numberOfBuffersToProduce) {
    return "--physicalSources." + NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG + "=" + std::to_string(numberOfBuffersToProduce);
}

[[nodiscard]] std::string sourceGatheringInterval(uint64_t sourceGatheringInterval) {
    return "--physicalSources." + SOURCE_GATHERING_INTERVAL_CONFIG + "=" + std::to_string(sourceGatheringInterval);
}

[[nodiscard]] std::string restPort(uint64_t restPort) { return "--restPort=" + std::to_string(restPort); }

[[nodiscard]] std::string enableDebug() { return "--logLevel=LOG_DEBUG"; }

[[nodiscard]] std::string workerHealthCheckWaitTime(uint64_t workerWaitTime) {
    return "--healthCheckWaitTime=" + std::to_string(workerWaitTime);
}

[[nodiscard]] std::string coordinatorHealthCheckWaitTime(uint64_t coordinatorWaitTime) {
    return "--healthCheckWaitTime=" + std::to_string(coordinatorWaitTime);
}

[[nodiscard]] std::string enableMonitoring(bool prefix = false) { return configOption(ENABLE_MONITORING_CONFIG, true, prefix); }

// 2884: Fix configuration to disable distributed window rule
[[nodiscard]] std::string disableDistributedWindowingOptimization() {
    return "--optimizer.performDistributedWindowOptimization=false";
}

[[nodiscard]] std::string enableNemoPlacement() { return "--optimizer.enableNemoPlacement=true"; }

[[nodiscard]] std::string setDistributedWindowChildThreshold(uint64_t val) {
    return "--optimizer.distributedWindowChildThreshold=" + std::to_string(val);
}

[[nodiscard]] std::string setDistributedWindowCombinerThreshold(uint64_t val) {
    return "--optimizer.distributedWindowCombinerThreshold=" + std::to_string(val);
}

[[nodiscard]] std::string enableThreadLocalWindowing(bool prefix = false) {
    return configOption(QUERY_COMPILER_CONFIG + "." + QUERY_COMPILER_WINDOWING_STRATEGY_CONFIG,
                        std::string{"THREAD_LOCAL"},
                        prefix);
}

/**
   * @brief start a new instance of a nes coordinator with a set of configuration flags
   * @param flags
   * @return coordinator process, which terminates if it leaves the scope
   */
[[nodiscard]] Util::Subprocess startCoordinator(std::initializer_list<std::string> list) {
    NES_INFO("Start coordinator");
    return {std::string(PATH_TO_BINARY_DIR) + "/nes-core/nesCoordinator", list};
}

/**
     * @brief start a new instance of a nes worker with a set of configuration flags
     * @param flags
     * @return worker process, which terminates if it leaves the scope
     */
[[nodiscard]] Util::Subprocess startWorker(std::initializer_list<std::string> flags) {
    NES_INFO("Start worker");
    return {std::string(PATH_TO_BINARY_DIR) + "/nes-core/nesWorker", flags};
}

/**
     * @brief start a new instance of a nes worker with a set of configuration flags
     * @param flags
     * @return worker process, which terminates if it leaves the scope
     */
[[nodiscard]] std::shared_ptr<Util::Subprocess> startWorkerPtr(std::initializer_list<std::string> flags) {
    NES_INFO("Start worker");
    return std::make_shared<Util::Subprocess>(std::string(PATH_TO_BINARY_DIR) + "/nes-core/nesWorker", flags);
}

/**
     * @brief method to check the produced buffers and tasks for n seconds and either return true or timeout
     * @param ptr to Runtime
     * @param queryId
     * @param expectedResult
     * @return bool indicating if the expected results are matched
     */
[[nodiscard]] bool checkCompleteOrTimeout(const Runtime::NodeEnginePtr& ptr, QueryId queryId, uint64_t expectedResult) {
    if (ptr->getQueryStatistics(queryId).empty()) {
        NES_ERROR("checkCompleteOrTimeout query does not exists");
        return false;
    }
    auto timeoutInSec = std::chrono::seconds(defaultTimeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_TRACE("checkCompleteOrTimeout: check result NodeEnginePtr");
        //FIXME: handle vector of statistics properly in #977
        if (ptr->getQueryStatistics(queryId)[0]->getProcessedBuffers() == expectedResult
            && ptr->getQueryStatistics(queryId)[0]->getProcessedTasks() == expectedResult) {
            NES_TRACE("checkCompleteOrTimeout: NodeEnginePtr results are correct");
            return true;
        }
        NES_TRACE("checkCompleteOrTimeout: NodeEnginePtr sleep because val="
                  << ptr->getQueryStatistics(queryId)[0]->getProcessedTuple() << " < " << expectedResult);
        std::this_thread::sleep_for(sleepDuration);
    }
    NES_TRACE("checkCompleteOrTimeout: NodeEnginePtr expected results are not reached after timeout");
    return false;
}

/**
     * @brief This method is used for waiting till the query gets into running status or a timeout occurs
     * @param queryId : the query id to check for
     * @param queryCatalogService: the catalog to look into for status change
     * @param timeoutInSec: time to wait before stop checking
     * @return true if query gets into running status else false
     */
[[nodiscard]] bool waitForQueryToStart(QueryId queryId,
                                       const QueryCatalogServicePtr& queryCatalogService,
                                       std::chrono::seconds timeoutInSec = std::chrono::seconds(defaultStartQueryTimeout)) {
    NES_TRACE("TestUtils: wait till the query " << queryId << " gets into Running status.");
    auto start_timestamp = std::chrono::system_clock::now();

    NES_TRACE("TestUtils: Keep checking the status of query " << queryId << " until a fixed time out");
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        auto queryCatalogEntry = queryCatalogService->getEntryForQuery(queryId);
        if (!queryCatalogEntry) {
            NES_ERROR("TestUtils: unable to find the entry for query " << queryId << " in the query catalog.");
            return false;
        }
        NES_TRACE("TestUtils: Query " << queryId << " is now in status " << queryCatalogEntry->getQueryStatusAsString());
        QueryStatus::Value status = queryCatalogEntry->getQueryStatus();

        switch (queryCatalogEntry->getQueryStatus()) {
            case QueryStatus::MarkedForHardStop:
            case QueryStatus::MarkedForSoftStop:
            case QueryStatus::SoftStopCompleted:
            case QueryStatus::SoftStopTriggered:
            case QueryStatus::Stopped:
            case QueryStatus::Running: {
                return true;
            }
            case QueryStatus::Failed: {
                NES_ERROR("Query failed to start. Expected: Running or Optimizing but found " + QueryStatus::toString(status));
                return false;
            }
            default: {
                NES_WARNING("Expected: Running or Scheduling but found " + QueryStatus::toString(status));
                break;
            }
        }

        std::this_thread::sleep_for(sleepDuration);
    }
    NES_TRACE("checkCompleteOrTimeout: waitForStart expected results are not reached after timeout");
    return false;
}

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
                                         std::chrono::seconds timeout = defaultTimeout) {
    auto timeoutInSec = std::chrono::seconds(timeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_TRACE("checkStoppedOrTimeout: check query status for " << queryId);
        if (queryCatalogService->getEntryForQuery(queryId)->getQueryStatus() == QueryStatus::Stopped) {
            NES_TRACE("checkStoppedOrTimeout: status for " << queryId << " reached stopped");
            return true;
        }
        NES_DEBUG("checkStoppedOrTimeout: status not reached for "
                  << queryId << " as status is=" << queryCatalogService->getEntryForQuery(queryId)->getQueryStatusAsString());
        std::this_thread::sleep_for(sleepDuration);
    }
    NES_TRACE("checkStoppedOrTimeout: expected status not reached within set timeout");
    return false;
}

/**
     * @brief Check if the query has failed within the timeout.
     * @param queryId: Id of the query to be stopped
     * @param queryCatalogService: the catalog containig the queries in the system
     * @return true if successful
     */
[[nodiscard]] bool checkFailedOrTimeout(QueryId queryId,
                                        const QueryCatalogServicePtr& queryCatalogService,
                                        std::chrono::seconds timeout = defaultTimeout) {
    auto timeoutInSec = std::chrono::seconds(timeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_TRACE("checkFailedOrTimeout: check query status");
        if (queryCatalogService->getEntryForQuery(queryId)->getQueryStatus() == QueryStatus::Failed) {
            NES_DEBUG("checkFailedOrTimeout: status reached stopped");
            return true;
        }
        NES_TRACE("checkFailedOrTimeout: status not reached as status is="
                  << queryCatalogService->getEntryForQuery(queryId)->getQueryStatusAsString());
        std::this_thread::sleep_for(sleepDuration);
    }
    NES_WARNING("checkStoppedOrTimeout: expected status not reached within set timeout");
    return false;
}

/**
   * @brief Check if the query result was produced
   * @param expectedContent
   * @param outputFilePath
   * @return true if successful
   */
[[nodiscard]] bool checkOutputOrTimeout(string expectedContent, const string& outputFilePath, uint64_t customTimeout = 0) {
    std::chrono::seconds timeoutInSec;
    if (customTimeout == 0) {
        timeoutInSec = std::chrono::seconds(defaultTimeout);
    } else {
        timeoutInSec = std::chrono::seconds(customTimeout);
    }

    NES_TRACE("using timeout=" << timeoutInSec.count());
    auto start_timestamp = std::chrono::system_clock::now();
    uint64_t found = 0;
    uint64_t count = 0;
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        std::this_thread::sleep_for(sleepDuration);
        found = 0;
        count = 0;
        NES_TRACE("checkOutputOrTimeout: check content for file " << outputFilePath);
        std::ifstream ifs(outputFilePath);
        if (ifs.good() && ifs.is_open()) {
            std::vector<std::string> expectedlines = Util::splitWithStringDelimiter<std::string>(expectedContent, "\n");
            std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
            count = std::count(content.begin(), content.end(), '\n');
            if (expectedlines.size() != count) {
                NES_TRACE("checkoutputortimeout: number of expected lines " << expectedlines.size() << " not reached yet with "
                                                                            << count << " lines content=" << content
                                                                            << " file=" << outputFilePath);
                continue;
            }

            if (content.size() != expectedContent.size()) {
                NES_TRACE("checkoutputortimeout: number of chars " << expectedContent.size()
                                                                   << " not reached yet with chars content=" << content.size()
                                                                   << " lines content=" << content);
                continue;
            }

            for (auto& expectedline : expectedlines) {
                if (content.find(expectedline) != std::string::npos) {
                    found++;
                }
            }
            if (found == count) {
                NES_TRACE("all lines found final content=" << content);
                return true;
            }
            NES_TRACE("only " << found << " lines found final content=" << content);
        }
    }
    NES_ERROR("checkOutputOrTimeout: expected (" << count << ") result not reached (" << found << ") within set timeout content");
    return false;
}

/**
   * @brief Check if any query result was produced
   * @param outputFilePath
   * @return true if successful
   */
[[nodiscard]] bool
checkIfOutputFileIsNotEmtpy(uint64_t minNumberOfLines, const string& outputFilePath, uint64_t customTimeout = 0) {
    std::chrono::seconds timeoutInSec;
    if (customTimeout == 0) {
        timeoutInSec = std::chrono::seconds(defaultTimeout);
    } else {
        timeoutInSec = std::chrono::seconds(customTimeout);
    }

    NES_TRACE("using timeout=" << timeoutInSec.count());
    auto start_timestamp = std::chrono::system_clock::now();
    uint64_t count = 0;
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        std::this_thread::sleep_for(sleepDuration);
        count = 0;
        NES_TRACE("checkIfOutputFileIsNotEmtpy: check content for file " << outputFilePath);
        std::ifstream ifs(outputFilePath);
        if (ifs.good() && ifs.is_open()) {
            std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
            count = std::count(content.begin(), content.end(), '\n');
            if (count < minNumberOfLines) {
                NES_TRACE("checkIfOutputFileIsNotEmtpy: number of min lines " << minNumberOfLines << " not reached yet with "
                                                                              << count << " lines content=" << content);
                continue;
            }
            NES_TRACE("at least" << minNumberOfLines << " are found in content=" << content);
            return true;
        }
    }
    NES_ERROR("checkIfOutputFileIsNotEmtpy: expected (" << count << ") result not reached (" << minNumberOfLines
                                                        << ") within set timeout content");
    return false;
}

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
        if (entry->getQueryStatus() == QueryStatus::Failed) {
            // the query failed so we return true as a failure append during execution.
            NES_TRACE("checkStoppedOrTimeout: status reached failed");
            return false;
        }

        auto isQueryStopped = entry->getQueryStatus() == QueryStatus::Stopped;

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
[[nodiscard]] bool checkFileCreationOrTimeout(const string& outputFilePath) {
    auto timeoutInSec = std::chrono::seconds(defaultTimeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        std::this_thread::sleep_for(sleepDuration);
        NES_TRACE("checkFileCreationOrTimeout: for file " << outputFilePath);
        std::ifstream ifs(outputFilePath);
        if (ifs.good() && ifs.is_open()) {
            return true;
        }
    }
    NES_TRACE("checkFileCreationOrTimeout: expected result not reached within set timeout");
    return false;
}

/**
   * @brief Check if Coordinator REST API is available or timeout
   * @param expectedContent
   * @param outputFilePath
   * @return true if successful
   */
[[nodiscard]] bool checkRESTServerStartedOrTimeout(uint64_t restPort, uint64_t customTimeout = 0) {
    std::chrono::seconds timeoutInSec;
    if (customTimeout == 0) {
        timeoutInSec = std::chrono::seconds(defaultTimeout);
    } else {
        timeoutInSec = std::chrono::seconds(customTimeout);
    }
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        std::this_thread::sleep_for(sleepDuration);
        NES_INFO("check if NES REST interface is up");
        auto future =
            cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(restPort) + "/v1/nes/connectivity/check"}, cpr::Timeout{3000});
        future.wait();
        cpr::Response r = future.get();
        if (r.status_code == 200l) {
            return true;
        }
    }
    NES_TRACE("checkFileCreationOrTimeout: expected result not reached within set timeout");
    return false;
}

/**
     * @brief This method is used for checking if the submitted query produced the expected result within the timeout
     * @param queryId: Id of the query
     * @param expectedNumberBuffers: The expected value
     * @return true if matched the expected result within the timeout
     */
[[nodiscard]] bool checkCompleteOrTimeout(QueryId queryId, uint64_t expectedNumberBuffers, const std::string& restPort = "8081") {
    auto timeoutInSec = std::chrono::seconds(defaultTimeout);
    auto start_timestamp = std::chrono::system_clock::now();
    uint64_t currentResult = 0;
    nlohmann::json json_return;
    std::string currentStatus;

    NES_DEBUG("checkCompleteOrTimeout: Check if the query goes into the Running status within the timeout");
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        std::string url = "http://localhost:" + restPort + "/v1/nes/queryCatalog/status";
        nlohmann::json jsonReturn;
        auto future = cpr::GetAsync(cpr::Url{url}, cpr::Parameters{{"queryId", std::to_string(queryId)}});
        future.wait();
        auto response = future.get();
        nlohmann::json result = nlohmann::json::parse(response.text);
        if (response.status_code == cpr::status::HTTP_OK && result.contains("status")) {
            currentStatus = result["status"];
            if (currentStatus == "RUNNING" || currentStatus == "STOPPED") {
                break;
            }
        }
        NES_DEBUG("checkCompleteOrTimeout: sleep because current status =" << currentStatus);
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepDuration));
    }
    NES_DEBUG("checkCompleteOrTimeout: end with status =" << currentStatus);

    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_DEBUG("checkCompleteOrTimeout: check result NodeEnginePtr");

        std::string url = "http://localhost:" + restPort + "/v1/nes/queryCatalog/getNumberOfProducedBuffers";
        nlohmann::json jsonReturn;
        auto future = cpr::GetAsync(cpr::Url{url}, cpr::Parameters{{"queryId", std::to_string(queryId)}});
        future.wait();
        auto response = future.get();
        nlohmann::json result = nlohmann::json::parse(response.text);
        if (response.status_code == cpr::status::HTTP_OK && result.contains("producedBuffers")) {
            currentResult = result["producedBuffers"];
            if (currentResult >= expectedNumberBuffers) {
                NES_DEBUG("checkCompleteOrTimeout: results are correct");
                return true;
            }
        }
        NES_DEBUG("checkCompleteOrTimeout: sleep because val=" << currentResult << " < " << expectedNumberBuffers);
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepDuration));
    }
    NES_DEBUG("checkCompleteOrTimeout: QueryId expected results are not reached after timeout currentResult="
              << currentResult << " expectedNumberBuffers=" << expectedNumberBuffers);
    return false;
}

/**
     * @brief This method is used for checking if the submitted query is running
     * @param queryId: Id of the query
     * @return true if is running within the timeout, else false
     */
[[nodiscard]] bool checkRunningOrTimeout(QueryId queryId, const std::string& restPort = "8081") {
    auto timeoutInSec = std::chrono::seconds(defaultTimeout);
    auto start_timestamp = std::chrono::system_clock::now();
    uint64_t currentResult = 0;
    nlohmann::json json_return;
    std::string currentStatus;

    NES_DEBUG("checkCompleteOrTimeout: Check if the query goes into the Running status within the timeout");
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        std::string url = "http://localhost:" + restPort + "/v1/nes/queryCatalog/status";
        nlohmann::json jsonReturn;
        auto future = cpr::GetAsync(cpr::Url{url}, cpr::Parameters{{"queryId", std::to_string(queryId)}});
        future.wait();
        auto response = future.get();
        nlohmann::json result = nlohmann::json::parse(response.text);
        currentStatus = result["status"];
        if (currentStatus == "RUNNING" || currentStatus == "STOPPED") {
            return true;
        }
        NES_DEBUG("checkCompleteOrTimeout: sleep because current status =" << currentStatus);
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepDuration));
    }
    NES_DEBUG("checkCompleteOrTimeout: QueryId expected results are not reached after timeout");
    return false;
}

/**
     * @brief This method is used for stop a query
     * @param queryId: Id of the query
     * @return if stopped
     */
[[nodiscard]] bool stopQueryViaRest(QueryId queryId, const std::string& restPort = "8081") {
    nlohmann::json json_return;

    std::string url = BASE_URL + restPort + "/v1/nes/query/stop-query";
    nlohmann::json jsonReturn;
    auto future = cpr::DeleteAsync(cpr::Url{url}, cpr::Parameters{{"queryId", std::to_string(queryId)}});
    future.wait();
    auto response = future.get();
    nlohmann::json result = nlohmann::json::parse(response.text);
    NES_DEBUG("stopQueryViaRest: status =" << result.dump());

    return result["success"].get<bool>();
}

/**
     * @brief This method is used for executing a query
     * @param query string
     * @return if stopped
     */
[[nodiscard]] nlohmann::json startQueryViaRest(const string& queryString, const std::string& restPort = "8081") {
    nlohmann::json json_return;

    std::string url = BASE_URL + restPort + "/v1/nes/query/execute-query";
    nlohmann::json jsonReturn;
    auto future = cpr::PostAsync(cpr::Url{url}, cpr::Header{{"Content-Type", "application/json"}}, cpr::Body{queryString});
    future.wait();
    auto response = future.get();
    nlohmann::json result = nlohmann::json::parse(response.text);
    NES_DEBUG("startQueryViaRest: status =" << result.dump());

    return result;
}

/**
     * @brief This method is used for making a monitoring rest call.
     * @param1 the rest call
     * @param2 the rest port
     * @return the json
     */
[[nodiscard]] nlohmann::json makeMonitoringRestCall(const string& restCall, const std::string& restPort = "8081") {
    std::string url = BASE_URL + restPort + "/v1/nes/monitoring/" + restCall;
    auto future = cpr::GetAsync(cpr::Url{url});
    future.wait();
    auto response = future.get();
    nlohmann::json result = nlohmann::json::parse(response.text);
    NES_DEBUG("getAllMonitoringMetricsViaRest: status =" << result.dump());

    return result;
}

/**
   * @brief This method is used adding a logical source
   * @param query string
   * @return
   */
[[nodiscard]] bool addLogicalSource(const string& schemaString, const std::string& restPort = "8081") {
    nlohmann::json json_returnSchema;

    std::string url = BASE_URL + restPort + "/v1/nes/sourceCatalog/addLogicalSource";
    auto future = cpr::PostAsync(cpr::Url{url}, cpr::Header{{"Content-Type", "application/json"}}, cpr::Body{schemaString});
    future.wait();
    cpr::Response response = future.get();
    nlohmann::json jsonResponse = nlohmann::json::parse(response.text);
    NES_DEBUG("addLogicalSource: status =" << jsonResponse.dump());
    return jsonResponse["success"].get<bool>();
}

bool waitForWorkers(uint64_t restPort, uint16_t maxTimeout, uint16_t expectedWorkers) {
    auto baseUri = "http://localhost:" + std::to_string(restPort) + "/v1/nes/topology";
    NES_INFO("TestUtil: Executing GET request on URI " << baseUri);
    nlohmann::json json_return;
    size_t nodeNo = 0;

    for (int i = 0; i < maxTimeout; i++) {
        try {
            auto future = cpr::GetAsync(cpr::Url{baseUri}, cpr::Timeout(3000));
            future.wait();
            cpr::Response response = future.get();
            if (!nlohmann::json::accept(response.text)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(sleepDuration));
            }
            nlohmann::json jsonResponse = nlohmann::json::parse(response.text);
            nodeNo = jsonResponse["nodes"].size();

            if (nodeNo == expectedWorkers + 1U) {
                NES_INFO("TestUtils: Expected worker number reached correctly " << expectedWorkers);
                NES_DEBUG("TestUtils: Received topology JSON:\n" << jsonResponse.dump());
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepDuration));
        } catch (const std::exception& e) {
            NES_ERROR("TestUtils: WaitForWorkers error occured " << e.what());
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepDuration));
        }
    }

    NES_ERROR("E2ECoordinatorMultiWorkerTest: Expected worker number not reached correctly " << nodeNo << " but expected "
                                                                                             << expectedWorkers);
    return false;
}

/**
     * @brief This method is used for making a REST call to coordinator to get the topology as Json
     * @param1 the rest port
     * @return the json
     */
[[nodiscard]] nlohmann::json getTopology(uint64_t restPort) {
    auto baseUri = "http://localhost:" + std::to_string(restPort) + "/v1/nes/topology";
    NES_INFO("TestUtil: Executing GET request on URI " << baseUri);

    auto future = cpr::GetAsync(cpr::Url{baseUri}, cpr::Timeout{3000});
    future.wait();
    auto response = future.get();
    nlohmann::json result = nlohmann::json::parse(response.text);
    return result;
}

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
std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> getWaypointsFromCsv(const std::string& csvPath,
                                                                                 Timestamp startTime) {
    std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> waypoints;
    std::string csvLine;
    std::ifstream inputStream(csvPath);
    std::string latitudeString;
    std::string longitudeString;
    std::string timeString;

    NES_DEBUG("Creating list of waypoints with startTime " << startTime)

    //read locations and time offsets from csv, calculate absolute timestamps from offsets by adding start time
    while (std::getline(inputStream, csvLine)) {
        std::stringstream stringStream(csvLine);
        getline(stringStream, latitudeString, ',');
        getline(stringStream, longitudeString, ',');
        getline(stringStream, timeString, ',');
        Timestamp time = std::stoul(timeString);
        NES_TRACE("Read from csv: " << latitudeString << ", " << longitudeString << ", " << time);

        //add startTime to the offset obtained from csv to get absolute timestamp
        time += startTime;

        //construct a pair containing a location and the time at which the device is at exactly that point
        // and save it to a vector containing all waypoints
        waypoints.push_back(NES::Spatial::DataTypes::Experimental::Waypoint(
            NES::Spatial::DataTypes::Experimental::GeoLocation(std::stod(latitudeString), std::stod(longitudeString)),
            time));
    }
    return waypoints;
}

/**
 * @brief write mobile device path waypoints to a csv file to use as input for the LocationProviderCSV class
 * @param csvPath path to the output file
 * @param waypoints a vector of waypoints to be written to the file
 */
void writeWaypointsToCsv(const std::string& csvPath, std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> waypoints) {
    remove(csvPath.c_str());
    std::ofstream outFile(csvPath);
    for (auto& point : waypoints) {
        ASSERT_TRUE(point.getTimestamp().has_value());
        outFile << point.getLocation().toString() << "," << std::to_string(point.getTimestamp().value()) << std::endl;
    }
    outFile.close();
    ASSERT_FALSE(outFile.fail());
}

}// namespace NES
#endif// NES_INCLUDE_UTIL_TESTUTILS_HPP_
