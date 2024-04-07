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

#include <API/AttributeField.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/Common.hpp>
#include <Util/Core.hpp>
#include <Util/Mobility/Waypoint.hpp>
#include <Util/StdInt.hpp>
#include <Util/Subprocess/Subprocess.hpp>
#include <Util/TestUtils.hpp>
#include <algorithm>
#include <chrono>
#include <cpr/cpr.h>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <nlohmann/json.hpp>

namespace NES {

/**
 * @brief this is a util class for the tests
 */
namespace TestUtils {

[[nodiscard]] const std::string configOption(const std::string& name, const std::string& value, bool prefix) {
    const std::string result = prefix ? "--worker." : "--";
    return result + name + "=" + value;
}

[[nodiscard]] std::string bufferSizeInBytes(uint64_t size, bool prefix) {
    return configOption(BUFFERS_SIZE_IN_BYTES_CONFIG, size, prefix);
}

[[nodiscard]] std::string configPath(const std::string& filename) { return "--" + CONFIG_PATH + "=" + filename; }

[[nodiscard]] std::string workerConfigPath(const std::string& filename) { return "--" + WORKER_CONFIG_PATH + "=" + filename; }

[[nodiscard]] std::string coordinatorPort(uint64_t coordinatorPort) {
    return "--" + COORDINATOR_PORT_CONFIG + "=" + std::to_string(coordinatorPort);
}

[[nodiscard]] std::string parentId(uint64_t parentId) { return "--" + PARENT_ID_CONFIG + "=" + std::to_string(parentId); }

[[nodiscard]] std::string numberOfSlots(uint64_t coordinatorPort, bool prefix) {
    return configOption(NUMBER_OF_SLOTS_CONFIG, coordinatorPort, prefix);
}

[[nodiscard]] std::string numLocalBuffers(uint64_t localBuffers, bool prefix) {
    return configOption(NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG, localBuffers, prefix);
}

[[nodiscard]] std::string numGlobalBuffers(uint64_t globalBuffers, bool prefix) {
    return configOption(NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG, globalBuffers, prefix);
}

[[nodiscard]] std::string rpcPort(uint64_t rpcPort) { return "--" + RPC_PORT_CONFIG + "=" + std::to_string(rpcPort); }

[[nodiscard]] std::string sourceType(SourceType sourceType) {
    return "--physicalSources." + SOURCE_TYPE_CONFIG + "=" + std::string(magic_enum::enum_name(sourceType));
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

[[nodiscard]] std::string enableMonitoring(bool prefix) {
    std::cout << prefix << " aha " << ENABLE_MONITORING_CONFIG;
    return configOption(ENABLE_MONITORING_CONFIG, true, prefix);
}

[[nodiscard]] std::string monitoringWaitTime(uint64_t monitoringWaitTime) {
    return "--monitoringWaitTime=" + std::to_string(monitoringWaitTime);
}

[[nodiscard]] std::string enableNemoPlacement() { return "--optimizer.enableNemoPlacement=true"; }

[[nodiscard]] std::string enableSlicingWindowing(bool prefix) {
    return configOption(QUERY_COMPILER_CONFIG + "." + QUERY_COMPILER_WINDOWING_STRATEGY_CONFIG, std::string{"SLICING"}, prefix);
}

[[nodiscard]] std::string enableNautilusWorker() { return "--queryCompiler.queryCompilerType=NAUTILUS_QUERY_COMPILER"; }

[[nodiscard]] std::string enableNautilusCoordinator() {
    return "--worker.queryCompiler.queryCompilerType=NAUTILUS_QUERY_COMPILER";
}

/**
   * @brief start a new instance of a nes coordinator with a set of configuration flags
   * @param flags
   * @return coordinator process, which terminates if it leaves the scope
   */
[[nodiscard]] Util::Subprocess startCoordinator(std::initializer_list<std::string> list) {
    NES_INFO("Start coordinator");
    return {std::string(PATH_TO_BINARY_DIR) + "/nes-coordinator/nesCoordinator", list};
}

/**
     * @brief start a new instance of a nes worker with a set of configuration flags
     * @param flags
     * @return worker process, which terminates if it leaves the scope
     */
[[nodiscard]] Util::Subprocess startWorker(std::initializer_list<std::string> flags) {
    NES_INFO("Start worker");
    return {std::string(PATH_TO_BINARY_DIR) + "/nes-worker/nesWorker", flags};
}

/**
     * @brief start a new instance of a nes worker with a set of configuration flags
     * @param flags
     * @return worker process, which terminates if it leaves the scope
     */
[[nodiscard]] std::shared_ptr<Util::Subprocess> startWorkerPtr(std::initializer_list<std::string> flags) {
    NES_INFO("Start worker");
    return std::make_shared<Util::Subprocess>(std::string(PATH_TO_BINARY_DIR) + "/nes-worker/nesWorker", flags);
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
        NES_TRACE("checkCompleteOrTimeout: NodeEnginePtr sleep because val={} < {}",
                  ptr->getQueryStatistics(queryId)[0]->getProcessedTuple(),
                  expectedResult);
        std::this_thread::sleep_for(sleepDuration);
    }
    NES_TRACE("checkCompleteOrTimeout: NodeEnginePtr expected results are not reached after timeout");
    return false;
}

/**
     * @brief This method is used for waiting till the query gets into running status or a timeout occurs
     * @param queryId : the query id to check for
     * @param queryCatalog: the catalog to look into for status change
     * @param timeoutInSec: time to wait before stop checking
     * @return true if query gets into running status else false
     */
[[nodiscard]] bool
waitForQueryToStart(QueryId queryId, const Catalogs::Query::QueryCatalogPtr& queryCatalog, std::chrono::seconds timeoutInSec) {
    NES_TRACE("TestUtils: wait till the query {} gets into Running status.", queryId);
    auto start_timestamp = std::chrono::system_clock::now();

    NES_TRACE("TestUtils: Keep checking the status of query {} until a fixed time out", queryId);
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        QueryState queryState = queryCatalog->getQueryState(queryId);
        NES_TRACE("TestUtils: Query {} is now in status {}", queryId, magic_enum::enum_name(queryState));
        switch (queryState) {
            case QueryState::MARKED_FOR_HARD_STOP:
            case QueryState::MARKED_FOR_SOFT_STOP:
            case QueryState::SOFT_STOP_COMPLETED:
            case QueryState::SOFT_STOP_TRIGGERED:
            case QueryState::STOPPED:
            case QueryState::RUNNING: {
                return true;
            }
            case QueryState::FAILED: {
                NES_ERROR("Query failed to start. Expected: Running or Optimizing but found {}",
                          magic_enum::enum_name(queryState));
                return false;
            }
            default: {
                NES_WARNING("Expected: Running or Scheduling but found {}", magic_enum::enum_name(queryState));
                break;
            }
        }

        std::this_thread::sleep_for(sleepDuration);
    }
    NES_TRACE("checkCompleteOrTimeout: waitForStart expected results are not reached after timeout");
    return false;
}

/**
     * @brief Check if the query is been stopped successfully within the timeout.
     * @param queryId: Id of the query to be stopped
     * @param queryCatalog: the catalog containig the queries in the system
     * @return true if successful
     */
[[nodiscard]] bool
checkStoppedOrTimeout(QueryId queryId, const Catalogs::Query::QueryCatalogPtr& queryCatalog, std::chrono::seconds timeout) {
    auto timeoutInSec = std::chrono::seconds(timeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_TRACE("checkStoppedOrTimeout: check query status for {}", queryId);
        QueryState queryState = queryCatalog->getQueryState(queryId);
        if (queryState == QueryState::STOPPED) {
            NES_DEBUG("checkStoppedOrTimeout: status for {} reached stopped", queryId);
            return true;
        }
        NES_WARNING("checkStoppedOrTimeout: status not reached for {} as status is={}",
                    queryId,
                    magic_enum::enum_name(queryState));
        std::this_thread::sleep_for(sleepDuration);
    }
    NES_TRACE("checkStoppedOrTimeout: expected status not reached within set timeout");
    return false;
}

/**
     * @brief Check if the query is been stopped successfully within the timeout.
     * @param queryId: Id of the query to be stopped
     * @param worker: the worker which the query runs on
     * @return true if successful
     */
[[nodiscard]] bool checkStoppedOrTimeoutAtWorker(QueryId queryId, NesWorkerPtr worker, std::chrono::seconds timeout) {
    auto timeoutInSec = std::chrono::seconds(timeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_TRACE("checkStoppedOrTimeout: check query status for {}", queryId);
        if (worker->getNodeEngine()->getQueryStatus(queryId) == Runtime::Execution::ExecutableQueryPlanStatus::Finished) {
            NES_TRACE("checkStoppedOrTimeout: status for {} reached stopped", queryId);
            return true;
        }
        std::string status;
        switch (worker->getNodeEngine()->getQueryStatus(queryId)) {
            case Runtime::Execution::ExecutableQueryPlanStatus::Created: status = "created"; break;
            case Runtime::Execution::ExecutableQueryPlanStatus::Deployed: status = "deployed"; break;
            case Runtime::Execution::ExecutableQueryPlanStatus::Running: status = "running"; break;
            case Runtime::Execution::ExecutableQueryPlanStatus::Finished: status = "finished"; break;
            case Runtime::Execution::ExecutableQueryPlanStatus::Stopped: status = "stopped"; break;
            case Runtime::Execution::ExecutableQueryPlanStatus::ErrorState: status = "error"; break;
            case Runtime::Execution::ExecutableQueryPlanStatus::Invalid: status = "invalid"; break;
        }
        NES_DEBUG("checkStoppedOrTimeout: status not reached for {} at worker {}. Current state =  {}",
                  queryId,
                  worker->getWorkerId(),
                  status);
        std::this_thread::sleep_for(sleepDuration);
    }
    NES_TRACE("checkStoppedOrTimeout: expected status not reached within set timeout");
    return false;
}

/**
     * @brief Check if the query has failed within the timeout.
     * @param queryId: Id of the query to be stopped
     * @param queryCatalog: the catalog containig the queries in the system
     * @return true if successful
     */
[[nodiscard]] bool
checkFailedOrTimeout(QueryId queryId, const Catalogs::Query::QueryCatalogPtr& queryCatalog, std::chrono::seconds timeout) {
    auto timeoutInSec = std::chrono::seconds(timeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        QueryState queryState = queryCatalog->getQueryState(queryId);
        if (queryState == QueryState::FAILED) {
            NES_DEBUG("checkFailedOrTimeout: status reached stopped");
            return true;
        }
        NES_TRACE("checkFailedOrTimeout: status not reached as status is={}", magic_enum::enum_name(queryState));
        std::this_thread::sleep_for(sleepDuration);
    }
    NES_WARNING("checkStoppedOrTimeout: expected status not reached within set timeout");
    return false;
}

/**
   * @brief Check if the query result was produced
   * @param expectedContent
   * @param outputFilePath
   * @param customTimeoutInSeconds
   * @return true if successful
   */
[[nodiscard]] bool checkOutputOrTimeout(string expectedContent, const string& outputFilePath, uint64_t customTimeoutInSeconds) {
    std::chrono::seconds timeoutInSec;
    if (customTimeoutInSeconds == 0) {
        timeoutInSec = std::chrono::seconds(defaultTimeout);
    } else {
        timeoutInSec = std::chrono::seconds(customTimeoutInSeconds);
    }

    NES_TRACE("using timeout={}", timeoutInSec.count());
    auto start_timestamp = std::chrono::system_clock::now();
    uint64_t found = 0;
    uint64_t count = 0;
    std::string content;
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        std::this_thread::sleep_for(sleepDuration);
        found = 0;
        count = 0;
        NES_TRACE("checkOutputOrTimeout: check content for file {}", outputFilePath);
        std::ifstream ifs(outputFilePath);
        if (ifs.good() && ifs.is_open()) {
            std::vector<std::string> expectedlines = NES::Util::splitWithStringDelimiter<std::string>(expectedContent, "\n");
            content = std::string((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
            count = std::count(content.begin(), content.end(), '\n');
            if (expectedlines.size() != count) {
                NES_TRACE("checkoutputortimeout: number of expected lines {} not reached yet with {} lines content={} file={}",
                          expectedlines.size(),
                          count,
                          content,
                          outputFilePath);
                continue;
            }

            if (content.size() != expectedContent.size()) {
                NES_TRACE("checkoutputortimeout: number of chars {} not reached yet with chars content={} lines content={}",
                          expectedContent.size(),
                          content.size(),
                          content);
                continue;
            }

            for (auto& expectedline : expectedlines) {
                if (content.find(expectedline) != std::string::npos) {
                    found++;
                }
            }
            if (found == count) {
                NES_TRACE("all lines found final content={}", content);
                return true;
            }
            NES_TRACE("only {} lines found final content={}", found, content);
            if (found > count) {
                break;
            }
        }
    }
    NES_ERROR("checkOutputOrTimeout: expected ({}) result not reached ({}) within set timeout content", count, found);
    NES_ERROR("checkOutputOrTimeout: expected:\n {} \n but was:\n {}", expectedContent, content);
    return false;
}

/**
   * @brief Check if any query result was produced
   * @param outputFilePath
   * @return true if successful
   */
[[nodiscard]] bool checkIfOutputFileIsNotEmtpy(uint64_t minNumberOfLines, const string& outputFilePath, uint64_t customTimeout) {
    std::chrono::seconds timeoutInSec;
    if (customTimeout == 0) {
        timeoutInSec = std::chrono::seconds(defaultTimeout);
    } else {
        timeoutInSec = std::chrono::seconds(customTimeout);
    }

    NES_TRACE("using timeout={}", timeoutInSec.count());
    auto start_timestamp = std::chrono::system_clock::now();
    uint64_t count = 0;
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        std::this_thread::sleep_for(sleepDuration);
        count = 0;
        NES_TRACE("checkIfOutputFileIsNotEmtpy: check content for file {}", outputFilePath);
        std::ifstream ifs(outputFilePath);
        if (ifs.good() && ifs.is_open()) {
            std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
            count = std::count(content.begin(), content.end(), '\n');
            if (count < minNumberOfLines) {
                NES_TRACE("checkIfOutputFileIsNotEmtpy: number of min lines {} not reached yet with {} lines content={}",
                          minNumberOfLines,
                          count,
                          content);
                continue;
            }
            NES_TRACE("at least {} are found in content={}", minNumberOfLines, content);
            return true;
        }
    }
    NES_ERROR("checkIfOutputFileIsNotEmtpy: expected ({}) result not reached ({}) within set timeout content",
              count,
              minNumberOfLines);
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
        NES_TRACE("checkFileCreationOrTimeout: for file {}", outputFilePath);
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
[[nodiscard]] bool checkRESTServerStartedOrTimeout(uint64_t restPort, uint64_t customTimeout) {
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
[[nodiscard]] bool checkCompleteOrTimeout(QueryId queryId, uint64_t expectedNumberBuffers, const std::string& restPort) {
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
        NES_DEBUG("checkCompleteOrTimeout: sleep because current status ={}", currentStatus);
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepDuration));
    }
    NES_DEBUG("checkCompleteOrTimeout: end with status ={}", currentStatus);

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
        NES_DEBUG("checkCompleteOrTimeout: sleep because val={} < {}", currentResult, expectedNumberBuffers);
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepDuration));
    }
    NES_DEBUG("checkCompleteOrTimeout: QueryId expected results are not reached after timeout currentResult={}"
              " expectedNumberBuffers={}",
              currentResult,
              expectedNumberBuffers);
    return false;
}

std::vector<Runtime::MemoryLayouts::TestTupleBuffer> createTestTupleBuffers(std::vector<Runtime::TupleBuffer>& buffers,
                                                                            const SchemaPtr& schema) {
    std::vector<Runtime::MemoryLayouts::TestTupleBuffer> testBuffers;
    for (const auto& tupleBuffer : buffers) {
        auto testTupleBuffer = Runtime::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(tupleBuffer, schema);
        testBuffers.emplace_back(testTupleBuffer);
    }
    return testBuffers;
}

bool buffersContainSameTuples(std::vector<Runtime::MemoryLayouts::TestTupleBuffer>& expectedBuffers,
                              std::vector<Runtime::MemoryLayouts::TestTupleBuffer>& actualBuffers,
                              bool orderSensitive) {

    auto numTuplesExpected =
        std::accumulate(expectedBuffers.begin(), expectedBuffers.end(), 0_u64, [](const uint64_t sum, const auto& buf) {
            return sum + buf.getNumberOfTuples();
        });
    auto numTuplesActual =
        std::accumulate(actualBuffers.begin(), actualBuffers.end(), 0_u64, [](const uint64_t sum, const auto& buf) {
            return sum + buf.getNumberOfTuples();
        });

    if (numTuplesExpected != numTuplesActual) {
        NES_ERROR("expected {} and actual buffers {} do not contain the same number of tuples!",
                  numTuplesExpected,
                  numTuplesActual);
        return false;
    }
    if (numTuplesExpected == 0 || numTuplesActual == 0) {
        NES_ERROR("Buffers can not be empty for comparison!");
        return false;
    }

    if (orderSensitive) {
        // We care about the order and therefore, we can just iterate over expected and actual tuples.
        auto expectedBufferPos = 0_u64;
        auto actualBufferPos = 0_u64;
        auto expectedBufferTupleIdx = 0_u64;
        auto actualBufferTupleIdx = 0_u64;

        while (expectedBufferPos < expectedBuffers.size() && actualBufferPos < actualBuffers.size()) {
            if (expectedBufferTupleIdx == expectedBuffers[expectedBufferPos].getNumberOfTuples()) {
                expectedBufferTupleIdx = 0_u64;
            }

            if (actualBufferTupleIdx == actualBuffers[actualBufferPos].getNumberOfTuples()) {
                actualBufferTupleIdx = 0_u64;
            }

            auto expectedTuple = expectedBuffers[expectedBufferPos][expectedBufferTupleIdx];
            auto actualTuple = actualBuffers[actualBufferPos][actualBufferTupleIdx];
            if (expectedTuple != actualTuple) {
                const auto expectedSchema = expectedBuffers[expectedBufferPos].getMemoryLayout()->getSchema();
                const auto actualSchema = actualBuffers[actualBufferPos].getMemoryLayout()->getSchema();
                NES_ERROR("Tuples {} and {} are not equal!",
                          expectedTuple.toString(expectedSchema),
                          actualTuple.toString(actualSchema));
                return false;
            }

            ++expectedBufferTupleIdx;
            ++actualBufferTupleIdx;

            if (expectedBufferTupleIdx == expectedBuffers[expectedBufferPos].getNumberOfTuples()) {
                expectedBufferPos += 1;
            }
            if (actualBufferTupleIdx == actualBuffers[actualBufferPos].getNumberOfTuples()) {
                actualBufferPos += 1;
            }
        }

    } else {
        // We count the occurrences of each tuple in the expectedBuffers. Then we count the occurrences of the expectedTuple in the
        // actualBuffers. If the occurrences are not equal, then not all tuples are in both buffers (expected and actual).
        for (auto& expectedBuffer : expectedBuffers) {
            for (auto&& expectedTuple : expectedBuffer) {
                uint64_t occurrencesExpected = std::accumulate(expectedBuffers.begin(),
                                                               expectedBuffers.end(),
                                                               0_u64,
                                                               [&](auto sum, const auto& innerActualBuffer) {
                                                                   return sum + innerActualBuffer.countOccurrences(expectedTuple);
                                                               });
                if (occurrencesExpected == 0) {
                    NES_ERROR("Something is wrong as the expected tuple should be occurring at least once!");
                    return false;
                }

                uint64_t occurrencesActual = std::accumulate(actualBuffers.begin(),
                                                             actualBuffers.end(),
                                                             0_u64,
                                                             [&](auto sum, const auto& innerActualBuffer) {
                                                                 return sum + innerActualBuffer.countOccurrences(expectedTuple);
                                                             });

                if (occurrencesExpected != occurrencesActual) {
                    NES_ERROR("Could not find same number of occurrences expected: {} actual: {}",
                              occurrencesExpected,
                              occurrencesActual);
                    return false;
                }
            }
        }
    }

    return true;
}

/**
     * @brief This method is used for checking if the submitted query is running
     * @param queryId: Id of the query
     * @return true if is running within the timeout, else false
     */
[[nodiscard]] bool checkRunningOrTimeout(QueryId queryId, const std::string& restPort) {
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
        NES_DEBUG("checkCompleteOrTimeout: sleep because current status ={}", currentStatus);
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
[[nodiscard]] bool stopQueryViaRest(QueryId queryId, const std::string& restPort) {
    nlohmann::json json_return;

    std::string url = BASE_URL + restPort + "/v1/nes/query/stop-query";
    nlohmann::json jsonReturn;
    auto future = cpr::DeleteAsync(cpr::Url{url}, cpr::Parameters{{"queryId", std::to_string(queryId)}});
    future.wait();
    auto response = future.get();
    nlohmann::json result = nlohmann::json::parse(response.text);
    NES_DEBUG("stopQueryViaRest: status ={}", result.dump());

    return result["success"].get<bool>();
}

/**
     * @brief This method is used for executing a query
     * @param query string
     * @return if stopped
     */
[[nodiscard]] nlohmann::json startQueryViaRest(const string& queryString, const std::string& restPort) {
    nlohmann::json json_return;

    std::string url = BASE_URL + restPort + "/v1/nes/query/execute-query";
    nlohmann::json jsonReturn;
    auto future = cpr::PostAsync(cpr::Url{url}, cpr::Header{{"Content-Type", "application/json"}}, cpr::Body{queryString});
    future.wait();
    auto response = future.get();
    nlohmann::json result = nlohmann::json::parse(response.text);
    NES_DEBUG("startQueryViaRest: status ={}", result.dump());

    return result;
}

/**
     * @brief This method is used for making a monitoring rest call.
     * @param1 the rest call
     * @param2 the rest port
     * @return the json
     */
[[nodiscard]] nlohmann::json makeMonitoringRestCall(const string& restCall, const std::string& restPort) {
    std::string url = BASE_URL + restPort + "/v1/nes/monitoring/" + restCall;
    auto future = cpr::GetAsync(cpr::Url{url});
    future.wait();
    auto response = future.get();
    nlohmann::json result = nlohmann::json::parse(response.text);
    NES_DEBUG("getAllMonitoringMetricsViaRest: status ={}", result.dump());

    return result;
}

/**
   * @brief This method is used adding a logical source
   * @param query string
   * @return
   */
[[nodiscard]] bool addLogicalSource(const string& schemaString, const std::string& restPort) {
    nlohmann::json json_returnSchema;

    std::string url = BASE_URL + restPort + "/v1/nes/sourceCatalog/addLogicalSource";
    auto future = cpr::PostAsync(cpr::Url{url}, cpr::Header{{"Content-Type", "application/json"}}, cpr::Body{schemaString});
    future.wait();
    cpr::Response response = future.get();
    nlohmann::json jsonResponse = nlohmann::json::parse(response.text);
    NES_DEBUG("addLogicalSource: status ={}", jsonResponse.dump());
    return jsonResponse["success"].get<bool>();
}

bool waitForWorkers(uint64_t restPort, uint16_t maxTimeout, uint16_t expectedWorkers) {
    auto baseUri = "http://localhost:" + std::to_string(restPort) + "/v1/nes/topology";
    NES_INFO("TestUtil: Executing GET request on URI {}", baseUri);
    nlohmann::json json_return;
    size_t nodeNo = 0;

    for (int i = 0; i < maxTimeout; i++) {
        try {
            auto future = cpr::GetAsync(cpr::Url{baseUri}, cpr::Timeout(3000));
            future.wait();
            cpr::Response response = future.get();
            if (!nlohmann::json::accept(response.text)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(sleepDuration));
                continue;
            }
            nlohmann::json jsonResponse = nlohmann::json::parse(response.text);
            nodeNo = jsonResponse["nodes"].size();

            if (nodeNo == expectedWorkers + 1U) {
                NES_INFO("TestUtils: Expected worker number reached correctly {}", expectedWorkers);
                NES_DEBUG("TestUtils: Received topology JSON:\n{}", jsonResponse.dump());
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepDuration));
        } catch (const std::exception& e) {
            NES_ERROR("TestUtils: WaitForWorkers error occured {}", e.what());
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepDuration));
        }
    }

    NES_ASSERT2_FMT(nodeNo == expectedWorkers,
                    "Expected worker number not reached correctly " << nodeNo << " but expected " << expectedWorkers);
    return false;
}

/**
     * @brief This method is used for making a REST call to coordinator to get the topology as Json
     * @param1 the rest port
     * @return the json
     */
[[nodiscard]] nlohmann::json getTopology(uint64_t restPort) {
    auto baseUri = "http://localhost:" + std::to_string(restPort) + "/v1/nes/topology";
    NES_INFO("TestUtil: Executing GET request on URI {}", baseUri);

    auto future = cpr::GetAsync(cpr::Url{baseUri}, cpr::Timeout{3000});
    future.wait();
    auto response = future.get();
    nlohmann::json result = nlohmann::json::parse(response.text);
    return result;
}
};// namespace TestUtils

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

    NES_DEBUG("Creating list of waypoints with startTime {}", startTime)

    //read locations and time offsets from csv, calculate absolute timestamps from offsets by adding start time
    while (std::getline(inputStream, csvLine)) {
        std::stringstream stringStream(csvLine);
        getline(stringStream, latitudeString, ',');
        getline(stringStream, longitudeString, ',');
        getline(stringStream, timeString, ',');
        Timestamp time = std::stoul(timeString);
        NES_TRACE("Read from csv: {}, {}, {}", latitudeString, longitudeString, time);

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
void writeWaypointsToCsv(const std::string& csvPath,
                         const std::vector<NES::Spatial::DataTypes::Experimental::Waypoint>& waypoints) {
    remove(csvPath.c_str());
    std::ofstream outFile(csvPath);
    for (auto& point : waypoints) {
        ASSERT_TRUE(point.getTimestamp().has_value());
        outFile << point.getLocation().toString() << "," << std::to_string(point.getTimestamp().value()) << std::endl;
    }
    outFile.close();
    ASSERT_FALSE(outFile.fail());
}

std::vector<Runtime::TupleBuffer> TestUtils::createExpectedBuffersFromCsv(const string& csvFileName,
                                                                          const SchemaPtr& schema,
                                                                          const Runtime::BufferManagerPtr& bufferManager,
                                                                          uint64_t numTuplesPerBuffer) {
    return createExpectedBuffersFromCsv(csvFileName, schema, bufferManager, false, numTuplesPerBuffer);
}

std::vector<Runtime::TupleBuffer> TestUtils::createExpectedBuffersFromCsv(const std::string& csvFileName,
                                                                          const SchemaPtr& schema,
                                                                          const Runtime::BufferManagerPtr& bufferManager,
                                                                          bool skipHeader,
                                                                          uint64_t numTuplesPerBuffer,
                                                                          const std::string& delimiter) {
    std::vector<Runtime::TupleBuffer> allBuffers;

    auto fullPath = std::filesystem::path(TEST_DATA_DIRECTORY) / csvFileName;
    NES_DEBUG("read file={}", fullPath.string());
    NES_ASSERT2_FMT(std::filesystem::exists(std::filesystem::path(fullPath)), "File " << fullPath << " does not exist!!!");
    std::ifstream inputFile(fullPath);
    return createExpectedBufferFromStream(inputFile, schema, bufferManager, skipHeader, numTuplesPerBuffer, delimiter);
}

uint64_t TestUtils::countTuples(std::vector<Runtime::TupleBuffer>& buffers) {
    return std::accumulate(buffers.begin(), buffers.end(), 0_u64, [](const uint64_t sum, const auto& buf) {
        return sum + buf.getNumberOfTuples();
    });
}

uint64_t TestUtils::countTuples(std::vector<Runtime::MemoryLayouts::TestTupleBuffer>& buffers) {
    return std::accumulate(buffers.begin(), buffers.end(), 0_u64, [](const uint64_t sum, const auto& buf) {
        return sum + buf.getNumberOfTuples();
    });
}

std::vector<Runtime::TupleBuffer> TestUtils::createExpectedBufferFromCSVString(std::string str,
                                                                               const SchemaPtr& schema,
                                                                               const Runtime::BufferManagerPtr& bufferManager,
                                                                               bool skipHeader,
                                                                               uint64_t numTuplesPerBuffer,
                                                                               const string& delimiter) {
    std::istringstream strStream(str);
    return createExpectedBufferFromStream(strStream, schema, bufferManager, skipHeader, numTuplesPerBuffer, delimiter);
}

std::vector<Runtime::TupleBuffer> TestUtils::createExpectedBufferFromStream(std::istream& istream,
                                                                            const SchemaPtr& schema,
                                                                            const Runtime::BufferManagerPtr& bufferManager,
                                                                            bool skipHeader,
                                                                            uint64_t numTuplesPerBuffer,
                                                                            const std::string& delimiter) {

    std::vector<Runtime::TupleBuffer> allBuffers;
    auto tupleCount = 0_u64;
    auto parser = std::make_shared<CSVParser>(schema->fields.size(), getPhysicalTypes(schema), delimiter);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto maxTuplePerBuffer = bufferManager->getBufferSize() / schema->getSchemaSizeInBytes();
    numTuplesPerBuffer = (numTuplesPerBuffer == 0) ? maxTuplePerBuffer : numTuplesPerBuffer;

    // Setting the position indicator to the beginning and clearing potential any error flags
    istream.clear();
    istream.seekg(0, std::ios::beg);

    std::string line;
    if (skipHeader) {
        std::getline(istream, line);
        NES_DEBUG("Skipping first line!");
    }
    while (std::getline(istream, line)) {
        auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(tupleBuffer, schema);
        parser->writeInputTupleToTupleBuffer(line, tupleCount, testBuffer, schema, bufferManager);
        tupleCount++;

        if (tupleCount >= numTuplesPerBuffer) {
            tupleBuffer.setNumberOfTuples(tupleCount);
            allBuffers.emplace_back(tupleBuffer);
            tupleCount = 0;

            tupleBuffer = bufferManager->getBufferBlocking();
        }
    }

    if (tupleCount > 0) {
        tupleBuffer.setNumberOfTuples(tupleCount);
        allBuffers.emplace_back(tupleBuffer);
    }

    return allBuffers;
}

CSVSourceTypePtr TestUtils::createSourceTypeCSV(const SourceTypeConfigCSV& sourceTypeConfigCSV) {
    CSVSourceTypePtr sourceType =
        CSVSourceType::create(sourceTypeConfigCSV.logicalSourceName, sourceTypeConfigCSV.physicalSourceName);
    sourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / sourceTypeConfigCSV.fileName);
    sourceType->setGatheringInterval(sourceTypeConfigCSV.gatheringInterval);
    sourceType->setNumberOfTuplesToProducePerBuffer(sourceTypeConfigCSV.numberOfTuplesToProduce);
    sourceType->setNumberOfBuffersToProduce(sourceTypeConfigCSV.numberOfBuffersToProduce);
    sourceType->setSkipHeader(sourceTypeConfigCSV.isSkipHeader);
    return sourceType;
}

std::vector<PhysicalTypePtr> TestUtils::getPhysicalTypes(const SchemaPtr& schema) {
    std::vector<PhysicalTypePtr> retVector;
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
    for (const auto& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        retVector.push_back(physicalField);
    }

    return retVector;
}

uint64_t countOccurrences(const std::string& subString, const std::string& mainString) {
    int count = 0;
    size_t pos = mainString.find(subString, 0);

    while (pos != std::string::npos) {
        count++;
        pos = mainString.find(subString, pos + 1);
    }

    return count;
}

}// namespace NES
