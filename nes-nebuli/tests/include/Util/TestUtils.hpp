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

#ifndef NES_NEBULI_TESTS_INCLUDE_UTIL_TESTUTILS_HPP_
#define NES_NEBULI_TESTS_INCLUDE_UTIL_TESTUTILS_HPP_

#include <chrono>
#include <fstream>
#include <iostream>
#include <memory>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/Waypoint.hpp>
#include <Util/StdInt.hpp>
#include <Util/Subprocess/Subprocess.hpp>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

using Clock = std::chrono::high_resolution_clock;
using std::cout;
using std::endl;
using std::string;
using namespace std::string_literals;

namespace NES
{
static const char* BASE_URL = "http://127.0.0.1:";

namespace Runtime
{
class NodeEngine;
using NodeEnginePtr = std::shared_ptr<NodeEngine>;
} // namespace Runtime

/**
 * @brief This define states all join strategies that will be tested in all join-specific tests
 */
#define ALL_JOIN_STRATEGIES \
    ::testing::Values( \
        QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN, \
        QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCKING, \
        QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCK_FREE, \
        QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL, \
        QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED)

/**
 * @brief This define states all window strategies that will be tested in all join-specific tests. Note that BUCKETING
 * is not supported for HASH_JOIN_VAR_SIZED
 */
#define ALL_WINDOW_STRATEGIES \
    ::testing::Values(QueryCompilation::WindowingStrategy::SLICING, QueryCompilation::WindowingStrategy::BUCKETING)

/**
 * @brief This combines all join strategies and window strategies that will be tested in all join-specific test cases
 */
#define JOIN_STRATEGIES_WINDOW_STRATEGIES ::testing::Combine(ALL_JOIN_STRATEGIES, ALL_WINDOW_STRATEGIES)

/**
 * @brief this is a util class for the tests
 */
namespace TestUtils
{

/**
 * @brief Struct for storing all csv file params for tests. It is solely a container for grouping csv files.
 */
struct CsvFileParams
{
    CsvFileParams(const string& csvFileLeft, const string& csvFileRight, const string& expectedFile)
        : inputCsvFiles({csvFileLeft, csvFileRight}), expectedFile(expectedFile)
    {
    }

    explicit CsvFileParams(const std::vector<std::string>& inputCsvFiles, const string& expectedFile = "")
        : inputCsvFiles(inputCsvFiles), expectedFile(expectedFile)
    {
    }

    const std::vector<std::string> inputCsvFiles;
    const std::string expectedFile;
};

struct SourceTypeConfigCSV
{
    const std::string logicalSourceName;
    const std::string physicalSourceName;
    const std::string fileName;
    uint64_t gatheringInterval = 0;
    uint64_t numberOfTuplesToProduce = 0;
    uint64_t numberOfBuffersToProduce = 0;
    bool isSkipHeader = false;
};

static constexpr auto defaultTimeout = std::chrono::seconds(60);
static constexpr auto defaultStartQueryTimeout = std::chrono::seconds(180); // starting a query requires time
static constexpr auto sleepDuration = std::chrono::milliseconds(250);
static constexpr auto defaultCooldown = std::chrono::seconds(3); // 3s after last processed task, the query should be done.

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
template <typename T>
[[nodiscard]] std::string configOption(const std::string& name, T value, bool prefix = false)
{
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
 * @brief Check if the query result was produced
 * @param expectedContent
 * @param outputFilePath
 * @return true if successful
 */
[[nodiscard]] bool checkOutputOrTimeout(string expectedContent, const string& outputFilePath, uint64_t customTimeoutInSeconds = 0);

/**
 * @brief Check if any query result was produced
 * @param outputFilePath
 * @return true if successful
 */
[[nodiscard]] bool checkIfOutputFileIsNotEmtpy(uint64_t minNumberOfLines, const string& outputFilePath, uint64_t customTimeout = 0);

/**
 * @brief Check if a outputfile is created
 * @param expectedContent
 * @param outputFilePath
 * @return true if successful
 */
[[nodiscard]] bool checkFileCreationOrTimeout(const string& outputFilePath);

/**
 * @brief Creates a csv source that produces as many buffers as the csv file contains
 * @param SourceTypeConfig: container for configuration parameters of a source type.
 * @return CSVSourceTypePtr
 */
CSVSourceTypePtr createSourceTypeCSV(const SourceTypeConfigCSV& sourceTypeConfigCSV);

std::vector<PhysicalTypePtr> getPhysicalTypes(const SchemaPtr& schema);
}; // namespace TestUtils

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
void writeWaypointsToCsv(const std::string& csvPath, const std::vector<NES::Spatial::DataTypes::Experimental::Waypoint>& waypoints);

/**
 * This function counts the number of times the search string appears within the
 * target string. It uses the std::string::find() method to locate occurrences.
 *
 * @param searchString The string to search for.
 * @param targetString The string in which to search for occurrences.
 * @return The number of occurrences of the search string within the target string.
 */
uint64_t countOccurrences(const std::string& searchString, const std::string& targetString);

} // namespace NES
#endif /// NES_NEBULI_TESTS_INCLUDE_UTIL_TESTUTILS_HPP_
