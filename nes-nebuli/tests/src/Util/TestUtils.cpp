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

#include <algorithm>
#include <chrono>
#include <iostream>
#include <memory>
#include <API/AttributeField.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Util/Common.hpp>
#include <Util/Mobility/Waypoint.hpp>
#include <Util/StdInt.hpp>
#include <Util/Subprocess/Subprocess.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

#include <filesystem>

namespace NES
{
namespace Runtime
{
class TupleBuffer;
}
using namespace Configurations;
/**
 * @brief this is a util class for the tests
 */
namespace TestUtils
{

[[nodiscard]] const std::string configOption(const std::string& name, const std::string& value, bool prefix)
{
    const std::string result = prefix ? "--worker." : "--";
    return result + name + "=" + value;
}

[[nodiscard]] std::string bufferSizeInBytes(uint64_t size, bool prefix)
{
    return configOption(BUFFERS_SIZE_IN_BYTES_CONFIG, size, prefix);
}

[[nodiscard]] std::string configPath(const std::string& filename)
{
    return "--" + CONFIG_PATH + "=" + filename;
}

[[nodiscard]] std::string workerConfigPath(const std::string& filename)
{
    return "--" + WORKER_CONFIG_PATH + "=" + filename;
}

[[nodiscard]] std::string coordinatorPort(uint64_t coordinatorPort)
{
    return "--" + COORDINATOR_PORT_CONFIG + "=" + std::to_string(coordinatorPort);
}

[[nodiscard]] std::string parentId(uint64_t parentId)
{
    return "--" + PARENT_ID_CONFIG + "=" + std::to_string(parentId);
}

[[nodiscard]] std::string numberOfSlots(uint64_t coordinatorPort, bool prefix)
{
    return configOption(NUMBER_OF_SLOTS_CONFIG, coordinatorPort, prefix);
}

[[nodiscard]] std::string numLocalBuffers(uint64_t localBuffers, bool prefix)
{
    return configOption(NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG, localBuffers, prefix);
}

[[nodiscard]] std::string numGlobalBuffers(uint64_t globalBuffers, bool prefix)
{
    return configOption(NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG, globalBuffers, prefix);
}

[[nodiscard]] std::string rpcPort(uint64_t rpcPort)
{
    return "--" + RPC_PORT_CONFIG + "=" + std::to_string(rpcPort);
}

[[nodiscard]] std::string sourceType(SourceType sourceType)
{
    return "--physicalSources." + SOURCE_TYPE_CONFIG + "=" + std::string(magic_enum::enum_name(sourceType));
}

[[nodiscard]] std::string csvSourceFilePath(std::string filePath)
{
    return "--physicalSources." + FILE_PATH_CONFIG + "=" + filePath;
}

[[nodiscard]] std::string dataPort(uint64_t dataPort)
{
    return "--" + DATA_PORT_CONFIG + "=" + std::to_string(dataPort);
}

[[nodiscard]] std::string numberOfTuplesToProducePerBuffer(uint64_t numberOfTuplesToProducePerBuffer)
{
    return "--physicalSources." + NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG + "=" + std::to_string(numberOfTuplesToProducePerBuffer);
}

[[nodiscard]] std::string physicalSourceName(std::string physicalSourceName)
{
    return "--physicalSources." + PHYSICAL_SOURCE_NAME_CONFIG + "=" + physicalSourceName;
}

[[nodiscard]] std::string logicalSourceName(std::string logicalSourceName)
{
    return "--physicalSources." + LOGICAL_SOURCE_NAME_CONFIG + "=" + logicalSourceName;
}

[[nodiscard]] std::string numberOfBuffersToProduce(uint64_t numberOfBuffersToProduce)
{
    return "--physicalSources." + NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG + "=" + std::to_string(numberOfBuffersToProduce);
}

[[nodiscard]] std::string sourceGatheringInterval(uint64_t sourceGatheringInterval)
{
    return "--physicalSources." + SOURCE_GATHERING_INTERVAL_CONFIG + "=" + std::to_string(sourceGatheringInterval);
}

[[nodiscard]] std::string restPort(uint64_t restPort)
{
    return "--restPort=" + std::to_string(restPort);
}

[[nodiscard]] std::string enableDebug()
{
    return "--logLevel=LOG_DEBUG";
}

[[nodiscard]] std::string workerHealthCheckWaitTime(uint64_t workerWaitTime)
{
    return "--healthCheckWaitTime=" + std::to_string(workerWaitTime);
}

[[nodiscard]] std::string coordinatorHealthCheckWaitTime(uint64_t coordinatorWaitTime)
{
    return "--healthCheckWaitTime=" + std::to_string(coordinatorWaitTime);
}

[[nodiscard]] std::string enableMonitoring(bool prefix)
{
    std::cout << prefix << " aha " << ENABLE_MONITORING_CONFIG;
    return configOption(ENABLE_MONITORING_CONFIG, true, prefix);
}

[[nodiscard]] std::string monitoringWaitTime(uint64_t monitoringWaitTime)
{
    return "--monitoringWaitTime=" + std::to_string(monitoringWaitTime);
}

[[nodiscard]] std::string enableNemoPlacement()
{
    return "--optimizer.enableNemoPlacement=true";
}

[[nodiscard]] std::string enableNemoJoin()
{
    return "--optimizer.distributedJoinOptimizationMode=NEMO";
}

[[nodiscard]] std::string enableSlicingWindowing(bool prefix)
{
    return configOption(QUERY_COMPILER_CONFIG + "." + QUERY_COMPILER_WINDOWING_STRATEGY_CONFIG, std::string{"SLICING"}, prefix);
}

[[nodiscard]] std::string enableNautilusWorker()
{
    return "--queryCompiler.queryCompilerType=NAUTILUS_QUERY_COMPILER";
}

[[nodiscard]] std::string enableNautilusCoordinator()
{
    return "--worker.queryCompiler.queryCompilerType=NAUTILUS_QUERY_COMPILER";
}

/**
   * @brief start a new instance of a nes coordinator with a set of configuration flags
   * @param flags
   * @return coordinator process, which terminates if it leaves the scope
   */
[[nodiscard]] Util::Subprocess startCoordinator(std::initializer_list<std::string> list)
{
    auto crdPath = std::string(PATH_TO_BINARY_DIR) + "/nes-coordinator/nesCoordinator";
    if (std::filesystem::exists(crdPath))
    {
        NES_INFO("Start coordinator");
    }
    else
    {
        NES_ERROR("TestUtils: Coordinator binary does not exist in {}", crdPath);
    }
    return {crdPath, list};
}

/**
     * @brief start a new instance of a nes worker with a set of configuration flags
     * @param flags
     * @return worker process, which terminates if it leaves the scope
     */
[[nodiscard]] Util::Subprocess startWorker(std::initializer_list<std::string> flags)
{
    auto workerPath = std::string(PATH_TO_BINARY_DIR) + "/nes-worker/nesWorker";
    if (std::filesystem::exists(workerPath))
    {
        NES_INFO("Start worker");
    }
    else
    {
        NES_ERROR("TestUtils: Worker binary does not exist in {}", workerPath);
    }
    return {workerPath, flags};
}

/**
     * @brief start a new instance of a nes worker with a set of configuration flags
     * @param flags
     * @return worker process, which terminates if it leaves the scope
     */
[[nodiscard]] std::shared_ptr<Util::Subprocess> startWorkerPtr(std::initializer_list<std::string> flags)
{
    NES_INFO("Start worker");
    return std::make_shared<Util::Subprocess>(std::string(PATH_TO_BINARY_DIR) + "/nes-worker/nesWorker", flags);
}

/**
   * @brief Check if the query result was produced
   * @param expectedContent
   * @param outputFilePath
   * @param customTimeoutInSeconds
   * @return true if successful
   */
[[nodiscard]] bool checkOutputOrTimeout(string expectedContent, const string& outputFilePath, uint64_t customTimeoutInSeconds)
{
    std::chrono::seconds timeoutInSec;
    if (customTimeoutInSeconds == 0)
    {
        timeoutInSec = std::chrono::seconds(defaultTimeout);
    }
    else
    {
        timeoutInSec = std::chrono::seconds(customTimeoutInSeconds);
    }

    NES_TRACE("using timeout={}", timeoutInSec.count());
    auto start_timestamp = std::chrono::system_clock::now();
    uint64_t found = 0;
    uint64_t count = 0;
    std::string content;
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec)
    {
        std::this_thread::sleep_for(sleepDuration);
        found = 0;
        count = 0;
        NES_TRACE("checkOutputOrTimeout: check content for file {}", outputFilePath);
        std::ifstream ifs(outputFilePath);
        if (ifs.good() && ifs.is_open())
        {
            std::vector<std::string> expectedlines = NES::Util::splitWithStringDelimiter<std::string>(expectedContent, "\n");
            content = std::string((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
            count = std::count(content.begin(), content.end(), '\n');
            if (expectedlines.size() != count)
            {
                NES_TRACE(
                    "checkoutputortimeout: number of expected lines {} not reached yet with {} lines content={} file={}",
                    expectedlines.size(),
                    count,
                    content,
                    outputFilePath);
                continue;
            }

            if (content.size() != expectedContent.size())
            {
                NES_TRACE(
                    "checkoutputortimeout: number of chars {} not reached yet with chars content={} lines content={}",
                    expectedContent.size(),
                    content.size(),
                    content);
                continue;
            }

            for (auto& expectedline : expectedlines)
            {
                if (content.find(expectedline) != std::string::npos)
                {
                    found++;
                }
            }
            if (found == count)
            {
                NES_TRACE("all lines found final content={}", content);
                return true;
            }
            NES_TRACE("only {} lines found final content={}", found, content);
            if (found > count)
            {
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
[[nodiscard]] bool checkIfOutputFileIsNotEmtpy(uint64_t minNumberOfLines, const string& outputFilePath, uint64_t customTimeout)
{
    std::chrono::seconds timeoutInSec;
    if (customTimeout == 0)
    {
        timeoutInSec = std::chrono::seconds(defaultTimeout);
    }
    else
    {
        timeoutInSec = std::chrono::seconds(customTimeout);
    }

    NES_TRACE("using timeout={}", timeoutInSec.count());
    auto start_timestamp = std::chrono::system_clock::now();
    uint64_t count = 0;
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec)
    {
        std::this_thread::sleep_for(sleepDuration);
        count = 0;
        NES_TRACE("checkIfOutputFileIsNotEmtpy: check content for file {}", outputFilePath);
        std::ifstream ifs(outputFilePath);
        if (ifs.good() && ifs.is_open())
        {
            std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
            count = std::count(content.begin(), content.end(), '\n');
            if (count < minNumberOfLines)
            {
                NES_TRACE(
                    "checkIfOutputFileIsNotEmtpy: number of min lines {} not reached yet with {} lines content={}",
                    minNumberOfLines,
                    count,
                    content);
                continue;
            }
            NES_TRACE("at least {} are found in content={}", minNumberOfLines, content);
            return true;
        }
    }
    NES_ERROR("checkIfOutputFileIsNotEmtpy: expected ({}) result not reached ({}) within set timeout content", count, minNumberOfLines);
    return false;
}

/**
   * @brief Check if a outputfile is created
   * @param expectedContent
   * @param outputFilePath
   * @return true if successful
   */
[[nodiscard]] bool checkFileCreationOrTimeout(const string& outputFilePath)
{
    auto timeoutInSec = std::chrono::seconds(defaultTimeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec)
    {
        std::this_thread::sleep_for(sleepDuration);
        NES_TRACE("checkFileCreationOrTimeout: for file {}", outputFilePath);
        std::ifstream ifs(outputFilePath);
        if (ifs.good() && ifs.is_open())
        {
            return true;
        }
    }
    NES_TRACE("checkFileCreationOrTimeout: expected result not reached within set timeout");
    return false;
}

}; // namespace TestUtils

/**
 * @brief read mobile device path waypoints from csv
 * @param csvPath path to the csv with lines in the format <latitude, longitude, offsetFromStartTime>
 * @param startTime the real or simulated start time of the LocationProvider
 * @return a vector of waypoints with timestamps calculated by adding startTime to the offset obtained from csv
 */
std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> getWaypointsFromCsv(const std::string& csvPath, Timestamp startTime)
{
    std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> waypoints;
    std::string csvLine;
    std::ifstream inputStream(csvPath);
    std::string latitudeString;
    std::string longitudeString;
    std::string timeString;

    NES_DEBUG("Creating list of waypoints with startTime {}", startTime)

    //read locations and time offsets from csv, calculate absolute timestamps from offsets by adding start time
    while (std::getline(inputStream, csvLine))
    {
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
            NES::Spatial::DataTypes::Experimental::GeoLocation(std::stod(latitudeString), std::stod(longitudeString)), time));
    }
    return waypoints;
}

/**
 * @brief write mobile device path waypoints to a csv file to use as input for the LocationProviderCSV class
 * @param csvPath path to the output file
 * @param waypoints a vector of waypoints to be written to the file
 */
void writeWaypointsToCsv(const std::string& csvPath, const std::vector<NES::Spatial::DataTypes::Experimental::Waypoint>& waypoints)
{
    remove(csvPath.c_str());
    std::ofstream outFile(csvPath);
    for (auto& point : waypoints)
    {
        ASSERT_TRUE(point.getTimestamp().has_value());
        outFile << point.getLocation().toString() << "," << std::to_string(point.getTimestamp().value()) << std::endl;
    }
    outFile.close();
    ASSERT_FALSE(outFile.fail());
}

CSVSourceTypePtr TestUtils::createSourceTypeCSV(const SourceTypeConfigCSV& sourceTypeConfigCSV)
{
    CSVSourceTypePtr sourceType = CSVSourceType::create(sourceTypeConfigCSV.logicalSourceName, sourceTypeConfigCSV.physicalSourceName);
    sourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / sourceTypeConfigCSV.fileName);
    sourceType->setGatheringInterval(sourceTypeConfigCSV.gatheringInterval);
    sourceType->setNumberOfTuplesToProducePerBuffer(sourceTypeConfigCSV.numberOfTuplesToProduce);
    sourceType->setNumberOfBuffersToProduce(sourceTypeConfigCSV.numberOfBuffersToProduce);
    sourceType->setSkipHeader(sourceTypeConfigCSV.isSkipHeader);
    return sourceType;
}

std::vector<PhysicalTypePtr> TestUtils::getPhysicalTypes(const SchemaPtr& schema)
{
    std::vector<PhysicalTypePtr> retVector;
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
    for (const auto& field : schema->fields)
    {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        retVector.push_back(physicalField);
    }

    return retVector;
}

uint64_t countOccurrences(const std::string& subString, const std::string& mainString)
{
    int count = 0;
    size_t pos = mainString.find(subString, 0);

    while (pos != std::string::npos)
    {
        count++;
        pos = mainString.find(subString, pos + 1);
    }

    return count;
}

} // namespace NES
