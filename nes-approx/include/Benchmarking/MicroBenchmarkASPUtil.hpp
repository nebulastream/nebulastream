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

#ifndef NES_MICROBENCHMARKASPUTIL_HPP
#define NES_MICROBENCHMARKASPUTIL_HPP

#include <Synopses/AbstractSynopsis.hpp>
#include <Benchmarking/MicroBenchmarkResult.hpp>
#include <Benchmarking/Parsing/YamlAggregation.hpp>
#include <Benchmarking/Parsing/SynopsisArguments.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Util/yaml/Yaml.hpp>
#include <string>

namespace NES::ASP::Util {

/**
 * @brief Truncates the file and then writes the header string as is to the file
 * @param csvFileName
 * @param header
 */
void writeHeaderToCsvFile(const std::string& csvFileName, const std::string& header);

/**
 * @brief Appends the row as is to the csv file
 * @param csvFileName
 * @param row
 */
void writeRowToCsvFile(const std::string& csvFileName, const std::string& row);

/**
 * @brief Parses the csv file from the yamlFile
 * @param yamlFileName
 * @return Csv file name
 */
std::string parseCsvFileFromYaml(const std::string& yamlFileName);

/**
 * @brief Parses the yaml node and creates synopsis arguments to later on create synopsis from them
 * @param synopsesNode
 * @return Vector of synopsis arguments
 */
std::vector<SynopsisArguments> parseSynopsisArguments(const Yaml::Node& synopsesNode);

/**
 * @brief Parses the yaml node and creates YamlAggregation (wrapper for the aggregation, e.g., input and accuracy file)
 * @param aggregationsNode
 * @param data folder to the data files
 * @return Vector of YamlAggregation objects
 */
std::vector<Benchmarking::YamlAggregation> parseAggregations(const Yaml::Node& aggregationsNode,
                                                             const std::filesystem::path& data);

/**
 * @brief Parses the window size
 * @param windowSizesNode
 * @return Vector of different window sizes
 */
std::vector<size_t> parseWindowSizes(const Yaml::Node& windowSizesNode);

/**
 * @brief Parses the number of Buffers for the buffer manager
 * @param numberOfBuffersNode
 * @param defaultValue
 * @return Either the parsed value or the default value
 */
std::vector<uint32_t> parseNumberOfBuffers(const Yaml::Node& numberOfBuffersNode, const uint32_t defaultValue = 1024);

/**
 * @brief Parses the buffer size for the buffer manager
 * @param bufferSizesNode
 * @return Vector of different buffer sizes
 */
std::vector<uint32_t> parseBufferSizes(const Yaml::Node& bufferSizesNode);

/**
 * @brief Parses the number of repetitions for each micro-benchmark run
 * @param repsNode
 * @return Reps
 */
uint64_t parseReps(const Yaml::Node& repsNode);

/**
 * @brief Helper function for creating a memory provider from the buffer size and the schema
 * @param bufferSize
 * @param schema
 * @return MemoryProvider
 */
Runtime::Execution::MemoryProvider::MemoryProviderPtr createMemoryProvider(const uint64_t bufferSize, const SchemaPtr schema);

/**
 * @brief Creates a DynamicTupleBuffer from the TupleBuffer and the schema
 * @param buffer
 * @param schema
 * @return DynamicTupleBuffer
 */
Runtime::MemoryLayouts::DynamicTupleBuffer createDynamicTupleBuffer(Runtime::TupleBuffer buffer, const SchemaPtr& schema);

/**
 * @brief Returns the physical types of all fields of the schema
 * @param schema
 * @return PhysicalTypes of the schema's field
 */
std::vector<PhysicalTypePtr> getPhysicalTypes(SchemaPtr schema);

/**
 * @brief Creates multiple TupleBuffers from the csv file until the lastTimeStamp has been read
 * @param csvFile
 * @param schema
 * @param timeStampFieldName
 * @param lastTimeStamp
 * @param bufferManager
 * @return Vector of TupleBuffers
 */
[[maybe_unused]] std::vector<Runtime::TupleBuffer> createBuffersFromCSVFile(const std::string& csvFile, const SchemaPtr& schema,
                                                                            Runtime::BufferManagerPtr bufferManager,
                                                                            const std::string& timeStampFieldName,
                                                                            uint64_t lastTimeStamp);


} // namespace NES::ASP

#endif//NES_MICROBENCHMARKASPUTIL_HPP
