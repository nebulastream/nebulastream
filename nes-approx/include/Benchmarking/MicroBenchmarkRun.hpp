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

#ifndef NES_MICROBENCHMARKRUN_HPP
#define NES_MICROBENCHMARKRUN_HPP

#include <API/Schema.hpp>
#include <Benchmarking/MicroBenchmarkResult.hpp>
#include <Benchmarking/Parsing/MicroBenchmarkSchemas.hpp>
#include <Benchmarking/Parsing/YamlAggregation.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Synopses/AbstractSynopsis.hpp>

namespace NES::ASP::Benchmarking {

static constexpr auto NANO_TO_SECONDS_MULTIPLIER = 1 * 1000 * 1000 * 1000UL;

/**
 * @brief This class encapsulates a single micro-benchmark run
 */
class MicroBenchmarkRun {

  private:
    /**
     * @brief Constructor for a MicroBenchmarkRun
     * @param synopsesArguments
     * @param yamlAggregation
     * @param bufferSize
     * @param numberOfBuffers
     * @param windowSize
     * @param reps
     */
    MicroBenchmarkRun(const SynopsisArguments& synopsesArguments,
                      const YamlAggregation& yamlAggregation,
                      const uint32_t bufferSize,
                      const uint32_t numberOfBuffers,
                      const size_t windowSize,
                      const size_t reps);

  public:
    MicroBenchmarkRun() = default;

    MicroBenchmarkRun(const MicroBenchmarkRun& other);
    MicroBenchmarkRun& operator=(const MicroBenchmarkRun& other);

    /**
     * @brief Parses all micro-benchmarks from the yaml file
     * @param yamlConfigFile
     * @param absoluteDataPath
     * @return Vector of all micro-benchmarks
     */
    static std::vector<MicroBenchmarkRun> parseMicroBenchmarksFromYamlFile(const std::string& yamlConfigFile,
                                                                           const std::filesystem::path& absoluteDataPath);


    /**
     * @brief Runs this Microbenchmark and then writes the result to microBenchmarkResult
     */
    void run();

    /**
     * @brief Creates a string representation of this MicroBenchmarkRun
     * @return String representation
     */
    std::string toString();

    /**
     * @brief Creates a header for the output csv file from this MicroBenchmarkRun
     * @return Header as a string with comma separated values
     */
    std::string getHeaderAsCsv();

    /**
     * @brief Creates one or more rows for the output csv file from this MicroBenchmarkRun
     * @return Rows as a string with comma separated values
     */
    std::string getRowsAsCsv();

  private:
    /**
     * @brief Creates a vector of type TupleBuffer from the inputFile
     * @return Vector of TupleBuffer
     */
    std::vector<Runtime::TupleBuffer> createInputRecords(Runtime::BufferManagerPtr bufferManager);

    /**
     * @brief Creates a vector of type TupleBuffer by executing the exact query
     * @param inputBuffers
     * @param bufferManager
     * @return Vector of records
     */
    std::vector<Runtime::TupleBuffer> createAccuracyRecords(std::vector<Runtime::TupleBuffer>& inputBuffers,
                                                                        Runtime::BufferManagerPtr bufferManager);

    /**
     * @brief Compares the approximated with the exact query output and returns an accuracy.
     * @param allAccuracyRecords
     * @param allApproximateBuffers
     * @return Accuracy of approximate
     */
    double compareAccuracy(std::vector<Runtime::TupleBuffer>& allAccuracyRecords,
                           std::vector<Runtime::TupleBuffer>& allApproximateBuffers);

  private:
    SynopsisArguments synopsesArguments;
    YamlAggregation aggregation;
    uint32_t bufferSize;
    uint32_t numberOfBuffers;
    size_t windowSize;
    uint64_t reps;
    std::vector<MicroBenchmarkResult> microBenchmarkResult;
};

}// namespace NES::ASP::Benchmarking

#endif//NES_MICROBENCHMARKRUN_HPP
