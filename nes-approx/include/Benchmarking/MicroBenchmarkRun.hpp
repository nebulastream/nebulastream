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
#include <Runtime/BufferManager.hpp>
#include <Benchmarking/MicroBenchmarkResult.hpp>
#include <Benchmarking/MicroBenchmarkSchemas.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Synopses/AbstractSynopsis.hpp>

namespace NES::ASP::Benchmarking {

static constexpr auto NANO_TO_SECONDS_MULTIPLIER = 1 * 1000 * 1000 * 1000UL;

/**
 * @brief This class encapsulates a single micro-benchmark run
 */
class MicroBenchmarkRun {

  public:
    /**
     * @brief Constructor for a MicroBenchmarkRun
     * @param synopsesArguments
     * @param schema
     * @param bufferSize
     * @param numberOfBuffers
     * @param windowSize
     * @param reps
     * @param inputFile
     * @param accuracyFile
     */
    MicroBenchmarkRun(const SynopsisArguments& synopsesArguments,
                      const SchemaPtr& schema,
                      const uint32_t bufferSize,
                      const uint32_t numberOfBuffers,
                      const size_t windowSize,
                      const size_t reps,
                      const std::string& inputFile,
                      const std::string& accuracyFile);


    /**
     * @brief Parses all micro-benchmarks from the yaml file
     * @param yamlConfigFile
     * @return Vector of all micro-benchmarks
     */
    static std::vector<MicroBenchmarkRun> parseMicroBenchmarksFromYamlFile(const std::string& yamlConfigFile);

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
     * @brief Creates rows for the output csv file from this MicroBenchmarkRun
     * @return Rows as a string with comma separated values
     */
    std::string getRowsAsCsv();

  private:
    /**
     * @brief Creates a vector of type Nautilus::Record from the inputFile
     * @return Vector of records
     */
    std::vector<Runtime::Execution::RecordBuffer> createInputRecords(Runtime::BufferManagerPtr bufferManager);

    /**
     * @brief Creates a vector of type Runtime::Execution::RecordBuffer from the accuracy file
     * @return Vector of records
     */
    std::vector<Runtime::Execution::RecordBuffer> createAccuracyRecords(Runtime::BufferManagerPtr bufferManager);

    /**
     * @brief Compares the approximated with the exact query output and returns an accuracy
     * @param allAccuracyRecords
     * @param allApproximateRecords
     * @return Accuracy of approximate
     */
    double compareAccuracy(std::vector<Runtime::Execution::RecordBuffer> allAccuracyRecords,
                           std::vector<Runtime::Execution::RecordBuffer> allApproximateRecords);

  private:
    const SynopsisArguments synopsesArguments;
    const SchemaPtr schema;
    const uint32_t bufferSize;
    const uint32_t numberOfBuffers;
    const size_t windowSize;
    const uint64_t reps;
    const std::string inputFile;
    const std::string accuracyFile;
    std::vector<MicroBenchmarkResult> microBenchmarkResult;
};

}// namespace NES::ASP::Benchmarking

#endif//NES_MICROBENCHMARKRUN_HPP
