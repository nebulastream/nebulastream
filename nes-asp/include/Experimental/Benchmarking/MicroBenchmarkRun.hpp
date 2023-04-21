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
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Experimental/Benchmarking/MicroBenchmarkResult.hpp>
#include <Experimental/Benchmarking/MicroBenchmarkSchemas.hpp>
#include <Experimental/Parsing/SynopsisAggregationConfig.hpp>
#include <Experimental/Synopses/AbstractSynopsis.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

namespace NES::ASP::Benchmarking {

static constexpr auto NANO_TO_SECONDS_MULTIPLIER = 1 * 1000 * 1000 * 1000UL;


class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    MockedPipelineExecutionContext(std::vector<Runtime::Execution::OperatorHandlerPtr> handlers = {})
        : PipelineExecutionContext(
            -1,// mock pipeline id
            1,         // mock query id
            nullptr,
            1, //noWorkerThreads
            {},
            {},
            handlers){};
};

/**
 * @brief MockedPipelineExecutionContext for our executable pipeline
 */
class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    MockedPipelineExecutionContext(std::vector<Runtime::Execution::OperatorHandlerPtr> handlers = {})
        : PipelineExecutionContext(
            -1,// mock pipeline id
            1,         // mock query id
            nullptr,
            1, //noWorkerThreads
            {},
            {},
            handlers){};
};

/**
 * @brief This class encapsulates a single micro-benchmark run, so that means it takes care of creating input buffers, running the
 * operations and then storing the metrics of each repetition.
 */
class MicroBenchmarkRun {
  private:
    /**
     * @brief Constructor for a MicroBenchmarkRun
     * @param synopsesConfig
     * @param yamlAggregation
     * @param bufferSize
     * @param numberOfBuffers
     * @param windowSize
     * @param inputFile
     * @param reps
     */
    MicroBenchmarkRun(Parsing::SynopsisConfigurationPtr synopsesConfig,
                      const Parsing::SynopsisAggregationConfig& yamlAggregation,
                      const uint32_t bufferSize,
                      const uint32_t numberOfBuffers,
                      const size_t windowSize,
                      const std::string& inputFile,
                      const size_t reps);

  public:
    /**
     * @brief Default constructor
     */
    MicroBenchmarkRun() = default;

    /**
     * @brief Copy constructor
     * @param other
     */
    MicroBenchmarkRun(const MicroBenchmarkRun& other);

    /**
     * @brief Assignment operator
     * @param other
     */
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
    const std::string toString() const;

    /**
     * @brief Creates a header for the output csv file from this MicroBenchmarkRun
     * @return Header as a string with comma separated values
     */
    const std::string getHeaderAsCsv() const;

    /**
     * @brief Creates one or more rows for the output csv file from this MicroBenchmarkRun
     * @return Rows as a string with comma separated values
     */
    const std::string getRowsAsCsv() const;

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
    /**
     * @brief Creates an executable pipeline and the pipeline context from the synopsis for this benchmark run
     * @param synopsis
     * @return Executable pipeline and its context
     */
    std::pair<std::shared_ptr<Runtime::Execution::PhysicalOperatorPipeline>, std::shared_ptr<MockedPipelineExecutionContext>>
        createExecutablePipeline(AbstractSynopsesPtr synopsis);

    Parsing::SynopsisConfigurationPtr synopsesArguments;
    Parsing::SynopsisAggregationConfig aggregation;
    uint32_t bufferSize;
    uint32_t numberOfBuffers;
    size_t windowSize;
    std::string inputFile;
    uint64_t reps;
    std::vector<MicroBenchmarkResult> microBenchmarkResult;
};

}// namespace NES::ASP::Benchmarking

#endif//NES_MICROBENCHMARKRUN_HPP
