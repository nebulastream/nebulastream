/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_BENCHMARK_INCLUDE_E2ETESTS_E2EBASE_HPP_
#define NES_BENCHMARK_INCLUDE_E2ETESTS_E2EBASE_HPP_
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <E2EBenchmarkConfig.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <chrono>
#include <iostream>

using namespace NES;
class E2EBase {
  public:
    /**
     * @brief Enums for type
     * FileMode = read from file
     * CacheMode = read always the same buffer
     * MemMode = generate always the same data but in different buffers
     * WindowMode = generate increasing time stamps in different buffers (each buffer will have time(0) time stamps
     * JoinMode = generate two sources with increasing timestamp
     */
    enum class InputType { FileMode, MemoryMode, LambdaMode, WindowMode, YSBMode, JoinMode, Auto, UndefinedInputType };

    /**
     * @brief Enums for modes
     * frequency = produce values given a frequency
     * ingestionrate = produce values following a particular ingestion rate
     */
    enum class InputMode { Frequency,  IngestionRate, UndefinedInputMode };

    /**
     * @brief Method to perform the benchmark
     * @param threadCntWorker threads in the worker engine
     * @param threadCntCoordinator threads in the worker engine of the coordinator
     * @param sourceCnt number of sources
     * @param mode if source is coming from file or memory
     * @param query the query to be run
     * @return csv list of the results
     */
    std::string runExperiment(const QueryPlanPtr& updatedQueryPlan);

    E2EBase(uint64_t threadCntWorker,
            uint64_t sourceCnt,
            uint64_t numberOfBuffersInGlobalBufferManager,
            uint64_t numberOfBuffersPerPipeline,
            uint64_t numberOfBuffersInSourceLocalBufferPool,
            uint64_t bufferSizeInBytes,
            uint64_t gatheringValue,
            E2EBenchmarkConfigPtr config);
    ~E2EBase();
    static std::string getInputTypeAsString(E2EBase::InputType type);
    static std::string getInputModeAsString(E2EBase::InputMode mode);
    static E2EBase::InputType getInputTypeFromString(std::string type);
    static E2EBase::InputMode getInputModeFromString(std::string mode);
    string getTsInRfc3339();

  private:
    /**
     * @brief run the query and start the measurement
     * @param query to be run
     */
    bool runQuery( const QueryPlanPtr& updatedQueryPlan);

    /**
     * @brief setup one coordinator and one worker
     */
    void setup();

    /**
     * @brief this method will setup 1-n sources
     */
    void setupSources();

    /**
     * @brief stop worker and coordinator and release all resources
     */
    void tearDown();

    /**
     * @brief get the result as a csv list with entries: ProcessedBuffers,ProcessedTasks,ProcessedTuples,ProcessedBytes
     * @return csv list
     */
    std::string getResult();

    /**
     * @brief start the measurement of the throughput, will read the stats of the worker ever n second
     */
    std::chrono::nanoseconds recordStatistics();

  private:
    uint64_t numberOfWorkerThreads;
    uint64_t sourceCnt;
    uint64_t numberOfBuffersInGlobalBufferManager;
    uint64_t numberOfBuffersPerPipeline;
    uint64_t numberOfBuffersInSourceLocalBufferPool;
    uint64_t bufferSizeInBytes;
    uint64_t gatheringValue;

    std::chrono::nanoseconds runtime;
    NES::NesCoordinatorPtr crd;
    NES::NesWorkerPtr wrk;

    //maps top sum up the results from the individual pipelines and sources
    std::map<uint64_t, uint64_t> subPlanIdToTaskCnt;
    std::map<uint64_t, uint64_t> subPlanIdToBufferCnt;
    std::map<uint64_t, uint64_t> subPlanIdToTupleCnt;
    std::map<uint64_t, uint64_t> subPlanIdToLatencyCnt;
    std::map<uint64_t, uint64_t> subPlanIdToQueueCnt;
    std::map<uint64_t, uint64_t> subPlanIdToAvailableGlobalBufferCnt;
    std::map<uint64_t, uint64_t> subPlanIdToAvailableFixedBufferCnt;
    std::map<uint64_t, std::map<uint64_t, std::vector<uint64_t>>> subPlanToTsToLatencyMap;
    std::map<uint64_t, uint64_t> secondsToLatencyMap;
    std::map<uint64_t, std::tuple<uint64_t, uint64_t>> hundredMsToLatencyMap;

    NES::QueryServicePtr queryService;
    QueryId queryId;
    NES::QueryCatalogPtr queryCatalog;
    NES::SchemaPtr defaultSchema;
    const QueryPlanPtr queryPlan;
    //uint64_t nodeId;

    std::vector<uint8_t*> memoryAreas;
    E2EBenchmarkConfigPtr config;
};
typedef std::shared_ptr<E2EBase> E2EBasePtr;

#endif//NES_BENCHMARK_INCLUDE_E2ETESTS_E2EBASE_HPP_
