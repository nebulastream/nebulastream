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
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <benchmark/gbenchmark/src/gbenchmark/include/benchmark/benchmark.h>
#include <chrono>
#include <iostream>

class E2EBase {
  public:
    /**
     * @brief Enums for modes
     * FileMode = read from file
     * CacheMode = read always the same buffer
     * MemMode = generate always the same data but in different buffers
     * WindowMode = generate increasing time stamps in different buffers (each buffer will have time(0) time stamps
     * JoinMode = generate two sources with increasing timestamp
     */
    enum class InputOutputMode { FileMode, CacheMode, MemMode, WindowMode, JoinMode };

    /**
     * @brief Method to perform the benchmark
     * @param threadCntWorker threads in the worker engine
     * @param threadCntCoordinator threads in the worker engine of the coordinator
     * @param sourceCnt number of sources
     * @param mode if source is coming from file or memory
     * @param query the query to be run
     * @return csv list of the results
     */
    std::string runExperiment(std::string query);

    E2EBase(uint64_t threadCntWorker, uint64_t threadCntCoordinator, uint64_t sourceCnt, InputOutputMode mode);
    ~E2EBase();
    static std::string getInputOutputModeAsString(E2EBase::InputOutputMode mode);
    string getTsInRfc3339();

  private:
    /**
     * @brief run the query and start the measurement
     * @param query to be run
     */
    void runQuery(std::string query);

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
     * @param nodeEngine
     */
    void recordStatistics(NES::NodeEngine::NodeEnginePtr nodeEngine);

  private:
    uint64_t numberOfWorkerThreads;
    uint64_t numberOfCoordinatorThreads;
    uint64_t sourceCnt;
    InputOutputMode mode;

    std::chrono::nanoseconds runtime;
    NES::NesCoordinatorPtr crd;
    NES::NesWorkerPtr wrk1;

    std::map<uint64_t, uint64_t> subPlanIdToTaskCnt;
    std::map<uint64_t, uint64_t> subPlanIdToBufferCnt;
    std::map<uint64_t, uint64_t> subPlanIdToTuplelCnt;
    NES::QueryServicePtr queryService;
    QueryId queryId;
    NES::QueryCatalogPtr queryCatalog;
    NES::SchemaPtr schema;

    std::vector<uint8_t*> memoryAreas;
};
typedef std::shared_ptr<E2EBase> E2EBasePtr;

#endif//NES_BENCHMARK_INCLUDE_E2ETESTS_E2EBASE_HPP_
