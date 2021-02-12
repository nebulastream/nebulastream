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
#include <benchmark/benchmark.h>
#include <iostream>

using namespace std;
using namespace NES;

class E2EBase {
  public:
    enum class InputOutputMode { FileMode, MemoryMode };

    static std::string runExperiment(uint64_t threadCntWorker, uint64_t threadCntCoordinator, uint64_t sourceCnt, InputOutputMode mode,
                              std::string query);

    E2EBase(uint64_t threadCntWorker, uint64_t threadCntCoordinator, uint64_t sourceCnt, InputOutputMode mode);
    ~E2EBase();

  private:
    void runQuery(std::string query);
    void setup();
    void setupSources();
    void tearDown();
    std::string getResult();
    void recordStatistics(NodeEngine::NodeEnginePtr nodeEngine);

    uint64_t numberOfWorkerThreads;
    uint64_t numberOfCoordinatorThreads;
    uint64_t sourceCnt;
    InputOutputMode mode;

    NesCoordinatorPtr crd;
    NesWorkerPtr wrk1;

    std::vector<NES::NodeEngine::QueryStatisticsPtr> statisticsVec;
    QueryServicePtr queryService;
    QueryId queryId;
    QueryCatalogPtr queryCatalog;
    SchemaPtr schema;
};
typedef std::shared_ptr<E2EBase> E2EBasePtr;

#endif//NES_BENCHMARK_INCLUDE_E2ETESTS_E2EBASE_HPP_
