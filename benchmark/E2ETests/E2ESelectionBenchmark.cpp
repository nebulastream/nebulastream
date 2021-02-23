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
#include <filesystem>
#include <util/BenchmarkUtils.hpp>
#include <util/E2EBase.hpp>

using namespace Benchmarking;

/**
 * @brief This benchmarks runs a selection query on one worker and one coordinator
 * @return
 */
int main() {
    // Number of workerThreads in nodeEngine
    std::vector<uint16_t> allWorkerThreads;
    BenchmarkUtils::createRangeVector<uint16_t>(allWorkerThreads, 1, 2, 1);

    // Number of workerThreads in nodeEngine
    std::vector<uint16_t> allCoordinatorThreads;
    BenchmarkUtils::createRangeVector<uint16_t>(allCoordinatorThreads, 1, 2, 1);

    // Number of dataSources
    std::vector<uint16_t> allDataSources;
    BenchmarkUtils::createRangeVector<uint16_t>(allDataSources, 1, 2, 1);

    // source mode
//    std::vector<E2EBase::InputOutputMode> allSourceModes{E2EBase::InputOutputMode::FileMode, E2EBase::InputOutputMode::CacheMode,
//                                                         E2EBase::InputOutputMode::MemMode};
        std::vector<E2EBase::InputOutputMode> allSourceModes {E2EBase::InputOutputMode::MemMode};
    //    std::vector<E2EBase::InputOutputMode> allSourceModes {E2EBase::InputOutputMode::CacheMode};
    //    std::vector<E2EBase::InputOutputMode> allSourceModes {E2EBase::InputOutputMode::FileMode};

    //roughly 50% selectivity
    string query = "Query::from(\"input\").filter(Attribute(\"value\") > 5).sink(NullOutputSinkDescriptor::create());";

    std::string benchmarkName = "E2ESelectionBenchmark";
    std::string nesVersion = NES_VERSION;

    std::stringstream ss;
    ss << "Time,BM_Name,NES_Version,WorkerThreads,CoordinatorThreadCnt,SourceCnt,Mode,ProcessedBuffers,ProcessedTasks,"
          "ProcessedTuples,"
          "ProcessedBytes"
       << std::endl;

    for (auto workerThreadCnt : allWorkerThreads) {
        std::cout << "workerThreadCnt=" << workerThreadCnt << std::endl;
        for (auto coordinatorThreadCnt : allCoordinatorThreads) {
            std::cout << "coordinatorThreadCnt=" << coordinatorThreadCnt << std::endl;
            for (auto dataSourceCnt : allDataSources) {
                for (auto sourceMode : allSourceModes) {
                    std::cout << "dataSourceCnt=" << dataSourceCnt << std::endl;
                    ss << time(0) << "," << benchmarkName << "," << nesVersion << "," << workerThreadCnt << ","
                       << coordinatorThreadCnt << "," << dataSourceCnt << "," << E2EBase::getInputOutputModeAsString(sourceMode);
                    auto test = std::make_shared<E2EBase>(workerThreadCnt, coordinatorThreadCnt, dataSourceCnt, sourceMode);
                    ss << test->runExperiment(query);
                }
            }
        }
    }

    std::cout << "result=" << std::endl;
    std::cout << ss.str() << std::endl;

    std::ofstream out("E2ESelectionBenchmark.csv");
    out << ss.str();
    out.close();
    std::cout << "benchmark finish" << std::endl;

    return 0;
}