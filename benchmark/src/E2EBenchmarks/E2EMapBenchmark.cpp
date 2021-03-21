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

const NES::DebugLevel LOG_LEVEL = NES::LOG_NONE;

/**
 * @brief This benchmarks runs a selection query on one worker and one coordinator
 * @return
 */
int main() {
    NES::setupLogging("E2EMapBenchmark.log", LOG_LEVEL);
    std::cout << "Setup E2EMapBenchmark test class." << std::endl;

    // Number of workerThreads in nodeEngine
    std::vector<uint16_t> allWorkerThreads;
    BenchmarkUtils::createRangeVector<uint16_t>(allWorkerThreads, 1, 2, 1);

    // Number of workerThreads in nodeEngine
    std::vector<uint16_t> allCoordinatorThreads;
    BenchmarkUtils::createRangeVector<uint16_t>(allCoordinatorThreads, 1, 2, 1);

    // Number of dataSources
    std::vector<uint16_t> allDataSources;
    BenchmarkUtils::createRangeVector<uint16_t>(allDataSources, 1, 2, 1);

    // source modes
    std::vector<E2EBase::InputOutputMode> allSourceModes{E2EBase::InputOutputMode::MemMode};

    //add one field
    string query =
        "Query::from(\"input\").map(Attribute(\"value2\") = Attribute(\"value\") + 5).sink(NullOutputSinkDescriptor::create());";

    std::string benchmarkName = "E2EMapBenchmark";
    std::string nesVersion = NES_VERSION;

    std::stringstream ss;
    ss << "Time,BM_Name,NES_Version,WorkerThreads,CoordinatorThreadCnt,SourceCnt,Mode,ProcessedBuffersTotal,ProcessedTasksTotal,"
          "ProcessedTuplesTotal,ProcessedBytesTotal,ThroughputInTupsPerSec,ThroughputInMBPerSec"
       << std::endl;

    for (auto workerThreadCnt : allWorkerThreads) {
        std::cout << "workerThreadCnt=" << workerThreadCnt << std::endl;
        for (auto coordinatorThreadCnt : allCoordinatorThreads) {
            std::cout << "coordinatorThreadCnt=" << coordinatorThreadCnt << std::endl;
            for (auto dataSourceCnt : allDataSources) {
                std::cout << "dataSourceCnt=" << dataSourceCnt << std::endl;
                for (auto sourceMode : allSourceModes) {
                    std::cout << "sourceMode=" << E2EBase::getInputOutputModeAsString(sourceMode) << std::endl;
                    auto test = std::make_shared<E2EBase>(workerThreadCnt, coordinatorThreadCnt, dataSourceCnt, sourceMode, query);
                    ss << test->getTsInRfc3339() << "," << benchmarkName << "," << nesVersion << "," << workerThreadCnt << ","
                       << coordinatorThreadCnt << "," << dataSourceCnt << "," << E2EBase::getInputOutputModeAsString(sourceMode);
                    ss << test->runExperiment();
                }
            }
        }
    }

    std::cout << "result=" << std::endl;
    std::cout << ss.str() << std::endl;

    std::ofstream out("E2EMapBenchmark.csv");
    out << ss.str();
    out.close();
    std::cout << "benchmark finish" << std::endl;

    return 0;
}