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

#ifndef NES_BENCHMARK_INCLUDE_E2ETESTS_E2ERUNNER_HPP_
#define NES_BENCHMARK_INCLUDE_E2ETESTS_E2ERUNNER_HPP_
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Configurations/ConfigOptions/E2EBenchmarkConfig.hpp>
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
#include <util/E2EBase.hpp>
#include <Configurations/ConfigOption.hpp>
#include <Version/version.hpp>

using namespace NES;
using namespace std;
const std::string logo = "/********************************************************\n"
                         " *     _   _   ______    _____\n"
                         " *    | \\ | | |  ____|  / ____|\n"
                         " *    |  \\| | | |__    | (___\n"
                         " *    | . ` | |  __|    \\___ \\     Benchmark Runner\n"
                         " *    | |\\  | | |____   ____) |\n"
                         " *    |_| \\_| |______| |_____/\n"
                         " *\n"
                         " ********************************************************/";
int main(int argc, const char* argv[]) {
    std::cout << logo << std::endl;

    NES::setupLogging("benchmarkRunner.log", NES::getStringAsDebugLevel("LOG_NONE"));
    E2EBenchmarkConfigPtr benchmarkConfig = E2EBenchmarkConfig::create();

    map<string, string> commandLineParams;

    for (int i = 1; i < argc; ++i) {
        commandLineParams.insert(
            pair<string, string>(string(argv[i]).substr(0, string(argv[i]).find("=")),
                                 string(argv[i]).substr(string(argv[i]).find("=") + 1, string(argv[i]).length() - 1)));
    }

    auto configPath = commandLineParams.find("--configPath");

    if (configPath != commandLineParams.end()) {
        benchmarkConfig->overwriteConfigWithYAMLFileInput(configPath->second);
    }
    if (argc >= 1) {
        benchmarkConfig->overwriteConfigWithCommandLineInput(commandLineParams);
    }

    NES::setLogLevel(NES::getStringAsDebugLevel(benchmarkConfig->getLogLevel()->getValue()));

    NES_INFO(
        "start benchmark with numberOfWorkerThreads="
        << benchmarkConfig->getNumberOfWorkerThreads()->getValue()
        << " numberOfCoordinatorThreads=" << benchmarkConfig->getNumberOfCoordinatorThreads()->getValue()
        << " numberOfBuffersToProduce=" << benchmarkConfig->getNumberOfBuffersToProduce()->getValue()
        << " numberOfBuffersInGlobalBufferManager=" << benchmarkConfig->getNumberOfBuffersInGlobalBufferManager()->getValue()
        << " numberOfBuffersInTaskLocalBufferPool=" << benchmarkConfig->getNumberOfBuffersInTaskLocalBufferPool()->getValue()
        << " numberOfBuffersInSourceLocalBufferPool=" << benchmarkConfig->getNumberOfBuffersInSourceLocalBufferPool()->getValue()
        << " bufferSizeInBytes=" << benchmarkConfig->getBufferSizeInBytes()->getValue()
        << " numberOfSources=" << benchmarkConfig->getNumberOfSources()->getValue()
        << " inputOutputMode=" << benchmarkConfig->getInputOutputMode()->getValue()
        << " query=" << benchmarkConfig->getQuery()->getValue() << " logLevel=" << benchmarkConfig->getLogLevel()->getValue());

    std::string benchmarkName = "E2EBenchmarkRunner";
    std::string nesVersion = NES_VERSION;

    std::stringstream ss;
    ss << "Time,BM_Name,NES_Version,WorkerThreads,CoordinatorThreadCnt,SourceCnt,Mode,ProcessedBuffersTotal,ProcessedTasksTotal,"
          "ProcessedTuplesTotal,ProcessedBytesTotal,ThroughputInTupsPerSec,ThroughputInMBPerSec"
       << std::endl;

    auto test = std::make_shared<E2EBase>(benchmarkConfig->getNumberOfWorkerThreads()->getValue(),
                                          benchmarkConfig->getNumberOfCoordinatorThreads()->getValue(),
                                          benchmarkConfig->getNumberOfSources()->getValue(),
                                          E2EBase::getInputOutputModeFromString(benchmarkConfig->getInputOutputMode()->getValue()));
    ss << test->getTsInRfc3339() << "," << benchmarkName << "," << nesVersion << "," << benchmarkConfig->getNumberOfWorkerThreads() << ","
       << benchmarkConfig->getNumberOfCoordinatorThreads() << "," << benchmarkConfig->getNumberOfSources() << ","
       << benchmarkConfig->getInputOutputMode()->getValue();
    ss << test->runExperiment(benchmarkConfig->getQuery()->getValue());

    std::cout << "result=" << std::endl;
    std::cout << ss.str() << std::endl;

    std::ofstream out("E2ERunner.csv");
    out << ss.str();
    out.close();
    std::cout << "benchmark finish" << std::endl;
}

#endif//NES_BENCHMARK_INCLUDE_E2ETESTS_E2ERunner_HPP_
