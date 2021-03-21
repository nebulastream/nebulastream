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
#include "util/E2EBenchmarkConfig.hpp"
#include <Configurations/ConfigOption.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Version/version.hpp>
#include <iostream>
#include <util/E2EBase.hpp>

using namespace NES;
using namespace std;
const std::string logo = "/********************************************************\n"
                         " *     _   _   ______    _____\n"
                         " *    | \\ | | |  ____|  / ____|\n"
                         " *    |  \\| | | |__    | (___\n"
                         " *    |     | |  __|    \\___ \\     Benchmark Runner\n"
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
        std::cout << "using config file=" << configPath->second << std::endl;
        benchmarkConfig->overwriteConfigWithYAMLFileInput(configPath->second);
    }
    else if (argc >= 1) {
        benchmarkConfig->overwriteConfigWithCommandLineInput(commandLineParams);
    }

    std::cout
        << "start benchmark with numberOfWorkerThreads=" << benchmarkConfig->getNumberOfWorkerThreads()->getValue()
        << " numberOfCoordinatorThreads=" << benchmarkConfig->getNumberOfCoordinatorThreads()->getValue()
        << " numberOfBuffersToProduce=" << benchmarkConfig->getNumberOfBuffersToProduce()->getValue()
        << " numberOfSources=" << benchmarkConfig->getNumberOfSources()->getValue()
        << " numberOfBuffersInGlobalBufferManager=" << benchmarkConfig->getNumberOfBuffersInGlobalBufferManager()->getValue()
        << " numberOfBuffersInTaskLocalBufferPool=" << benchmarkConfig->getNumberOfBuffersInTaskLocalBufferPool()->getValue()
        << " numberOfBuffersInSourceLocalBufferPool=" << benchmarkConfig->getNumberOfBuffersInSourceLocalBufferPool()->getValue()
        << " bufferSizeInBytes=" << benchmarkConfig->getBufferSizeInBytes()->getValue()
        << " inputOutputMode=" << benchmarkConfig->getInputOutputMode()->getValue()
        << " outputFile=" << benchmarkConfig->getOutputFile()->getValue()
        << " query=" << benchmarkConfig->getQuery()->getValue()
        << " logLevel=" << benchmarkConfig->getLogLevel()->getValue() << std::endl;

    NES::setLogLevel(NES::getStringAsDebugLevel(benchmarkConfig->getLogLevel()->getValue()));
    std::string benchmarkName = benchmarkConfig->getBenchmarkName()->getDefaultValue();
    std::string nesVersion = NES_VERSION;

    std::stringstream ss;
    ss << "Time,BM_Name,NES_Version,WorkerThreads,CoordinatorThreadCnt,SourceCnt,Mode,ProcessedBuffersTotal,ProcessedTasksTotal,"
          "ProcessedTuplesTotal,ProcessedBytesTotal,ThroughputInTupsPerSec,ThroughputInMBPerSec"
       << std::endl;

    std::vector<string> workerThreadCnt = UtilityFunctions::split(benchmarkConfig->getNumberOfWorkerThreads()->getValue(), ',');
    std::vector<string> coordinatorThreadCnt =
        UtilityFunctions::split(benchmarkConfig->getNumberOfCoordinatorThreads()->getValue(), ',');
    std::vector<string> sourceCnt = UtilityFunctions::split(benchmarkConfig->getNumberOfSources()->getValue(), ',');

    NES_ASSERT(workerThreadCnt.size() == coordinatorThreadCnt.size() && coordinatorThreadCnt.size()  == sourceCnt.size(),
               "worker threads, coordinator threads, and source cnt have to have the same count");

    for (size_t i = 0; i < workerThreadCnt.size(); i++) {
        std::cout << " run configuration workerThreads=" << workerThreadCnt[i]
                  << " coordinatorThreads=" << coordinatorThreadCnt[i] << " sourceCnt=" << sourceCnt[i] << std::endl;
        auto test =
            std::make_shared<E2EBase>(stoi(workerThreadCnt[i]), stoi(coordinatorThreadCnt[i]), stoi(sourceCnt[i]),
                                      E2EBase::getInputOutputModeFromString(benchmarkConfig->getInputOutputMode()->getValue()),
                                      benchmarkConfig->getQuery()->getValue());
        ss << test->getTsInRfc3339() << "," << benchmarkName << "," << nesVersion << "," << workerThreadCnt[i] << ","
           << coordinatorThreadCnt[i] << "," << sourceCnt[i] << "," << benchmarkConfig->getInputOutputMode()->getValue();
        ss << test->runExperiment();
    }

    std::cout << "result=" << std::endl;
    std::cout << ss.str() << std::endl;

    std::ofstream out(benchmarkConfig->getOutputFile()->getValue());
    out << ss.str();
    out.close();
    std::cout << "benchmark finish" << std::endl;
}

#endif//NES_BENCHMARK_INCLUDE_E2ETESTS_E2ERunner_HPP_
