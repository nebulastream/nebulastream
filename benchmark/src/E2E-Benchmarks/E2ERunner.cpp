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
#include <Version/version.hpp>
#include <iostream>
#include <util/BenchmarkUtils.hpp>
#include <util/E2EBase.hpp>

using namespace NES;
using namespace std;
using namespace Benchmarking;

std::vector<uint64_t> split(const std::string& s, char delim) {
    std::stringstream ss(s);
    std::string item;
    std::vector<std::uint64_t> elems;
    while (std::getline(ss, item, delim)) {
        elems.push_back(stoi(item));
    }
    return elems;
}

std::vector<uint64_t> getParameterFromString(std::string str) {
    std::vector<uint64_t> retVec;
    if (str.find(",") != string::npos) {
        retVec = split(str, ',');
    } else if (str.find("-") != string::npos) {
        auto tempVec = split(str, '-');
        NES_ASSERT(tempVec.size() == 3, " wrong number of parameter");
        BenchmarkUtils::createRangeVector<uint64_t>(retVec, tempVec[0], tempVec[1], tempVec[2]);
    } else {
        retVec.push_back(stoi(str));
    }
    return retVec;
}

void padParamters(std::map<std::string, std::vector<uint64_t>>& parameterNameToValueVectorMap, uint64_t maxSize) {
    std::cout << parameterNameToValueVectorMap.size() << maxSize << std::endl;
    //pad parameters if there are not all specified, use the last parameter up to size
    for (auto& param : parameterNameToValueVectorMap) {
        if (param.second.size() < maxSize) {
            auto lastValue = param.second.back();
            while (param.second.size() != maxSize) {
                param.second.push_back(lastValue);
            }
        }
    }

//    std::cout << "parameter map for changing paramters=" << std::endl;
//    for (auto& param : parameterNameToValueVectorMap) {
//        std::cout << "name=" << param.first << "params=";
//        for (auto i : param.second) {
//            std::cout << i << ',';
//        }
//        std::cout << std::endl;
//    }
}

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
    } else if (argc >= 1) {
        benchmarkConfig->overwriteConfigWithCommandLineInput(commandLineParams);
    }

    std::cout << "start benchmark with " << benchmarkConfig->toString() << std::endl;

    NES::setLogLevel(NES::getStringAsDebugLevel(benchmarkConfig->getLogLevel()->getValue()));
    std::string benchmarkName = benchmarkConfig->getBenchmarkName()->getDefaultValue();
    std::string nesVersion = NES_VERSION;

    std::stringstream ss;
    std::string resultPrefix = "Time,BM_Name,NES_Version";
    std::string changeableParameterString = "WorkerThreads,CoordinatorThreadCnt,SourceCnt";
    std::string benchmarkResultString = "ProcessedBuffersTotal,ProcessedTasksTotal,ProcessedTuplesTotal,ProcessedBytesTotal,"
                                        "ThroughputInTupsPerSec,ThroughputInMBPerSec";
    std::string fixParameterString = "NumberOfBuffersToProduce,NumberOfBuffersInGlobalBufferManager,numberOfBuffersPerPipeline,"
                                     "NumberOfBuffersInSourceLocalBufferPool,BufferSizeInBytes,query,InputOutputMode";

    //output csv header
    ss << resultPrefix << "," << changeableParameterString << "," << benchmarkResultString << "," << fixParameterString << "\n";

    std::map<std::string, std::vector<uint64_t>> parameterNameToValueVectorMap;
    parameterNameToValueVectorMap["workerThreads"] =
        getParameterFromString(benchmarkConfig->getNumberOfWorkerThreads()->getValue());
    parameterNameToValueVectorMap["coordinatorThreads"] =
        getParameterFromString(benchmarkConfig->getNumberOfWorkerThreads()->getValue());
    parameterNameToValueVectorMap["sources"] = getParameterFromString(benchmarkConfig->getNumberOfSources()->getValue());
    parameterNameToValueVectorMap["numberOfBuffersInGlobalBufferManagers"] =
        getParameterFromString(benchmarkConfig->getNumberOfBuffersInGlobalBufferManager()->getValue());
    parameterNameToValueVectorMap["numberOfBuffersPerPipelines"] =
        getParameterFromString(benchmarkConfig->getNumberOfBuffersPerPipeline()->getValue());
    parameterNameToValueVectorMap["numberOfBuffersInSourceLocalBufferPools"] =
        getParameterFromString(benchmarkConfig->getNumberOfBuffersInSourceLocalBufferPool()->getValue());
    parameterNameToValueVectorMap["bufferSizeInBytes"] =
        getParameterFromString(benchmarkConfig->getBufferSizeInBytes()->getValue());

    std::map<std::string, std::vector<uint64_t>>::iterator maxIter =
        std::max_element(parameterNameToValueVectorMap.begin(), parameterNameToValueVectorMap.end(),
                         [](const std::pair<std::string, std::vector<uint64_t>>& a,
                            const std::pair<std::string, std::vector<uint64_t>>& b) -> bool {
                             return a.second.size() < b.second.size();
                         });

    uint64_t maxParamCnt = maxIter->second.size();
    std::cout << "largest param vector=" << maxIter->first << " , " << maxParamCnt << std::endl;
    padParamters(parameterNameToValueVectorMap, maxParamCnt);

    for (size_t i = 0; i < maxParamCnt; i++) {
        //print resultPrefix
        std::shared_ptr<E2EBase> testRun = std::make_shared<E2EBase>(
            parameterNameToValueVectorMap["workerThreads"].at(i), parameterNameToValueVectorMap["coordinatorThreads"].at(i),
            parameterNameToValueVectorMap["sources"].at(i),
            parameterNameToValueVectorMap["numberOfBuffersInGlobalBufferManagers"].at(i),
            parameterNameToValueVectorMap["numberOfBuffersPerPipelines"].at(i),
            parameterNameToValueVectorMap["numberOfBuffersInSourceLocalBufferPools"].at(i),
            parameterNameToValueVectorMap["bufferSizeInBytes"].at(i), benchmarkConfig);

        ss << testRun->getTsInRfc3339() << "," << benchmarkName << "," << nesVersion;
        ss << "," << parameterNameToValueVectorMap["workerThreads"].at(i) << ","
           << parameterNameToValueVectorMap["coordinatorThreads"].at(i) << "," << parameterNameToValueVectorMap["sources"].at(i)
           << ",";

        //print benchmarkResultString
        std::string result = testRun->runExperiment();
        ss << result.c_str() << ",";

        //print fixParameterString
        ss << benchmarkConfig->getNumberOfBuffersToProduce()->getValue() << ",";

        ss << parameterNameToValueVectorMap["numberOfBuffersInGlobalBufferManagers"].at(i) << ","
           << parameterNameToValueVectorMap["numberOfBuffersPerPipelines"].at(i) << ","
           << parameterNameToValueVectorMap["numberOfBuffersInSourceLocalBufferPools"].at(i) << ","
           << parameterNameToValueVectorMap["bufferSizeInBytes"].at(i) << ",";

        ss << benchmarkConfig->getQuery()->getValue() << "," << benchmarkConfig->getInputOutputMode()->getValue() << std::endl;
    }

    std::cout << "result=" << std::endl;
    std::cout << ss.str() << std::endl;

    std::ofstream out(benchmarkConfig->getOutputFile()->getValue());
    out << ss.str();
    out.close();
    std::cout << "benchmark finish" << std::endl;
}

#endif//NES_BENCHMARK_INCLUDE_E2ETESTS_E2ERunner_HPP_
