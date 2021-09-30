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
#include <Configurations/ConfigOption.hpp>
#include <E2EBase.hpp>
#include <E2EBenchmarkConfig.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Util/Logger.hpp>
#include <Version/version.hpp>
#include <iostream>

using namespace NES;
using namespace std;
//using DefaultSourcePtr = std::shared_ptr<DefaultSource>;

std::vector<uint64_t> split(const std::string& str, char delim);

/**
 * This function takes a config and provide the following options
 *      - if "," is between values, we use each value for one run
 *      - if "-" is between values, we interprete them as start, stop, and increment to create value ranges
 *      - if only one value, the we do nothing
 * @param str
 * @return
 * **/

/**
    * @brief creates a vector with a range of [start, stop) and step size
    */
template<typename T>
static void createRangeVector(std::vector<T>& vector, T start, T stop, T stepSize) {
    for (T i = start; i < stop; i += stepSize) {
        vector.push_back(i);
    }
}

std::vector<uint64_t> getParameterFromString(std::string str) {
    std::vector<uint64_t> retVec;
    if (str.find(",") != string::npos) {//if comma separated, we create one test run for each of the values
        retVec = split(str, ',');
    } else if (str.find("-") != string::npos) {//if - separated, we will create a range from start to end with increment
        auto tempVec = split(str, '-');
        NES_ASSERT(tempVec.size() == 3, " wrong number of parameter");
        createRangeVector<uint64_t>(retVec, tempVec[0], tempVec[1], tempVec[2]);
    } else {//if only one value we will just push it
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

    E2EBenchmarkConfigPtr benchmarkConfig = E2EBenchmarkConfig::create();
    //Query queryInp = Query::from("input").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(NullOutputSinkDescriptor::create());
    QueryPtr queryInp = queryParsingService->createQueryFromCodeString(queryString);
    const QueryPlanPtr queryPlan = queryInp.getQueryPlan();
    /*
    ifstream input_stream;
    string configPathTest = "\/Users\/shetty\/Documents\/Thesis\/CLionProjects\/nebulastream\/tests\/UnitTests\/Benchmarks\/CIRunningBenchmark\/E2eSelection.yaml";
        input_stream.open(configPathTest, ios::in);
    if (!input_stream) {
        std::cout << "FILE NOT FOUND in " << "E2eSelection.yaml" << std::endl;
        return -1;
    }
    std::cout << "using config file=" << "E2eSelection.yaml" << std::endl;
    //benchmarkConfig->overwriteConfigWithYAMLFileInput("/Users/shetty/Documents/Thesis/CLionProjects/nebulastream/tests/UnitTests/Benchmarks/CIRunningBenchmark/E2eSelection.yaml");
   */
    map<string, string> commandLineParams;

    for (int i = 1; i < argc; ++i) {
        commandLineParams.insert(
            pair<string, string>(string(argv[i]).substr(0, string(argv[i]).find("=")),
                                 string(argv[i]).substr(string(argv[i]).find("=") + 1, string(argv[i]).length() - 1)));
    }

    /*auto configPath = commandLineParams.find("--configPath");

    if (configPath != commandLineParams.end()) {
        ifstream input_stream;
        input_stream.open(configPath->second.c_str(), ios::in);
        if (!input_stream) {
            std::cout << "FILE NOT FOUND in " << configPath->second << std::endl;
        //    return -1;
        }
        std::cout << "using config file=" << configPath->second << std::endl;
        benchmarkConfig->overwriteConfigWithYAMLFileInput(configPath->second);
        benchmarkConfig->overwriteConfigWithYAMLFileInput();
    } else if (argc >= 1) {
        benchmarkConfig->overwriteConfigWithCommandLineInput(commandLineParams);
    }*/
    NES::setupLogging("benchmarkRunner.log", NES::getDebugLevelFromString(benchmarkConfig->getLogLevel()->getValue()));

    std::cout << "start benchmark with " << benchmarkConfig->toString() << std::endl;

    std::string benchmarkName = benchmarkConfig->getBenchmarkName()->getValue();
    std::string nesVersion = NES_VERSION;

    std::stringstream ss;
    std::string resultPrefix = "Time,BM_Name,NES_Version";
    std::string changeableParameterString = "WorkerThreads,SourceCnt";
    std::string
        benchmarkResultString = "ProcessedBuffersTotal,ProcessedTasksTotal,ProcessedTuplesTotal,ProcessedBytesTotal,"
                                "ThroughputInTupsPerSec,ThroughputInTasksPerSec,ThroughputInMBPerSec,AvgTaskProcessingTime,AvgQueueSize,AvgAvailableGlobalBuffer,AvgAvailableFixed";

    std::string
        fixParameterString = "NumberOfBuffersToProduce,NumberOfBuffersInGlobalBufferManager,numberOfBuffersPerPipeline,"
                             "NumberOfBuffersInSourceLocalBufferPool,BufferSizeInBytes,GatheringValues,query,InputType, InputMode, InputRates, SourcePlacement";

    //output csv header
    ss << resultPrefix << "," << changeableParameterString << "," << benchmarkResultString << "," << fixParameterString
       << "\n";

    std::map<std::string, std::vector<uint64_t>> parameterNameToValueVectorMap;
    parameterNameToValueVectorMap["workerThreads"] =
        getParameterFromString(benchmarkConfig->getNumberOfWorkerThreads()->getValue());
    parameterNameToValueVectorMap["sources"] =
        getParameterFromString(benchmarkConfig->getNumberOfSources()->getValue());
    parameterNameToValueVectorMap["numberOfBuffersInGlobalBufferManagers"] =
        getParameterFromString(benchmarkConfig->getNumberOfBuffersInGlobalBufferManager()->getValue());
    parameterNameToValueVectorMap["numberOfBuffersPerPipelines"] =
        getParameterFromString(benchmarkConfig->getNumberOfBuffersPerPipeline()->getValue());
    parameterNameToValueVectorMap["numberOfBuffersInSourceLocalBufferPools"] =
        getParameterFromString(benchmarkConfig->getNumberOfBuffersInSourceLocalBufferPool()->getValue());
    parameterNameToValueVectorMap["bufferSizeInBytes"] =
        getParameterFromString(benchmarkConfig->getBufferSizeInBytes()->getValue());
    parameterNameToValueVectorMap["gatheringValues"] =
        getParameterFromString(benchmarkConfig->getGatheringValues()->getValue());

    std::map<std::string, std::vector<uint64_t>>::iterator maxIter =
        std::max_element(parameterNameToValueVectorMap.begin(),
                         parameterNameToValueVectorMap.end(),
                         [](const std::pair<std::string, std::vector<uint64_t>>& a,
                            const std::pair<std::string, std::vector<uint64_t>>& b) -> bool {
                           return a.second.size() < b.second.size();
                         });

    uint64_t maxParamCnt = maxIter->second.size();
    std::cout << "largest param vector=" << maxIter->first << " , " << maxParamCnt << std::endl;
    padParamters(parameterNameToValueVectorMap, maxParamCnt);

    auto filterPushDownRule = Optimizer::FilterPushDownRule::create();
    std::cout << "Input Query Plan: " + (queryPlan)->toString() << std::endl;
    filterPushDownRule->apply(queryPlan);

    for (size_t i = 0; i < maxParamCnt; i++) {
        sleep(1);
        //print resultPrefix
        std::shared_ptr<E2EBase> testRun =
            std::make_shared<E2EBase>(parameterNameToValueVectorMap["workerThreads"].at(i),
                                      parameterNameToValueVectorMap["sources"].at(i),
                                      parameterNameToValueVectorMap["numberOfBuffersInGlobalBufferManagers"].at(i),
                                      parameterNameToValueVectorMap["numberOfBuffersPerPipelines"].at(i),
                                      parameterNameToValueVectorMap["numberOfBuffersInSourceLocalBufferPools"].at(i),

                                      parameterNameToValueVectorMap["bufferSizeInBytes"].at(i),
                                      parameterNameToValueVectorMap["gatheringValues"].at(i),
                                      benchmarkConfig);

        ss << testRun->getTsInRfc3339() << "," << benchmarkName << "," << nesVersion;
        ss << "," << parameterNameToValueVectorMap["workerThreads"].at(i) << ","
           << parameterNameToValueVectorMap["sources"].at(i)
           << ",";

        ss << testRun->runExperiment(queryPlan) << ",";

        //print fixParameterString
        ss << benchmarkConfig->getNumberOfBuffersToProduce()->getValue() << ",";

        ss << parameterNameToValueVectorMap["numberOfBuffersInGlobalBufferManagers"].at(i) << ","
           << parameterNameToValueVectorMap["numberOfBuffersPerPipelines"].at(i) << ","
           << parameterNameToValueVectorMap["numberOfBuffersInSourceLocalBufferPools"].at(i) << ","
           << parameterNameToValueVectorMap["bufferSizeInBytes"].at(i) << ","
           << parameterNameToValueVectorMap["gatheringValues"].at(i) << ",";

        auto queryWithoutComma = benchmarkConfig->getQuery()->getValue();
        std::replace(queryWithoutComma.begin(), queryWithoutComma.end(), ',', '_');
        auto gatheringValuesWithoutComma = benchmarkConfig->getGatheringValues()->getValue();
        std::replace(gatheringValuesWithoutComma.begin(), gatheringValuesWithoutComma.end(), ',', '_');

        auto sourcePlacementWithoutComma = benchmarkConfig->getSourcePinList()->getValue();
        std::replace(sourcePlacementWithoutComma.begin(), sourcePlacementWithoutComma.end(), ',', '_');

        ss << queryWithoutComma << "," << benchmarkConfig->getInputType()->getValue()
           << "," << benchmarkConfig->getGatheringMode()->getValue()
           << "," << gatheringValuesWithoutComma
           << "," << sourcePlacementWithoutComma
           << std::endl;

        std::cout << "result=" << std::endl;
        std::cout << ss.str() << std::endl;

        //auto t = std::time(nullptr);
        //auto tm = *std::localtime(&t);

        //std::ostringstream oss;
        //oss << std::put_time(&tm, "%d-%m-%Y_%H-%M-%S") << "_v" << NES_VERSION << "__";

        std::ofstream ofs;
        //ofs.open (oss.str()+benchmarkConfig->getOutputFile()->getValue(), std::ofstream::out | std::ofstream::app);
        ofs.open (benchmarkConfig->getOutputFile()->getValue(), std::ofstream::out | std::ofstream::app);
        ofs << ss.str();
        ofs.close();
        ss.clear();
        ss.str(std::string());

        //        testRun.reset();
    }


    std::cout << "benchmark finish" << std::endl;
}

#endif//NES_BENCHMARK_INCLUDE_E2ETESTS_E2ERunner_HPP_
