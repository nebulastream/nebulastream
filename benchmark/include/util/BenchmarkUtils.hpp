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

#ifndef NES_BENCHMARK_UTIL_BENCHMARKUTILS_HPP_
#define NES_BENCHMARK_UTIL_BENCHMARKUTILS_HPP_

#include <API/Query.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/QueryStatistics.hpp>
#include <Version/version.hpp>
#include <cstdint>
#include <list>
#include <random>
#include <vector>

namespace NES::Benchmarking {
/**
 * @brief This class provides several helper functions for creating benchmarks.
 */
class BenchmarkUtils {
  public:
    static uint64_t runSingleExperimentSeconds;
    static uint64_t periodLengthInSeconds;

    /**
    * @brief creates a vector with a range of [start, stop) and step size
    */
    template<typename T>
    static void createRangeVector(std::vector<T>& vector, T start, T stop, T stepSize) {
        for (T i = start; i < stop; i += stepSize) {
            vector.push_back(i);
        }
    }

    /**
     * @brief creates a list with values drawn from an uniform distribution of the range [0,999]. The list size is totalNumberOfTuples
     * @param dataList
     * @param totalNumberOfTuples
     */
    static void createUniformData(std::list<uint64_t>& dataList, uint64_t totalNumberOfTuples);

    /**
     * @brief This function calculates the cardinality of a simple selection with a given selectivity.
     * @param vector Expects a vector with values in the range of [0,999] as they are used for the selectivity
     * @param selectivity
     * @return
     */
    static uint64_t calcExpectedTuplesSelectivity(std::list<uint64_t> list, uint64_t selectivity);

    /**
     * @brief saves every second statistics from QueryStatistics into statisticsVec
     * @param statisticsVec
     * @param nodeEngine
     */
    static void recordStatistics(std::vector<QueryStatistics*>& statisticsVec, NodeEnginePtr nodeEngine);

    /**
     * @brief computes difference between each vector item of its predecessor. Also deletes statisticsVec[0]
     * @param statisticsVec
     */
    static void computeDifferenceOfStatistics(std::vector<QueryStatistics*>& statisticsVec);

    /**
     * @return string with format {dateTime}_v{current NES_VERSION}
     */
    static std::string getCurDateTimeStringWithNESVersion();

    /**
     * @brief ProcessedBytes is calculated by ProcessedTuples * SchemaSizeInBytes
     * @param statistic
     * @param schema
     * @return comma seperated string {ProcessedBuffers},{ProcessedTasks},{ProcessedTuples},{ProcessedBytes}
     */
    static std::string getStatisticsAsCSV(QueryStatistics* statistic, SchemaPtr schema);

    static void printOutConsole(QueryStatistics* statistic, SchemaPtr schema);

    /**
     * @brief runs a benchmark with the given ingestion rate, given query, and a benchmark schema. The statistics (processedTuples)
     * of this benchmark run are also saved.
     * @param benchmarkSchema
     * @param statisticsVec
     * @param query
     * @param ingestionRate
     */

    static void runBenchmark(std::vector<NES::QueryStatistics*>& statisticsVec, std::vector<DataSourcePtr> benchmarkSource,
                             NES::DataSinkPtr benchmarkSink, NES::NodeEnginePtr nodeEngine, NES::Query query);
};

/**
 * @brief BM_AddBenchmark helps creating a new benchmark by providing a solid building block.
 * It requires std::vectors of type uint64_t named {allIngestionRates, allExperimentsDuration, allPeriodLengths, allWorkerThreads}
 */
#define BM_AddBenchmark(benchmarkName, benchmarkQuery, benchmarkSource, benchmarkSink, csvHeaderString, customCSVOutputs)        \
    {                                                                                                                            \
        NES::setupLogging(benchmarkFolderName + "/" + (benchmarkName) + ".log", NES::LOG_WARNING);                                  \
                                                                                                                                 \
        try {                                                                                                                    \
            std::ofstream benchmarkFile;                                                                                         \
            benchmarkFile.open(benchmarkFolderName + "/" + (benchmarkName) + "_results.csv", std::ios_base::app);                \
            benchmarkFile << "BM_Name,NES_Version,Ingestionrate,WorkerThreads,SourceCnt,RunSingleExperiment,PeriodLength,"       \
                             "ProcessedBuffers,ProcessedTasks,ProcessedTuples,ProcessedBytes"                                    \
                          << (csvHeaderString) << "\n";                                                                          \
            benchmarkFile.close();                                                                                               \
                                                                                                                                 \
            for (auto ingestionRate : allIngestionRates) {                                                                       \
                for (auto experimentDuration : allExperimentsDuration) {                                                         \
                    for (auto periodLength : allPeriodLengths) {                                                                 \
                        for (auto workerThreads : allWorkerThreads)                                                              \
                            for (auto sourceCnt : allDataSources) {                                                              \
                                PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();                             \
                                auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf, workerThreads);             \
                                                                                                                                 \
                                BenchmarkUtils::runSingleExperimentSeconds = experimentDuration;                                 \
                                BenchmarkUtils::periodLengthInSeconds = periodLength;                                            \
                                                                                                                                 \
                                std::vector<QueryStatistics*> statisticsVec;                                                     \
                                NES_WARNING("Starting benchmark with ingestRate=" + std::to_string(ingestionRate) + ", "         \
                                            + "singleExpSec=" + std::to_string(BenchmarkUtils::runSingleExperimentSeconds)       \
                                            + ", " + "benchPeriod=" + std::to_string(BenchmarkUtils::periodLengthInSeconds)      \
                                            + ", " + "workerThreads=" + std::to_string(workerThreads)                            \
                                            + " sources=" + std::to_string(sourceCnt));                                          \
                                std::vector<DataSourcePtr> sources;                                                              \
                                for (int i = 0; i < sourceCnt; i++) {                                                            \
                                    DataSourcePtr thisSource = (benchmarkSource);                                                \
                                    thisSource->setOperatorId(++i);                                                              \
                                    sources.push_back(thisSource);                                                               \
                                }                                                                                                \
                                                                                                                                 \
                                DataSinkPtr thisSink = (benchmarkSink);                                                          \
                                SchemaPtr thisSchema = (benchmarkSchema);                                                        \
                                Query thisQuery = (benchmarkQuery);                                                              \
                                BenchmarkUtils::runBenchmark(statisticsVec, sources, thisSink, nodeEngine, thisQuery);           \
                                                                                                                                 \
                                benchmarkFile.open(benchmarkFolderName + "/" + (benchmarkName) + "_results.csv",                 \
                                                   std::ios_base::app);                                                          \
                                                                                                                                 \
                                for (auto statistic : statisticsVec) {                                                           \
                                    benchmarkFile << (benchmarkName) << ",\"" << NES_VERSION << "\""                             \
                                                  << "," << std::to_string(ingestionRate) << ","                                 \
                                                  << std::to_string(workerThreads) << "," << std::to_string(sourceCnt) << ","    \
                                                  << std::to_string(BenchmarkUtils::runSingleExperimentSeconds) << ","           \
                                                  << std::to_string(BenchmarkUtils::periodLengthInSeconds)                       \
                                                  << BenchmarkUtils::getStatisticsAsCSV(statistic, thisSchema)                   \
                                                  << (customCSVOutputs) << "\n";                                                 \
                                                                                                                                 \
                                    delete statistic;                                                                            \
                                }                                                                                                \
                                benchmarkFile.close();                                                                           \
                                std::ifstream t(benchmarkFolderName + "/" + (benchmarkName) + "_results.csv");                   \
                                std::string content((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());      \
                                std::cout << "benchmark content" << content << std::endl;                                        \
                            }                                                                                                    \
                    }                                                                                                            \
                }                                                                                                                \
            }                                                                                                                    \
        } catch (RuntimeException & e) {                                                                                         \
            NES_ERROR("Caught RuntimeException: " << e.what());                                                                  \
        }                                                                                                                        \
                                                                                                                                 \
        NES::NESLogger->removeAllAppenders();                                                                                    \
    }

#define printPIDandParentID                                                                                                      \
    (std::cout << __FUNCTION__ << " called by process " << ::getpid() << " (parent: " << ::getppid() << ")" << std::endl)
}// namespace NES::Benchmarking

#endif//NES_BENCHMARK_UTIL_BENCHMARKUTILS_HPP_