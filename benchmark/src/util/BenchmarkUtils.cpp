/*
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

#include <util/BenchmarkUtils.hpp>

#include "../../../tests/util/DummySink.hpp"
#include "../../../tests/util/TestQueryCompiler.hpp"
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/DistributeWindowRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/GeneratableOperators/TranslateToGeneratableOperatorPhase.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlanBuilder.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Version/version.hpp>
#include <cstdint>
#include <list>
#include <random>
#include <vector>

namespace NES::Benchmarking {

uint64_t BenchmarkUtils::runSingleExperimentSeconds;
uint64_t BenchmarkUtils::periodLengthInSeconds;

void BenchmarkUtils::createUniformData(std::list<uint64_t>& dataList, uint64_t totalNumberOfTuples) {
    // uniform distribution
    int min = 0, max = 999;

    std::random_device random_device;
    std::mt19937 generator(random_device());
    std::uniform_int_distribution<> distrib(min, max);

    for (uint64_t i = 0; i < totalNumberOfTuples; ++i) {
        int tmp;
        do {
            tmp = distrib(generator);
        } while (tmp < min || tmp > max);

        dataList.emplace_back(tmp);
    }
}

uint64_t BenchmarkUtils::calcExpectedTuplesSelectivity(std::list<uint64_t> list, uint64_t selectivity) {
    uint64_t countExpectedTuples = 0;

    for (unsigned long& listIterator : list) {
        if (listIterator < selectivity)
            ++countExpectedTuples;
    }

    return countExpectedTuples;
}

void BenchmarkUtils::recordStatistics(std::vector<Runtime::QueryStatistics*>& statisticsVec, Runtime::NodeEnginePtr nodeEngine) {

    for (uint64_t i = 0; i < BenchmarkUtils::runSingleExperimentSeconds + 1; ++i) {
        int64_t nextPeriodStartTime = BenchmarkUtils::periodLengthInSeconds * 1000
            + std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        auto queryStatisticsPtrs = nodeEngine->getQueryStatistics(1);
        for (auto it : queryStatisticsPtrs) {

            auto* statistics = new Runtime::QueryStatistics(0, 0);
            statistics->setProcessedBuffers(it->getProcessedBuffers());
            statistics->setProcessedTasks(it->getProcessedTasks());
            statistics->setProcessedTuple(it->getProcessedTuple());

            statisticsVec.push_back(statistics);
            NES_WARNING("Statistic: " << it->getQueryStatisticsAsString());
        }
        auto curTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        while (curTime < nextPeriodStartTime) {
            curTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                          .count();
        }
    }
}

void BenchmarkUtils::computeDifferenceOfStatistics(std::vector<Runtime::QueryStatistics*>& statisticsVec) {
    for (uint64_t i = statisticsVec.size() - 1; i > 1; --i) {
        statisticsVec[i]->setProcessedTuple(statisticsVec[i]->getProcessedTuple() - statisticsVec[i - 1]->getProcessedTuple());
        statisticsVec[i]->setProcessedBuffers(statisticsVec[i]->getProcessedBuffers()
                                              - statisticsVec[i - 1]->getProcessedBuffers());
        statisticsVec[i]->setProcessedTasks(statisticsVec[i]->getProcessedTasks() - statisticsVec[i - 1]->getProcessedTasks());
    }

    // Deleting first Element as it serves no purpose anymore
    statisticsVec.erase(statisticsVec.begin());
}

std::string BenchmarkUtils::getCurDateTimeStringWithNESVersion() {
    auto t = std::time(nullptr);
    auto tm = *std::localtime(&t);

    std::ostringstream oss;
    oss << std::put_time(&tm, "%d-%m-%Y_%H-%M-%S") << "_v" << NES_VERSION;
    return oss.str();
}

std::string BenchmarkUtils::getStatisticsAsCSV(Runtime::QueryStatistics* statistic, SchemaPtr schema) {
    return "," + std::to_string(statistic->getProcessedBuffers()) + "," + std::to_string(statistic->getProcessedTasks()) + ","
        + std::to_string(statistic->getProcessedTuple()) + ","
        + std::to_string(statistic->getProcessedTuple() * schema->getSchemaSizeInBytes()) + ","
        + std::to_string(statistic->getProcessedTuple() / BenchmarkUtils::runSingleExperimentSeconds) + ","
        + std::to_string((statistic->getProcessedTuple() * schema->getSchemaSizeInBytes())
                         / (1024 * 1024 * BenchmarkUtils::runSingleExperimentSeconds));
}

void BenchmarkUtils::printOutConsole(Runtime::QueryStatistics* statistic, SchemaPtr schema) {
    std::cout << "numberOfTuples/sec=" << statistic << schema;
}

void BenchmarkUtils::runBenchmark(std::vector<Runtime::QueryStatistics*>& statisticsVec,
                                  std::vector<DataSourcePtr> benchmarkSource,
                                  std::shared_ptr<Benchmarking::SimpleBenchmarkSink> benchmarkSink,
                                  Runtime::NodeEnginePtr nodeEngine,
                                  Query query) {

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    auto generateableOperators = translatePhase->transform(query.getQueryPlan()->getRootOperators()[0]);
    NES_DEBUG("dump output=" << &statisticsVec << &benchmarkSource << &benchmarkSink << &nodeEngine);
    NES_NOT_IMPLEMENTED();
    //
    //    auto builder = GeneratedQueryExecutionPlanBuilder::create()
    //                       .setQueryManager(nodeEngine->getQueryManager())
    //                       .setBufferManager(nodeEngine->getBufferManager())
    //                       .setCompiler(nodeEngine->getCompiler())
    //                       .setQueryId(1)
    //                       .setQuerySubPlanId(1)
    //                       .addSink(benchmarkSink)
    //                       .addOperatorQueryPlan(generateableOperators);
    //
    //    for (auto src : benchmarkSource) {
    //        builder.addSource(src);
    //    }
    //    auto plan = builder.build();
    //    nodeEngine->registerQueryInNodeEngine(plan);
    //    NES_INFO("BenchmarkUtils: QEP for " << queryPlan->toString() << " was registered in Runtime. Starting query now...");
    //
    //    NES_INFO("BenchmarkUtils: Starting query...");
    //    nodeEngine->startQuery(1);
    //    recordStatistics(statisticsVec, nodeEngine);
    //
    //    NES_WARNING("BenchmarkUtils: Stopping query...");
    //    nodeEngine->stopQuery(1, false);
    //    NES_WARNING("Query was stopped!");

    /* This is not necessary anymore as we do not want to have the differences anymore. We are only interested in the total
     * number of tuples, buffers, tasks. Via the total number and runSingleExperimentSeconds we can calculate the throughput
     *
     * computeDifferenceOfStatistics(statisticsVec);
     */
}

std::string BenchmarkUtils::getTsInRfc3339() {
    const auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
    const auto now_s = std::chrono::time_point_cast<std::chrono::seconds>(now_ms);
    const auto millis = now_ms - now_s;
    const auto c_now = std::chrono::system_clock::to_time_t(now_s);

    std::stringstream ss;
    ss << std::put_time(gmtime(&c_now), "%FT%T") << '.' << std::setfill('0') << std::setw(3) << millis.count() << 'Z';
    return ss.str();
}

}// namespace NES::Benchmarking