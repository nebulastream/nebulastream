#include <util/BenchmarkUtils.hpp>

#include <cstdint>
#include <vector>
#include <list>
#include <random>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/GeneratableOperators/TranslateToGeneratableOperatorPhase.hpp>
#include <Version/version.hpp>

namespace NES::Benchmarking{

uint64_t BenchmarkUtils::runSingleExperimentSeconds;
uint64_t BenchmarkUtils::periodLengthInSeconds;

void BenchmarkUtils::createRangeVector(std::vector<uint64_t>& vector, uint64_t start, uint64_t stop, uint64_t stepSize){
    for(uint64_t i = start; i < stop; i += stepSize){
        vector.push_back(i);
    }
}

 void BenchmarkUtils::createUniformData(std::list<uint64_t>& dataList, uint64_t totalNumberOfTuples) {
    // uniform distribution
    int min = 0, max = 999;

    std::random_device random_device;
    std::mt19937 generator(random_device());
    std::uniform_int_distribution<> distrib(min, max);

    for (size_t i = 0; i < totalNumberOfTuples; ++i) {
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
        if (listIterator < selectivity) ++countExpectedTuples;
    }

    return countExpectedTuples;
}

void BenchmarkUtils::recordStatistics(std::vector<QueryStatistics*>& statisticsVec, NodeEnginePtr nodeEngine) {
    for(size_t i = 0; i < BenchmarkUtils::runSingleExperimentSeconds + 1; ++i){
        int64_t nextPeriodStartTime = BenchmarkUtils::periodLengthInSeconds * 1000 + std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        auto queryStatisticsPtrs = nodeEngine->getQueryStatistics(1);
        for (auto it : queryStatisticsPtrs){

            auto *statistics = new QueryStatistics();
            statistics->setProcessedBuffers(it->getProcessedBuffers());
            statistics->setProcessedTasks(it->getProcessedTasks());
            statistics->setProcessedTuple(it->getProcessedTuple());

            statisticsVec.push_back(statistics);
            NES_WARNING("Statistic: " << it->getQueryStatisticsAsString());
        }
        auto curTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        while(curTime < nextPeriodStartTime){
            curTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        }
    }
}

void BenchmarkUtils::computeDifferenceOfStatistics(std::vector<QueryStatistics*>& statisticsVec){
    for(size_t i = statisticsVec.size() - 1; i > 1; --i){
        statisticsVec[i]->setProcessedTuple(statisticsVec[i]->getProcessedTuple() - statisticsVec[i - 1]->getProcessedTuple());
        statisticsVec[i]->setProcessedBuffers(statisticsVec[i]->getProcessedBuffers() - statisticsVec[i - 1]->getProcessedBuffers());
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

std::string BenchmarkUtils::getStatisticsAsCSV(QueryStatistics* statistic, SchemaPtr schema){
    return  "," + std::to_string(statistic->getProcessedBuffers()) +
        "," + std::to_string(statistic->getProcessedTasks()) +
        "," + std::to_string(statistic->getProcessedTuple()) +
        "," + std::to_string(statistic->getProcessedTuple() * schema->getSchemaSizeInBytes());
}


void BenchmarkUtils::runBenchmark(std::vector<QueryStatistics*>& statisticsVec,
                         DataSourcePtr benchmarkSource,
                         DataSinkPtr benchmarkSink,
                         NodeEnginePtr nodeEngine,
                         Query query,
                         uint64_t workerThreads){

    auto typeInferencePhase = TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    std::cout << " plan=" << queryPlan->toString() << std::endl;

    auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    auto generateableOperators = translatePhase->transform(query.getQueryPlan()->getRootOperators()[0]);

    auto builder = GeneratedQueryExecutionPlanBuilder::create()
                    .setQueryManager(nodeEngine->getQueryManager())
                    .setBufferManager(nodeEngine->getBufferManager())
                    .setCompiler(nodeEngine->getCompiler())
                    .setQueryId(1)
                    .setQuerySubPlanId(1)
                    .addSource(benchmarkSource)
                    .addSink(benchmarkSink)
                    .addOperatorQueryPlan(generateableOperators);

    auto plan = builder.build();
    nodeEngine->registerQueryInNodeEngine(plan);
    NES_INFO("QEP for " << queryPlan->toString() << " was registered in NodeEngine. Starting query now...");

    NES_INFO("Starting benchmark with workerThreads=" << workerThreads);
    nodeEngine->startQuery(1);
    recordStatistics(statisticsVec, nodeEngine);

    NES_WARNING("Stopping query...");
    benchmarkSource->stop();
    benchmarkSink->shutdown();
    nodeEngine->stopQuery(1);
    NES_WARNING("Query was stopped!");

    computeDifferenceOfStatistics(statisticsVec);

}
}