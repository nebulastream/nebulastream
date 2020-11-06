#include <iostream>
#include <vector>
#include <util/BenchmarkUtils.hpp>
#include <filesystem>
#include <util/SimpleBenchmarkSource.hpp>
#include <util/SimpleBenchmarkSink.hpp>
#include "../../tests/util/TestQuery.hpp"
#include "../../tests/util/DummySink.hpp"

using namespace NES;
using namespace NES::Benchmarking;

/**
 * @brief This file/main shows how a benchmark can be created. The benchmark seen below is a filter query that was implemented by using the BM_AddBenchmark macro from <util/BenchmarkUtils.hpp>.
 */
int main() {

    // All ingestion rates from 90M to 120M in a step range of 10M
    std::vector<uint64_t> allIngestionRates;
    BenchmarkUtils::createRangeVector(allIngestionRates, 50 * 1000 * 1000, 70 * 1000 * 1000, 10 * 1000 * 1000);
    //BenchmarkUtils::createRangeVector(allIngestionRates, 50 * 1000 * 1000, 180 * 1000 * 1000, 10 * 1000 * 1000);

    std::vector<uint64_t> allExperimentsDuration;
    BenchmarkUtils::createRangeVector(allExperimentsDuration, 10, 20, 10);

    std::vector<uint64_t> allPeriodLengths;
    BenchmarkUtils::createRangeVector(allPeriodLengths, 1, 2, 1);
    BenchmarkUtils::createRangeVector(allPeriodLengths, 2, 3, 1);

    std::string benchmarkFolderName = "FilterQueries_" + BenchmarkUtils::getCurDateTimeStringWithNESVersion();
    if (!std::filesystem::create_directory(benchmarkFolderName)) throw RuntimeException("Could not create folder " + benchmarkFolderName);

    //-----------------------------------------Start of BM_SimpleFilterQuery----------------------------------------------------------------------------------------------
    std::vector<uint64_t> allSelectivities;
    BenchmarkUtils::createRangeVector(allSelectivities, 500, 700, 100);

    for (auto selectivity : allSelectivities){
        auto benchmarkSchema = Schema::create()->addField("key", BasicType::INT16)->addField("value", BasicType::INT16);
        BM_AddBenchmark("BM_SimpleFilterQuery",
                        TestQuery::from(thisSchema).filter(Attribute("key") < selectivity).sink(DummySink::create()),
                        1,
                        SimpleBenchmarkSource::create(nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), benchmarkSchema, ingestionRate),
                        SimpleBenchmarkSink::create(benchmarkSchema, nodeEngine->getBufferManager()),
                        ",Selectivity",
                        "," + std::to_string(selectivity))

    }

    //-----------------------------------------End of BM_SimpleFilterQuery-----------------------------------------------------------------------------------------------

    return 0;
}