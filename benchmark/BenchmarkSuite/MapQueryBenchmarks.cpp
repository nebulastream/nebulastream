#include <iostream>
#include <util/BenchmarkUtils.hpp>
#include <filesystem>
#include <util/SimpleBenchmarkSource.hpp>
#include <util/SimpleBenchmarkSink.hpp>

#include "../../tests/util/TestQuery.hpp"
#include "../../tests/util/DummySink.hpp"

using namespace NES;
using namespace NES::Benchmarking;

/**
 * @brief This file/main shows how a benchmark can be created. The benchmark seen below is a map query that was implemented by using the BM_AddBenchmark macro from <util/BenchmarkUtils.hpp>.
 */
int main() {

    std::vector<uint64_t> allIngestionRates;
    BenchmarkUtils::createRangeVector(allIngestionRates, 1*1000*1000, 10*1000*1000, 1*1000*1000);
    BenchmarkUtils::createRangeVector(allIngestionRates, 10*1000*1000, 650*1000*1000, 10*1000*1000);

    std::vector<uint64_t> allExperimentsDuration;
    BenchmarkUtils::createRangeVector(allExperimentsDuration, 10, 20, 10);

    std::vector<uint64_t> allPeriodLengths;
    BenchmarkUtils::createRangeVector(allPeriodLengths, 1, 2, 1);

    std::string benchmarkFolderName = "MapQueries_" + BenchmarkUtils::getCurDateTimeStringWithNESVersion();
    if (!std::filesystem::create_directory(benchmarkFolderName))
        throw RuntimeException("Could not create folder " + benchmarkFolderName);

    //-----------------------------------------Start of BM_SimpleMapQuery----------------------------------------------------------------------------------------------
    auto benchmarkSchema = Schema::create()->addField("key", BasicType::INT16)->addField("value", BasicType::INT16);
    BM_AddBenchmark("BM_SimpleMapQuery",
                    TestQuery::from(thisSchema).map(Attribute("value") = Attribute("key") + Attribute("value")).sink(DummySink::create()),
                    1,
                    SimpleBenchmarkSource::create(nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), benchmarkSchema, ingestionRate),
                    SimpleBenchmarkSink::create(benchmarkSchema, nodeEngine->getBufferManager()),
                    "",
                    "")
    //-----------------------------------------End of BM_SimpleMapQuery-----------------------------------------------------------------------------------------------


    return 0;
}