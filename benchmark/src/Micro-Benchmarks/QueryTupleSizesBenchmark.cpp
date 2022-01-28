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

#include "../../../tests/util/DummySink.hpp"
#include "../../../tests/util/TestQuery.hpp"
#include <filesystem>
#include <iostream>
#include <util/BenchmarkSchemas.hpp>
#include <util/BenchmarkUtils.hpp>
#include <util/SimpleBenchmarkSink.hpp>
#include <util/SimpleBenchmarkSource.hpp>
#include <vector>

using namespace NES;
using namespace NES::Benchmarking;

/**
 * @brief This file/main benchmarks what effect the tuple size has on the performance of the nodeEngine. A filter query and a map query is chosen as query types.
 * The different tuple sizes can be found in BenchmarkSchemas::getBenchmarkSchemas()
 */
int main() {

    // All ingestion rates that the nodeEngine is exposed
    std::vector<uint64_t> allIngestionRates;
    BenchmarkUtils::createRangeVector<uint64_t>(allIngestionRates, 600 * 1000 * 1000, 610 * 1000 * 1000, 10 * 1000 * 1000);

    // Duration of one experiment
    std::vector<uint64_t> allExperimentsDuration;
    BenchmarkUtils::createRangeVector<uint64_t>(allExperimentsDuration, 10, 20, 10);

    // In what frequency new tuples should be inserted into nodeEngine
    std::vector<uint64_t> allPeriodLengths;
    BenchmarkUtils::createRangeVector<uint64_t>(allPeriodLengths, 1, 2, 1);

    // Number of workerThreads in nodeEngine
    std::vector<uint16_t> allWorkerThreads;
    BenchmarkUtils::createRangeVector<uint16_t>(allWorkerThreads, 1, 4, 1);

    // Number of dataSources
    std::vector<uint16_t> allDataSources;
    BenchmarkUtils::createRangeVector<uint16_t>(allDataSources, 1, 2, 1);

    // Size of one tupleBuffer which stores tuples
    std::vector<uint64_t> allBufferSizes;
    BenchmarkUtils::createRangeVector<uint64_t>(allBufferSizes, 4 * 1024, 8 * 1024, 4 * 1024);

    std::string benchmarkFolderName = "QueriesTupleSize_" + BenchmarkUtils::getCurDateTimeStringWithNESVersion();
    if (!std::filesystem::create_directory(benchmarkFolderName)) {
        throw RuntimeException("Could not create folder " + benchmarkFolderName);
    }

    //-----------------------------------------Start of BM_SimpleMapQuery----------------------------------------------------------------------------------------------
    for (auto benchmarkSchema : BenchmarkSchemas::getBenchmarkSchemas()) {
        std::cout << "QueryTupleSizesBenchmark: benchmarkSchema is " << benchmarkSchema->toString() << std::endl;
        BM_AddBenchmark(
            "BM_SimpleMapQuery",
            TestQuery::from(thisSchema).map(Attribute("value") = Attribute("key") + Attribute("value")).sink(DummySink::create()),
            SimpleBenchmarkSource::create(nodeEngine->getBufferManager(),
                                          nodeEngine->getQueryManager(),
                                          benchmarkSchema,
                                          ingestionRate,
                                          1,
                                          12),
            SimpleBenchmarkSink::create(benchmarkSchema, nodeEngine->getBufferManager()),
            ",BufferSize,SchemaSize",
            "," + std::to_string(bufferSize) + "," + std::to_string(benchmarkSchema->getSchemaSizeInBytes()));
    }
    //-----------------------------------------End of BM_SimpleMapQuery-----------------------------------------------------------------------------------------------

    //-----------------------------------------Start of BM_SimpleFilterQuery----------------------------------------------------------------------------------------------
    std::vector<uint64_t> allSelectivities;
    BenchmarkUtils::createRangeVector<uint64_t>(allSelectivities, 500, 600, 100);
    for (auto benchmarkSchema : BenchmarkSchemas::getBenchmarkSchemas()) {
        for (auto selectivity : allSelectivities) {
            BM_AddBenchmark("BM_SimpleFilterQuery",
                            TestQuery::from(thisSchema).filter(Attribute("key") < selectivity).sink(DummySink::create()),
                            SimpleBenchmarkSource::create(nodeEngine->getBufferManager(),
                                                          nodeEngine->getQueryManager(),
                                                          benchmarkSchema,
                                                          ingestionRate,
                                                          1),
                            SimpleBenchmarkSink::create(benchmarkSchema, nodeEngine->getBufferManager()),
                            ",Selectivity,BufferSize,SchemaSize",
                            "," + std::to_string(selectivity) + "," + std::to_string(bufferSize) + ","
                                + std::to_string(benchmarkSchema->getSchemaSizeInBytes()))
        }
    }
    //-----------------------------------------End of BM_SimpleFilterQuery-----------------------------------------------------------------------------------------------

    return 0;
}