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

#include "../../../tests/util/DummySink.hpp"
#include "../../../tests/util/TestQuery.hpp"
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <filesystem>
#include <iostream>
#include <util/BenchmarkUtils.hpp>
#include <util/SimpleBenchmarkSink.hpp>
#include <util/SimpleBenchmarkSource.hpp>
#include <vector>


using namespace NES;
using namespace NES::Benchmarking;

/**
 * @brief This file/main shows how a benchmark can be created. The benchmark seen below is a filter query that was implemented by using the BM_AddBenchmark macro from <util/BenchmarkUtils.hpp>.
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
    BenchmarkUtils::createRangeVector<uint16_t>(allWorkerThreads, 1, 5, 1);

    // Number of dataSources
    std::vector<uint16_t> allDataSources;
    BenchmarkUtils::createRangeVector<uint16_t>(allDataSources, 1, 2, 1);

    std::string benchmarkFolderName = "FilterQueries_" + BenchmarkUtils::getCurDateTimeStringWithNESVersion();
    if (!std::filesystem::create_directory(benchmarkFolderName)) {
        throw RuntimeException("Could not create folder " + benchmarkFolderName);
    }

    auto benchmarkSchema = Schema::create()->addField("test$key", INT32)->addField("test$value", INT32);
    auto bufferSize = 4096;
    auto numBuffers = 1024;

    //-----------------------------------------Start of BM_SimpleFilterQuery----------------------------------------------------------------------------------------------
    std::vector<uint64_t> allSelectivities;
    BenchmarkUtils::createRangeVector<uint64_t>(allSelectivities, 500, 600, 100);
/*
for (auto selectivity : allSelectivities) {
     BM_AddBenchmark("BM_FPDQuery",
                     TestQuery::from(thisSchema).filter(Attribute("key") < 50).filter(Attribute("value") < selectivity).sink(DummySink::create()),
                        SimpleBenchmarkSource::create(nodeEngine->getBufferManager(),
                                                      nodeEngine->getQueryManager(),
                                                      benchmarkSchema,
                                                      ingestionRate,
                                                      1),
                                                      SimpleBenchmarkSink::create(benchmarkSchema, nodeEngine->getBufferManager()),
                                                      ",Selectivity,BufferSize,SchemaSize",
                                                      "," + std::to_string(selectivity) + "," + std::to_string(bufferSize) + ","
                                                      + std::to_string(benchmarkSchema->getSchemaSizeInBytes()));
}
*/



        BM_AddBenchmarkCustomBufferSize("FPDQueryBenchmarks",
                                        TestQuery::from(thisSchema).filter(Attribute("key") < 50).filter(Attribute("value") < 500).sink(DummySink::create()),
                                        SimpleBenchmarkSource::create(nodeEngine->getBufferManager(),
                                                                      nodeEngine->getQueryManager(),
                                                                      benchmarkSchema,
                                                                      ingestionRate,
                                                                      1),
                                        SimpleBenchmarkSink::create(benchmarkSchema, nodeEngine->getBufferManager()),
                                        ",Selectivity,BufferSize,SchemaSize",
                                        "," + std::to_string(500) + "," + std::to_string(bufferSize) + ","
                                            + std::to_string(benchmarkSchema->getSchemaSizeInBytes()));





/*Query query =
        Query::from("default_logical").map(Attribute("value") > 30).map(Attribute("value") < 40).filter(Attribute("id") < selectivity).sink(DummySink::create());
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    /DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr mapOperator = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    Optimizer::FilterPushDownRulePtr filterPushDownRule = Optimizer::FilterPushDownRule::create();
    const QueryPlanPtr updatedPlan = filterPushDownRule->apply(queryPlan);*/

    // Validate



    /* BM_AddBenchmark("BM_SimpleFilterQuery",
                     Query(updatedPlan),
                     SimpleBenchmarkSource::create(nodeEngine->getBufferManager(),
                                                   nodeEngine->getQueryManager(),
                                                   benchmarkSchema,
                                                   ingestionRate,
                                                   1),
                                                   SimpleBenchmarkSink::create(benchmarkSchema, nodeEngine->getBufferManager()),
                                                   ",Selectivity,BufferSize,SchemaSize",
                                                   "," + std::to_string(selectivity) + "," + std::to_string(bufferSize) + ","
                                                   + std::to_string(benchmarkSchema->getSchemaSizeInBytes()));



    for (auto selectivity : allSelectivities) {
        BM_AddBenchmark("BM_UpdatedFilterQuery",
                        Query(updatedPlan),
                        SimpleBenchmarkSource::create(nodeEngine->getBufferManager(),
                                                      nodeEngine->getQueryManager(),
                                                      benchmarkSchema,
                                                      ingestionRate,
                                                      1),
                                                      SimpleBenchmarkSink::create(benchmarkSchema, nodeEngine->getBufferManager()),
                                                      ",Selectivity,BufferSize,SchemaSize",
                                                      "," + std::to_string(selectivity) + "," + std::to_string(bufferSize) + ","
                                                      + std::to_string(benchmarkSchema->getSchemaSizeInBytes()));
    }*/

    return 0;
}