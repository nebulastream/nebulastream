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
#include "../include/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp"
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <filesystem>
#include <iostream>
#include <util/BenchmarkUtils.hpp>
#include <util/SimpleBenchmarkSink.hpp>
#include <util/SimpleBenchmarkSource.hpp>
#include <vector>

using namespace NES;
using namespace NES::Benchmarking;
static uint64_t portOffset = 13;

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
    portOffset += 13;
    NES::CoordinatorConfigPtr crdConf = NES::CoordinatorConfig::create();
    crdConf->setNumWorkerThreads(1);
    crdConf->setNumberOfBuffersInGlobalBufferManager(numberOfWorkerThreads);
    crdConf->setNumberOfBuffersPerWorker(numberOfBuffersPerPipeline);
    crdConf->setNumberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool);
    crdConf->setBufferSizeInBytes(bufferSizeInBytes);
    crdConf->setRpcPort(4000 + portOffset);
    crdConf->setRestPort(8081 + portOffset);

    NES::WorkerConfigPtr wrkConf = NES::WorkerConfig::create();
    wrkConf->setNumWorkerThreads(numberOfWorkerThreads);
    wrkConf->setNumberOfBuffersInGlobalBufferManager(numberOfBuffersInGlobalBufferManager);
    wrkConf->setNumberOfBuffersPerWorker(numberOfBuffersPerPipeline);
    wrkConf->setNumberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool);
    wrkConf->setBufferSizeInBytes(bufferSizeInBytes);
    //wrkConf->setCoordinatorPort(crdConf->getRpcPort()->getValue());
    //wrkConf->setRpcPort(crdConf->getRpcPort()->getValue() + 1);
    //wrkConf->setDataPort(crdConf->getRpcPort()->getValue() + 2);
    //wrkConf->setWorkerPinList(config->getWorkerPinList()->getValue());
    //wrkConf->setSourcePinList(config->getSourcePinList()->getValue());
    //wrkConf->setCoordinatorIp(crdConf->getRestIp()->getValue());
    //wrkConf->setLocalWorkerIp(crdConf->getCoordinatorIp()->getValue());
    wrkConf->setNumaAware(true);
    std::cout << "E2EBase: Start coordinator" << std::endl;
    crd = std::make_shared<NES::NesCoordinator>(crdConf, wrkConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);


    auto benchmarkSchema = Schema::create()->addField(createField("key", NES::INT64))->addField(createField("value", NES::UINT64));
    defaultSchema = NES::Schema::create()
                        ->addField(createField("key", NES::UINT64))
                        ->addField(createField("value", NES::UINT64));

    std::string benchmarkSchemaStr =
        R"(Schema::create()->addField(createField("id", UINT64))->addField(createField("value", UINT64))->addField(createField("payload", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string benchmarkSchemaFileName = "defaultSchema.hpp";
    std::ofstream out(benchmarkSchemaFileName);
    out << benchmarkSchemaStr;
    out.close();
    crd->getNesWorker()->registerLogicalStream("benchmarkSchemaStr", benchmarkSchemaFileName);

    //input in MemoryMode

    //auto streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
    //streamCatalog->addLogicalStream(thisSchema, benchmarkSchema);
    //-----------------------------------------Start of BM_SimpleFilterQuery----------------------------------------------------------------------------------------------
    std::vector<uint64_t> allSelectivities;
    BenchmarkUtils::createRangeVector<uint64_t>(allSelectivities, 500, 600, 100);
    for (auto selectivity : allSelectivities) {
        BM_AddBenchmark("BM_SimpleFilterQuery",
                        TestQuery::from(thisSchema).filter(Attribute("key") < selectivity).filter(Attribute("value") < 500).sink(NullOutputSinkDescriptor::create()),
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

    //-----------------------------------------End of BM_SimpleFilterQuery-----------------------------------------------------------------------------------------------
    return 0;
}