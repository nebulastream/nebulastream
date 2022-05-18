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
#include "gtest/gtest.h"

#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <utility>
#include <random>

#include "../../util/DummySink.hpp"
#include "../../util/SchemaSourceDescriptor.hpp"
#include "../../util/TestQuery.hpp"
#include "../../util/TestQueryCompiler.hpp"
#include "../../util/TestSink.hpp"
#include <Catalogs/StreamCatalog.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>
//#include <Util/libcuckoo/cuckoohash_map.hh>

#include <Views/AIMBenchmark.hpp>
#include <Views/MaterializedView.hpp>
#include <Views/MaterializedViewCentered/LogBased.hpp>
#include <Views/DummyMV.hpp>
#include "QueryManagement/AdHocQueryMananger.hpp"
#include "QueryManagement/MaintenanceQueryMananger.hpp"
#include "Util/BenchmarkWriter.hpp"

using namespace NES;
class MaterializedViewLogBenchmark : public testing::Test {
public:
    static void SetUpTestCase() {
        NES::setupLogging("MaterializedViewLogBenchmark.log", NES::LOG_DEBUG);
        NES_INFO("Setup MaterializedViewLogBenchmark test class.");
    }
    void SetUp() override {
        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
        nodeEngine = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 31337, streamConf);
        schema = Schema::create()->addField("mv$id", BasicType::INT32)
                ->addField("mv$cost", BasicType::FLOAT32)->addField("mv$duration", BasicType::INT32)
                ->addField("mv$timestamp", BasicType::INT64)->addField("mv$isLongDistance", BasicType::BOOLEAN);
    }
    SchemaPtr schema;
    Runtime::NodeEnginePtr nodeEngine;
};

/**
 * Find the maximal sustainable throughput using approximation
 * Benchmark measures are written into "nebulastream/benchmark_result/<date_time>.csv"
 */
TEST_F(MaterializedViewLogBenchmark, MaxMaintenanceThroughput) {
    // Benchmark configuration parameters
    constexpr auto startThroughput = 1000; // BufferPerSecond

    constexpr auto repetitions = 2;
    constexpr auto iterationLimit = 10;
    constexpr auto slack = 0.1;
    const auto bufferSizeInByte = nodeEngine->getBufferManager()->getBufferSize();
    const auto bufferSize = std::floor(double(bufferSizeInByte) / double(schema->getSchemaSizeInBytes()));

    // Setting up MV
    //auto mv = std::make_shared<DummyMV<uint64_t>>();
    auto mv = std::make_shared<LogBasedMV<uint64_t>>();

    auto testSink = std::make_shared<TestSink>(1, schema, nodeEngine->getBufferManager());

    // initialisation of variables
    auto throughput = startThroughput;
    auto r = throughput * 2;
    auto l = 0;
    auto iterations = 0;
    auto queryId = 0;

    // stop search for max sustainable throughput if:
    // 1) iterationLimit is reached
    // 2) left limit and right limit are equal
    while (iterations != iterationLimit && l != throughput && r != throughput) {

        // initialisation of statistics variables
        float elapsed = 0;
        float mvSize = 0;

        for (int i = 0; i < repetitions; i++){
            // Start query
            SourceDescriptorPtr sourceDescriptor = getSourceDescriptor(schema, nodeEngine, bufferSizeInByte, throughput);
            auto start = startMaintenanceQuery(testSink, sourceDescriptor, nodeEngine, queryId, mv);

            // runtime
            sleep(5);

            // stop query
            stopMaintenanceQuery(testSink, nodeEngine, queryId);
            auto end = std::chrono::high_resolution_clock::now();

            // sleep to prevent interference between runs
            sleep(1);

            // update statistics
            elapsed += (float) std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            NES_INFO("Thr: " << throughput << ", " << mv->size());
            mvSize += (float) mv->size();

            // reset window
            mv->newWindow();

            // new queryId to start
            queryId++;
        }

        // take average elapsed time
        elapsed /= (float) repetitions;

        // take average of inserted MV element count
        mvSize = mvSize / ((float) repetitions * elapsed / 1000);

        auto gbPerSec = double(mvSize * schema->getSchemaSizeInBytes()) / double(1e+9);

        // sufficient performance with current ingestion rate if: "inserted MV element count" + slack == ingestion rate
        bool sufficient = mvSize + (float)(throughput * bufferSize) * slack >= throughput * bufferSize;

        if(sufficient){
            // write result into csv file
            writeResult("true, " + mv->name() + ", " + std::to_string(elapsed) +  ", "  + std::to_string(mvSize) + ", " + std::to_string(throughput * bufferSize) + ", " + std::to_string(gbPerSec));

            // move left limit to the right
            l = throughput;

        } else {
            // write result into csv file
            writeResult("false, " + mv->name() + ", " + std::to_string(elapsed) +  ", "  + std::to_string(mvSize) + ", " + std::to_string(throughput * bufferSize) + ", " + std::to_string(gbPerSec));

            // move right limit to the left
            r = throughput;
        }

        // place current throughput in the middle of both limits
        throughput = l + (r - l) / 2;

        // update iteration
        iterations++;
    }
}

/**
 * Measure the latency of adhoc queries performing concurrently to the maintenance query
 * Benchmark measures are written into "nebulastream/benchmark_result/<date_time>.csv"
 */
TEST_F(MaterializedViewLogBenchmark, AdHocLatency) {
    // Benchmark configuration parameters
    const auto throughput = 5;
    const auto bufferSizeInByte = nodeEngine->getBufferManager()->getBufferSize();
    const auto maxInsertionTime = 15; // sec
    const auto repetitions = 5;

    // setting MV
    auto mv = std::make_shared<LogBasedMV<uint64_t>>();

    auto testSink = std::make_shared<TestSink>(1 , schema, nodeEngine->getBufferManager());

    // initialisation of variables
    auto insertionTime = 14;
    auto queryId = 0;

    for (; insertionTime < maxInsertionTime; insertionTime++) {

        // initialisation of statistics variables
        float elapsed = 0;
        float mvSize = 0;
        float adhocTime = 0;

        for (int i = 0; i < repetitions; i++){
            // start maintenance query
            SourceDescriptorPtr sourceDescriptor = getSourceDescriptor(schema, nodeEngine, bufferSizeInByte, throughput);
            auto start = startMaintenanceQuery(testSink, sourceDescriptor, nodeEngine, queryId++, mv);

            // wait for maintenance query to insert tuples
            sleep(insertionTime);

            // run adhoc query
            adhocTime += runAdhocQuery(1, nodeEngine, schema, queryId++, mv);

            // ensure ad hoc completion
            sleep(2);

            // stop maintenance query
            stopMaintenanceQuery(testSink, nodeEngine, queryId-2);

            // take total elapsed time
            auto end = std::chrono::high_resolution_clock::now();

            // sleep to prevent interference between runs
            sleep(2);

            // update statistics
            elapsed += (float) std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            mvSize += (float) mv->size();

            // reset window
            mv->newWindow();
        }

        // take average of statistics
        elapsed /= (float) repetitions;
        adhocTime /= (float) repetitions;
        mvSize = mvSize / ((float) repetitions * elapsed / 1000);

        // write result into csv file
        //writeResult(mv->name() + ", " + std::to_string(elapsed) +  ", "  + std::to_string(mvSize) + ", " + std::to_string(adhocTime) + ", " + std::to_string(throughput * bufferSize) + ", " + std::to_string(insertionTime));
    }
}