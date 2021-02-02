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

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <benchmark/benchmark.h>
#include <random>

namespace NES::Benchmarking {

#define REPETITIONS 20

#define benchmarkSchemaI8 (Schema::create()->addField("key", BasicType::INT8)->addField("value", BasicType::INT8))
#define benchmarkSchemaI16 (Schema::create()->addField("key", BasicType::INT16)->addField("value", BasicType::INT16))
#define benchmarkSchemaI32 (Schema::create()->addField("key", BasicType::INT32)->addField("value", BasicType::INT32))
#define benchmarkSchemaI64 (Schema::create()->addField("key", BasicType::INT64)->addField("value", BasicType::INT64))

struct TupleI8 {
    int8_t key;
    int8_t value;
};
struct TupleI16 {
    int16_t key;
    int16_t value;
};
struct TupleI32 {
    int32_t key;
    int32_t value;
};
struct TupleI64 {
    int64_t key;
    int64_t value;
};

/**
 * @brief In this version, the rowLayout will be created for every tuple insertion
  * @param state.range(0): 0 == INT8, 1 == INT16, 2 == INT32, 3 == INT64
 */
static void BM_DefaultFilling_V1(benchmark::State& state) {
    SchemaPtr benchmarkSchema;
    switch (state.range(0)) {
        case 0: benchmarkSchema = benchmarkSchemaI8; break;
        case 1: benchmarkSchema = benchmarkSchemaI16; break;
        case 2: benchmarkSchema = benchmarkSchemaI32; break;
        default: benchmarkSchema = benchmarkSchemaI64; break;
    }

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    auto bufferManager = nodeEngine->getBufferManager();
    auto buffer = bufferManager->getBufferBlocking();

    uint64_t maxTuplesPerBuffer = bufferManager->getBufferSize() / benchmarkSchema->getSchemaSizeInBytes();

    switch (state.range(0)) {
        case 0: {
            for (auto singleState : state) {
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int8_t>(i, 0)->write(buffer, 42);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int8_t>(i, 1)->write(buffer, 1);
                }
            }
            break;
        }
        case 1: {
            for (auto singleState : state) {
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int16_t>(i, 0)->write(buffer, 42);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int16_t>(i, 1)->write(buffer, 1);
                }
            }
            break;
        }
        case 2: {
            for (auto singleState : state) {
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 0)->write(buffer, 42);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 1)->write(buffer, 1);
                }
            }
            break;
        }
        default: {
            for (auto singleState : state) {
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int64_t>(i, 0)->write(buffer, 42);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int64_t>(i, 1)->write(buffer, 1);
                }
            }
            break;
        }
    }

    state.SetItemsProcessed(state.iterations() * maxTuplesPerBuffer);
}

/**
 * @brief In this version, the rowLayout will be created once and then used
  * @param state.range(0): 0 == INT8, 1 == INT16, 2 == INT32, 3 == INT64
 */
static void BM_DefaultFilling_V2(benchmark::State& state) {
    SchemaPtr benchmarkSchema;
    switch (state.range(0)) {
        case 0: benchmarkSchema = benchmarkSchemaI8; break;
        case 1: benchmarkSchema = benchmarkSchemaI16; break;
        case 2: benchmarkSchema = benchmarkSchemaI32; break;
        default: benchmarkSchema = benchmarkSchemaI64; break;
    }

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    auto bufferManager = nodeEngine->getBufferManager();
    auto buffer = bufferManager->getBufferBlocking();

    uint64_t maxTuplesPerBuffer = bufferManager->getBufferSize() / benchmarkSchema->getSchemaSizeInBytes();
    switch (state.range(0)) {
        case 0: {
            for (auto singleState : state) {
                auto rowLayout = NodeEngine::createRowLayout(benchmarkSchema);
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    rowLayout->getValueField<int8_t>(i, 0)->write(buffer, 42);
                    rowLayout->getValueField<int8_t>(i, 1)->write(buffer, 1);
                }
            }
            break;
        }
        case 1: {
            for (auto singleState : state) {
                auto rowLayout = NodeEngine::createRowLayout(benchmarkSchema);
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    rowLayout->getValueField<int16_t>(i, 0)->write(buffer, 42);
                    rowLayout->getValueField<int16_t>(i, 1)->write(buffer, 1);
                }
            }
            break;
        }
        case 2: {
            for (auto singleState : state) {
                auto rowLayout = NodeEngine::createRowLayout(benchmarkSchema);
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    rowLayout->getValueField<int32_t>(i, 0)->write(buffer, 42);
                    rowLayout->getValueField<int32_t>(i, 1)->write(buffer, 1);
                }
            }
            break;
        }
        default: {
            for (auto singleState : state) {
                auto rowLayout = NodeEngine::createRowLayout(benchmarkSchema);
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    rowLayout->getValueField<int64_t>(i, 0)->write(buffer, 42);
                    rowLayout->getValueField<int64_t>(i, 1)->write(buffer, 1);
                }
            }
            break;
        }
    }

    state.SetItemsProcessed(state.iterations() * maxTuplesPerBuffer);
}

/**
 *
  * @param state.range(0): 0 == INT8, 1 == INT16, 2 == INT32, 3 == INT64
 */
static void BM_CustomFilling(benchmark::State& state) {

    SchemaPtr benchmarkSchema;
    switch (state.range(0)) {
        case 0: benchmarkSchema = benchmarkSchemaI8; break;
        case 1: benchmarkSchema = benchmarkSchemaI16; break;
        case 2: benchmarkSchema = benchmarkSchemaI32; break;
        default: benchmarkSchema = benchmarkSchemaI64; break;
    }

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    auto bufferManager = nodeEngine->getBufferManager();
    auto buffer = bufferManager->getBufferBlocking();

    uint64_t maxTuplesPerBuffer = bufferManager->getBufferSize() / benchmarkSchema->getSchemaSizeInBytes();
    switch (state.range(0)) {
        case 0: {
            for (auto singleState : state) {
                auto tupleIt = buffer.getBuffer<TupleI8>();
                for (uint64_t i = 0; i < maxTuplesPerBuffer; i++) {
                    tupleIt[i].key = 42;
                    tupleIt[i].value = 1;
                }
            }
            break;
        }

        case 1: {
            for (auto singleState : state) {
                auto tupleIt = buffer.getBuffer<TupleI16>();
                for (uint64_t i = 0; i < maxTuplesPerBuffer; i++) {
                    tupleIt[i].key = 42;
                    tupleIt[i].value = 1;
                }
            }
            break;
        }

        case 2: {
            for (auto singleState : state) {
                auto tupleIt = buffer.getBuffer<TupleI32>();
                for (uint64_t i = 0; i < maxTuplesPerBuffer; i++) {
                    tupleIt[i].key = 42;
                    tupleIt[i].value = 1;
                }
            }
            break;
        }

        default: {
            for (auto singleState : state) {
                auto tupleIt = buffer.getBuffer<TupleI64>();
                for (uint64_t i = 0; i < maxTuplesPerBuffer; i++) {
                    tupleIt[i].key = 42;
                    tupleIt[i].value = 1;
                }
            }
            break;
        }
    }

    state.SetItemsProcessed(state.iterations() * maxTuplesPerBuffer);
}

BENCHMARK(BM_DefaultFilling_V1)->DenseRange(0, 3, 1)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_DefaultFilling_V2)->DenseRange(0, 3, 1)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_CustomFilling)->DenseRange(0, 3, 1)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);

int main(int argc, char** argv) {
    NES::setupLogging("TupleBufferBenchmark.log", LOG_WARNING);

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();

    return 0;
}
}// namespace NES::Benchmarking