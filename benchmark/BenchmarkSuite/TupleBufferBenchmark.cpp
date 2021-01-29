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


#define benchmarkSchemaI8 (Schema::create()->addField("key", BasicType::UINT8)->addField("value", BasicType::UINT16))
#define benchmarkSchemaI16 (Schema::create()->addField("key", BasicType::INT16)->addField("value", BasicType::INT16))
#define benchmarkSchemaI32 (Schema::create()->addField("key", BasicType::INT32)->addField("value", BasicType::INT32))
#define benchmarkSchemaI64 (Schema::create()->addField("key", BasicType::INT64)->addField("value", BasicType::INT64))
#define benchmarkSchemaCacheLine (Schema::create()->addField("key", BasicType::INT32)                                         \
                                         ->addField("value", BasicType::INT32)                                                \
                                         ->addField("value", BasicType::INT32)                                                \
                                         ->addField("value", BasicType::INT32)                                                \
                                         ->addField("value", BasicType::INT32)                                                \
                                         ->addField("value", BasicType::INT32)                                                \
                                         ->addField("value", BasicType::INT32)                                                \
                                         ->addField("value", BasicType::INT32)                                                \
                                         ->addField("value", BasicType::INT32)                                                \
                                         ->addField("value", BasicType::INT32)                                                \
                                         ->addField("value", BasicType::INT32)                                                \
                                         ->addField("value", BasicType::INT32)                                                \
                                         ->addField("value", BasicType::INT32)                                                \
                                         ->addField("value", BasicType::INT32)                                                \
                                         ->addField("value", BasicType::INT32)                                                \
                                         ->addField("value", BasicType::INT32))

struct TupleI8 {
    uint8_t key;
    uint16_t value;
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

struct TupleCacheLine {
    int32_t key;
    int32_t value1;
    int32_t value2;
    int32_t value3;
    int32_t value4;
    int32_t value5;
    int32_t value6;
    int32_t value7;
    int32_t value8;
    int32_t value9;
    int32_t value10;
    int32_t value11;
    int32_t value12;
    int32_t value13;
    int32_t value14;
    int32_t value15;
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
        case 3: benchmarkSchema = benchmarkSchemaI64; break;
        case 4: benchmarkSchema = benchmarkSchemaCacheLine; break;
        default: break;
    }

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    auto bufferManager = nodeEngine->getBufferManager();
    auto buffer = bufferManager->getBufferBlocking();

    uint64_t maxTuplesPerBuffer = bufferManager->getBufferSize() / benchmarkSchema->getSchemaSizeInBytes();

    switch (state.range(0)) {
        case 0: {
            for (auto singleState : state) {
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<uint8_t>(i, 0)->write(buffer, 42);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<uint16_t>(i, 1)->write(buffer, 1);
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
        case 3: {
            for (auto singleState : state) {
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int64_t>(i, 0)->write(buffer, 42);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int64_t>(i, 1)->write(buffer, 1);
                }
            }
            break;
        }
        case 4: {
            for (auto singleState : state) {
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 0)->write(buffer, 42);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 1)->write(buffer, 1);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 2)->write(buffer, 1);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 3)->write(buffer, 1);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 4)->write(buffer, 1);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 5)->write(buffer, 1);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 6)->write(buffer, 1);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 7)->write(buffer, 1);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 8)->write(buffer, 1);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 9)->write(buffer, 1);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 10)->write(buffer, 1);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 11)->write(buffer, 1);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 12)->write(buffer, 1);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 13)->write(buffer, 1);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 14)->write(buffer, 1);
                    NodeEngine::createRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 15)->write(buffer, 1);
                }
            }
            break;
        }
        default: break;
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
        case 3: benchmarkSchema = benchmarkSchemaI64; break;
        case 4: benchmarkSchema = benchmarkSchemaCacheLine; break;
        default: break;
    }

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    auto bufferManager = nodeEngine->getBufferManager();
    auto buffer = bufferManager->getBufferBlocking();

    uint64_t maxTuplesPerBuffer = bufferManager->getBufferSize() / benchmarkSchema->getSchemaSizeInBytes();
    switch (state.range(0)) {
        case 0: {
            for (auto singleState : state) {
                auto rowLayout = NodeEngine::createRowLayout(benchmarkSchema);
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    rowLayout->getValueField<uint8_t>(i, 0)->write(buffer, 42);
                    rowLayout->getValueField<uint16_t>(i, 1)->write(buffer, 1);
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
        case 3: {
            for (auto singleState : state) {
                auto rowLayout = NodeEngine::createRowLayout(benchmarkSchema);
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    rowLayout->getValueField<int64_t>(i, 0)->write(buffer, 42);
                    rowLayout->getValueField<int64_t>(i, 1)->write(buffer, 1);
                }
            }
            break;
        }
        case 4: {
            for (auto singleState : state) {
                auto rowLayout = NodeEngine::createRowLayout(benchmarkSchema);
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    rowLayout->getValueField<int32_t>(i, 0)->write(buffer, 42);
                    rowLayout->getValueField<int32_t>(i, 1)->write(buffer, 1);
                    rowLayout->getValueField<int32_t>(i, 2)->write(buffer, 1);
                    rowLayout->getValueField<int32_t>(i, 3)->write(buffer, 1);
                    rowLayout->getValueField<int32_t>(i, 4)->write(buffer, 1);
                    rowLayout->getValueField<int32_t>(i, 5)->write(buffer, 1);
                    rowLayout->getValueField<int32_t>(i, 6)->write(buffer, 1);
                    rowLayout->getValueField<int32_t>(i, 7)->write(buffer, 1);
                    rowLayout->getValueField<int32_t>(i, 8)->write(buffer, 1);
                    rowLayout->getValueField<int32_t>(i, 9)->write(buffer, 1);
                    rowLayout->getValueField<int32_t>(i, 10)->write(buffer, 1);
                    rowLayout->getValueField<int32_t>(i, 11)->write(buffer, 1);
                    rowLayout->getValueField<int32_t>(i, 12)->write(buffer, 1);
                    rowLayout->getValueField<int32_t>(i, 13)->write(buffer, 1);
                    rowLayout->getValueField<int32_t>(i, 14)->write(buffer, 1);
                    rowLayout->getValueField<int32_t>(i, 15)->write(buffer, 1);
                }
            }
            break;
        }
        default: break;
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
        case 3: benchmarkSchema = benchmarkSchemaI64; break;
        case 4: benchmarkSchema = benchmarkSchemaCacheLine; break;
        default: break;
    }

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    auto bufferManager = nodeEngine->getBufferManager();
    auto buffer = bufferManager->getBufferBlocking();

    uint64_t maxTuplesPerBuffer = 0;

    switch (state.range(0)) {
        case 0: {
            for (auto singleState : state) {
                maxTuplesPerBuffer = bufferManager->getBufferSize() / sizeof(TupleI8);
                auto tupleIt = buffer.getBufferAs<TupleI8>();
                for (uint64_t i = 0; i < maxTuplesPerBuffer; i++) {
                    tupleIt[i].key = 42;
                    tupleIt[i].value = 1;
                }
            }
            break;
        }

        case 1: {
            for (auto singleState : state) {
                maxTuplesPerBuffer = bufferManager->getBufferSize() / sizeof(TupleI16);
                auto tupleIt = buffer.getBufferAs<TupleI16>();
                for (uint64_t i = 0; i < maxTuplesPerBuffer; i++) {
                    tupleIt[i].key = 42;
                    tupleIt[i].value = 1;
                }
            }
            break;
        }

        case 2: {
            for (auto singleState : state) {
                maxTuplesPerBuffer = bufferManager->getBufferSize() / sizeof(TupleI32);
                auto tupleIt = buffer.getBufferAs<TupleI32>();
                for (uint64_t i = 0; i < maxTuplesPerBuffer; i++) {
                    tupleIt[i].key = 42;
                    tupleIt[i].value = 1;
                }
            }
            break;
        }

        case 3: {
            for (auto singleState : state) {
                maxTuplesPerBuffer = bufferManager->getBufferSize() / sizeof(TupleI64);
                auto tupleIt = buffer.getBuffer<TupleI64>();
                for (uint64_t i = 0; i < maxTuplesPerBuffer; i++) {
                    tupleIt[i].key = 42;
                    tupleIt[i].value = 1;
                }
            }
            break;
        }
        case 4: {
            for (auto singleState : state) {
                maxTuplesPerBuffer = bufferManager->getBufferSize() / sizeof(TupleCacheLine);
                auto tupleIt = buffer.getBuffer<TupleCacheLine>();
                for (uint64_t i = 0; i < maxTuplesPerBuffer; i++) {
                    tupleIt[i].key = 42;
                    tupleIt[i].value1 = 1;
                    tupleIt[i].value2 = 1;
                    tupleIt[i].value3 = 1;
                    tupleIt[i].value4 = 1;
                    tupleIt[i].value5 = 1;
                    tupleIt[i].value6 = 1;
                    tupleIt[i].value7 = 1;
                    tupleIt[i].value8 = 1;
                    tupleIt[i].value9 = 1;
                    tupleIt[i].value10 = 1;
                    tupleIt[i].value11 = 1;
                    tupleIt[i].value12 = 1;
                    tupleIt[i].value13 = 1;
                    tupleIt[i].value14 = 1;
                    tupleIt[i].value15 = 1;
                }
            }
            break;
        }
        default:
            break;
    }

    state.SetItemsProcessed(state.iterations() * maxTuplesPerBuffer);
}

#define REPETITIONS 20
/* Just for using it with  */
BENCHMARK(BM_DefaultFilling_V1)->DenseRange(4, 4, 1)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_DefaultFilling_V2)->DenseRange(4, 4, 1)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_CustomFilling)->DenseRange(4, 4, 1)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);

//BENCHMARK(BM_DefaultFilling_V1)->DenseRange(0, 3, 1)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
//BENCHMARK(BM_DefaultFilling_V2)->DenseRange(0, 3, 1)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
//BENCHMARK(BM_CustomFilling)->DenseRange(0, 3, 1)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);

int main(int argc, char** argv) {
    NESLogger->removeAllAppenders();
    NES::setupLogging("TupleBufferBenchmark.log", LOG_DEBUG);

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();

    return 0;
}
}// namespace NES::Benchmarking