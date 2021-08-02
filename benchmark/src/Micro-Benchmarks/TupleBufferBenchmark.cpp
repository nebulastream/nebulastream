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
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <benchmark/benchmark.h>
#include <random>

namespace NES::Benchmarking {

#define bufferSize (40 * 1024 * 1024)
#define benchmarkSchemaI8 (Schema::create()->addField("key", BasicType::UINT8)->addField("value", BasicType::UINT16))
#define benchmarkSchemaI16 (Schema::create()->addField("key", BasicType::INT16)->addField("value", BasicType::INT16))
#define benchmarkSchemaI32 (Schema::create()->addField("key", BasicType::INT32)->addField("value", BasicType::INT32))
#define benchmarkSchemaI64 (Schema::create()->addField("key", BasicType::INT64)->addField("value", BasicType::INT64))
#define benchmarkSchemaCacheLine                                                                                                 \
    (Schema::create()                                                                                                            \
         ->addField("key", BasicType::INT32)                                                                                     \
         ->addField("value", BasicType::INT32)                                                                                   \
         ->addField("value", BasicType::INT32)                                                                                   \
         ->addField("value", BasicType::INT32)                                                                                   \
         ->addField("value", BasicType::INT32)                                                                                   \
         ->addField("value", BasicType::INT32)                                                                                   \
         ->addField("value", BasicType::INT32)                                                                                   \
         ->addField("value", BasicType::INT32)                                                                                   \
         ->addField("value", BasicType::INT32)                                                                                   \
         ->addField("value", BasicType::INT32)                                                                                   \
         ->addField("value", BasicType::INT32)                                                                                   \
         ->addField("value", BasicType::INT32)                                                                                   \
         ->addField("value", BasicType::INT32)                                                                                   \
         ->addField("value", BasicType::INT32)                                                                                   \
         ->addField("value", BasicType::INT32)                                                                                   \
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
 * @param state.range(0): 0 == INT8, 1 == INT16, 2 == INT32, 3 == INT64, 4 == cache line
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
    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto buffer = bufferManager->getBufferBlocking();

    uint64_t maxTuplesPerBuffer = bufferManager->getBufferSize() / benchmarkSchema->getSchemaSizeInBytes();

    switch (state.range(0)) {
        case 0: {
            for (auto singleState : state) {
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<uint8_t>(i, 0)->write(buffer, 42);
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<uint16_t>(i, 1)->write(buffer, 1);
                }
            }
            break;
        }
        case 1: {
            for (auto singleState : state) {
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int16_t>(i, 0)->write(buffer, 42);
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int16_t>(i, 1)->write(buffer, 1);
                }
            }
            break;
        }
        case 2: {
            for (auto singleState : state) {
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 0)->write(buffer, 42);
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 1)->write(buffer, 1);
                }
            }
            break;
        }
        case 3: {
            for (auto singleState : state) {
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int64_t>(i, 0)->write(buffer, 42);
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int64_t>(i, 1)->write(buffer, 1);
                }
            }
            break;
        }
        case 4: {
            for (auto singleState : state) {
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 0)->write(buffer, 42);
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 1)->write(buffer, 1);
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 2)->write(buffer, 1);
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 3)->write(buffer, 1);
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 4)->write(buffer, 1);
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 5)->write(buffer, 1);
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 6)->write(buffer, 1);
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 7)->write(buffer, 1);
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 8)->write(buffer, 1);
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 9)->write(buffer, 1);
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 10)->write(buffer, 1);
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 11)->write(buffer, 1);
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 12)->write(buffer, 1);
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 13)->write(buffer, 1);
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 14)->write(buffer, 1);
                    Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema)->getValueField<int32_t>(i, 15)->write(buffer, 1);
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
 * @param state.range(0): 0 == INT8, 1 == INT16, 2 == INT32, 3 == INT64, 4 == cache line
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
    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto buffer = bufferManager->getBufferBlocking();

    uint64_t maxTuplesPerBuffer = bufferManager->getBufferSize() / benchmarkSchema->getSchemaSizeInBytes();
    switch (state.range(0)) {
        case 0: {
            for (auto singleState : state) {
                auto rowLayout = Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema);
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    rowLayout->getValueField<uint8_t>(i, 0)->write(buffer, 42);
                    rowLayout->getValueField<uint16_t>(i, 1)->write(buffer, 1);
                }
            }
            break;
        }
        case 1: {
            for (auto singleState : state) {
                auto rowLayout = Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema);
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    rowLayout->getValueField<int16_t>(i, 0)->write(buffer, 42);
                    rowLayout->getValueField<int16_t>(i, 1)->write(buffer, 1);
                }
            }
            break;
        }
        case 2: {
            for (auto singleState : state) {
                auto rowLayout = Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema);
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    rowLayout->getValueField<int32_t>(i, 0)->write(buffer, 42);
                    rowLayout->getValueField<int32_t>(i, 1)->write(buffer, 1);
                }
            }
            break;
        }
        case 3: {
            for (auto singleState : state) {
                auto rowLayout = Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema);
                for (uint64_t i = 0; i < maxTuplesPerBuffer; ++i) {
                    rowLayout->getValueField<int64_t>(i, 0)->write(buffer, 42);
                    rowLayout->getValueField<int64_t>(i, 1)->write(buffer, 1);
                }
            }
            break;
        }
        case 4: {
            for (auto singleState : state) {
                auto rowLayout = Runtime::NodeEngineFactory::createDefaultNodeEngineRowLayout(benchmarkSchema);
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
 * @param state.range(0): 0 == INT8, 1 == INT16, 2 == INT32, 3 == INT64, 4 == cache line
 */
static void BM_WriteRecordsStruct(benchmark::State& state) {

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
    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
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
        default: break;
    }

    state.SetItemsProcessed(state.iterations() * maxTuplesPerBuffer);
}

/**
 *
  * @param state.range(0): 0 == INT8, 1 == INT16, 2 == INT32, 3 == INT64, 4 == cache line
 */
static void BM_ReadRecordsStruct(benchmark::State& state) {

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
    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
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
            maxTuplesPerBuffer = bufferManager->getBufferSize() / sizeof(TupleCacheLine);
            auto tupleIt = buffer.getBuffer<TupleCacheLine>();
            for (uint64_t i = 0; i < maxTuplesPerBuffer; i++) {
                tupleIt[i].key = 1;
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

            break;
        }
        default: break;
    }

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
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0;
                int32_t tmp8 = 0, tmp9 = 0, tmp10 = 0, tmp11 = 0, tmp12 = 0, tmp13 = 0, tmp14 = 0, tmp15 = 0;
                for (uint64_t i = 0; i < maxTuplesPerBuffer; i++) {
                    tmp0 += tupleIt[i].key;
                    tmp1 += tupleIt[i].value1;
                    tmp2 += tupleIt[i].value2;
                    tmp3 += tupleIt[i].value3;
                    tmp4 += tupleIt[i].value4;
                    tmp5 += tupleIt[i].value5;
                    tmp6 += tupleIt[i].value6;
                    tmp7 += tupleIt[i].value7;
                    tmp8 += tupleIt[i].value8;
                    tmp9 += tupleIt[i].value9;
                    tmp10 += tupleIt[i].value10;
                    tmp11 += tupleIt[i].value11;
                    tmp12 += tupleIt[i].value12;
                    tmp13 += tupleIt[i].value13;
                    tmp14 += tupleIt[i].value14;
                    tmp15 += tupleIt[i].value15;
                }
                if (tmp0 != 1 * maxTuplesPerBuffer) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * maxTuplesPerBuffer");
                }
                if (tmp1 != 1 * maxTuplesPerBuffer) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * maxTuplesPerBuffer");
                }
                if (tmp2 != 1 * maxTuplesPerBuffer) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * maxTuplesPerBuffer");
                }
                if (tmp3 != 1 * maxTuplesPerBuffer) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * maxTuplesPerBuffer");
                }
                if (tmp4 != 1 * maxTuplesPerBuffer) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * maxTuplesPerBuffer");
                }
                if (tmp5 != 1 * maxTuplesPerBuffer) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * maxTuplesPerBuffer");
                }
                if (tmp6 != 1 * maxTuplesPerBuffer) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * maxTuplesPerBuffer");
                }
                if (tmp7 != 1 * maxTuplesPerBuffer) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * maxTuplesPerBuffer");
                }
                if (tmp8 != 1 * maxTuplesPerBuffer) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp8 != 1 * maxTuplesPerBuffer");
                }
                if (tmp9 != 1 * maxTuplesPerBuffer) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp9 != 1 * maxTuplesPerBuffer");
                }
                if (tmp10 != 1 * maxTuplesPerBuffer) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp10 != 1 * maxTuplesPerBuffer");
                }
                if (tmp12 != 1 * maxTuplesPerBuffer) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp12 != 1 * maxTuplesPerBuffer");
                }
                if (tmp13 != 1 * maxTuplesPerBuffer) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp13 != 1 * maxTuplesPerBuffer");
                }
                if (tmp14 != 1 * maxTuplesPerBuffer) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp14 != 1 * maxTuplesPerBuffer");
                }
                if (tmp15 != 1 * maxTuplesPerBuffer) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp15 != 1 * maxTuplesPerBuffer");
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
 * @param state.range(0): 0 == INT8, 1 == INT16, 2 == INT32, 3 == INT64, 4 == cache line
 */
static void BM_WriteFieldStruct(benchmark::State& state) {

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
    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
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
  * @param state.range(0): 0 == INT8, 1 == INT16, 2 == INT32, 3 == INT64, 4 == cache line
 */
static void BM_ReadFieldStruct(benchmark::State& state) {

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
    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
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
                int32_t tmp0 = 0;
                for (uint64_t i = 0; i < maxTuplesPerBuffer; i++) {
                    tmp0 += tupleIt[i].key;
                }
                if (tmp0 != maxTuplesPerBuffer * 1)
                    NES_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != maxTuplesPerBuffer * 1");
            }
            break;
        }
        default: break;
    }

    state.SetItemsProcessed(state.iterations() * maxTuplesPerBuffer);
}

#define REPETITIONS 20
/* Just for using it with schema benchmarkCacheLine */
BENCHMARK(BM_WriteRecordsStruct)->DenseRange(4, 4, 1)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_ReadRecordsStruct)->DenseRange(4, 4, 1)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_WriteFieldStruct)->DenseRange(4, 4, 1)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_ReadFieldStruct)->DenseRange(4, 4, 1)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);

//BENCHMARK(BM_DefaultFilling_V1)->DenseRange(0, 3, 1)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
//BENCHMARK(BM_DefaultFilling_V2)->DenseRange(0, 3, 1)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
//BENCHMARK(BM_CustomFilling)->DenseRange(0, 3, 1)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);

int main(int argc, char** argv) {
    NESLogger->removeAllAppenders();
    NES::setupLogging("TupleBufferBenchmark.log", LOG_WARNING);

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();

    return 0;
}
}// namespace NES::Benchmarking
