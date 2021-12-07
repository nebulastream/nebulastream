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

#include <API/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutField.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/MemoryLayout/RowLayoutTupleBuffer.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <benchmark/benchmark.h>

using namespace NES::Runtime::DynamicMemoryLayout;
namespace NES::Benchmarking {

#define bufferSize (40 * 1024 * 1024)
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

static void BM_WriteRecordsRowLayoutNewLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    for (auto singleState : state) {
        state.PauseTiming();

        RowLayoutPtr rowLayout = DynamicRowLayout::create(schema, bufferManager->getBufferSize());
        DynamicRowLayoutBufferPtr bindedRowLayout = rowLayout->bind(tupleBuffer);
        state.ResumeTiming();
        std::tuple<int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t>
            writeRecord(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            bindedRowLayout->pushRecord<false>(writeRecord);
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_ReadRecordsRowLayoutNewLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    RowLayoutPtr rowLayout = DynamicRowLayout::create(schema, bufferManager->getBufferSize());
    DynamicRowLayoutBufferPtr bindedRowLayout = rowLayout->bind(tupleBuffer);

    std::tuple<int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t>
        writeRecord(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        bindedRowLayout->pushRecord<false>(writeRecord);
    }

    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            std::tuple<int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t>
                readRecord = bindedRowLayout->readRecord<false,
                                                         int32_t,
                                                         int32_t,
                                                         int32_t,
                                                         int32_t,
                                                         int32_t,
                                                         int32_t,
                                                         int32_t,
                                                         int32_t,
                                                         int32_t,
                                                         int32_t,
                                                         int32_t,
                                                         int32_t,
                                                         int32_t,
                                                         int32_t,
                                                         int32_t,
                                                         int32_t>(recordIndex);
            ((void) readRecord);
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_WriteRecordsColumnLayoutNewLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    for (auto singleState : state) {
        state.PauseTiming();

        DynamicColumnLayoutPtr columnLayout = DynamicColumnLayout::create(schema, false);
        DynamicColumnLayoutBufferPtr bindedColumnLayout = columnLayout->bind(tupleBuffer);
        state.ResumeTiming();
        std::tuple<int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t>
            writeRecord(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            bindedColumnLayout->pushRecord<false>(writeRecord);
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_ReadRecordsColumnLayoutNewLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    DynamicColumnLayoutPtr columnLayout = DynamicColumnLayout::create(schema, false);
    DynamicColumnLayoutBufferPtr bindedColumnLayout = columnLayout->bind(tupleBuffer);

    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        std::tuple<int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t>
            writeRecord(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
        bindedColumnLayout->pushRecord<false>(writeRecord);
    }

    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            std::tuple<int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t,
                       int32_t>
                readRecord = bindedColumnLayout->readRecord<false,
                                                            int32_t,
                                                            int32_t,
                                                            int32_t,
                                                            int32_t,
                                                            int32_t,
                                                            int32_t,
                                                            int32_t,
                                                            int32_t,
                                                            int32_t,
                                                            int32_t,
                                                            int32_t,
                                                            int32_t,
                                                            int32_t,
                                                            int32_t,
                                                            int32_t,
                                                            int32_t>(recordIndex);
            ((void) readRecord);
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_ReadFieldRowLayoutNewLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    RowLayoutPtr rowLayout = DynamicRowLayout::create(schema, bufferManager->getBufferSize());
    DynamicRowLayoutBufferPtr bindedRowLayout = rowLayout->bind(tupleBuffer);

    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        std::tuple<int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t,
                   int32_t>
            writeRecord(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
        bindedRowLayout->pushRecord<false>(writeRecord);
    }

    auto field0 = DynamicRowLayoutField<int32_t, false>::create(0, bindedRowLayout);
    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            int32_t tmp = field0[recordIndex];
            if (tmp != 1)
                NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp != 1");
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_ReadFieldColumnLayoutNewLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    DynamicColumnLayoutPtr columnLayout = DynamicColumnLayout::create(schema, false);
    DynamicColumnLayoutBufferPtr bindedColumnLayout = columnLayout->bind(tupleBuffer);

    std::tuple<int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t>
        writeRecord(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        bindedColumnLayout->pushRecord<false>(writeRecord);
    }

    auto field0 = DynamicColumnLayoutField<int32_t, false>::create(0, bindedColumnLayout);
    for (auto singleState : state) {
        int32_t tmp = 0;
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            tmp += field0[recordIndex];
        }
        if (tmp != NUM_TUPLES * 1)
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp != NUM_TUPLES * 1");
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_WriteFieldRowLayoutNewLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    RowLayoutPtr rowLayout = DynamicRowLayout::create(schema, bufferManager->getBufferSize());
    DynamicRowLayoutBufferPtr bindedRowLayout = rowLayout->bind(tupleBuffer);

    auto field0 = DynamicRowLayoutField<int32_t, false>::create(0, bindedRowLayout);
    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            field0[recordIndex] = 2;
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_WriteFieldColumnLayoutNewLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    DynamicColumnLayoutPtr columnLayout = DynamicColumnLayout::create(schema, false);
    DynamicColumnLayoutBufferPtr bindedColumnLayout = columnLayout->bind(tupleBuffer);

    auto field0 = DynamicColumnLayoutField<int32_t, false>::create(0, bindedColumnLayout);
    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            field0[recordIndex] = 2;
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_WriteWholeRecordWithFieldColumnLayoutNewLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    DynamicColumnLayoutPtr columnLayout = DynamicColumnLayout::create(schema, false);
    DynamicColumnLayoutBufferPtr bindedColumnLayout = columnLayout->bind(tupleBuffer);

    auto field0 = DynamicColumnLayoutField<int32_t, false>::create(0, bindedColumnLayout);
    auto field1 = DynamicColumnLayoutField<int32_t, false>::create(1, bindedColumnLayout);
    auto field2 = DynamicColumnLayoutField<int32_t, false>::create(2, bindedColumnLayout);
    auto field3 = DynamicColumnLayoutField<int32_t, false>::create(3, bindedColumnLayout);

    auto field4 = DynamicColumnLayoutField<int32_t, false>::create(4, bindedColumnLayout);
    auto field5 = DynamicColumnLayoutField<int32_t, false>::create(5, bindedColumnLayout);
    auto field6 = DynamicColumnLayoutField<int32_t, false>::create(6, bindedColumnLayout);
    auto field7 = DynamicColumnLayoutField<int32_t, false>::create(7, bindedColumnLayout);

    auto field8 = DynamicColumnLayoutField<int32_t, false>::create(8, bindedColumnLayout);
    auto field9 = DynamicColumnLayoutField<int32_t, false>::create(9, bindedColumnLayout);
    auto field10 = DynamicColumnLayoutField<int32_t, false>::create(10, bindedColumnLayout);
    auto field11 = DynamicColumnLayoutField<int32_t, false>::create(11, bindedColumnLayout);

    auto field12 = DynamicColumnLayoutField<int32_t, false>::create(12, bindedColumnLayout);
    auto field13 = DynamicColumnLayoutField<int32_t, false>::create(13, bindedColumnLayout);
    auto field14 = DynamicColumnLayoutField<int32_t, false>::create(14, bindedColumnLayout);
    auto field15 = DynamicColumnLayoutField<int32_t, false>::create(15, bindedColumnLayout);

    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            field0[recordIndex] = 2;
            field1[recordIndex] = 2;
            field2[recordIndex] = 2;
            field3[recordIndex] = 2;

            field4[recordIndex] = 2;
            field5[recordIndex] = 2;
            field6[recordIndex] = 2;
            field7[recordIndex] = 2;

            field8[recordIndex] = 2;
            field9[recordIndex] = 2;
            field10[recordIndex] = 2;
            field11[recordIndex] = 2;

            field12[recordIndex] = 2;
            field13[recordIndex] = 2;
            field14[recordIndex] = 2;
            field15[recordIndex] = 2;
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_WriteWholeRecordWithFieldRowLayoutNewLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    RowLayoutPtr rowLayout = DynamicRowLayout::create(schema, bufferManager->getBufferSize());
    DynamicRowLayoutBufferPtr bindedRowLayout = rowLayout->bind(tupleBuffer);

    auto field0 = DynamicRowLayoutField<int32_t, false>::create(0, bindedRowLayout);
    auto field1 = DynamicRowLayoutField<int32_t, false>::create(1, bindedRowLayout);
    auto field2 = DynamicRowLayoutField<int32_t, false>::create(2, bindedRowLayout);
    auto field3 = DynamicRowLayoutField<int32_t, false>::create(3, bindedRowLayout);

    auto field4 = DynamicRowLayoutField<int32_t, false>::create(4, bindedRowLayout);
    auto field5 = DynamicRowLayoutField<int32_t, false>::create(5, bindedRowLayout);
    auto field6 = DynamicRowLayoutField<int32_t, false>::create(6, bindedRowLayout);
    auto field7 = DynamicRowLayoutField<int32_t, false>::create(7, bindedRowLayout);

    auto field8 = DynamicRowLayoutField<int32_t, false>::create(8, bindedRowLayout);
    auto field9 = DynamicRowLayoutField<int32_t, false>::create(9, bindedRowLayout);
    auto field10 = DynamicRowLayoutField<int32_t, false>::create(10, bindedRowLayout);
    auto field11 = DynamicRowLayoutField<int32_t, false>::create(11, bindedRowLayout);

    auto field12 = DynamicRowLayoutField<int32_t, false>::create(12, bindedRowLayout);
    auto field13 = DynamicRowLayoutField<int32_t, false>::create(13, bindedRowLayout);
    auto field14 = DynamicRowLayoutField<int32_t, false>::create(14, bindedRowLayout);
    auto field15 = DynamicRowLayoutField<int32_t, false>::create(15, bindedRowLayout);

    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            field0[recordIndex] = 2;
            field1[recordIndex] = 2;
            field2[recordIndex] = 2;
            field3[recordIndex] = 2;

            field4[recordIndex] = 2;
            field5[recordIndex] = 2;
            field6[recordIndex] = 2;
            field7[recordIndex] = 2;

            field8[recordIndex] = 2;
            field9[recordIndex] = 2;
            field10[recordIndex] = 2;
            field11[recordIndex] = 2;

            field12[recordIndex] = 2;
            field13[recordIndex] = 2;
            field14[recordIndex] = 2;
            field15[recordIndex] = 2;
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_ReadWholeRecordWithFieldColumnLayoutNewLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    DynamicColumnLayoutPtr columnLayout = DynamicColumnLayout::create(schema, false);
    DynamicColumnLayoutBufferPtr bindedColumnLayout = columnLayout->bind(tupleBuffer);

    std::tuple<int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t>
        writeRecord(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        bindedColumnLayout->pushRecord<false>(writeRecord);
    }

    auto field0 = DynamicColumnLayoutField<int32_t, false>::create(0, bindedColumnLayout);
    auto field1 = DynamicColumnLayoutField<int32_t, false>::create(1, bindedColumnLayout);
    auto field2 = DynamicColumnLayoutField<int32_t, false>::create(2, bindedColumnLayout);
    auto field3 = DynamicColumnLayoutField<int32_t, false>::create(3, bindedColumnLayout);

    auto field4 = DynamicColumnLayoutField<int32_t, false>::create(4, bindedColumnLayout);
    auto field5 = DynamicColumnLayoutField<int32_t, false>::create(5, bindedColumnLayout);
    auto field6 = DynamicColumnLayoutField<int32_t, false>::create(6, bindedColumnLayout);
    auto field7 = DynamicColumnLayoutField<int32_t, false>::create(7, bindedColumnLayout);

    auto field8 = DynamicColumnLayoutField<int32_t, false>::create(8, bindedColumnLayout);
    auto field9 = DynamicColumnLayoutField<int32_t, false>::create(9, bindedColumnLayout);
    auto field10 = DynamicColumnLayoutField<int32_t, false>::create(10, bindedColumnLayout);
    auto field11 = DynamicColumnLayoutField<int32_t, false>::create(11, bindedColumnLayout);

    auto field12 = DynamicColumnLayoutField<int32_t, false>::create(12, bindedColumnLayout);
    auto field13 = DynamicColumnLayoutField<int32_t, false>::create(13, bindedColumnLayout);
    auto field14 = DynamicColumnLayoutField<int32_t, false>::create(14, bindedColumnLayout);
    auto field15 = DynamicColumnLayoutField<int32_t, false>::create(15, bindedColumnLayout);

    for (auto singleState : state) {
        int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0;
        int32_t tmp8 = 0, tmp9 = 0, tmp10 = 0, tmp11 = 0, tmp12 = 0, tmp13 = 0, tmp14 = 0, tmp15 = 0;
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            tmp0 += field0[recordIndex];
            tmp1 += field1[recordIndex];
            tmp2 += field2[recordIndex];
            tmp3 += field3[recordIndex];
            tmp4 += field4[recordIndex];
            tmp5 += field5[recordIndex];
            tmp6 += field6[recordIndex];
            tmp7 += field7[recordIndex];
            tmp8 += field8[recordIndex];
            tmp9 += field9[recordIndex];
            tmp10 += field10[recordIndex];
            tmp11 += field11[recordIndex];
            tmp12 += field12[recordIndex];
            tmp13 += field13[recordIndex];
            tmp14 += field14[recordIndex];
            tmp15 += field15[recordIndex];
        }
        if (tmp0 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * maxTuplesPerBuffer");
        }
        if (tmp1 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * maxTuplesPerBuffer");
        }
        if (tmp2 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * maxTuplesPerBuffer");
        }
        if (tmp3 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * maxTuplesPerBuffer");
        }
        if (tmp4 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
        }
        if (tmp5 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
        }
        if (tmp6 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
        }
        if (tmp7 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * NUM_TUPLES");
        }
        if (tmp8 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp8 != 1 * NUM_TUPLES");
        }
        if (tmp9 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp9 != 1 * NUM_TUPLES");
        }
        if (tmp10 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp10 != 1 * NUM_TUPLES");
        }
        if (tmp12 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp12 != 1 * NUM_TUPLES");
        }
        if (tmp13 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp13 != 1 * NUM_TUPLES");
        }
        if (tmp14 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp14 != 1 * NUM_TUPLES");
        }
        if (tmp15 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp15 != 1 * NUM_TUPLES");
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_ReadWholeRecordWithFieldRowLayoutNewLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    RowLayoutPtr rowLayout = DynamicRowLayout::create(schema, bufferManager->getBufferSize());
    DynamicRowLayoutBufferPtr bindedRowLayout = rowLayout->bind(tupleBuffer);

    std::tuple<int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t>
        writeRecord(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        bindedRowLayout->pushRecord<false>(writeRecord);
    }

    auto field0 = DynamicRowLayoutField<int32_t, false>::create(0, bindedRowLayout);
    auto field1 = DynamicRowLayoutField<int32_t, false>::create(1, bindedRowLayout);
    auto field2 = DynamicRowLayoutField<int32_t, false>::create(2, bindedRowLayout);
    auto field3 = DynamicRowLayoutField<int32_t, false>::create(3, bindedRowLayout);

    auto field4 = DynamicRowLayoutField<int32_t, false>::create(4, bindedRowLayout);
    auto field5 = DynamicRowLayoutField<int32_t, false>::create(5, bindedRowLayout);
    auto field6 = DynamicRowLayoutField<int32_t, false>::create(6, bindedRowLayout);
    auto field7 = DynamicRowLayoutField<int32_t, false>::create(7, bindedRowLayout);

    auto field8 = DynamicRowLayoutField<int32_t, false>::create(8, bindedRowLayout);
    auto field9 = DynamicRowLayoutField<int32_t, false>::create(9, bindedRowLayout);
    auto field10 = DynamicRowLayoutField<int32_t, false>::create(10, bindedRowLayout);
    auto field11 = DynamicRowLayoutField<int32_t, false>::create(11, bindedRowLayout);

    auto field12 = DynamicRowLayoutField<int32_t, false>::create(12, bindedRowLayout);
    auto field13 = DynamicRowLayoutField<int32_t, false>::create(13, bindedRowLayout);
    auto field14 = DynamicRowLayoutField<int32_t, false>::create(14, bindedRowLayout);
    auto field15 = DynamicRowLayoutField<int32_t, false>::create(15, bindedRowLayout);

    for (auto singleState : state) {
        int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0;
        int32_t tmp8 = 0, tmp9 = 0, tmp10 = 0, tmp11 = 0, tmp12 = 0, tmp13 = 0, tmp14 = 0, tmp15 = 0;
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            tmp0 += field0[recordIndex];
            tmp1 += field1[recordIndex];
            tmp2 += field2[recordIndex];
            tmp3 += field3[recordIndex];
            tmp4 += field4[recordIndex];
            tmp5 += field5[recordIndex];
            tmp6 += field6[recordIndex];
            tmp7 += field7[recordIndex];
            tmp8 += field8[recordIndex];
            tmp9 += field9[recordIndex];
            tmp10 += field10[recordIndex];
            tmp11 += field11[recordIndex];
            tmp12 += field12[recordIndex];
            tmp13 += field13[recordIndex];
            tmp14 += field14[recordIndex];
            tmp15 += field15[recordIndex];
        }
        if (tmp0 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * maxTuplesPerBuffer");
        }
        if (tmp1 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * maxTuplesPerBuffer");
        }
        if (tmp2 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * maxTuplesPerBuffer");
        }
        if (tmp3 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * maxTuplesPerBuffer");
        }
        if (tmp4 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
        }
        if (tmp5 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
        }
        if (tmp6 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
        }
        if (tmp7 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * NUM_TUPLES");
        }
        if (tmp8 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp8 != 1 * NUM_TUPLES");
        }
        if (tmp9 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp9 != 1 * NUM_TUPLES");
        }
        if (tmp10 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp10 != 1 * NUM_TUPLES");
        }
        if (tmp12 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp12 != 1 * NUM_TUPLES");
        }
        if (tmp13 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp13 != 1 * NUM_TUPLES");
        }
        if (tmp14 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp14 != 1 * NUM_TUPLES");
        }
        if (tmp15 != 1 * NUM_TUPLES) {
            NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp15 != 1 * NUM_TUPLES");
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

/**
 *
 * @param state 0
 */
static void BM_ReadingNumberOfFieldsRowLayoutNewLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());
    RowLayoutPtr rowLayout = DynamicRowLayout::create(schema, bufferManager->getBufferSize());
    DynamicRowLayoutBufferPtr bindedRowLayout = rowLayout->bind(tupleBuffer);
    std::tuple<int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t>
        writeRecord(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        bindedRowLayout->pushRecord<false>(writeRecord);
    }

    auto field0 = DynamicRowLayoutField<int32_t, false>::create(0, bindedRowLayout);
    auto field1 = DynamicRowLayoutField<int32_t, false>::create(1, bindedRowLayout);
    auto field2 = DynamicRowLayoutField<int32_t, false>::create(2, bindedRowLayout);
    auto field3 = DynamicRowLayoutField<int32_t, false>::create(3, bindedRowLayout);

    auto field4 = DynamicRowLayoutField<int32_t, false>::create(4, bindedRowLayout);
    auto field5 = DynamicRowLayoutField<int32_t, false>::create(5, bindedRowLayout);
    auto field6 = DynamicRowLayoutField<int32_t, false>::create(6, bindedRowLayout);
    auto field7 = DynamicRowLayoutField<int32_t, false>::create(7, bindedRowLayout);

    auto field8 = DynamicRowLayoutField<int32_t, false>::create(8, bindedRowLayout);
    auto field9 = DynamicRowLayoutField<int32_t, false>::create(9, bindedRowLayout);
    auto field10 = DynamicRowLayoutField<int32_t, false>::create(10, bindedRowLayout);
    auto field11 = DynamicRowLayoutField<int32_t, false>::create(11, bindedRowLayout);

    auto field12 = DynamicRowLayoutField<int32_t, false>::create(12, bindedRowLayout);
    auto field13 = DynamicRowLayoutField<int32_t, false>::create(13, bindedRowLayout);
    auto field14 = DynamicRowLayoutField<int32_t, false>::create(14, bindedRowLayout);
    auto field15 = DynamicRowLayoutField<int32_t, false>::create(15, bindedRowLayout);

    switch (state.range(0)) {
        case 0: {
            for (auto singleState : state) {
                int32_t tmp0 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 1: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 = field0[recordIndex];
                    tmp1 = field1[recordIndex];

                    if (tmp0 != 1) {
                        NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1");
                    }
                    if (tmp1 != 1) {
                        NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1");
                    }
                }
                ((void) tmp0);
                ((void) tmp1);
            }
            break;
        }
        case 2: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 3: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 4: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 5: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 6: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                    tmp6 += field6[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
                if (tmp6 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 7: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                    tmp6 += field6[recordIndex];
                    tmp7 += field7[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
                if (tmp6 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
                }
                if (tmp7 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 8: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0, tmp8 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                    tmp6 += field6[recordIndex];
                    tmp7 += field7[recordIndex];
                    tmp8 += field8[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
                if (tmp6 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
                }
                if (tmp7 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * NUM_TUPLES");
                }
                if (tmp8 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp8 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 9: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0;
                int32_t tmp8 = 0, tmp9 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                    tmp6 += field6[recordIndex];
                    tmp7 += field7[recordIndex];
                    tmp8 += field8[recordIndex];
                    tmp9 += field9[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
                if (tmp6 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
                }
                if (tmp7 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * NUM_TUPLES");
                }
                if (tmp8 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp8 != 1 * NUM_TUPLES");
                }
                if (tmp9 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp9 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 10: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0;
                int32_t tmp8 = 0, tmp9 = 0, tmp10 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                    tmp6 += field6[recordIndex];
                    tmp7 += field7[recordIndex];
                    tmp8 += field8[recordIndex];
                    tmp9 += field9[recordIndex];
                    tmp10 += field10[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
                if (tmp6 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
                }
                if (tmp7 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * NUM_TUPLES");
                }
                if (tmp8 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp8 != 1 * NUM_TUPLES");
                }
                if (tmp9 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp9 != 1 * NUM_TUPLES");
                }
                if (tmp10 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp10 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 11: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0;
                int32_t tmp8 = 0, tmp9 = 0, tmp10 = 0, tmp11 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                    tmp6 += field6[recordIndex];
                    tmp7 += field7[recordIndex];
                    tmp8 += field8[recordIndex];
                    tmp9 += field9[recordIndex];
                    tmp10 += field10[recordIndex];
                    tmp11 += field11[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
                if (tmp6 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
                }
                if (tmp7 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * NUM_TUPLES");
                }
                if (tmp8 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp8 != 1 * NUM_TUPLES");
                }
                if (tmp9 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp9 != 1 * NUM_TUPLES");
                }
                if (tmp10 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp10 != 1 * NUM_TUPLES");
                }
                if (tmp11 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp11 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 12: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0;
                int32_t tmp8 = 0, tmp9 = 0, tmp10 = 0, tmp11 = 0, tmp12 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                    tmp6 += field6[recordIndex];
                    tmp7 += field7[recordIndex];
                    tmp8 += field8[recordIndex];
                    tmp9 += field9[recordIndex];
                    tmp10 += field10[recordIndex];
                    tmp11 += field11[recordIndex];
                    tmp12 += field12[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
                if (tmp6 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
                }
                if (tmp7 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * NUM_TUPLES");
                }
                if (tmp8 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp8 != 1 * NUM_TUPLES");
                }
                if (tmp9 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp9 != 1 * NUM_TUPLES");
                }
                if (tmp10 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp10 != 1 * NUM_TUPLES");
                }
                if (tmp12 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp12 != 1 * NUM_TUPLES");
                }
            }

            break;
        }
        case 13: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0;
                int32_t tmp8 = 0, tmp9 = 0, tmp10 = 0, tmp11 = 0, tmp12 = 0, tmp13 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                    tmp6 += field6[recordIndex];
                    tmp7 += field7[recordIndex];
                    tmp8 += field8[recordIndex];
                    tmp9 += field9[recordIndex];
                    tmp10 += field10[recordIndex];
                    tmp11 += field11[recordIndex];
                    tmp12 += field12[recordIndex];
                    tmp13 += field13[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
                if (tmp6 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
                }
                if (tmp7 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * NUM_TUPLES");
                }
                if (tmp8 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp8 != 1 * NUM_TUPLES");
                }
                if (tmp9 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp9 != 1 * NUM_TUPLES");
                }
                if (tmp10 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp10 != 1 * NUM_TUPLES");
                }
                if (tmp12 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp12 != 1 * NUM_TUPLES");
                }
                if (tmp13 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp13 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 14: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0;
                int32_t tmp8 = 0, tmp9 = 0, tmp10 = 0, tmp11 = 0, tmp12 = 0, tmp13 = 0, tmp14 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                    tmp6 += field6[recordIndex];
                    tmp7 += field7[recordIndex];
                    tmp8 += field8[recordIndex];
                    tmp9 += field9[recordIndex];
                    tmp10 += field10[recordIndex];
                    tmp11 += field11[recordIndex];
                    tmp12 += field12[recordIndex];
                    tmp13 += field13[recordIndex];
                    tmp14 += field14[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
                if (tmp6 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
                }
                if (tmp7 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * NUM_TUPLES");
                }
                if (tmp8 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp8 != 1 * NUM_TUPLES");
                }
                if (tmp9 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp9 != 1 * NUM_TUPLES");
                }
                if (tmp10 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp10 != 1 * NUM_TUPLES");
                }
                if (tmp12 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp12 != 1 * NUM_TUPLES");
                }
                if (tmp13 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp13 != 1 * NUM_TUPLES");
                }
                if (tmp14 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp14 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 15: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0;
                int32_t tmp8 = 0, tmp9 = 0, tmp10 = 0, tmp11 = 0, tmp12 = 0, tmp13 = 0, tmp14 = 0, tmp15 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                    tmp6 += field6[recordIndex];
                    tmp7 += field7[recordIndex];
                    tmp8 += field8[recordIndex];
                    tmp9 += field9[recordIndex];
                    tmp10 += field10[recordIndex];
                    tmp11 += field11[recordIndex];
                    tmp12 += field12[recordIndex];
                    tmp13 += field13[recordIndex];
                    tmp14 += field14[recordIndex];
                    tmp15 += field15[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
                if (tmp6 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
                }
                if (tmp7 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * NUM_TUPLES");
                }
                if (tmp8 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp8 != 1 * NUM_TUPLES");
                }
                if (tmp9 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp9 != 1 * NUM_TUPLES");
                }
                if (tmp10 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp10 != 1 * NUM_TUPLES");
                }
                if (tmp12 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp12 != 1 * NUM_TUPLES");
                }
                if (tmp13 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp13 != 1 * NUM_TUPLES");
                }
                if (tmp14 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp14 != 1 * NUM_TUPLES");
                }
                if (tmp15 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp15 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
    }

    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_ReadingNumberOfFieldsColLayoutNewLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());
    DynamicColumnLayoutPtr colLayout = DynamicColumnLayout::create(schema, false);
    DynamicColumnLayoutBufferPtr bindedColumnLayout = colLayout->bind(tupleBuffer);
    state.ResumeTiming();
    std::tuple<int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t>
        writeRecord(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        bindedColumnLayout->pushRecord<false>(writeRecord);
    }

    auto field0 = DynamicColumnLayoutField<int32_t, false>::create(0, bindedColumnLayout);
    auto field1 = DynamicColumnLayoutField<int32_t, false>::create(1, bindedColumnLayout);
    auto field2 = DynamicColumnLayoutField<int32_t, false>::create(2, bindedColumnLayout);
    auto field3 = DynamicColumnLayoutField<int32_t, false>::create(3, bindedColumnLayout);

    auto field4 = DynamicColumnLayoutField<int32_t, false>::create(4, bindedColumnLayout);
    auto field5 = DynamicColumnLayoutField<int32_t, false>::create(5, bindedColumnLayout);
    auto field6 = DynamicColumnLayoutField<int32_t, false>::create(6, bindedColumnLayout);
    auto field7 = DynamicColumnLayoutField<int32_t, false>::create(7, bindedColumnLayout);

    auto field8 = DynamicColumnLayoutField<int32_t, false>::create(8, bindedColumnLayout);
    auto field9 = DynamicColumnLayoutField<int32_t, false>::create(9, bindedColumnLayout);
    auto field10 = DynamicColumnLayoutField<int32_t, false>::create(10, bindedColumnLayout);
    auto field11 = DynamicColumnLayoutField<int32_t, false>::create(11, bindedColumnLayout);

    auto field12 = DynamicColumnLayoutField<int32_t, false>::create(12, bindedColumnLayout);
    auto field13 = DynamicColumnLayoutField<int32_t, false>::create(13, bindedColumnLayout);
    auto field14 = DynamicColumnLayoutField<int32_t, false>::create(14, bindedColumnLayout);
    auto field15 = DynamicColumnLayoutField<int32_t, false>::create(15, bindedColumnLayout);

    switch (state.range(0)) {
        case 0: {
            for (auto singleState : state) {
                int32_t tmp0 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 1: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 = field0[recordIndex];
                    tmp1 = field1[recordIndex];

                    if (tmp0 != 1) {
                        NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1");
                    }
                    if (tmp1 != 1) {
                        NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1");
                    }
                }
                ((void) tmp0);
                ((void) tmp1);
            }
            break;
        }
        case 2: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 3: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 4: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 5: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 6: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                    tmp6 += field6[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
                if (tmp6 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 7: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                    tmp6 += field6[recordIndex];
                    tmp7 += field7[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
                if (tmp6 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
                }
                if (tmp7 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 8: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0, tmp8 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                    tmp6 += field6[recordIndex];
                    tmp7 += field7[recordIndex];
                    tmp8 += field8[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
                if (tmp6 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
                }
                if (tmp7 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * NUM_TUPLES");
                }
                if (tmp8 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp8 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 9: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0;
                int32_t tmp8 = 0, tmp9 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                    tmp6 += field6[recordIndex];
                    tmp7 += field7[recordIndex];
                    tmp8 += field8[recordIndex];
                    tmp9 += field9[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
                if (tmp6 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
                }
                if (tmp7 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * NUM_TUPLES");
                }
                if (tmp8 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp8 != 1 * NUM_TUPLES");
                }
                if (tmp9 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp9 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 10: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0;
                int32_t tmp8 = 0, tmp9 = 0, tmp10 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                    tmp6 += field6[recordIndex];
                    tmp7 += field7[recordIndex];
                    tmp8 += field8[recordIndex];
                    tmp9 += field9[recordIndex];
                    tmp10 += field10[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
                if (tmp6 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
                }
                if (tmp7 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * NUM_TUPLES");
                }
                if (tmp8 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp8 != 1 * NUM_TUPLES");
                }
                if (tmp9 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp9 != 1 * NUM_TUPLES");
                }
                if (tmp10 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp10 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 11: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0;
                int32_t tmp8 = 0, tmp9 = 0, tmp10 = 0, tmp11 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                    tmp6 += field6[recordIndex];
                    tmp7 += field7[recordIndex];
                    tmp8 += field8[recordIndex];
                    tmp9 += field9[recordIndex];
                    tmp10 += field10[recordIndex];
                    tmp11 += field11[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
                if (tmp6 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
                }
                if (tmp7 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * NUM_TUPLES");
                }
                if (tmp8 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp8 != 1 * NUM_TUPLES");
                }
                if (tmp9 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp9 != 1 * NUM_TUPLES");
                }
                if (tmp10 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp10 != 1 * NUM_TUPLES");
                }
                if (tmp11 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp11 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 12: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0;
                int32_t tmp8 = 0, tmp9 = 0, tmp10 = 0, tmp11 = 0, tmp12 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                    tmp6 += field6[recordIndex];
                    tmp7 += field7[recordIndex];
                    tmp8 += field8[recordIndex];
                    tmp9 += field9[recordIndex];
                    tmp10 += field10[recordIndex];
                    tmp11 += field11[recordIndex];
                    tmp12 += field12[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
                if (tmp6 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
                }
                if (tmp7 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * NUM_TUPLES");
                }
                if (tmp8 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp8 != 1 * NUM_TUPLES");
                }
                if (tmp9 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp9 != 1 * NUM_TUPLES");
                }
                if (tmp10 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp10 != 1 * NUM_TUPLES");
                }
                if (tmp12 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp12 != 1 * NUM_TUPLES");
                }
            }

            break;
        }
        case 13: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0;
                int32_t tmp8 = 0, tmp9 = 0, tmp10 = 0, tmp11 = 0, tmp12 = 0, tmp13 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                    tmp6 += field6[recordIndex];
                    tmp7 += field7[recordIndex];
                    tmp8 += field8[recordIndex];
                    tmp9 += field9[recordIndex];
                    tmp10 += field10[recordIndex];
                    tmp11 += field11[recordIndex];
                    tmp12 += field12[recordIndex];
                    tmp13 += field13[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
                if (tmp6 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
                }
                if (tmp7 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * NUM_TUPLES");
                }
                if (tmp8 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp8 != 1 * NUM_TUPLES");
                }
                if (tmp9 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp9 != 1 * NUM_TUPLES");
                }
                if (tmp10 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp10 != 1 * NUM_TUPLES");
                }
                if (tmp12 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp12 != 1 * NUM_TUPLES");
                }
                if (tmp13 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp13 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 14: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0;
                int32_t tmp8 = 0, tmp9 = 0, tmp10 = 0, tmp11 = 0, tmp12 = 0, tmp13 = 0, tmp14 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                    tmp6 += field6[recordIndex];
                    tmp7 += field7[recordIndex];
                    tmp8 += field8[recordIndex];
                    tmp9 += field9[recordIndex];
                    tmp10 += field10[recordIndex];
                    tmp11 += field11[recordIndex];
                    tmp12 += field12[recordIndex];
                    tmp13 += field13[recordIndex];
                    tmp14 += field14[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
                if (tmp6 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
                }
                if (tmp7 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * NUM_TUPLES");
                }
                if (tmp8 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp8 != 1 * NUM_TUPLES");
                }
                if (tmp9 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp9 != 1 * NUM_TUPLES");
                }
                if (tmp10 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp10 != 1 * NUM_TUPLES");
                }
                if (tmp12 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp12 != 1 * NUM_TUPLES");
                }
                if (tmp13 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp13 != 1 * NUM_TUPLES");
                }
                if (tmp14 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp14 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
        case 15: {
            for (auto singleState : state) {
                int32_t tmp0 = 0, tmp1 = 0, tmp2 = 0, tmp3 = 0, tmp4 = 0, tmp5 = 0, tmp6 = 0, tmp7 = 0;
                int32_t tmp8 = 0, tmp9 = 0, tmp10 = 0, tmp11 = 0, tmp12 = 0, tmp13 = 0, tmp14 = 0, tmp15 = 0;
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    tmp0 += field0[recordIndex];
                    tmp1 += field1[recordIndex];
                    tmp2 += field2[recordIndex];
                    tmp3 += field3[recordIndex];
                    tmp4 += field4[recordIndex];
                    tmp5 += field5[recordIndex];
                    tmp6 += field6[recordIndex];
                    tmp7 += field7[recordIndex];
                    tmp8 += field8[recordIndex];
                    tmp9 += field9[recordIndex];
                    tmp10 += field10[recordIndex];
                    tmp11 += field11[recordIndex];
                    tmp12 += field12[recordIndex];
                    tmp13 += field13[recordIndex];
                    tmp14 += field14[recordIndex];
                    tmp15 += field15[recordIndex];
                }
                if (tmp0 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp0 != 1 * NUM_TUPLES");
                }
                if (tmp1 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp1 != 1 * NUM_TUPLES");
                }
                if (tmp2 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp2 != 1 * NUM_TUPLES");
                }
                if (tmp3 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp3 != 1 * NUM_TUPLES");
                }
                if (tmp4 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp4 != 1 * NUM_TUPLES");
                }
                if (tmp5 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp5 != 1 * NUM_TUPLES");
                }
                if (tmp6 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp6 != 1 * NUM_TUPLES");
                }
                if (tmp7 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp7 != 1 * NUM_TUPLES");
                }
                if (tmp8 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp8 != 1 * NUM_TUPLES");
                }
                if (tmp9 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp9 != 1 * NUM_TUPLES");
                }
                if (tmp10 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp10 != 1 * NUM_TUPLES");
                }
                if (tmp12 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp12 != 1 * NUM_TUPLES");
                }
                if (tmp13 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp13 != 1 * NUM_TUPLES");
                }
                if (tmp14 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp14 != 1 * NUM_TUPLES");
                }
                if (tmp15 != 1 * NUM_TUPLES) {
                    NES_THROW_RUNTIME_ERROR("BenchmarkDynamicMemoryLayout: tmp15 != 1 * NUM_TUPLES");
                }
            }
            break;
        }
    }

    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_WritingNumberOfFieldsRowLayoutNewLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());
    RowLayoutPtr rowLayout = DynamicRowLayout::create(schema, bufferManager->getBufferSize());
    DynamicRowLayoutBufferPtr bindedRowLayout = rowLayout->bind(tupleBuffer);
    state.ResumeTiming();
    std::tuple<int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t>
        writeRecord(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        bindedRowLayout->pushRecord<false>(writeRecord);
    }

    auto field0 = DynamicRowLayoutField<int32_t, false>::create(0, bindedRowLayout);
    auto field1 = DynamicRowLayoutField<int32_t, false>::create(1, bindedRowLayout);
    auto field2 = DynamicRowLayoutField<int32_t, false>::create(2, bindedRowLayout);
    auto field3 = DynamicRowLayoutField<int32_t, false>::create(3, bindedRowLayout);

    auto field4 = DynamicRowLayoutField<int32_t, false>::create(4, bindedRowLayout);
    auto field5 = DynamicRowLayoutField<int32_t, false>::create(5, bindedRowLayout);
    auto field6 = DynamicRowLayoutField<int32_t, false>::create(6, bindedRowLayout);
    auto field7 = DynamicRowLayoutField<int32_t, false>::create(7, bindedRowLayout);

    auto field8 = DynamicRowLayoutField<int32_t, false>::create(8, bindedRowLayout);
    auto field9 = DynamicRowLayoutField<int32_t, false>::create(9, bindedRowLayout);
    auto field10 = DynamicRowLayoutField<int32_t, false>::create(10, bindedRowLayout);
    auto field11 = DynamicRowLayoutField<int32_t, false>::create(11, bindedRowLayout);

    auto field12 = DynamicRowLayoutField<int32_t, false>::create(12, bindedRowLayout);
    auto field13 = DynamicRowLayoutField<int32_t, false>::create(13, bindedRowLayout);
    auto field14 = DynamicRowLayoutField<int32_t, false>::create(14, bindedRowLayout);
    auto field15 = DynamicRowLayoutField<int32_t, false>::create(15, bindedRowLayout);

    switch (state.range(0)) {
        case 0: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                }
            }
            break;
        }
        case 1: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                }
            }
            break;
        }
        case 2: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                }
            }
            break;
        }
        case 3: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                }
            }
            break;
        }
        case 4: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                }
            }
            break;
        }
        case 5: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                }
            }
            break;
        }
        case 6: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                    field6[recordIndex] = 1;
                }
            }
            break;
        }
        case 7: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                    field6[recordIndex] = 1;
                    field7[recordIndex] = 1;
                }
            }
            break;
        }
        case 8: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                    field6[recordIndex] = 1;
                    field7[recordIndex] = 1;
                    field8[recordIndex] = 1;
                }
            }
            break;
        }
        case 9: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                    field6[recordIndex] = 1;
                    field7[recordIndex] = 1;
                    field8[recordIndex] = 1;
                    field9[recordIndex] = 1;
                }
            }
            break;
        }
        case 10: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                    field6[recordIndex] = 1;
                    field7[recordIndex] = 1;
                    field8[recordIndex] = 1;
                    field9[recordIndex] = 1;
                    field10[recordIndex] = 1;
                }
            }
            break;
        }
        case 11: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                    field6[recordIndex] = 1;
                    field7[recordIndex] = 1;
                    field8[recordIndex] = 1;
                    field9[recordIndex] = 1;
                    field10[recordIndex] = 1;
                    field11[recordIndex] = 1;
                }
            }
            break;
        }
        case 12: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                    field6[recordIndex] = 1;
                    field7[recordIndex] = 1;
                    field8[recordIndex] = 1;
                    field9[recordIndex] = 1;
                    field10[recordIndex] = 1;
                    field11[recordIndex] = 1;
                    field12[recordIndex] = 1;
                }
            }
            break;
        }
        case 13: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                    field6[recordIndex] = 1;
                    field7[recordIndex] = 1;
                    field8[recordIndex] = 1;
                    field9[recordIndex] = 1;
                    field10[recordIndex] = 1;
                    field11[recordIndex] = 1;
                    field12[recordIndex] = 1;
                    field13[recordIndex] = 1;
                }
            }
            break;
        }
        case 14: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                    field6[recordIndex] = 1;
                    field7[recordIndex] = 1;
                    field8[recordIndex] = 1;
                    field9[recordIndex] = 1;
                    field10[recordIndex] = 1;
                    field11[recordIndex] = 1;
                    field12[recordIndex] = 1;
                    field13[recordIndex] = 1;
                    field14[recordIndex] = 1;
                }
            }
            break;
        }
        case 15: {
            for (auto singleState : state) {

                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                    field6[recordIndex] = 1;
                    field7[recordIndex] = 1;
                    field8[recordIndex] = 1;
                    field9[recordIndex] = 1;
                    field10[recordIndex] = 1;
                    field11[recordIndex] = 1;
                    field12[recordIndex] = 1;
                    field13[recordIndex] = 1;
                    field14[recordIndex] = 1;
                    field15[recordIndex] = 1;
                }
            }
            break;
        }
    }

    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_WritingNumberOfFieldsColLayoutNewLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());
    DynamicColumnLayoutPtr colLayout = DynamicColumnLayout::create(schema, false);
    DynamicColumnLayoutBufferPtr mappedColumnLayout = colLayout->bind(tupleBuffer);
    state.ResumeTiming();
    std::tuple<int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t,
               int32_t>
        writeRecord(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        mappedColumnLayout->pushRecord<false>(writeRecord);
    }

    auto field0 = DynamicColumnLayoutField<int32_t, false>::create(0, mappedColumnLayout);
    auto field1 = DynamicColumnLayoutField<int32_t, false>::create(1, mappedColumnLayout);
    auto field2 = DynamicColumnLayoutField<int32_t, false>::create(2, mappedColumnLayout);
    auto field3 = DynamicColumnLayoutField<int32_t, false>::create(3, mappedColumnLayout);

    auto field4 = DynamicColumnLayoutField<int32_t, false>::create(4, mappedColumnLayout);
    auto field5 = DynamicColumnLayoutField<int32_t, false>::create(5, mappedColumnLayout);
    auto field6 = DynamicColumnLayoutField<int32_t, false>::create(6, mappedColumnLayout);
    auto field7 = DynamicColumnLayoutField<int32_t, false>::create(7, mappedColumnLayout);

    auto field8 = DynamicColumnLayoutField<int32_t, false>::create(8, mappedColumnLayout);
    auto field9 = DynamicColumnLayoutField<int32_t, false>::create(9, mappedColumnLayout);
    auto field10 = DynamicColumnLayoutField<int32_t, false>::create(10, mappedColumnLayout);
    auto field11 = DynamicColumnLayoutField<int32_t, false>::create(11, mappedColumnLayout);

    auto field12 = DynamicColumnLayoutField<int32_t, false>::create(12, mappedColumnLayout);
    auto field13 = DynamicColumnLayoutField<int32_t, false>::create(13, mappedColumnLayout);
    auto field14 = DynamicColumnLayoutField<int32_t, false>::create(14, mappedColumnLayout);
    auto field15 = DynamicColumnLayoutField<int32_t, false>::create(15, mappedColumnLayout);

    switch (state.range(0)) {
        case 0: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                }
            }
            break;
        }
        case 1: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                }
            }
            break;
        }
        case 2: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                }
            }
            break;
        }
        case 3: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                }
            }
            break;
        }
        case 4: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                }
            }
            break;
        }
        case 5: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                }
            }
            break;
        }
        case 6: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                    field6[recordIndex] = 1;
                }
            }
            break;
        }
        case 7: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                    field6[recordIndex] = 1;
                    field7[recordIndex] = 1;
                }
            }
            break;
        }
        case 8: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                    field6[recordIndex] = 1;
                    field7[recordIndex] = 1;
                    field8[recordIndex] = 1;
                }
            }
            break;
        }
        case 9: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                    field6[recordIndex] = 1;
                    field7[recordIndex] = 1;
                    field8[recordIndex] = 1;
                    field9[recordIndex] = 1;
                }
            }
            break;
        }
        case 10: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                    field6[recordIndex] = 1;
                    field7[recordIndex] = 1;
                    field8[recordIndex] = 1;
                    field9[recordIndex] = 1;
                    field10[recordIndex] = 1;
                }
            }
            break;
        }
        case 11: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                    field6[recordIndex] = 1;
                    field7[recordIndex] = 1;
                    field8[recordIndex] = 1;
                    field9[recordIndex] = 1;
                    field10[recordIndex] = 1;
                    field11[recordIndex] = 1;
                }
            }
            break;
        }
        case 12: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                    field6[recordIndex] = 1;
                    field7[recordIndex] = 1;
                    field8[recordIndex] = 1;
                    field9[recordIndex] = 1;
                    field10[recordIndex] = 1;
                    field11[recordIndex] = 1;
                    field12[recordIndex] = 1;
                }
            }
            break;
        }
        case 13: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                    field6[recordIndex] = 1;
                    field7[recordIndex] = 1;
                    field8[recordIndex] = 1;
                    field9[recordIndex] = 1;
                    field10[recordIndex] = 1;
                    field11[recordIndex] = 1;
                    field12[recordIndex] = 1;
                    field13[recordIndex] = 1;
                }
            }
            break;
        }
        case 14: {
            for (auto singleState : state) {
                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                    field6[recordIndex] = 1;
                    field7[recordIndex] = 1;
                    field8[recordIndex] = 1;
                    field9[recordIndex] = 1;
                    field10[recordIndex] = 1;
                    field11[recordIndex] = 1;
                    field12[recordIndex] = 1;
                    field13[recordIndex] = 1;
                    field14[recordIndex] = 1;
                }
            }
            break;
        }
        case 15: {
            for (auto singleState : state) {

                for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
                    field0[recordIndex] = 1;
                    field1[recordIndex] = 1;
                    field2[recordIndex] = 1;
                    field3[recordIndex] = 1;
                    field4[recordIndex] = 1;
                    field5[recordIndex] = 1;
                    field6[recordIndex] = 1;
                    field7[recordIndex] = 1;
                    field8[recordIndex] = 1;
                    field9[recordIndex] = 1;
                    field10[recordIndex] = 1;
                    field11[recordIndex] = 1;
                    field12[recordIndex] = 1;
                    field13[recordIndex] = 1;
                    field14[recordIndex] = 1;
                    field15[recordIndex] = 1;
                }
            }
            break;
        }
    }

    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

#define REPETITIONS 20
BENCHMARK(BM_WriteRecordsRowLayoutNewLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_WriteRecordsColumnLayoutNewLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_ReadRecordsRowLayoutNewLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_ReadRecordsColumnLayoutNewLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);

BENCHMARK(BM_WriteFieldRowLayoutNewLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_WriteFieldColumnLayoutNewLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_ReadFieldRowLayoutNewLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_ReadFieldColumnLayoutNewLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);

BENCHMARK(BM_ReadWholeRecordWithFieldColumnLayoutNewLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_ReadWholeRecordWithFieldRowLayoutNewLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_WriteWholeRecordWithFieldColumnLayoutNewLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_WriteWholeRecordWithFieldRowLayoutNewLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);

BENCHMARK(BM_ReadingNumberOfFieldsRowLayoutNewLayout)->DenseRange(0, 15, 1)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_WritingNumberOfFieldsRowLayoutNewLayout)->DenseRange(0, 15, 1)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_ReadingNumberOfFieldsColLayoutNewLayout)->DenseRange(0, 15, 1)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_WritingNumberOfFieldsColLayoutNewLayout)->DenseRange(0, 15, 1)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);

// A benchmark main is needed
int main(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    return 0;
}
}// namespace NES::Benchmarking