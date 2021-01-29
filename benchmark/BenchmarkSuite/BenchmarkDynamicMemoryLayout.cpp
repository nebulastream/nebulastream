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
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayout.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayoutBuffer.hpp>
#include <NodeEngine/MemoryLayout/DynamicColumnLayout.hpp>
#include <NodeEngine/MemoryLayout/DynamicColumnLayoutBuffer.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayoutField.hpp>
#include <NodeEngine/MemoryLayout/DynamicColumnLayoutField.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <benchmark/benchmark.h>


namespace NES::Benchmarking {

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

static void BM_WriteRecordsRowLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    std::shared_ptr<NES::NodeEngine::BufferManager> bufferManager = std::make_shared<NES::NodeEngine::BufferManager>(4096, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    for (auto singleState : state) {
        state.PauseTiming();

        NodeEngine::DynamicRowLayoutPtr rowLayout = NodeEngine::DynamicRowLayout::create(schema, false);
        NodeEngine::DynamicRowLayoutBufferPtr mappedRowLayout = std::unique_ptr<NodeEngine::DynamicRowLayoutBuffer>(static_cast<NodeEngine::DynamicRowLayoutBuffer*>(rowLayout->map(tupleBuffer).release()));
        state.ResumeTiming();
        std::tuple<int32_t, int32_t, int32_t, int32_t,
                   int32_t, int32_t, int32_t, int32_t,
                   int32_t, int32_t, int32_t, int32_t,
                   int32_t, int32_t, int32_t, int32_t> writeRecord(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            mappedRowLayout->pushRecord<false>(writeRecord);
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_ReadRecordsRowLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    std::shared_ptr<NES::NodeEngine::BufferManager> bufferManager = std::make_shared<NES::NodeEngine::BufferManager>(4096, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    NodeEngine::DynamicRowLayoutPtr rowLayout = NodeEngine::DynamicRowLayout::create(schema, false);
    NodeEngine::DynamicRowLayoutBufferPtr mappedRowLayout = std::unique_ptr<NodeEngine::DynamicRowLayoutBuffer>(static_cast<NodeEngine::DynamicRowLayoutBuffer*>(rowLayout->map(tupleBuffer).release()));

    std::tuple<int32_t, int32_t, int32_t, int32_t,
        int32_t, int32_t, int32_t, int32_t,
        int32_t, int32_t, int32_t, int32_t,
        int32_t, int32_t, int32_t, int32_t> writeRecord(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        mappedRowLayout->pushRecord<false>(writeRecord);
    }


    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            std::tuple<int32_t, int32_t, int32_t, int32_t,
                        int32_t, int32_t, int32_t, int32_t,
                        int32_t, int32_t, int32_t, int32_t,
                        int32_t, int32_t, int32_t, int32_t> readRecord = mappedRowLayout->readRecord<false, int32_t, int32_t, int32_t, int32_t,
                                                                                                    int32_t, int32_t, int32_t, int32_t,
                                                                                                    int32_t, int32_t, int32_t, int32_t,
                                                                                                    int32_t, int32_t, int32_t, int32_t>(recordIndex);
            ((void)readRecord);
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_WriteRecordsColumnLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    std::shared_ptr<NES::NodeEngine::BufferManager> bufferManager = std::make_shared<NES::NodeEngine::BufferManager>(4096, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    for (auto singleState : state) {
        state.PauseTiming();

        NodeEngine::DynamicColumnLayoutPtr columnLayout = NodeEngine::DynamicColumnLayout::create(schema, false);
        NodeEngine::DynamicColumnLayoutBufferPtr mappedColumnLayout = std::unique_ptr<NodeEngine::DynamicColumnLayoutBuffer>(static_cast<NodeEngine::DynamicColumnLayoutBuffer*>(columnLayout->map(tupleBuffer).release()));
        state.ResumeTiming();
        std::tuple<int32_t, int32_t, int32_t, int32_t,
            int32_t, int32_t, int32_t, int32_t,
            int32_t, int32_t, int32_t, int32_t,
            int32_t, int32_t, int32_t, int32_t> writeRecord(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            mappedColumnLayout->pushRecord<false>(writeRecord);
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_ReadRecordsColumnLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    std::shared_ptr<NES::NodeEngine::BufferManager> bufferManager = std::make_shared<NES::NodeEngine::BufferManager>(4096, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    NodeEngine::DynamicColumnLayoutPtr columnLayout = NodeEngine::DynamicColumnLayout::create(schema, false);
    NodeEngine::DynamicColumnLayoutBufferPtr mappedColumnLayout = std::unique_ptr<NodeEngine::DynamicColumnLayoutBuffer>(static_cast<NodeEngine::DynamicColumnLayoutBuffer*>(columnLayout->map(tupleBuffer).release()));

    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        std::tuple<int32_t, int32_t, int32_t, int32_t,
            int32_t, int32_t, int32_t, int32_t,
            int32_t, int32_t, int32_t, int32_t,
            int32_t, int32_t, int32_t, int32_t> writeRecord(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
        mappedColumnLayout->pushRecord<false>(writeRecord);
    }


    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            std::tuple< int32_t, int32_t, int32_t, int32_t,
                        int32_t, int32_t, int32_t, int32_t,
                        int32_t, int32_t, int32_t, int32_t,
                        int32_t, int32_t, int32_t, int32_t> readRecord = mappedColumnLayout->readRecord<false,  int32_t, int32_t, int32_t, int32_t,
                                                                                                        int32_t, int32_t, int32_t, int32_t,
                                                                                                        int32_t, int32_t, int32_t, int32_t,
                                                                                                        int32_t, int32_t, int32_t, int32_t>(recordIndex);
            ((void)readRecord);
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_ReadFieldRowLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    std::shared_ptr<NES::NodeEngine::BufferManager> bufferManager = std::make_shared<NES::NodeEngine::BufferManager>(4096, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    NodeEngine::DynamicRowLayoutPtr rowLayout = NodeEngine::DynamicRowLayout::create(schema, false);
    NodeEngine::DynamicRowLayoutBufferPtr mappedRowLayout = std::unique_ptr<NodeEngine::DynamicRowLayoutBuffer>(static_cast<NodeEngine::DynamicRowLayoutBuffer*>(rowLayout->map(tupleBuffer).release()));

    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        std::tuple<int32_t, int32_t, int32_t, int32_t,
            int32_t, int32_t, int32_t, int32_t,
            int32_t, int32_t, int32_t, int32_t,
            int32_t, int32_t, int32_t, int32_t> writeRecord(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
        mappedRowLayout->pushRecord<false>(writeRecord);
    }

    auto field0 = NES::NodeEngine::DynamicRowLayoutField<int32_t, false>::create(0, mappedRowLayout);
    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            int32_t tmp = field0[recordIndex];
            if (tmp != 1) NES_ERROR("BenchmarkDynamicMemoryLayout: tmp != 1");
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_ReadFieldColumnLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    std::shared_ptr<NES::NodeEngine::BufferManager> bufferManager = std::make_shared<NES::NodeEngine::BufferManager>(4096, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    NodeEngine::DynamicColumnLayoutPtr columnLayout = NodeEngine::DynamicColumnLayout::create(schema, false);
    NodeEngine::DynamicColumnLayoutBufferPtr mappedColumnLayout = std::unique_ptr<NodeEngine::DynamicColumnLayoutBuffer>(static_cast<NodeEngine::DynamicColumnLayoutBuffer*>(columnLayout->map(tupleBuffer).release()));

    std::tuple<int32_t, int32_t, int32_t, int32_t,
                int32_t, int32_t, int32_t, int32_t,
                int32_t, int32_t, int32_t, int32_t,
                int32_t, int32_t, int32_t, int32_t> writeRecord(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        mappedColumnLayout->pushRecord<false>(writeRecord);
    }


    auto field0 = NES::NodeEngine::DynamicColumnLayoutField<int32_t, false>::create(0, mappedColumnLayout);
    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            int32_t tmp = field0[recordIndex];
            if (tmp != 1) NES_ERROR("BenchmarkDynamicMemoryLayout: tmp != 1");
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_WriteFieldRowLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    std::shared_ptr<NES::NodeEngine::BufferManager> bufferManager = std::make_shared<NES::NodeEngine::BufferManager>(4096, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    NodeEngine::DynamicRowLayoutPtr rowLayout = NodeEngine::DynamicRowLayout::create(schema, false);
    NodeEngine::DynamicRowLayoutBufferPtr mappedRowLayout = std::unique_ptr<NodeEngine::DynamicRowLayoutBuffer>(static_cast<NodeEngine::DynamicRowLayoutBuffer*>(rowLayout->map(tupleBuffer).release()));

    auto field0 = NES::NodeEngine::DynamicRowLayoutField<int32_t, false>::create(0, mappedRowLayout);
    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            field0[recordIndex] = 2;
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_WriteFieldColumnLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    std::shared_ptr<NES::NodeEngine::BufferManager> bufferManager = std::make_shared<NES::NodeEngine::BufferManager>(4096, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    NodeEngine::DynamicColumnLayoutPtr columnLayout = NodeEngine::DynamicColumnLayout::create(schema, false);
    NodeEngine::DynamicColumnLayoutBufferPtr mappedColumnLayout = std::unique_ptr<NodeEngine::DynamicColumnLayoutBuffer>(static_cast<NodeEngine::DynamicColumnLayoutBuffer*>(columnLayout->map(tupleBuffer).release()));

    auto field0 = NES::NodeEngine::DynamicColumnLayoutField<int32_t, false>::create(0, mappedColumnLayout);
    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            field0[recordIndex] = 2;
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

#define REPETITIONS 20
BENCHMARK(BM_WriteRecordsRowLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_WriteRecordsColumnLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_ReadRecordsRowLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_ReadRecordsColumnLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);

BENCHMARK(BM_WriteFieldRowLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_WriteFieldColumnLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_ReadFieldRowLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_ReadFieldColumnLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);


// A benchmark main is needed
int main(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    return 0;
}
}