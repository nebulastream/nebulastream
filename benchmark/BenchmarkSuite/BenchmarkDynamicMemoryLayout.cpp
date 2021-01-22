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


static void BM_WriteRecordsRowLayout(benchmark::State& state) {
    SchemaPtr schema = Schema::create()
                            ->addField("t1", BasicType::UINT8)
                            ->addField("t2", BasicType::UINT16)
                            ->addField("t3", BasicType::UINT32);

    std::shared_ptr<NES::NodeEngine::BufferManager> bufferManager = std::make_shared<NES::NodeEngine::BufferManager>(4096, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    for (auto singleState : state) {
        state.PauseTiming();

        NodeEngine::DynamicRowLayoutPtr rowLayout = NodeEngine::DynamicRowLayout::create(schema, false);
        NodeEngine::DynamicRowLayoutBufferPtr mappedRowLayout = std::unique_ptr<NodeEngine::DynamicRowLayoutBuffer>(static_cast<NodeEngine::DynamicRowLayoutBuffer*>(rowLayout->map(tupleBuffer).release()));
        state.ResumeTiming();
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(1, 1, 1);
            mappedRowLayout->pushRecord<false>(writeRecord);
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_ReadRecordsRowLayout(benchmark::State& state) {
    SchemaPtr schema = Schema::create()
                           ->addField("t1", BasicType::UINT8)
                           ->addField("t2", BasicType::UINT16)
                           ->addField("t3", BasicType::UINT32);

    std::shared_ptr<NES::NodeEngine::BufferManager> bufferManager = std::make_shared<NES::NodeEngine::BufferManager>(4096, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    NodeEngine::DynamicRowLayoutPtr rowLayout = NodeEngine::DynamicRowLayout::create(schema, false);
    NodeEngine::DynamicRowLayoutBufferPtr mappedRowLayout = std::unique_ptr<NodeEngine::DynamicRowLayoutBuffer>(static_cast<NodeEngine::DynamicRowLayoutBuffer*>(rowLayout->map(tupleBuffer).release()));

    uint8_t value = 1;
    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(value, value, value);
        mappedRowLayout->pushRecord<false>(writeRecord);
    }


    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            std::tuple<uint8_t, uint16_t, uint32_t> readRecord = mappedRowLayout->readRecord<false, uint8_t, uint16_t, uint32_t>(recordIndex);
            if (std::get<0>(readRecord) != value) NES_ERROR("BenchmarkMemoryLayout: wrong value");
            if (std::get<1>(readRecord) != value) NES_ERROR("BenchmarkMemoryLayout: wrong value");
            if (std::get<2>(readRecord) != value) NES_ERROR("BenchmarkMemoryLayout: wrong value");
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_WriteRecordsColumnLayout(benchmark::State& state) {
    SchemaPtr schema = Schema::create()
        ->addField("t1", BasicType::UINT8)
        ->addField("t2", BasicType::UINT16)
        ->addField("t3", BasicType::UINT32);

    std::shared_ptr<NES::NodeEngine::BufferManager> bufferManager = std::make_shared<NES::NodeEngine::BufferManager>(4096, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    for (auto singleState : state) {
        state.PauseTiming();

        NodeEngine::DynamicColumnLayoutPtr columnLayout = NodeEngine::DynamicColumnLayout::create(schema, false);
        NodeEngine::DynamicColumnLayoutBufferPtr mappedColumnLayout = std::unique_ptr<NodeEngine::DynamicColumnLayoutBuffer>(static_cast<NodeEngine::DynamicColumnLayoutBuffer*>(columnLayout->map(tupleBuffer).release()));
        state.ResumeTiming();
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(1, 1, 1);
            mappedColumnLayout->pushRecord<false>(writeRecord);
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_ReadRecordsColumnLayout(benchmark::State& state) {
    SchemaPtr schema = Schema::create()
        ->addField("t1", BasicType::UINT8)
        ->addField("t2", BasicType::UINT16)
        ->addField("t3", BasicType::UINT32);

    std::shared_ptr<NES::NodeEngine::BufferManager> bufferManager = std::make_shared<NES::NodeEngine::BufferManager>(4096, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    NodeEngine::DynamicColumnLayoutPtr columnLayout = NodeEngine::DynamicColumnLayout::create(schema, false);
    NodeEngine::DynamicColumnLayoutBufferPtr mappedColumnLayout = std::unique_ptr<NodeEngine::DynamicColumnLayoutBuffer>(static_cast<NodeEngine::DynamicColumnLayoutBuffer*>(columnLayout->map(tupleBuffer).release()));

    uint8_t value = 1;
    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(value, value, value);
        mappedColumnLayout->pushRecord<false>(writeRecord);
    }


    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            std::tuple<uint8_t, uint16_t, uint32_t> readRecord = mappedColumnLayout->readRecord<false, uint8_t, uint16_t, uint32_t>(recordIndex);
            if (std::get<0>(readRecord) != value) NES_ERROR("BenchmarkMemoryLayout: wrong value");
            if (std::get<1>(readRecord) != value) NES_ERROR("BenchmarkMemoryLayout: wrong value");
            if (std::get<2>(readRecord) != value) NES_ERROR("BenchmarkMemoryLayout: wrong value");
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_ReadFieldRowLayout(benchmark::State& state) {
    SchemaPtr schema = Schema::create()
        ->addField("t1", BasicType::UINT8)
        ->addField("t2", BasicType::UINT16)
        ->addField("t3", BasicType::UINT32);

    std::shared_ptr<NES::NodeEngine::BufferManager> bufferManager = std::make_shared<NES::NodeEngine::BufferManager>(4096, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    NodeEngine::DynamicRowLayoutPtr rowLayout = NodeEngine::DynamicRowLayout::create(schema, false);
    NodeEngine::DynamicRowLayoutBufferPtr mappedRowLayout = std::unique_ptr<NodeEngine::DynamicRowLayoutBuffer>(static_cast<NodeEngine::DynamicRowLayoutBuffer*>(rowLayout->map(tupleBuffer).release()));

    uint8_t value = 1;
    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(value, value, value);
        mappedRowLayout->pushRecord<false>(writeRecord);
    }

    auto field0 = NES::NodeEngine::DynamicRowLayoutField<uint8_t, false>::create(0, mappedRowLayout);
    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            if (field0[recordIndex] != value) NES_ERROR("BenchmarkMemoryLayout: wrong value");
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_ReadFieldColumnLayout(benchmark::State& state) {
    SchemaPtr schema = Schema::create()
        ->addField("t1", BasicType::UINT8)
        ->addField("t2", BasicType::UINT16)
        ->addField("t3", BasicType::UINT32);

    std::shared_ptr<NES::NodeEngine::BufferManager> bufferManager = std::make_shared<NES::NodeEngine::BufferManager>(4096, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    NodeEngine::DynamicColumnLayoutPtr columnLayout = NodeEngine::DynamicColumnLayout::create(schema, false);
    NodeEngine::DynamicColumnLayoutBufferPtr mappedColumnLayout = std::unique_ptr<NodeEngine::DynamicColumnLayoutBuffer>(static_cast<NodeEngine::DynamicColumnLayoutBuffer*>(columnLayout->map(tupleBuffer).release()));

    uint8_t value = 1;
    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(value, value, value);
        mappedColumnLayout->pushRecord<false>(writeRecord);
    }


    auto field0 = NES::NodeEngine::DynamicColumnLayoutField<uint8_t, false>::create(0, mappedColumnLayout);
    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            if (field0[recordIndex] != value) NES_ERROR("BenchmarkMemoryLayout: wrong value");
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_WriteFieldRowLayout(benchmark::State& state) {
    SchemaPtr schema = Schema::create()
        ->addField("t1", BasicType::UINT8)
        ->addField("t2", BasicType::UINT16)
        ->addField("t3", BasicType::UINT32);

    std::shared_ptr<NES::NodeEngine::BufferManager> bufferManager = std::make_shared<NES::NodeEngine::BufferManager>(4096, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    NodeEngine::DynamicRowLayoutPtr rowLayout = NodeEngine::DynamicRowLayout::create(schema, false);
    NodeEngine::DynamicRowLayoutBufferPtr mappedRowLayout = std::unique_ptr<NodeEngine::DynamicRowLayoutBuffer>(static_cast<NodeEngine::DynamicRowLayoutBuffer*>(rowLayout->map(tupleBuffer).release()));

    uint8_t value = 1;
    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(value, value, value);
        mappedRowLayout->pushRecord<false>(writeRecord);
    }

    auto field0 = NES::NodeEngine::DynamicRowLayoutField<uint8_t, false>::create(0, mappedRowLayout);
    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            field0[recordIndex] = 2;
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_WriteFieldColumnLayout(benchmark::State& state) {
    SchemaPtr schema = Schema::create()
        ->addField("t1", BasicType::UINT8)
        ->addField("t2", BasicType::UINT16)
        ->addField("t3", BasicType::UINT32);

    std::shared_ptr<NES::NodeEngine::BufferManager> bufferManager = std::make_shared<NES::NodeEngine::BufferManager>(4096, 10);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (tupleBuffer.getBufferSize() / schema->getSchemaSizeInBytes());

    NodeEngine::DynamicColumnLayoutPtr columnLayout = NodeEngine::DynamicColumnLayout::create(schema, false);
    NodeEngine::DynamicColumnLayoutBufferPtr mappedColumnLayout = std::unique_ptr<NodeEngine::DynamicColumnLayoutBuffer>(static_cast<NodeEngine::DynamicColumnLayoutBuffer*>(columnLayout->map(tupleBuffer).release()));

    uint8_t value = 1;
    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        std::tuple<uint8_t, uint16_t, uint32_t> writeRecord(value, value, value);
        mappedColumnLayout->pushRecord<false>(writeRecord);
    }


    auto field0 = NES::NodeEngine::DynamicColumnLayoutField<uint8_t, false>::create(0, mappedColumnLayout);
    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            field0[recordIndex] = 2;
        }
    }
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

// Register benchmark
BENCHMARK(BM_WriteRecordsRowLayout);
BENCHMARK(BM_ReadRecordsRowLayout);
BENCHMARK(BM_WriteRecordsColumnLayout);
BENCHMARK(BM_ReadRecordsColumnLayout);

BENCHMARK(BM_WriteFieldRowLayout);
BENCHMARK(BM_ReadFieldRowLayout);
BENCHMARK(BM_WriteFieldColumnLayout);
BENCHMARK(BM_ReadFieldColumnLayout);


// A benchmark main is needed
int main(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    return 0;
}
}