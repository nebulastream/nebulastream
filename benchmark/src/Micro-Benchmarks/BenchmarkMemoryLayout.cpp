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
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <benchmark/benchmark.h>

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

static void BM_WriteRecordsRowLayoutOldLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto rowLayout = Runtime::createRowLayout(schema);
    auto buf = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (buf.getBufferSize() / schema->getSchemaSizeInBytes());

    for (auto singleState : state) {
        auto fields = schema->fields;
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            for (uint64_t fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                const auto value = 1;
                auto dataType = fields[fieldIndex]->getDataType();
                auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);
                if (physicalType->isBasicType()) {
                    auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
                    if (basicPhysicalType->getNativeType() == BasicPhysicalType::CHAR) {
                        rowLayout->getValueField<char>(recordIndex, fieldIndex)->write(buf, value);
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_8) {
                        rowLayout->getValueField<uint8_t>(recordIndex, fieldIndex)->write(buf, value);
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_16) {
                        rowLayout->getValueField<uint16_t>(recordIndex, fieldIndex)->write(buf, value);
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_32) {
                        rowLayout->getValueField<uint32_t>(recordIndex, fieldIndex)->write(buf, value);
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_64) {
                        rowLayout->getValueField<uint64_t>(recordIndex, fieldIndex)->write(buf, value);
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_8) {
                        rowLayout->getValueField<int8_t>(recordIndex, fieldIndex)->write(buf, value);
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_16) {
                        rowLayout->getValueField<int16_t>(recordIndex, fieldIndex)->write(buf, value);
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_32) {
                        rowLayout->getValueField<int32_t>(recordIndex, fieldIndex)->write(buf, value);
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_64) {
                        rowLayout->getValueField<int64_t>(recordIndex, fieldIndex)->write(buf, value);
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::FLOAT) {
                        rowLayout->getValueField<float>(recordIndex, fieldIndex)->write(buf, value);
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::DOUBLE) {
                        rowLayout->getValueField<double>(recordIndex, fieldIndex)->write(buf, value);
                    } else {
                        NES_DEBUG("BenchmarkMemoryLayout: Field is not supported!");
                    }
                } else {
                    NES_DEBUG("BenchmarkMemoryLayout: Field is not a basic type!");
                }
            }
        }
    }

    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_WriteRecordsCustomRowLayoutOldLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto rowLayout = Runtime::createRowLayout(schema);
    auto buf = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (buf.getBufferSize() / schema->getSchemaSizeInBytes());

    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            rowLayout->getValueField<int32_t>(recordIndex, 0)->write(buf, 42);
            rowLayout->getValueField<int32_t>(recordIndex, 1)->write(buf, 1);
            rowLayout->getValueField<int32_t>(recordIndex, 2)->write(buf, 1);
            rowLayout->getValueField<int32_t>(recordIndex, 3)->write(buf, 1);
            rowLayout->getValueField<int32_t>(recordIndex, 4)->write(buf, 1);
            rowLayout->getValueField<int32_t>(recordIndex, 5)->write(buf, 1);
            rowLayout->getValueField<int32_t>(recordIndex, 6)->write(buf, 1);
            rowLayout->getValueField<int32_t>(recordIndex, 7)->write(buf, 1);
            rowLayout->getValueField<int32_t>(recordIndex, 8)->write(buf, 1);
            rowLayout->getValueField<int32_t>(recordIndex, 9)->write(buf, 1);
            rowLayout->getValueField<int32_t>(recordIndex, 10)->write(buf, 1);
            rowLayout->getValueField<int32_t>(recordIndex, 11)->write(buf, 1);
            rowLayout->getValueField<int32_t>(recordIndex, 12)->write(buf, 1);
            rowLayout->getValueField<int32_t>(recordIndex, 13)->write(buf, 1);
            rowLayout->getValueField<int32_t>(recordIndex, 14)->write(buf, 1);
            rowLayout->getValueField<int32_t>(recordIndex, 15)->write(buf, 1);
        }
    }

    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_ReadRecordsCustomRowLayoutOldLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto rowLayout = Runtime::createRowLayout(schema);
    auto buf = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (buf.getBufferSize() / schema->getSchemaSizeInBytes());
    const auto value = 1;
    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        rowLayout->getValueField<int32_t>(recordIndex, 0)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 1)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 2)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 3)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 4)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 5)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 6)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 7)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 8)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 9)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 10)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 11)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 12)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 13)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 14)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 15)->write(buf, 1);
    }
    int32_t *field0, *field1, *field2, *field3, *field4, *field5, *field6, *field7, *field8, *field9, *field10, *field11,
        *field12, *field13, *field14, *field15;

    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            field0 = rowLayout->getFieldPointer<int32_t>(buf, recordIndex, 0);
            field1 = rowLayout->getFieldPointer<int32_t>(buf, recordIndex, 1);
            field2 = rowLayout->getFieldPointer<int32_t>(buf, recordIndex, 2);
            field3 = rowLayout->getFieldPointer<int32_t>(buf, recordIndex, 3);

            field4 = rowLayout->getFieldPointer<int32_t>(buf, recordIndex, 4);
            field5 = rowLayout->getFieldPointer<int32_t>(buf, recordIndex, 5);
            field6 = rowLayout->getFieldPointer<int32_t>(buf, recordIndex, 6);
            field7 = rowLayout->getFieldPointer<int32_t>(buf, recordIndex, 7);

            field8 = rowLayout->getFieldPointer<int32_t>(buf, recordIndex, 8);
            field9 = rowLayout->getFieldPointer<int32_t>(buf, recordIndex, 9);
            field10 = rowLayout->getFieldPointer<int32_t>(buf, recordIndex, 10);
            field11 = rowLayout->getFieldPointer<int32_t>(buf, recordIndex, 11);

            field12 = rowLayout->getFieldPointer<int32_t>(buf, recordIndex, 12);
            field13 = rowLayout->getFieldPointer<int32_t>(buf, recordIndex, 13);
            field14 = rowLayout->getFieldPointer<int32_t>(buf, recordIndex, 14);
            field15 = rowLayout->getFieldPointer<int32_t>(buf, recordIndex, 15);
        }
    }

    if (*field0 != value)
        NES_ERROR("BenchmarkMemoryLayout: Wrong value!!!");
    if (*field1 != value)
        NES_ERROR("BenchmarkMemoryLayout: Wrong value!!!");
    if (*field2 != value)
        NES_ERROR("BenchmarkMemoryLayout: Wrong value!!!");
    if (*field3 != value)
        NES_ERROR("BenchmarkMemoryLayout: Wrong value!!!");

    if (*field4 != value)
        NES_ERROR("BenchmarkMemoryLayout: Wrong value!!!");
    if (*field5 != value)
        NES_ERROR("BenchmarkMemoryLayout: Wrong value!!!");
    if (*field6 != value)
        NES_ERROR("BenchmarkMemoryLayout: Wrong value!!!");
    if (*field7 != value)
        NES_ERROR("BenchmarkMemoryLayout: Wrong value!!!");

    if (*field8 != value)
        NES_ERROR("BenchmarkMemoryLayout: Wrong value!!!");
    if (*field9 != value)
        NES_ERROR("BenchmarkMemoryLayout: Wrong value!!!");
    if (*field10 != value)
        NES_ERROR("BenchmarkMemoryLayout: Wrong value!!!");
    if (*field11 != value)
        NES_ERROR("BenchmarkMemoryLayout: Wrong value!!!");

    if (*field12 != value)
        NES_ERROR("BenchmarkMemoryLayout: Wrong value!!!");
    if (*field13 != value)
        NES_ERROR("BenchmarkMemoryLayout: Wrong value!!!");
    if (*field14 != value)
        NES_ERROR("BenchmarkMemoryLayout: Wrong value!!!");
    if (*field15 != value)
        NES_ERROR("BenchmarkMemoryLayout: Wrong value!!!");
    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_ReadRecordsRowLayoutOldLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto rowLayout = Runtime::createRowLayout(schema);
    auto buf = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (buf.getBufferSize() / schema->getSchemaSizeInBytes());

    const auto value = 1;
    auto fields = schema->fields;
    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        for (uint64_t fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
            auto dataType = fields[fieldIndex]->getDataType();
            auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);
            if (physicalType->isBasicType()) {
                auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
                if (basicPhysicalType->getNativeType() == BasicPhysicalType::CHAR) {
                    rowLayout->getValueField<char>(recordIndex, fieldIndex)->write(buf, value);
                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_8) {
                    rowLayout->getValueField<uint8_t>(recordIndex, fieldIndex)->write(buf, value);
                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_16) {
                    rowLayout->getValueField<uint16_t>(recordIndex, fieldIndex)->write(buf, value);
                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_32) {
                    rowLayout->getValueField<uint32_t>(recordIndex, fieldIndex)->write(buf, value);
                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_64) {
                    rowLayout->getValueField<uint64_t>(recordIndex, fieldIndex)->write(buf, value);
                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_8) {
                    rowLayout->getValueField<int8_t>(recordIndex, fieldIndex)->write(buf, value);
                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_16) {
                    rowLayout->getValueField<int16_t>(recordIndex, fieldIndex)->write(buf, value);
                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_32) {
                    rowLayout->getValueField<int32_t>(recordIndex, fieldIndex)->write(buf, value);
                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_64) {
                    rowLayout->getValueField<int64_t>(recordIndex, fieldIndex)->write(buf, value);
                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::FLOAT) {
                    rowLayout->getValueField<float>(recordIndex, fieldIndex)->write(buf, value);
                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::DOUBLE) {
                    rowLayout->getValueField<double>(recordIndex, fieldIndex)->write(buf, value);
                } else {
                    NES_DEBUG("BenchmarkMemoryLayout: Field is not supported!");
                }
            } else {
                NES_DEBUG("BenchmarkMemoryLayout: Field is not a basic type!");
            }
        }
    }

    for (auto singleState : state) {
        fields = schema->fields;
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            for (uint64_t fieldIndex = 0; fieldIndex < fields.size(); ++fieldIndex) {
                auto dataType = fields[fieldIndex]->getDataType();
                auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);
                if (physicalType->isBasicType()) {
                    auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
                    if (basicPhysicalType->getNativeType() == BasicPhysicalType::CHAR) {
                        auto field0 = rowLayout->getFieldPointer<char>(buf, recordIndex, fieldIndex);
                        if (*field0 != value)
                            NES_ERROR("BenchmarkMemoryLayout: wrong value");
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_8) {
                        auto field0 = rowLayout->getFieldPointer<uint8_t>(buf, recordIndex, fieldIndex);
                        if (*field0 != value)
                            NES_ERROR("BenchmarkMemoryLayout: wrong value");
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_16) {
                        auto field0 = rowLayout->getFieldPointer<uint16_t>(buf, recordIndex, fieldIndex);
                        if (*field0 != value)
                            NES_ERROR("BenchmarkMemoryLayout: wrong value");
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_32) {
                        auto field0 = rowLayout->getFieldPointer<uint32_t>(buf, recordIndex, fieldIndex);
                        if (*field0 != value)
                            NES_ERROR("BenchmarkMemoryLayout: wrong value");
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_64) {
                        auto field0 = rowLayout->getFieldPointer<uint64_t>(buf, recordIndex, fieldIndex);
                        if (*field0 != value)
                            NES_ERROR("BenchmarkMemoryLayout: wrong value");
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_8) {
                        auto field0 = rowLayout->getFieldPointer<int8_t>(buf, recordIndex, fieldIndex);
                        if (*field0 != value)
                            NES_ERROR("BenchmarkMemoryLayout: wrong value");
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_16) {
                        auto field0 = rowLayout->getFieldPointer<int16_t>(buf, recordIndex, fieldIndex);
                        if (*field0 != value)
                            NES_ERROR("BenchmarkMemoryLayout: wrong value");
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_32) {
                        auto field0 = rowLayout->getFieldPointer<int32_t>(buf, recordIndex, fieldIndex);
                        if (*field0 != value)
                            NES_ERROR("BenchmarkMemoryLayout: wrong value");
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_64) {
                        auto field0 = rowLayout->getFieldPointer<int64_t>(buf, recordIndex, fieldIndex);
                        if (*field0 != value)
                            NES_ERROR("BenchmarkMemoryLayout: wrong value");
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::FLOAT) {
                        auto field0 = rowLayout->getFieldPointer<float>(buf, recordIndex, fieldIndex);
                        if (*field0 != value)
                            NES_ERROR("BenchmarkMemoryLayout: wrong value");
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::DOUBLE) {
                        auto field0 = rowLayout->getFieldPointer<double>(buf, recordIndex, fieldIndex);
                        if (*field0 != value)
                            NES_ERROR("BenchmarkMemoryLayout: wrong value");
                    } else {
                        NES_DEBUG("BenchmarkMemoryLayout: Field is not supported!");
                    }
                } else {
                    NES_DEBUG("BenchmarkMemoryLayout: Field is not a basic type!");
                }
            }
        }
    }

    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_WriteFieldRowLayoutOldLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto rowLayout = Runtime::createRowLayout(schema);
    auto buf = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (buf.getBufferSize() / schema->getSchemaSizeInBytes());

    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            rowLayout->getValueField<int32_t>(recordIndex, 0)->write(buf, 1);
        }
    }

    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

static void BM_ReadFieldRowLayoutOldLayout(benchmark::State& state) {
    SchemaPtr schema = benchmarkSchemaCacheLine;

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, 10);
    auto rowLayout = Runtime::createRowLayout(schema);
    auto buf = bufferManager->getBufferBlocking();
    size_t NUM_TUPLES = (buf.getBufferSize() / schema->getSchemaSizeInBytes());
    const auto value = 1;
    for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
        rowLayout->getValueField<int32_t>(recordIndex, 0)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 1)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 2)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 3)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 4)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 5)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 6)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 7)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 8)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 9)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 10)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 11)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 12)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 13)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 14)->write(buf, 1);
        rowLayout->getValueField<int32_t>(recordIndex, 15)->write(buf, 1);
    }
    int32_t* field0;

    for (auto singleState : state) {
        for (size_t recordIndex = 0; recordIndex < NUM_TUPLES; ++recordIndex) {
            field0 = rowLayout->getFieldPointer<int32_t>(buf, recordIndex, 0);
        }
    }

    if (*field0 != value)
        NES_ERROR("BenchmarkMemoryLayout: Wrong value!!!");

    state.SetItemsProcessed(NUM_TUPLES * int64_t(state.iterations()));
}

#define REPETITIONS 20
BENCHMARK(BM_WriteRecordsRowLayoutOldLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_WriteRecordsCustomRowLayoutOldLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_ReadRecordsRowLayoutOldLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_ReadRecordsCustomRowLayoutOldLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);

BENCHMARK(BM_WriteFieldRowLayoutOldLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);
BENCHMARK(BM_ReadFieldRowLayoutOldLayout)->Repetitions(REPETITIONS)->ReportAggregatesOnly(true);

// A benchmark main is needed
int main(int argc, char** argv) {
    NESLogger->removeAllAppenders();
    NES::setupLogging("BenchmarkMemoryLayout.log", NES::LOG_WARNING);

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    return 0;
}
}// namespace NES::Benchmarking
