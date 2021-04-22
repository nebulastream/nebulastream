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

#ifndef NES_BENCHMARK_INCLUDE_UTIL_SIMPLEBENCHMARKSINK_HPP_
#define NES_BENCHMARK_INCLUDE_UTIL_SIMPLEBENCHMARKSINK_HPP_

#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayout.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayoutBuffer.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayoutField.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <future>

using namespace NES;
namespace NES::Benchmarking {
/**
 * @brief SimpleBenchmarkSink will set completed to true, after it gets @param expectedNumberOfTuples have been processed by SimpleBenchmarkSink
 * The schema must have a key field as this field is used to check if the benchmark has ended
 */
class SimpleBenchmarkSink : public SinkMedium {
  public:
    SimpleBenchmarkSink(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager)
        : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager), 0) {
        rowLayout = NodeEngine::DynamicMemoryLayout::DynamicRowLayout::create(schema, false);

        // An end of benchmark will be signaled by the source as key field will be equal to -1
        auto fields = getSchemaPtr()->fields;
        for (size_t i = 0; i < fields.size(); ++i) {
            if (fields[i]->getName() == "key") {
                this->fieldIndex = i;
                break;
            }
        }
        promiseSet = false;
    };

    static std::shared_ptr<SimpleBenchmarkSink> create(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager) {
        return std::make_shared<SimpleBenchmarkSink>(schema, bufferManager);
    }

    bool writeData(NodeEngine::TupleBuffer& input_buffer, NodeEngine::WorkerContext& workerContext) override {
        std::unique_lock lock(m);
        NES_DEBUG("SimpleBenchmarkSink: got buffer with " << input_buffer.getNumberOfTuples() << " number of tuples!");
        NES_INFO("WorkerContextID=" << workerContext.getId());

        currentTuples += input_buffer.getNumberOfTuples();
        bool endOfBenchmark = true;

        if (promiseSet)
            return true;

        auto bindedRowLayout = rowLayout->bind(input_buffer);
        auto fields = getSchemaPtr()->fields;
        uint64_t recordIndex = 1;
        auto dataType = fields[fieldIndex]->getDataType();
        auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);
        if (physicalType->isBasicType()) {
            auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
            if (basicPhysicalType->getNativeType() == BasicPhysicalType::CHAR) {
                if (NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<char, false>::create(fieldIndex, bindedRowLayout)[recordIndex] != (char) -1) {
                    endOfBenchmark = false;
                }
            } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_8) {
                if (NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint8_t, false>::create(fieldIndex, bindedRowLayout)[recordIndex] != (uint8_t) -1) {
                    endOfBenchmark = false;
                }
            } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_16) {
                if (NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint16_t, false>::create(fieldIndex, bindedRowLayout)[recordIndex] != (uint16_t) -1) {
                    endOfBenchmark = false;
                }
            } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_32) {
                if (NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint32_t, false>::create(fieldIndex, bindedRowLayout)[recordIndex] != (uint32_t) -1) {
                    endOfBenchmark = false;
                }
            } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_64) {
                if (NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, false>::create(fieldIndex, bindedRowLayout)[recordIndex] != (uint64_t) -1) {
                    endOfBenchmark = false;
                }
            } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_8) {
                if (NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<int8_t, false>::create(fieldIndex, bindedRowLayout)[recordIndex] != (int8_t) -1) {
                    endOfBenchmark = false;
                }
            } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_16) {
                if (NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<int16_t, false>::create(fieldIndex, bindedRowLayout)[recordIndex] != (int16_t) -1) {
                    endOfBenchmark = false;
                }
            } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_32) {
                if (NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<int32_t, false>::create(fieldIndex, bindedRowLayout)[recordIndex] != (int32_t) -1) {
                    endOfBenchmark = false;
                }
            } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_64) {
                if (NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, false>::create(fieldIndex, bindedRowLayout)[recordIndex] != (int64_t) -1) {
                    endOfBenchmark = false;
                }
            } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::FLOAT) {
                if (NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<float, false>::create(fieldIndex, bindedRowLayout)[recordIndex] != (float) -1) {
                    endOfBenchmark = false;
                }
            } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::DOUBLE) {
                if (NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<double, false>::create(fieldIndex, bindedRowLayout)[recordIndex] != (double) -1) {
                    endOfBenchmark = false;
                }
            } else {
                NES_DEBUG("This data sink only accepts data for numeric fields");
            }
        } else {
            NES_DEBUG("This data sink only accepts data for numeric fields");
        }
        return true;
    }

    SinkMediumTypes getSinkMediumType() override { return SinkMediumTypes::PRINT_SINK; }

    NodeEngine::TupleBuffer& get(uint64_t index) {
        std::unique_lock lock(m);
        return resultBuffers[index];
    }

    const std::string toString() const override { return ""; }

    void setup() override{};

    std::string toString() { return "Test_Sink"; }

    void shutdown() override {
        std::unique_lock lock(m);
        cleanupBuffers();
    };

    ~SimpleBenchmarkSink() override {
        std::unique_lock lock(m);
        cleanupBuffers();
    };

    uint32_t getNumberOfResultBuffers() {
        std::unique_lock lock(m);
        return resultBuffers.size();
    }

  private:
    void cleanupBuffers() {
        for (auto& buffer : resultBuffers) {
            buffer.release();
        }
        resultBuffers.clear();
    }

    bool promiseSet = false;
    uint64_t fieldIndex = 0;
    uint64_t currentTuples = 0;
    std::vector<NodeEngine::TupleBuffer> resultBuffers;
    std::mutex m;
    NodeEngine::DynamicMemoryLayout::DynamicRowLayoutPtr rowLayout;
};
}// namespace NES::Benchmarking

#endif//NES_BENCHMARK_INCLUDE_UTIL_SIMPLEBENCHMARKSINK_HPP_
