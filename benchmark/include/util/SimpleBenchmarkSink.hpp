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

#include <Sinks/Formats/NesFormat.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <future>

using namespace NES;
namespace NES::Benchmarking {
/**
 * @brief SimpleBenchmarkSink will set completed to true, after it gets @param expectedNumberOfTuples have been processed by SimpleBenchmarkSink
 */
class SimpleBenchmarkSink : public SinkMedium {
  public:
    SimpleBenchmarkSink(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager)
        : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager), 0){};

    static std::shared_ptr<SimpleBenchmarkSink> create(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager) {
        return std::make_shared<SimpleBenchmarkSink>(schema, bufferManager);
    }

    bool writeData(NodeEngine::TupleBuffer& input_buffer, NodeEngine::WorkerContext& workerContext) override {
        std::unique_lock lock(m);
        NES_DEBUG("SimpleBenchmarkSink: got buffer with " << input_buffer.getNumberOfTuples() << " number of tuples!");
        NES_INFO("WorkerContextID=" << workerContext.getId());

        currentTuples += input_buffer.getNumberOfTuples();

        NES_DEBUG("SimpleBenchmarkSink: currentTuples=" << currentTuples);

        return true;
    }

    SinkMediumTypes getSinkMediumType() override { return SinkMediumTypes::PRINT_SINK; }

    NodeEngine::TupleBuffer& get(uint64_t index) {
        std::unique_lock lock(m);
        return resultBuffers[index];
    }

    const std::string toString() const override { return ""; }

    void setup() override{};

    std::string toString() override { return "Test_Sink"; }

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

    uint64_t currentTuples = 0;
    std::vector<NodeEngine::TupleBuffer> resultBuffers;
    std::mutex m;

  public:
    std::promise<bool> completed;
};
}// namespace NES::Benchmarking

#endif//NES_BENCHMARK_INCLUDE_UTIL_SIMPLEBENCHMARKSINK_HPP_
