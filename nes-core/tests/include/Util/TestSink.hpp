/*
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

#ifndef NES_TESTS_UTIL_TEST_SINK_HPP_
#define NES_TESTS_UTIL_TEST_SINK_HPP_
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/MemoryLayout/RowLayoutTupleBuffer.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/SourceCreator.hpp>

namespace NES {

using DefaultSourcePtr = std::shared_ptr<DefaultSource>;

class TestSink : public SinkMedium {
  public:
    TestSink(uint64_t expectedBuffer,
             const SchemaPtr& schema,
             const Runtime::NodeEnginePtr& nodeEngine,
             uint32_t numOfProducers = 1)
        : SinkMedium(std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager(0)), nodeEngine, numOfProducers, 0, 0),
          expectedBuffer(expectedBuffer) {
        auto bufferManager = nodeEngine->getBufferManager(0);
        if (schema->getLayoutType() == Schema::ROW_LAYOUT) {
            memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bufferManager->getBufferSize());
        } else if (schema->getLayoutType() == Schema::COLUMNAR_LAYOUT) {
            memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, bufferManager->getBufferSize());
        }
    };

    static std::shared_ptr<TestSink>
    create(uint64_t expectedBuffer, const SchemaPtr& schema, const Runtime::NodeEnginePtr& engine, uint32_t numOfProducers = 1) {
        return std::make_shared<TestSink>(expectedBuffer, schema, engine, numOfProducers);
    }

    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext&) override {
        std::unique_lock lock(m);
        //        NES_DEBUG("TestSink: PrettyPrintTupleBuffer" << Util::prettyPrintTupleBuffer(inputBuffer, getSchemaPtr()));

        resultBuffers.emplace_back(std::move(inputBuffer));
        if (resultBuffers.size() == expectedBuffer) {
            completed.set_value(expectedBuffer);
        } else if (resultBuffers.size() > expectedBuffer) {
            EXPECT_TRUE(false);
        }
        return true;
    }

    Runtime::TupleBuffer get(uint64_t index) {
        std::unique_lock lock(m);
        return resultBuffers[index];
    }

    Runtime::MemoryLayouts::DynamicTupleBuffer getResultBuffer(uint64_t index) {
        auto buffer = get(index);
        return Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    }

    void setup() override{};

    std::string toString() const override { return "Test_Sink"; }

    uint32_t getNumberOfResultBuffers() {
        std::unique_lock lock(m);
        return resultBuffers.size();
    }

    SinkMediumTypes getSinkMediumType() override { return SinkMediumTypes::PRINT_SINK; }

    void cleanupBuffers() {
        NES_DEBUG("TestSink: cleanupBuffers()");
        std::unique_lock lock(m);
        resultBuffers.clear();
    }

    void waitTillCompleted() { completed.get_future().wait(); }

  public:
    void shutdown() override { cleanupBuffers(); }

    mutable std::recursive_mutex m;
    uint64_t expectedBuffer;

    std::promise<uint64_t> completed;
    /// this vector must be cleanup by the test -- do not rely on the engine to clean it up for you!!
    std::vector<Runtime::TupleBuffer> resultBuffers;
    Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout;
};

}// namespace NES

#endif// NES_TESTS_UTIL_TEST_SINK_HPP_
