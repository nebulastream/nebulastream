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

#ifndef NES_TESTS_UTIL_TEST_SINK_HPP_
#define NES_TESTS_UTIL_TEST_SINK_HPP_
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/MemoryLayout/RowLayoutTupleBuffer.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineFactory.hpp>
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
    TestSink(uint64_t expectedBuffer, const SchemaPtr& schema, const Runtime::BufferManagerPtr& bufferManager)
        : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager), nullptr, 0), expectedBuffer(expectedBuffer){};

    static std::shared_ptr<TestSink>
    create(uint64_t expectedBuffer, const SchemaPtr& schema, const Runtime::BufferManagerPtr& bufferManager) {
        return std::make_shared<TestSink>(expectedBuffer, schema, bufferManager);
    }

    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext&) override {
        std::unique_lock lock(m);
        NES_DEBUG("TestSink: PrettyPrintTupleBuffer" << Util::prettyPrintTupleBuffer(inputBuffer, getSchemaPtr()));

        resultBuffers.emplace_back(std::move(inputBuffer));
        if (resultBuffers.size() == expectedBuffer) {
            completed.set_value(expectedBuffer);
        } else if (resultBuffers.size() > expectedBuffer) {
            EXPECT_TRUE(false);
        }
        return true;
    }

    /**
     * @brief Factory to create a new TestSink.
     * @param expectedBuffer number of buffers expected this sink should receive.
     * @return
     */

    Runtime::TupleBuffer get(uint64_t index) {
        std::unique_lock lock(m);
        return resultBuffers[index];
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

  public:
    void shutdown() override {}

    mutable std::recursive_mutex m;
    uint64_t expectedBuffer;

    std::promise<uint64_t> completed;
    /// this vector must be cleanup by the test -- do not rely on the engine to clean it up for you!!
    std::vector<Runtime::TupleBuffer> resultBuffers;
};

}// namespace NES

#endif// NES_TESTS_UTIL_TEST_SINK_HPP_
