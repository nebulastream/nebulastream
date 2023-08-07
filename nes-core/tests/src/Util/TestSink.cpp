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

#include <Runtime/BufferManager.hpp>
#include <Util/TestSink.hpp>
#include <chrono>
#include <cstdint>

namespace NES {

TestSink::TestSink(uint64_t expectedBuffer,
                   const SchemaPtr& schema,
                   const Runtime::NodeEnginePtr& nodeEngine,
                   uint32_t numOfProducers)
    : SinkMedium(std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager(0)), nodeEngine, numOfProducers, 0, 0),
      numExpectedResultBuffers(expectedBuffer) {
    auto bufferManager = nodeEngine->getBufferManager(0);
    if (schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT) {
        memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bufferManager->getBufferSize());
    } else if (schema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT) {
        memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, bufferManager->getBufferSize());
    }
};

std::shared_ptr<TestSink> TestSink::create(uint64_t expectedBuffer,
                                           const SchemaPtr& schema,
                                           const Runtime::NodeEnginePtr& engine,
                                           uint32_t numOfProducers) {
    return std::make_shared<TestSink>(expectedBuffer, schema, engine, numOfProducers);
}

bool TestSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext&) {
    std::unique_lock lock(m);

    resultBuffers.emplace_back(inputBuffer);

    // Check whether the required amount of expected result buffers has been reached.
    if (resultBuffers.size() < numExpectedResultBuffers) {
        // Todo: 
        NES_DEBUG("Already saw {} buffers and expects a total of {}", resultBuffers.size(), numExpectedResultBuffers);
    }
    else if (resultBuffers.size() == numExpectedResultBuffers) {
        //Todo: #4043: this is never called if no data is written to the output
        // completed.set_value(numExpectedResultBuffers);
    } 
    // else if (resultBuffers.size() > numExpectedResultBuffers) {
    //     NES_ERROR("result buffer size {} and expected buffer={} do not match", resultBuffers.size(), numExpectedResultBuffers);
    //     EXPECT_TRUE(false);
    // }
    return true;
}

Runtime::TupleBuffer TestSink::get(uint64_t index) {
    std::unique_lock lock(m);
    return resultBuffers[index];
}

Runtime::MemoryLayouts::DynamicTupleBuffer TestSink::getResultBuffer(uint64_t index) {
    auto buffer = get(index);
    return Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
}

void TestSink::setup(){};

std::string TestSink::toString() const { return "Test_Sink"; }

uint32_t TestSink::getNumberOfResultBuffers() {
    std::unique_lock lock(m);
    return resultBuffers.size();
}

SinkMediumTypes TestSink::getSinkMediumType() { return SinkMediumTypes::PRINT_SINK; }

void TestSink::cleanupBuffers() {
    NES_DEBUG("TestSink: cleanupBuffers()");
    std::unique_lock lock(m);
    resultBuffers.clear();
}

void TestSink::waitTillCompleted() {
    completed.get_future().wait(); 
}

void TestSink::waitTillCompletedOrTimeout(uint64_t timeoutInMilliseconds) {
    completed.get_future().wait_for(std::chrono::milliseconds(timeoutInMilliseconds)); 
}

void TestSink::shutdown() { cleanupBuffers(); }

}// namespace NES