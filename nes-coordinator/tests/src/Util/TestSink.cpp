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
#include <Util/TestUtils.hpp>

namespace NES {

TestSink::TestSink(uint64_t expectedTuples,
                   const SchemaPtr& schema,
                   const Runtime::NodeEnginePtr& nodeEngine,
                   uint32_t numOfProducers)
    : SinkMedium(std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager(0)),
                 nodeEngine,
                 numOfProducers,
                 INVALID_SHARED_QUERY_ID,
                 INVALID_DECOMPOSED_QUERY_PLAN_ID),
      numOfExpectedTuples(expectedTuples) {
    auto bufferManager = nodeEngine->getBufferManager(0);
    if (schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT) {
        memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bufferManager->getBufferSize());
    } else if (schema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT) {
        memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, bufferManager->getBufferSize());
    }
};

std::shared_ptr<TestSink> TestSink::create(uint64_t expectedTuples,
                                           const SchemaPtr& schema,
                                           const Runtime::NodeEnginePtr& engine,
                                           uint32_t numOfProducers) {
    return std::make_shared<TestSink>(expectedTuples, schema, engine, numOfProducers);
}

bool TestSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext&) {
    std::unique_lock lock(m);

    resultBuffers.emplace_back(inputBuffer);
    const auto receivedTuples = TestUtils::countTuples(resultBuffers);

    // Check whether the required amount of expected result buffers has been reached.
    if (receivedTuples < numOfExpectedTuples) {
        NES_DEBUG("Already saw {} tuples and expects a total of {}", receivedTuples, numOfExpectedTuples);
    } else if (receivedTuples == numOfExpectedTuples) {
        completed.set_value(numOfExpectedTuples);
    } else if (receivedTuples > numOfExpectedTuples) {
        NES_ERROR("result number of tuples {} and expected number of tuples={} do not match",
                  receivedTuples,
                  numOfExpectedTuples);
        EXPECT_TRUE(false);
    }
    return true;
}

Runtime::TupleBuffer TestSink::get(uint64_t index) {
    std::unique_lock lock(m);
    return resultBuffers[index];
}

Runtime::MemoryLayouts::TestTupleBuffer TestSink::getResultBuffer(uint64_t index) {
    auto buffer = get(index);
    return Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, buffer);
}

std::vector<Runtime::MemoryLayouts::TestTupleBuffer> TestSink::getResultBuffers() {
    std::vector<Runtime::MemoryLayouts::TestTupleBuffer> allBuffers;
    for (auto bufIdx = 0_u64; bufIdx < getNumberOfResultBuffers(); ++bufIdx) {
        allBuffers.emplace_back(getResultBuffer(bufIdx));
    }
    return allBuffers;
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

void TestSink::waitTillCompleted() { completed.get_future().wait(); }

void TestSink::waitTillCompletedOrTimeout(uint64_t timeoutInMilliseconds) {
    completed.get_future().wait_for(std::chrono::milliseconds(timeoutInMilliseconds));
}

void TestSink::shutdown() { cleanupBuffers(); }

}// namespace NES
