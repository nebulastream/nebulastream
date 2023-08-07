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
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <chrono>
#include <cstdint>
#include <gtest/gtest.h>

namespace NES {

using DefaultSourcePtr = std::shared_ptr<DefaultSource>;

class TestSink : public SinkMedium {
  public:
    /**
     * @brief Constructor for a TestSink
     * @param expectedBuffer
     * @param schema
     * @param nodeEngine
     * @param numOfProducers
     */
    TestSink(uint64_t expectedBuffer,
             const SchemaPtr& schema,
             const Runtime::NodeEnginePtr& nodeEngine,
             uint32_t numOfProducers = 1);

    /**
     * @brief Factory method for a TestSink
     * @param expectedBuffer
     * @param schema
     * @param engine
     * @param numOfProducers
     * @return
     */
    static std::shared_ptr<TestSink>
    create(uint64_t expectedBuffer, const SchemaPtr& schema, const Runtime::NodeEnginePtr& engine, uint32_t numOfProducers = 1);

    /**
     * @brief Writes the input Buffer to the resultBuffer
     * @param inputBuffer
     * @return Success of write
     */
    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext&) override;

    /**
     * @brief Returns the TupleBuffer at the index
     * @param index
     * @return Runtime::TupleBuffer
     */
    Runtime::TupleBuffer get(uint64_t index);

    /**
     * @brief Returns the DynamicTupleBuffer at the index
     * @param index
     * @return DynamicTupleBuffer
     */
    Runtime::MemoryLayouts::DynamicTupleBuffer getResultBuffer(uint64_t index);

    /**
     * @brief Setup method
     */
    void setup() override;

    /**
     * @brief Creates a string representation
     * @return String representation
     */
    std::string toString() const override;

    /**
     * @brief Returns the number of buffers that have been received
     * @return NumberOfResultBuffers
     */
    uint32_t getNumberOfResultBuffers();

    /**
     * @brief Returns the MediumType
     * @return SinkMediumTypes
     */
    SinkMediumTypes getSinkMediumType() override;

    /**
     * @brief Clears all resultBuffers
     */
    void cleanupBuffers();

    /**
     * @brief Waits in a blocking fashion until all the number of expected buffers have been received
     */
    void waitTillCompleted();

    /**
     * @brief Waits in a blocking fashion until all the number of expected buffers have been received or
     *        until the provided timeout is met.
     */
    void waitTillCompletedOrTimeout(uint64_t timeoutInMilliseconds);

    /**
     * @brief Shuts this sink down
     */
    void shutdown() override;

    mutable std::recursive_mutex m;
    uint64_t numExpectedResultBuffers;

    std::promise<uint64_t> completed;
    /// this vector must be cleanup by the test -- do not rely on the engine to clean it up for you!!
    std::vector<Runtime::TupleBuffer> resultBuffers;
    Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout;
};

template<class Type>
class CollectTestSink : public SinkMedium {
  public:
    CollectTestSink(const SchemaPtr& schema, const Runtime::NodeEnginePtr& nodeEngine, uint32_t numOfProducers = 1)
        : SinkMedium(std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager(0)), nodeEngine, numOfProducers, 0, 0) {
        auto bufferManager = nodeEngine->getBufferManager(0);
        NES_ASSERT(schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT, "Currently only support for row layouts");
    };

    static std::shared_ptr<TestSink>
    create(uint64_t expectedBuffer, const SchemaPtr& schema, const Runtime::NodeEnginePtr& engine, uint32_t numOfProducers = 1) {
        return std::make_shared<TestSink>(expectedBuffer, schema, engine, numOfProducers);
    }

    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext&) override {
        std::unique_lock lock(m);
        NES_TRACE("TestSink: emit buffer with {} tuples", inputBuffer.getNumberOfTuples());
        auto typedResult = inputBuffer.getBuffer<Type>();
        for (size_t i = 0; i < inputBuffer.getNumberOfTuples(); i++) {
            results.emplace_back(typedResult[i]);
        }
        NES_DEBUG("CollectTestSink saw now {} tuples!", results.size())
        cv.notify_all();
        return true;
    }

    std::vector<Type>& getResult() { return results; }

    void setup() override { running = true; };

    std::string toString() const override { return "Test_Sink"; }

    SinkMediumTypes getSinkMediumType() override { return SinkMediumTypes::PRINT_SINK; }

    /**
     * @brief Waits until numberOfRecords records are produced.
     * 
     * @param numberOfRecords: The number of records to produce until we stop waiting.
     */
    void waitTillCompleted(size_t numberOfRecords) {
        std::unique_lock lock(m);
        cv.wait(lock, [&] {
            if (!running) {
                return true;
            }
            NES_DEBUG("Saw {} tuples in waitTillCompleted and waits for {} tuples", this->results.size(), numberOfRecords);
            return this->results.size() >= numberOfRecords;
        });
    }

    /**
     * @brief Either waits until numberOfRecords records are produced, or until the timeout is reached.
     * 
     * @param numberOfRecords: The number of records to produce until we stop waiting.
     * @param timeoutInMilliseconds: Amount of time that needs to pass until we stop waiting.
     */
    void waitTillCompletedOrTimeout(size_t numberOfRecords, uint64_t timeoutInMilliseconds) {
        // completed.get_future().wait_for(std::chrono::milliseconds(timeoutInMilliseconds)); 
        std::unique_lock lock(m);
        cv.wait_for(lock, std::chrono::milliseconds(timeoutInMilliseconds), [&] {
            if (!running) {
                return true;
            }
            NES_DEBUG("Saw {} tuples in waitTillCompletedOrTimeout and waits for {} tuples", this->results.size(), numberOfRecords);
            return this->results.size() >= numberOfRecords;
        });
    }

  public:
    void shutdown() override {
        // just in case someone is waiting. Check if they should be notified.
        running = false;
        cv.notify_all();
    }

    mutable std::mutex m;
    std::condition_variable cv;
    std::vector<Type> results;
    std::atomic<bool> running;
};

}// namespace NES

#endif// NES_TESTS_UTIL_TEST_SINK_HPP_
