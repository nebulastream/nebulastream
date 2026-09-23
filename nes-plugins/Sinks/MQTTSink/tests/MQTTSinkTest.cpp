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

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <mqtt/async_client.h>
#include <BackpressureChannel.hpp>
#include <BaseUnitTest.hpp>
#include <MQTTSink.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

namespace
{
/// SERVER_URI, TOPIC and the inherited OUTPUT_FORMAT are the only parameters without a default.
std::unordered_map<Identifier, std::string> validConfig()
{
    return {
        {Identifier::parse(ConfigParametersMQTTSink::SERVER_URI.name), "tcp://localhost:1883"},
        {Identifier::parse(ConfigParametersMQTTSink::TOPIC.name), "nes/test"},
        {Identifier::parse(SinkDescriptor::OUTPUT_FORMAT.name), "CSV"}};
}

/// A PipelineExecutionContext that fails the test if it is ever actually used. stop() with an unconnected
/// client never reaches any of these (no pending delivery tokens, not connected), so the fake only needs
/// to satisfy the interface.
class UnusedPipelineExecutionContext final : public PipelineExecutionContext
{
public:
    bool emitBuffer(const TupleBuffer&, ContinuationPolicy) override
    {
        ADD_FAILURE() << "emitBuffer should not be called in this test";
        return false;
    }

    void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override
    {
        ADD_FAILURE() << "repeatTask should not be called in this test";
    }

    TupleBuffer allocateTupleBuffer() override
    {
        ADD_FAILURE() << "allocateTupleBuffer should not be called in this test";
        return {};
    }

    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return INITIAL<WorkerThreadId>; }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1; }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return nullptr; }

    [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override
    {
        static std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;
        return handlers;
    }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>&) override { }
};
}

class MQTTSinkTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("MQTTSinkTest.log", LogLevel::LOG_DEBUG); }
};

/// Regression test: stop() used to guard teardown with `INVARIANT(backpressureHandler.empty(), ...)`, which terminated the
/// worker on a reachable runtime state (a query torn down mid-backpressure or after the broker dropped the connection).
/// That path is only reachable once `client` is set, which normally requires a live broker connection via start(); this
/// test sets up the same state directly instead of standing up a broker.
TEST_F(MQTTSinkTest, StopWithQueuedBufferWarnsInsteadOfAborting)
{
    SinkCatalog sinkCatalog;
    const Schema<UnqualifiedUnboundField, Ordered> schema{
        std::vector{UnqualifiedUnboundField{Identifier::parse("id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}}};
    auto descriptorResult = sinkCatalog.addSinkDescriptor(
        Identifier::parse("mqttStopWithBackpressureTestSink"),
        schema,
        Identifier::parse(std::string(MQTTSink::NAME)),
        Host{"localhost"},
        validConfig(),
        {});
    ASSERT_TRUE(descriptorResult.has_value()) << (descriptorResult.has_value() ? "" : descriptorResult.error().what());

    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    MQTTSink sink{std::move(backpressureController), *descriptorResult};

    /// Queue buffers directly instead of routing a real publish through paho: the regression under test is
    /// stop()'s handling of a non-empty handler, independent of how it filled up. The first onFull() call hands
    /// its buffer straight back as the pending retry buffer, so the handler stays empty; a second, distinctly
    /// sequenced buffer is what stays queued and makes empty() false.
    constexpr uint32_t bufferSizeBytes = 4096;
    auto bufferManager = BufferManager::create(
        128UL * bufferSizeBytes, 0.5, BufferAlignment{64}, bufferSizeBytes, std::make_shared<NesDefaultMemoryAllocator>());
    auto firstBuffer = bufferManager->getBufferBlocking();
    firstBuffer.setSequenceNumber(SequenceNumber(1));
    ASSERT_TRUE(sink.backpressureHandler.onFull(firstBuffer, sink.backpressureController).has_value());
    auto secondBuffer = bufferManager->getBufferBlocking();
    secondBuffer.setSequenceNumber(SequenceNumber(2));
    ASSERT_FALSE(sink.backpressureHandler.onFull(secondBuffer, sink.backpressureController).has_value());
    ASSERT_FALSE(sink.backpressureHandler.empty());

    /// `client` is only set by start(), which would need a live broker; construct it directly so stop()
    /// reaches the backpressureHandler check instead of taking its early `if (!client) return;` path. An
    /// unconnected paho client answers get_pending_delivery_tokens()/is_connected() locally, no network I/O.
    sink.client = std::make_unique<mqtt::async_client>(sink.serverURI, sink.clientId, sink.maxOutstandingMessages);

    UnusedPipelineExecutionContext pec;
    EXPECT_NO_THROW(sink.stop(pec));
    /// stop() does not clear undelivered buffers; the regression under test is that it warns and returns
    /// instead of aborting on this exact state.
    EXPECT_FALSE(sink.backpressureHandler.empty());
}

}
