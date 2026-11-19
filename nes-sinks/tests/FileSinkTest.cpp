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
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
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
#include <Schema/SchemaFwd.hpp>
#include <Sinks/FileSink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <gtest/gtest.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
namespace
{
constexpr auto FULL_DEVICE = "/dev/full";

class StubPipelineContext final : public PipelineExecutionContext
{
public:
    explicit StubPipelineContext(std::shared_ptr<AbstractBufferProvider> bufferManager) : bufferManager(std::move(bufferManager)) { }

    bool emitBuffer(const TupleBuffer&, ContinuationPolicy) override { return false; }

    void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { }

    TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }

    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return INITIAL<WorkerThreadId>; }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1; }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }

    [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return operatorHandlers; }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>&) override { }

private:
    std::shared_ptr<AbstractBufferProvider> bufferManager;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> operatorHandlers;
};

class FileSinkTest : public testing::Test
{
protected:
    std::shared_ptr<BufferManager> bufferManager
        = BufferManager::create(4096, 0.0, BufferAlignment{64}, 256, std::make_shared<NesDefaultMemoryAllocator>());
    StubPipelineContext context{bufferManager};

    static SinkDescriptor makeDescriptor()
    {
        const Schema<UnqualifiedUnboundField, Ordered> schema{
            {Identifier::parse("id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}};
        const std::unordered_map<Identifier, std::string> sinkConfig{
            {Identifier::parse("FILE_PATH"), "/dev/null"}, {Identifier::parse("OUTPUT_FORMAT"), "CSV"}};
        return SinkDescriptor::createAnonymous(INITIAL_SINK_ID, Identifier::parse("File"), schema, Host{"localhost"}, sinkConfig, {})
            .value();
    }

    TupleBuffer makeContentBuffer() const
    {
        auto buffer = bufferManager->getBufferBlocking();
        buffer.setNumberOfTuples(buffer.getBufferSize());
        return buffer;
    }
};

TEST_F(FileSinkTest, ExecuteThrowsWhenWriteFails)
{
    if (!std::filesystem::exists(FULL_DEVICE))
    {
        GTEST_SKIP() << FULL_DEVICE << " is not available";
    }
    auto [controller, listener] = createBackpressureChannel();
    FileSink sink{std::move(controller), FileSinkConfig{.filePath = FULL_DEVICE, .append = true}, makeDescriptor()};

    sink.start(context);
    EXPECT_THROW(sink.execute(makeContentBuffer(), context), Exception);
}

TEST_F(FileSinkTest, StopThrowsWhenFlushFails)
{
    if (!std::filesystem::exists(FULL_DEVICE))
    {
        GTEST_SKIP() << FULL_DEVICE << " is not available";
    }
    auto [controller, listener] = createBackpressureChannel();
    FileSink sink{std::move(controller), FileSinkConfig{.filePath = FULL_DEVICE, .append = true}, makeDescriptor()};

    sink.start(context);
    EXPECT_THROW(sink.stop(context), Exception);
}

}
}
