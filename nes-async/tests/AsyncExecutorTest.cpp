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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Async/AsyncRecordLayout.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <gtest/gtest.h>
#include <AsyncExecutorRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
constexpr uint32_t POOLED_BUFFER_SIZE = 4096;
constexpr uint32_t NUMBER_OF_POOLED_BUFFERS = 64;
constexpr BufferAlignment BUFFER_ALIGNMENT{64};
constexpr double UNPOOLED_MEMORY_FRACTION = 0.5;
constexpr size_t TOTAL_MEMORY_IN_BYTES = 10 * static_cast<size_t>(NUMBER_OF_POOLED_BUFFERS) * POOLED_BUFFER_SIZE;

std::shared_ptr<BufferManager> makeBufferManager()
{
    return BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        POOLED_BUFFER_SIZE,
        std::make_shared<NesDefaultMemoryAllocator>());
}

UnqualifiedUnboundField field(const std::string& name, const DataType::Type type)
{
    return UnqualifiedUnboundField{Identifier::parse(name), type};
}

Schema<UnqualifiedUnboundField, Ordered> inputSchema()
{
    return Schema<UnqualifiedUnboundField, Ordered>{field("reviewText", DataType::Type::VARSIZED)};
}

Schema<UnqualifiedUnboundField, Ordered> outputSchema()
{
    return Schema<UnqualifiedUnboundField, Ordered>{
        field("reviewText", DataType::Type::VARSIZED), field("sentiment", DataType::Type::VARSIZED)};
}
}

class AsyncExecutorTest : public ::testing::Test
{
public:
    AsyncRecordLayout input{inputSchema()};
    AsyncRecordLayout output{outputSchema()};

    AsyncOperatorContext contextWith(std::unordered_map<std::string, std::string> config) const
    {
        return AsyncOperatorContext{.inputLayout = &input, .outputLayout = &output, .config = std::move(config), .batchSize = 4};
    }

    static std::unordered_map<std::string, std::string> delayConfig(const std::string& delayMs)
    {
        return {{"input_field", "reviewText"}, {"output_field", "sentiment"}, {"delay_ms", delayMs}};
    }

    /// Fills a buffer with one VARSIZED value per record and returns views on them.
    static std::vector<AsyncRecordView>
    makeRecords(const AsyncRecordLayout& layout, TupleBuffer& buffer, BufferManager& bufferManager, const std::vector<std::string>& texts)
    {
        std::vector<AsyncRecordView> views;
        for (size_t index = 0; index < texts.size(); ++index)
        {
            AsyncRecordWriter writer{layout, buffer, bufferManager, index};
            writer.writeText(0, texts[index]);
            views.emplace_back(layout, buffer, index);
        }
        return views;
    }
};

TEST_F(AsyncExecutorTest, RegistryProvidesTheDelayExecutor)
{
    const auto factory = AsyncExecutorRegistry::instance().find("Delay");
    ASSERT_TRUE(factory.has_value()) << "DelayExecutor is not registered";

    /// Registry keys are matched case-insensitively, like the source and sink registries.
    EXPECT_TRUE(AsyncExecutorRegistry::instance().find("delay").has_value());
    EXPECT_FALSE(AsyncExecutorRegistry::instance().find("NoSuchExecutor").has_value());

    const auto executor = (*factory)(AsyncExecutorRegistryArguments{contextWith(delayConfig("0"))});
    EXPECT_NE(executor, nullptr);
}

TEST_F(AsyncExecutorTest, ReturnsOneResultPerRecordInOrder)
{
    const auto bufferManager = makeBufferManager();
    auto buffer = bufferManager->getBufferBlocking();
    const auto factory = AsyncExecutorRegistry::instance().find("Delay");
    ASSERT_TRUE(factory.has_value());
    const auto executor = (*factory)(AsyncExecutorRegistryArguments{contextWith(delayConfig("0"))});

    const std::vector<std::string> texts{"great documentary", "terrible", "wildly entertaining"};
    const auto records = makeRecords(input, buffer, *bufferManager, texts);

    const auto results = executor->process(records);

    ASSERT_EQ(results.size(), texts.size());
    for (size_t index = 0; index < texts.size(); ++index)
    {
        ASSERT_EQ(results[index].fields.size(), 1U);
        EXPECT_EQ(results[index].fields.front().fieldIndex, output.indexOf("SENTIMENT").value());
    }
    EXPECT_EQ(results[0].fields.front().value, "GREAT DOCUMENTARY");
    EXPECT_EQ(results[1].fields.front().value, "TERRIBLE");
    EXPECT_EQ(results[2].fields.front().value, "WILDLY ENTERTAINING");
}

/// Records keep their result slot when dropped: the framework matches results positionally.
TEST_F(AsyncExecutorTest, DropPrefixDropsMatchingRecords)
{
    const auto bufferManager = makeBufferManager();
    auto buffer = bufferManager->getBufferBlocking();
    const auto factory = AsyncExecutorRegistry::instance().find("Delay");
    ASSERT_TRUE(factory.has_value());
    auto config = delayConfig("0");
    config.emplace("drop_prefix", "spam");
    const auto executor = (*factory)(AsyncExecutorRegistryArguments{contextWith(std::move(config))});

    const auto records = makeRecords(input, buffer, *bufferManager, {"spam offer", "fine", "spam"});
    const auto results = executor->process(records);

    ASSERT_EQ(results.size(), 3U);
    EXPECT_FALSE(results[0].keep);
    EXPECT_TRUE(results[1].keep);
    EXPECT_EQ(results[1].fields.front().value, "FINE");
    EXPECT_FALSE(results[2].keep);
}

TEST_F(AsyncExecutorTest, WaitsOncePerBatchNotPerRecord)
{
    const auto bufferManager = makeBufferManager();
    auto buffer = bufferManager->getBufferBlocking();
    const auto factory = AsyncExecutorRegistry::instance().find("Delay");
    ASSERT_TRUE(factory.has_value());
    const auto executor = (*factory)(AsyncExecutorRegistryArguments{contextWith(delayConfig("100"))});

    const auto records = makeRecords(input, buffer, *bufferManager, {"a", "b", "c", "d"});

    const auto start = std::chrono::steady_clock::now();
    const auto results = executor->process(records);
    const auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_EQ(results.size(), 4U);
    EXPECT_GE(elapsed, std::chrono::milliseconds{100});
    /// Four records at 100 ms each would be 400 ms; one wait per batch is the point.
    EXPECT_LT(elapsed, std::chrono::milliseconds{350});
}

TEST_F(AsyncExecutorTest, ResultsCanBeWrittenIntoAnOutputRecord)
{
    const auto bufferManager = makeBufferManager();
    auto inputBuffer = bufferManager->getBufferBlocking();
    auto outputBuffer = bufferManager->getBufferBlocking();
    const auto factory = AsyncExecutorRegistry::instance().find("Delay");
    ASSERT_TRUE(factory.has_value());
    const auto executor = (*factory)(AsyncExecutorRegistryArguments{contextWith(delayConfig("0"))});

    const auto records = makeRecords(input, inputBuffer, *bufferManager, {"great documentary", "terrible"});
    const auto results = executor->process(records);
    ASSERT_EQ(results.size(), 2U);

    /// This is what the consumer source will do: carry the input fields over, then apply
    /// whatever the executor produced.
    for (size_t index = 0; index < results.size(); ++index)
    {
        AsyncRecordWriter writer{output, outputBuffer, *bufferManager, index};
        writer.copyMatchingFields(records[index]);
        for (const auto& [fieldIndex, value] : results[index].fields)
        {
            writer.writeAsText(fieldIndex, value);
        }
    }

    const AsyncRecordView first{output, outputBuffer, 0};
    EXPECT_EQ(first.readText(0), "great documentary");
    EXPECT_EQ(first.readText(1), "GREAT DOCUMENTARY");
    const AsyncRecordView second{output, outputBuffer, 1};
    EXPECT_EQ(second.readText(0), "terrible");
    EXPECT_EQ(second.readText(1), "TERRIBLE");
}

TEST_F(AsyncExecutorTest, WriteAsTextConvertsIntoTheDeclaredType)
{
    const auto bufferManager = makeBufferManager();
    auto buffer = bufferManager->getBufferBlocking();
    const AsyncRecordLayout numericLayout{
        Schema<UnqualifiedUnboundField, Ordered>{field("score", DataType::Type::FLOAT64), field("count", DataType::Type::UINT32)}};

    AsyncRecordWriter writer{numericLayout, buffer, *bufferManager, 0};
    writer.writeAsText(0, "0.75");
    writer.writeAsText(1, "17");

    const AsyncRecordView view{numericLayout, buffer, 0};
    EXPECT_EQ(view.readAsText(0), "0.75");
    EXPECT_EQ(view.readAsText(1), "17");

    /// A value that does not parse is an error, not a silently stored zero.
    EXPECT_THROW(writer.writeAsText(1, "not a number"), Exception);
}

TEST_F(AsyncExecutorTest, RejectsIncompleteConfiguration)
{
    const auto factory = AsyncExecutorRegistry::instance().find("Delay");
    ASSERT_TRUE(factory.has_value());

    /// Missing output_field.
    EXPECT_THROW((*factory)(AsyncExecutorRegistryArguments{contextWith({{"input_field", "reviewText"}})}), Exception);
    /// Field that the schema does not have.
    EXPECT_THROW(
        (*factory)(AsyncExecutorRegistryArguments{contextWith({{"input_field", "nope"}, {"output_field", "sentiment"}})}), Exception);
    /// Delay that is not a number.
    EXPECT_THROW(
        (*factory)(AsyncExecutorRegistryArguments{
            contextWith({{"input_field", "reviewText"}, {"output_field", "sentiment"}, {"delay_ms", "soon"}})}),
        Exception);
}

}
