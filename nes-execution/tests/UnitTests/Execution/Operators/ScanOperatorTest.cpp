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

#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Operators {

class ScanOperatorTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ScanOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ScanOperatorTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ScanOperatorTest test class."); }
};

/**
 * @brief Scan operator that reads row oriented tuple buffer.
 */
TEST_F(ScanOperatorTest, scanRowLayoutBuffer) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto memoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = Scan(std::move(memoryProviderPtr));
    auto collector = std::make_shared<CollectOperator>();
    scanOperator.setChild(collector);

    auto buffer = bm->getBufferBlocking();
    auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, buffer);
    for (auto i = 0_u64; i < testBuffer.getCapacity(); i++) {
        testBuffer[i]["f1"].write((int64_t) i % 2_s64);
        testBuffer[i]["f2"].write(+1_s64);
        testBuffer.setNumberOfTuples(i + 1);
    }
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(buffer)));
    scanOperator.open(ctx, recordBuffer);

    ASSERT_EQ(collector->records.size(), testBuffer.getNumberOfTuples());
    for (uint64_t i = 0; i < collector->records.size(); i++) {
        auto& record = collector->records[i];
        ASSERT_EQ(record.numberOfFields(), 2);
        ASSERT_EQ(record.read("f1"), (int64_t) i % 2);
        ASSERT_EQ(record.read("f2"), 1_s64);
    }
}

/**
 * @brief Scan operator that reads columnar oriented tuple buffer.
 */
TEST_F(ScanOperatorTest, scanColumnarLayoutBuffer) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto schema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto columnMemoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, bm->getBufferSize());

    std::unique_ptr<MemoryProvider::MemoryProvider> memoryProviderPtr =
        std::make_unique<MemoryProvider::ColumnMemoryProvider>(columnMemoryLayout);
    auto scanOperator = Scan(std::move(memoryProviderPtr));
    auto collector = std::make_shared<CollectOperator>();
    scanOperator.setChild(collector);

    auto buffer = bm->getBufferBlocking();
    auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(columnMemoryLayout, buffer);
    for (uint64_t i = 0; i < testBuffer.getCapacity(); i++) {
        testBuffer[i]["f1"].write((int64_t) i % 2);
        testBuffer[i]["f2"].write(+1_s64);
        testBuffer.setNumberOfTuples(i + 1);
    }
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(buffer)));
    scanOperator.open(ctx, recordBuffer);

    ASSERT_EQ(collector->records.size(), testBuffer.getNumberOfTuples());
    for (uint64_t i = 0; i < collector->records.size(); i++) {
        auto& record = collector->records[i];
        ASSERT_EQ(record.numberOfFields(), 2);
        ASSERT_EQ(record.read("f1"), (int64_t) i % 2);
        ASSERT_EQ(record.read("f2"), 1_s64);
    }
}

}// namespace NES::Runtime::Execution::Operators