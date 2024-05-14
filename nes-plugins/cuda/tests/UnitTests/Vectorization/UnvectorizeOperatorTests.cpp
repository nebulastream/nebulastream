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
#include <BaseUnitTest.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Vectorization/Unvectorize.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>

namespace NES::Runtime::Execution::Operators {

class UnvectorizeOperatorTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("UnvectorizeOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup UnvectorizeOperatorTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down UnvectorizeOperatorTest test class."); }
};

/**
 * @brief Unvectorize operator that processes a tuple buffer by calling the child operator on each tuple one by one.
 */
TEST_F(UnvectorizeOperatorTest, unvectorizeTupleBuffer__GPU) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto buffer = bm->getBufferBlocking();
    auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < testBuffer.getCapacity(); i++) {
        testBuffer[i]["f1"].write((int64_t) i % 2);
        testBuffer[i]["f2"].write((int64_t) 1);
        testBuffer.setNumberOfTuples(i + 1);
    }

    auto memoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    std::vector<Record::RecordFieldIdentifier> projections = {"f1", "f2"};
    auto unvectorizeOperator = Unvectorize(std::move(memoryProviderPtr), projections);
    auto collector = std::make_shared<CollectOperator>();
    unvectorizeOperator.setChild(collector);

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(buffer)));

    unvectorizeOperator.execute(ctx, recordBuffer);

    ASSERT_EQ(collector->records.size(), testBuffer.getNumberOfTuples());
    for (uint64_t i = 0; i < collector->records.size(); i++) {
        auto& record = collector->records[i];
        ASSERT_EQ(record.numberOfFields(), 2);
        EXPECT_EQ(record.read("f1"), (int64_t) i % 2);
        EXPECT_EQ(record.read("f2"), (int64_t) 1);
    }
}

}// namespace NES::Runtime::Execution::Operators
