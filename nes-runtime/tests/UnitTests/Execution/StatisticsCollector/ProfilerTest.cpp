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
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Execution/StatisticsCollector/Profiler.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Operators {

class ProfilerTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ProfilerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ScanOperatorTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ProfilerTest test class."); }
};

/**
 * @brief test the profiler
 */
TEST_F(ProfilerTest, scanRowLayoutBuffer) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto readF1 = std::make_shared<Expressions::ReadFieldExpression>("f1");
    auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
    auto equalsExpression = std::make_shared<Expressions::GreaterThanExpression>(readF1, readF2);
    auto selectionOperator = Operators::Selection(equalsExpression);
    auto collector = std::make_shared<Operators::CollectOperator>();
    selectionOperator.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));
    std::vector<Record> recordBuffer = std::vector<Record>();

    for (int i = 0; i < 10; ++i) {
        recordBuffer.push_back(Record({{"f1", Value<>(10)}, {"f2", Value<>(5)}}));
    }

    std::vector<perf_hw_id> events;
    events.push_back(PERF_COUNT_HW_BRANCH_MISSES);
    events.push_back(PERF_COUNT_HW_CACHE_MISSES);

    auto profiler = Profiler(events);
    uint64_t branchMissesId = profiler.getEventId(PERF_COUNT_HW_BRANCH_MISSES);
    uint64_t cacheMissesId = profiler.getEventId(PERF_COUNT_HW_CACHE_MISSES);
    const char *fileName = "profilerTestOutput.txt";

    for (Record record : recordBuffer) {
        profiler.startProfiling();

        selectionOperator.execute(ctx, record);

        profiler.stopProfiling();

        ASSERT_NE(profiler.getCount(branchMissesId), 0);
        ASSERT_NE(profiler.getCount(cacheMissesId), 0);
        profiler.writeToOutputFile(fileName);
    }

    ASSERT_EQ(collector->records.size(), 10);

    std::ifstream file(fileName);
    std::string line;
    if (file.is_open()) {
        int64_t count = 0;
        while (std::getline(file, line)) {
            std::cout << line << std::endl;
            ++count;
        }
        file.close();
        remove(fileName);
        ASSERT_EQ(count, events.size() * 10);
    } else {
        NES_THROW_RUNTIME_ERROR("Can not open file");
    }
}
}// namespace NES::Runtime::Execution::Operators