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
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <DataParser/RuntimeSchemaAdapter.hpp>
#include <DataParser/StaticParser.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/ScanRaw.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/FormatType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/ZmqSource.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <generation/CSVAdapter.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Operators {

class ScanRawOperatorTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ScanRawOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ScanRawOperatorTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ScanRawOperatorTest test class."); }

  protected:
    TupleBuffer prepareBuffer(std::string_view csv) {
        auto data = new uint8_t[csv.size()];
        auto buffer = TupleBuffer::wrapMemory(data, csv.size(), [](auto a, auto) {
            delete[] a->getPointer();
            delete a;
        });
        std::copy(csv.begin(), csv.end(), data);
        buffer.setNumberOfTuples(csv.size());

        return buffer;
    }
};

TEST_F(ScanRawOperatorTest, ScanRawBuffer) {
    using namespace NES::DataParser;
    using namespace std::literals;
    using Schema = StaticSchema<Field<INT64, "f1">, Field<INT64, "f2">>;
    auto parser = std::make_unique<StaticParser<Schema, FormatTypes::CSV_FORMAT>>();
    auto scanOperator = ScanRaw(runtimeSchema<Schema>(), std::move(parser));
    auto collector = std::make_shared<CollectOperator>();
    scanOperator.setChild(collector);

    auto buffer = prepareBuffer(R"(-1234,423
4232,-2345
)"sv);

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(reinterpret_cast<int8_t*>(std::addressof(buffer))));
    scanOperator.open(ctx, recordBuffer);

    ASSERT_EQ(collector->records.size(), 2);
    auto& record = collector->records[0];
    ASSERT_EQ(record.numberOfFields(), 2);
    ASSERT_EQ(record.read("f1").as<Int64>().value->getValue(), -1234);
    ASSERT_EQ(record.read("f2").as<Int64>().value->getValue(), 423);

    auto& record1 = collector->records[1];
    ASSERT_EQ(record1.numberOfFields(), 2);
    ASSERT_EQ(record1.read("f1").as<Int64>().value->getValue(), 4232);
    ASSERT_EQ(record1.read("f2").as<Int64>().value->getValue(), -2345);
}

}// namespace NES::Runtime::Execution::Operators
