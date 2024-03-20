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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Operators/Arrow/ArrowRecordBatchScan.hpp>
#include <Execution/Operators/Arrow/RecordBufferWrapper.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/file.h>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Operators {

class ArrowScanOperatorTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ArrowScanOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ArrowScanOperatorTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ArrowScanOperatorTest test class."); }
};

/**
 * @brief Scan operator that reads csv via arrow.
 */
TEST_F(ArrowScanOperatorTest, DISABLED_scanArrowBufferFromCSV) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto ptf = DefaultPhysicalTypeFactory();
    auto scanOperator = ArrowRecordBatchScan(schema);

    arrow::io::IOContext io_context = arrow::io::default_io_context();
    std::shared_ptr<arrow::io::InputStream> input =
        arrow::io::ReadableFile::Open(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv").ValueOrDie();

    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    // Instantiate TableReader from input stream and options
    auto maybe_reader = arrow::csv::TableReader::Make(io_context, input, read_options, parse_options, convert_options);
    if (!maybe_reader.ok()) {
        GTEST_FAIL();
    }
    std::shared_ptr<arrow::csv::TableReader> reader = *maybe_reader;

    // Read table from CSV file
    auto maybe_table = reader->Read();
    auto table = maybe_table.ValueOrDie();

    auto collector = std::make_shared<CollectOperator>();
    scanOperator.setChild(collector);

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));

    auto tbReader = arrow::TableBatchReader(*table.get());
    for (auto batch : tbReader) {
        auto wrapper = std::make_unique<Operators::RecordBufferWrapper>(batch.MoveValueUnsafe());
        auto tb = Runtime::TupleBuffer::wrapPtr(std::move(wrapper));
        RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(tb)));
        scanOperator.open(ctx, recordBuffer);
    }

    ASSERT_EQ(collector->records.size(), 27);
    ASSERT_EQ(collector->records[0].numberOfFields(), 2);
}
}// namespace NES::Runtime::Execution::Operators