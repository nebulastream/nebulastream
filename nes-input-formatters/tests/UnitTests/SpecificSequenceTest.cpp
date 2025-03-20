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

#include <cstdint>
#include <memory>
#include <API/Schema.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <MemoryLayout/RowLayoutField.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Util/TestUtil.hpp>
#include <BaseUnitTest.hpp>
#include <InputFormatterTestUtil.hpp>
#include <TestTaskQueue.hpp>


namespace NES
{

class SpecificSequenceTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("InputFormatterTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup InputFormatterTest test class.");
    }
    void SetUp() override { BaseUnitTest::SetUp(); }

    void TearDown() override { BaseUnitTest::TearDown(); }
};

TEST_F(SpecificSequenceTest, testTaskPipelineWithMultipleTasksOneRawByteBuffer)
{
    using namespace InputFormatterTestUtil;
    using enum TestDataTypes;
    using TestTuple = std::tuple<int32_t, int32_t>;
    runTest<TestTuple>(TestConfig<TestTuple>{
        .numRequiredBuffers = 3, /// 2 buffer for raw data, 1 buffer for results
        .bufferSize = 16,
        .parserConfig = {.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","},
        .testSchema = {INT32, INT32},
        .expectedResults = {WorkerThreadResults<TestTuple>{{{TestTuple(123456789, 123456789)}}}},
        .rawBytesPerThread
        = {/* buffer 1 */ {SequenceNumber(1), "123456789,123456"},
           /* buffer 2 */ {SequenceNumber(2), "789"}}});
}

/// Each thread should share the same InputFormatterTask, meaning that we need to check that threads don't interfere with each other's state
TEST_F(SpecificSequenceTest, testTaskPipelineExecutingOnTwoDifferentThreads)
{
    using namespace InputFormatterTestUtil;
    using enum TestDataTypes;
    using TestTuple = std::tuple<int32_t, int32_t>;
    runTest<TestTuple>(TestConfig<TestTuple>{
        .numRequiredBuffers = 3, /// 2 buffers for raw data, 1 buffer for results
        .bufferSize = 16,
        .parserConfig = {.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","},
        .testSchema = {INT32, INT32},
        .expectedResults = {WorkerThreadResults<TestTuple>{{{TestTuple(123456789, 123456789)}}}},
        .rawBytesPerThread
        = {/* buffer 1 */ {SequenceNumber(1), "123456789,123456"},
           /* buffer 2 */ {SequenceNumber(2), "789"}}});
}

/// Threads may process buffers out of order. This test simulates a scenario where the second thread process the second buffer first.
TEST_F(SpecificSequenceTest, testTaskPipelineExecutingOnTwoDifferentThreadsOutOfOrder)
{
    using namespace InputFormatterTestUtil;
    using enum TestDataTypes;
    using TestTuple = std::tuple<int32_t, int32_t>;
    runTest<TestTuple>(TestConfig<TestTuple>{
        .numRequiredBuffers = 3, /// 2 buffers for raw data, 1 buffer for results
        .bufferSize = 16,
        .parserConfig = {.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","},
        .testSchema = {INT32, INT32},
        .expectedResults = {WorkerThreadResults<TestTuple>{{{TestTuple(123456789, 123456789)}}}},
        .rawBytesPerThread
        = {/* buffer 1 */ {SequenceNumber(2), "789"},
           /* buffer 2 */ {SequenceNumber(1), "123456789,123456"}}});
}

/// Threads may process buffers out of order. This test simulates a scenario where the second thread process the second buffer first.
TEST_F(SpecificSequenceTest, testTwoFullTuplesInFirstAndLastBuffer)
{
    using namespace InputFormatterTestUtil;
    using enum TestDataTypes;
    using TestTuple = std::tuple<int32_t, int32_t>;
    runTest<TestTuple>(TestConfig<TestTuple>{
        .numRequiredBuffers = 4, /// 2 buffers for raw data, two buffers for results
        .bufferSize = 16,
        .parserConfig = {.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","},
        .testSchema = {INT32, INT32},
        .expectedResults = {WorkerThreadResults<TestTuple>{{{TestTuple(123456789, 12345)}, {TestTuple{12345, 123456789}}}}},
        .rawBytesPerThread
        = {/* buffer 1 */ {SequenceNumber(1), "123456789,12345\n"},
           /* buffer 2 */ {SequenceNumber(2), "12345,123456789\n"}}});
}

TEST_F(SpecificSequenceTest, testDelimiterThatIsMoreThanOneCharacter)
{
    using namespace InputFormatterTestUtil;
    using enum TestDataTypes;
    using TestTuple = std::tuple<int32_t, int32_t>;
    runTest<TestTuple>(TestConfig<TestTuple>{
        .numRequiredBuffers = 4, /// 2 buffers for raw data, two buffers for results
        .bufferSize = 16,
        .parserConfig = {.parserType = "CSV", .tupleDelimiter = "--", .fieldDelimiter = ","},
        .testSchema = {INT32, INT32},
        .expectedResults = {WorkerThreadResults<TestTuple>{{{TestTuple(123456789, 1234)}, {TestTuple{12345, 12345678}}}}},
        .rawBytesPerThread
        = {/* buffer 1 */ {SequenceNumber(1), "123456789,1234--"},
           /* buffer 2 */ {SequenceNumber(2), "12345,12345678--"}}});
}

TEST_F(SpecificSequenceTest, testMultipleTuplesInOneBuffer)
{
    using namespace InputFormatterTestUtil;
    using enum TestDataTypes;
    using TestTuple = std::tuple<int32_t>;
    runTest<TestTuple>(TestConfig<TestTuple>{
        .numRequiredBuffers = 6, /// 2 buffers for raw data, 4 buffers for results
        .bufferSize = 16,
        .parserConfig = {.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","},
        .testSchema = {INT32},
        .expectedResults = {WorkerThreadResults<TestTuple>{
            {{TestTuple{1}, TestTuple{2}, TestTuple{3}, TestTuple{4}},
             {TestTuple{5}, TestTuple{6}, TestTuple{7}, TestTuple{8}},
             {TestTuple{1234}, TestTuple{5678}, TestTuple{1001}},
             {TestTuple{1}}}}},
        .rawBytesPerThread
        = {/* buffer 1 */ {SequenceNumber(1), "1\n2\n3\n4\n5\n6\n7\n8\n"},
           /* buffer 2 */ {SequenceNumber(2), "1234\n5678\n1001\n1"}}});
}

/// The third buffer has sequence number 2, connecting the first buffer (implicit delimiter) and the third (explicit delimiter)
/// [TD][NTD][TD] (TD: TupleDelimiter, NTD: No TupleDelimiter)
TEST_F(SpecificSequenceTest, triggerSpanningTupleWithThirdBufferWithoutDelimiter)
{
    using namespace InputFormatterTestUtil;
    using enum TestDataTypes;
    using TestTuple = std::tuple<int32_t, int32_t, int32_t, int32_t>;
    runTest<TestTuple>(TestConfig<TestTuple>{
        .numRequiredBuffers = 4, /// 3 buffers for raw data, 1 buffer from results
        .bufferSize = 16,
        .parserConfig = {.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","},
        .testSchema = {INT32, INT32, INT32, INT32},
        .expectedResults = {WorkerThreadResults<TestTuple>{{{TestTuple(123456789, 123456789, 123456789, 123456789)}}}},
        /// The third buffer has sequence number 2, connecting the first buffer (implicit delimiter) and the third (explicit delimiter)
        .rawBytesPerThread
        = {{SequenceNumber(3), "3456789\n"}, {SequenceNumber(1), "123456789,123456"}, {SequenceNumber(2), "789,123456789,12"}}});
}

/// As long as we set the number of bytes in a buffer correctly, it should not matter whether it is only partially full
TEST_F(SpecificSequenceTest, testMultiplePartiallyFilledBuffers)
{
    using namespace InputFormatterTestUtil;
    using enum TestDataTypes;
    using TestTuple = std::tuple<int32_t, int32_t, int32_t, int32_t>;
    runTest<TestTuple>(TestConfig<TestTuple>{
        .numRequiredBuffers = 6, /// 4 buffers for raw data, 2 buffer from results
        .bufferSize = 16,
        .parserConfig = {.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","},
        .testSchema = {INT32, INT32, INT32, INT32},
        .expectedResults
        = {WorkerThreadResults<TestTuple>{{{TestTuple(123, 123, 123, 123)}}},
           WorkerThreadResults<TestTuple>{{{TestTuple(123, 123, 123, 456789)}}}},
        .rawBytesPerThread
        = {{SequenceNumber(4), ",456789"},
           {SequenceNumber(1), "123,123,"},
           {SequenceNumber(2), "123,123\n123,123"}, /// only full buffer
           {SequenceNumber(3), ",123"}}});
}

}
