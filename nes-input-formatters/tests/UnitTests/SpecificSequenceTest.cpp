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
#include <tuple>

#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <InputFormatterTestUtil.hpp>

/// NOLINTBEGIN(readability-magic-numbers)
namespace NES
{

/// Tests whether the SequenceShredder can handle specific sequences of input data, e.g., a single tuple without tuple delimiters, or splitting
/// up input data and giving it to the sequence shredder in a specific order.
class SpecificSequenceTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("InputFormatterTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup InputFormatterTest test class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }

    void TearDown() override { BaseUnitTest::TearDown(); }
};

TEST_F(SpecificSequenceTest, oneTupleWithTupleDelimiters)
{
    using namespace InputFormatterTestUtil;
    using enum TestDataTypes;
    using TestTuple = std::tuple<int32_t, int32_t>;
    runTest<TestTuple>(TestConfig<TestTuple>{
        .numRequiredBuffers = 3, /// 2 buffer for raw data, 1 buffer for results
        .sizeOfRawBuffers = 16,
        .sizeOfFormattedBuffers = 20,
        .parserConfig = {.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","},
        .testSchema = {INT32, INT32},
        .expectedResults = {WorkerThreadResults<TestTuple>{{{TestTuple(123456789, 123456789)}}}},
        .rawBytesPerThread
        = {/* buffer 1 */ {.sequenceNumber = SequenceNumber(1), .rawBytes = "123456789,123456"},
           /* buffer 2 */ {.sequenceNumber = SequenceNumber(2), .rawBytes = "789\n"}}});
}

/// Threads may process buffers out of order. This test simulates a scenario where the second thread process the second buffer first.
TEST_F(SpecificSequenceTest, testTaskPipelineExecutingOutOfOrder)
{
    using namespace InputFormatterTestUtil;
    using enum TestDataTypes;
    using TestTuple = std::tuple<int32_t, int32_t>;
    runTest<TestTuple>(TestConfig<TestTuple>{
        .numRequiredBuffers = 3, /// 2 buffers for raw data, 1 buffer for results
        .sizeOfRawBuffers = 16,
        .sizeOfFormattedBuffers = 20,
        .parserConfig = {.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","},
        .testSchema = {INT32, INT32},
        .expectedResults = {WorkerThreadResults<TestTuple>{{{TestTuple(123456789, 123456789)}}}},
        .rawBytesPerThread
        = {/* buffer 1 */ {.sequenceNumber = SequenceNumber(2), .rawBytes = "789\n"},
           /* buffer 2 */ {.sequenceNumber = SequenceNumber(1), .rawBytes = "123456789,123456"}}});
}

/// Threads may process buffers out of order. This test simulates a scenario where the second thread process the second buffer first.
TEST_F(SpecificSequenceTest, testTwoFullTuplesInFirstAndLastBuffer)
{
    using namespace InputFormatterTestUtil;
    using enum TestDataTypes;
    using TestTuple = std::tuple<int32_t, int32_t>;
    runTest<TestTuple>(TestConfig<TestTuple>{
        .numRequiredBuffers = 4, /// 2 buffers for raw data, two buffers for results
        .sizeOfRawBuffers = 16,
        .sizeOfFormattedBuffers = 20, /// 8 bytes metadata, 12 bytes per formatted tuple
        .parserConfig = {.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","},
        .testSchema = {INT32, INT32},
        .expectedResults = {WorkerThreadResults<TestTuple>{{{TestTuple(123456789, 12345)}, {TestTuple{12345, 123456789}}}}},
        .rawBytesPerThread
        = {/* buffer 1 */ {.sequenceNumber = SequenceNumber(1), .rawBytes = "123456789,12345\n"},
           /* buffer 2 */ {.sequenceNumber = SequenceNumber(2), .rawBytes = "12345,123456789\n"}}});
}

/// TODO #1154: We currently don't support delimiters that are larger than one byte
TEST_F(SpecificSequenceTest, DISABLED_testDelimiterThatIsMoreThanOneCharacter)
{
    using namespace InputFormatterTestUtil;
    using enum TestDataTypes;
    using TestTuple = std::tuple<int32_t, int32_t>;
    runTest<TestTuple>(TestConfig<TestTuple>{
        .numRequiredBuffers = 4, /// 2 buffers for raw data, two buffers for results
        .sizeOfRawBuffers = 16,
        .sizeOfFormattedBuffers = 20,
        .parserConfig = {.parserType = "CSV", .tupleDelimiter = "--", .fieldDelimiter = ","},
        .testSchema = {INT32, INT32},
        .expectedResults = {WorkerThreadResults<TestTuple>{{{TestTuple(123456789, 1234)}, {TestTuple{12345, 12345678}}}}},
        .rawBytesPerThread
        = {/* buffer 1 */ {.sequenceNumber = SequenceNumber(1), .rawBytes = "123456789,1234--"},
           /* buffer 2 */ {.sequenceNumber = SequenceNumber(2), .rawBytes = "12345,12345678--"}}});
}

/// Index buffer can only represent a single tuple. Requires 7 (nested) index buffers for first buffer ("1\n2\n3\n4\n5\n6\n7\n8\n").
TEST_F(SpecificSequenceTest, testMultipleTuplesInOneBuffer)
{
    using namespace InputFormatterTestUtil;
    using enum TestDataTypes;
    using TestTuple = std::tuple<int32_t>;
    runTest<TestTuple>(TestConfig<TestTuple>{
        .numRequiredBuffers = 18, /// 2 buffers for raw data, 4 buffers for results
        .sizeOfRawBuffers = 16,
        .sizeOfFormattedBuffers
        = 16, /// size of formatted tuple: 4 bytes, size of indexes: 8 bytes <-- 8 bytes metadata: 1 tuple per index buffer, 4 formatted buffers, 12 index buffers
        .parserConfig = {.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","},
        .testSchema = {INT32},
        .expectedResults = {WorkerThreadResults<TestTuple>{
            {{TestTuple{1}, TestTuple{2}, TestTuple{3}, TestTuple{4}},
             {TestTuple{5}, TestTuple{6}, TestTuple{7}, TestTuple{8}},
             {TestTuple{1234}, TestTuple{5678}, TestTuple{1001}}}}},
        .rawBytesPerThread
        = {/* buffer 1 */ {.sequenceNumber = SequenceNumber(1), .rawBytes = "1\n2\n3\n4\n5\n6\n7\n8\n"},
           /* buffer 2 */ {.sequenceNumber = SequenceNumber(2), .rawBytes = "1234\n5678\n1001\n"}}});
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
        .sizeOfRawBuffers = 16,
        .sizeOfFormattedBuffers = 28,
        .parserConfig = {.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","},
        .testSchema = {INT32, INT32, INT32, INT32},
        .expectedResults = {WorkerThreadResults<TestTuple>{{{TestTuple(123456789, 123456789, 123456789, 123456789)}}}},
        /// The third buffer has sequence number 2, connecting the first buffer (implicit delimiter) and the third (explicit delimiter)
        .rawBytesPerThread
        = {{.sequenceNumber = SequenceNumber(3), .rawBytes = "3456789\n"},
           {.sequenceNumber = SequenceNumber(1), .rawBytes = "123456789,123456"},
           {.sequenceNumber = SequenceNumber(2), .rawBytes = "789,123456789,12"}}});
}

/// As long as we set the number of bytes in a buffer correctly, it should not matter whether it is only partially full
TEST_F(SpecificSequenceTest, testMultiplePartiallyFilledBuffers)
{
    using namespace InputFormatterTestUtil;
    using enum TestDataTypes;
    using TestTuple = std::tuple<int32_t, int32_t, int32_t, int32_t>;
    runTest<TestTuple>(TestConfig<TestTuple>{
        .numRequiredBuffers = 6, /// 4 buffers for raw data, 2 buffer from results
        .sizeOfRawBuffers = 16,
        .sizeOfFormattedBuffers = 28,
        .parserConfig = {.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","},
        .testSchema = {INT32, INT32, INT32, INT32},
        .expectedResults
        = {WorkerThreadResults<TestTuple>{{{TestTuple(123, 124, 125, 126)}}},
           WorkerThreadResults<TestTuple>{{{TestTuple(127, 128, 129, 456789)}}}},
        .rawBytesPerThread
        = {{.sequenceNumber = SequenceNumber(4), .rawBytes = ",456789\n"},
           {.sequenceNumber = SequenceNumber(1), .rawBytes = "123,124,"},
           {.sequenceNumber = SequenceNumber(2), .rawBytes = "125,126\n127,128"},
           {.sequenceNumber = SequenceNumber(3), .rawBytes = ",129"}}});
}

}

/// NOLINTEND(readability-magic-numbers)
