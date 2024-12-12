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
#include <InputFormatters/InputFormatterOperatorHandler.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <MemoryLayout/RowLayoutField.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Util/TestUtil.hpp>
#include <BaseUnitTest.hpp>
#include <TestTaskQueue.hpp>


namespace NES
{
namespace Memory::MemoryLayouts
{
class RowLayout;
}

enum class DataTypes : uint8_t
{
    INT8,
    UINT8,
    INT16,
    UINT16,
    INT32,
    UINT32,
    INT64,
    FLOAT32,
    UINT64,
    FLOAT64,
    BOOLEAN,
    CHAR,
    VARSIZED,
};

class InputFormatterTest : public Testing::BaseUnitTest
{
public:
    using ResultBufferVectors = std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>>;
    ResultBufferVectors resultBuffers;
    std::shared_ptr<NES::Memory::BufferManager> testBufferManager;
    std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers;

    struct TestSchema {

        // Note: removed 'explicit' keyword (or never had it)
        TestSchema(std::initializer_list<DataTypes> dataTypes)
        {
            schema = Schema::create();
            for (size_t fieldNumber = 1; auto dataType : dataTypes)
            {
                switch (dataType)
                {
                    case DataTypes::UINT8:
                        schema->addField(std::to_string(fieldNumber) + "_UINT8", DataTypeFactory::createUInt8());
                        break;
                    case DataTypes::UINT16:
                        schema->addField(std::to_string(fieldNumber) + "_UINT16", DataTypeFactory::createUInt16());
                        break;
                    case DataTypes::UINT32:
                        schema->addField(std::to_string(fieldNumber) + "_UINT32", DataTypeFactory::createUInt32());
                        break;
                    case DataTypes::UINT64:
                        schema->addField(std::to_string(fieldNumber) + "_UINT64", DataTypeFactory::createUInt64());
                        break;
                    case DataTypes::INT8:
                        schema->addField(std::to_string(fieldNumber) + "_INT8", DataTypeFactory::createInt8());
                        break;
                    case DataTypes::INT16:
                        schema->addField(std::to_string(fieldNumber) + "_INT16", DataTypeFactory::createInt16());
                        break;
                    case DataTypes::INT32:
                        schema->addField(std::to_string(fieldNumber) + "_INT32", DataTypeFactory::createInt32());
                        break;
                    case DataTypes::INT64:
                        schema->addField(std::to_string(fieldNumber) + "_INT64", DataTypeFactory::createInt64());
                        break;
                    case DataTypes::FLOAT32:
                        schema->addField(std::to_string(fieldNumber) + "_FLOAT32", DataTypeFactory::createFloat());
                        break;
                    case DataTypes::FLOAT64:
                        schema->addField(std::to_string(fieldNumber) + "_FLOAT64", DataTypeFactory::createDouble());
                        break;
                    case DataTypes::BOOLEAN:
                        schema->addField(std::to_string(fieldNumber) + "_BOOLEAN", DataTypeFactory::createBoolean());
                        break;
                    case DataTypes::CHAR:
                        schema->addField(std::to_string(fieldNumber) + "_CHAR", DataTypeFactory::createChar());
                        break;
                    case DataTypes::VARSIZED:
                        schema->addField(std::to_string(fieldNumber) + "_VARSIZED", DataTypeFactory::createVariableSizedData());
                        break;
                }
            }
        }

        std::shared_ptr<Schema> schema;
    };

    template <typename TupleSchemaTemplate>
    struct TestConfig
    {
        const size_t numRequiredBuffers;
        const size_t numThreads;
        const uint64_t bufferSize;
        const Sources::ParserConfig parserConfig;
        const TestSchema testSchema;
        // const std::shared_ptr<Schema> schema;
        /// Each workerThread(vector) can produce multiple buffers(vector) with multiple tuples(vector<TupleSchemaTemplate>)
        const std::vector<std::vector<std::vector<TupleSchemaTemplate>>> expectedResults;
        const std::string rawBytes;
        using TupleSchema = TupleSchemaTemplate;
    };

    static void SetUpTestCase()
    {
        Logger::setupLogging("InputFormatterTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup InputFormatterTest test class.");
    }
    void SetUp() override { BaseUnitTest::SetUp(); }

    void TearDown() override
    {
        /// Let go of all TupleBuffer references owned by the vector holding the results, then destroy the BufferManager.
        resultBuffers->clear();
        operatorHandlers.clear();
        testBufferManager->destroy();
        BaseUnitTest::TearDown();
    }

    static std::shared_ptr<NES::Memory::BufferManager> getBufferManager(const size_t numRequiredBuffers, const size_t bufferSize)
    {
        return Memory::BufferManager::create(bufferSize, numRequiredBuffers);
    }


    template <typename TupleSchemaTemplate>
    void setupTest(const TestConfig<TupleSchemaTemplate>& TestConfig)
    {
        /// Multiplying result buffers x2, because we want to create one expected buffer, for each result buffer.
        this->resultBuffers = std::make_shared<std::vector<std::vector<NES::Memory::TupleBuffer>>>(TestConfig.numThreads);
        this->operatorHandlers = {std::make_shared<InputFormatterOperatorHandler>()};
        this->testBufferManager = getBufferManager(TestConfig.numRequiredBuffers * 2, TestConfig.bufferSize);
    }

    template <typename TupleSchemaTemplate>
    TestablePipelineTask createInputFormatterTask(
        SequenceNumber sequenceNumber,
        Memory::TupleBuffer taskBuffer,
        const TestConfig<TupleSchemaTemplate>& testConfig)
    {
        taskBuffer.setSequenceNumber(sequenceNumber);
        // Todo: refactor InputFormatterProvider to take parser config instead of individual parameters
        auto inputFormatter = InputFormatters::InputFormatterProvider::provideInputFormatter(
            testConfig.parserConfig.parserType, *testConfig.testSchema.schema, testConfig.parserConfig.tupleDelimiter, testConfig.parserConfig.fieldDelimiter);
        return TestablePipelineTask(sequenceNumber, taskBuffer, std::move(inputFormatter), testBufferManager, resultBuffers);
    }

    bool validateResult(
        const std::vector<std::vector<NES::Memory::TupleBuffer>>& actualResultVectorVector,
        const std::vector<std::vector<NES::Memory::TupleBuffer>>& expectedResultVectorVector,
        const size_t sizeOfSchemaInBytes)
    {
        /// check that vectors of vectors contain the same number of vectors.
        bool isValid = true;
        isValid &= (actualResultVectorVector.size() == expectedResultVectorVector.size());

        /// iterate over all vectors in the actual results
        for (size_t taskIndex = 0; const auto& actualResultVector : actualResultVectorVector)
        {
            /// check that the corresponding vector in the vector of vector containing the expected results is of the same size
            isValid &= (actualResultVector.size() == expectedResultVectorVector[taskIndex].size());
            /// iterate over all buffers in the vector containing the actual results and compare the buffer with the corresponding buffers
            /// in the expected results.
            for (size_t bufferIndex = 0; const auto& actualResultBuffer : actualResultVector)
            {
                isValid &= TestUtil::checkIfBuffersAreEqual(
                    actualResultBuffer, expectedResultVectorVector[taskIndex][bufferIndex], sizeOfSchemaInBytes);
                ++bufferIndex;
            }
            ++taskIndex;
        }
        return isValid;
    }

    template <typename TupleSchemaTemplate>
    std::vector<std::vector<Memory::TupleBuffer>>
    createExpectedResults(const TestConfig<TupleSchemaTemplate>& testConfig)
    {
        std::vector<std::vector<Memory::TupleBuffer>> expectedTupleBuffers(testConfig.numThreads);
        /// expectedWorkerThreadVector: vector<vector<TupleSchemaTemplate>>
        for (size_t workerThreadId = 0; const auto expectedTaskVector : testConfig.expectedResults)
        {
            /// expectedBuffersVector: vector<TupleSchemaTemplate>
            for (const auto& expectedBuffersVector : expectedTaskVector)
            {
                expectedTupleBuffers.at(workerThreadId)
                    .emplace_back(
                        TestUtil::createTestTupleBufferFromTuples<TupleSchemaTemplate, false, true>(
                            testConfig.testSchema.schema, *testBufferManager, expectedBuffersVector));
            }
            ++workerThreadId;
        }
        return expectedTupleBuffers;
    }

    template <typename TupleSchemaTemplate>
    void runTest(const TestConfig<TupleSchemaTemplate>& testConfig)
    {
        setupTest<TupleSchemaTemplate>(testConfig);
        auto inputBuffers = TestUtil::createTestTupleBuffersFromString(testConfig.rawBytes, *testBufferManager);

        std::vector<TestablePipelineTask> tasks;
        std::vector<WorkerThreadId> workerThreadIds;
        for (size_t i = 0; const auto& inputBuffer : inputBuffers)
        {
            tasks.emplace_back(createInputFormatterTask<TupleSchemaTemplate>(SequenceNumber(i), inputBuffers.at(i), testConfig));
            tasks.at(i).setOperatorHandlers(operatorHandlers);
            // Todo: make assignment of Tasks to worker ids explicit
            workerThreadIds.emplace_back(WorkerThreadId(i));
            ++i;
        }

        /// create task queue and add tasks to it.
        TestTaskQueue taskQueue(testConfig.numThreads);
        taskQueue.enqueueTasks(workerThreadIds, std::move(tasks));
        taskQueue.startProcessing();

        auto expectedResultVectors = createExpectedResults<TupleSchemaTemplate>(testConfig);


        ASSERT_TRUE(validateResult(*resultBuffers, expectedResultVectors, testConfig.testSchema.schema->getSchemaSizeInBytes()));
    }
};

TEST_F(InputFormatterTest, testTaskPipelineWithMultipleTasksOneRawByteBuffer)
{
    // Todo: make assignment of tasks to worker ids explicit
    using enum DataTypes;
    using TestTuple = std::tuple<int32_t, int32_t>;
    runTest(
        TestConfig<TestTuple>{
            .numRequiredBuffers = 2,
            .numThreads = 2,
            .bufferSize = 16,
            .parserConfig = {.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","},
            .testSchema = {INT32, INT32},
            .expectedResults = {{}, {{TestTuple(123456789, 123456789)}}},
            .rawBytes = "123456789,123456" // buffer 1
                        "789\n"} // buffer 2
    );
}

// Todo: test with multiple buffers/tasks
// TEST_F(InputFormatterTest, testTaskPipelineWithFunction)
// {
//     constexpr size_t NUM_THREADS = 1;
//     constexpr size_t NUM_TASKS = 1;
//     constexpr size_t NUM_RESULT_BUFFERS = 1;
//     auto testBufferManager = getBufferManager(1, 1, 16);
//     auto resultBuffers = std::make_shared<std::vector<std::vector<NES::Memory::TupleBuffer>>>(NUM_RESULT_BUFFERS);
//
//     /// Create and fill buffers for tasks.
//     using TestTuple = std::tuple<int>;
//     SchemaPtr schema = Schema::create()->addField("INT", BasicType::INT32);
//     auto testTupleBuffer = TestUtil::createTestTupleBufferFromTuples(schema, *testBufferManager, TestTuple(42));
//
//     /// create task queue and add tasks to it.
//     TestTaskQueue taskQueue(NUM_THREADS, NUM_TASKS);
//     auto stage1 = std::make_unique<TestablePipelineStage>(
//         "step_1",
//         [](const Memory::TupleBuffer& tupleBuffer, Runtime::Execution::PipelineExecutionContext& pec)
//         {
//             auto theNewBuffer = pec.getBufferManager()->getBufferBlocking();
//             const auto theOnlyIntInBuffer = *tupleBuffer.getBuffer<int>();
//             *theNewBuffer.getBuffer<int>() = theOnlyIntInBuffer;
//             pec.emitBuffer(theNewBuffer, NES::Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
//         });
//     auto task1
//         = std::make_unique<TestablePipelineTask>(SequenceNumber(0), testTupleBuffer, std::move(stage1), testBufferManager, resultBuffers);
//
//     taskQueue.enqueueTask(std::move(task1));
//
//     taskQueue.waitForCompletion();
//     ASSERT_FALSE(resultBuffers->at(0).empty());
//     auto resultBuffer = resultBuffers->at(0).at(0);
//     ASSERT_EQ(*resultBuffer.getBuffer<int>(), 42);
// }
}
