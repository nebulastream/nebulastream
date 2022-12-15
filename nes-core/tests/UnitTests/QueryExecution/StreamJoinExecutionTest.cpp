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
// clang-format: off
#include "gtest/gtest.h"
// clang-format: on
#include <NesBaseTest.hpp>
#include <Util/TestExecutionEngine.hpp>

namespace NES::Runtime::Execution {

class StreamJoinQueryExecutionTest : public Testing::TestWithErrorHandling<testing::Test>,
                                     public ::testing::WithParamInterface<QueryCompilation::QueryCompilerOptions::QueryCompiler> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StreamJoinQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("QueryExecutionTest: Setup StreamJoinQueryExecutionTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling<testing::Test>::SetUp();
        auto queryCompiler = this->GetParam();
        executionEngine = std::make_shared<TestExecutionEngine>(queryCompiler);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("QueryExecutionTest: Tear down StreamJoinQueryExecutionTest test case.");
        ASSERT_TRUE(executionEngine->stop());
        Testing::TestWithErrorHandling<testing::Test>::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("QueryExecutionTest: Tear down StreamJoinQueryExecutionTest test class."); }

    std::shared_ptr<TestExecutionEngine> executionEngine;
};

std::vector<Runtime::TupleBuffer> fillBuffersAndNLJ(Runtime::MemoryLayouts::DynamicTupleBuffer& leftBuffer,
                                                    Runtime::MemoryLayouts::DynamicTupleBuffer& rightBuffer,
                                                    std::shared_ptr<TestExecutionEngine> executionEngine,
                                                    size_t numberOfTuplesToProduce, size_t windowSize,
                                                    const std::string& timeStampFieldName, SchemaPtr joinSchema,
                                                    const std::string& joinFieldNameLeft, const std::string& joinFieldNameRight) {
    for (auto i = 0UL; i < numberOfTuplesToProduce + 1; ++i) {
        leftBuffer[i][0].write(i + 1000);
        leftBuffer[i][1].write((i % 10) + 10);
        leftBuffer[i][2].write(i);

        rightBuffer[i][0].write(i + 2000);
        rightBuffer[i][1].write((i % 10) + 10);
        rightBuffer[i][2].write(i);
    }


    uint64_t lastTupleTimeStampWindow = windowSize - 1;
    uint64_t firstTupleTimeStampWindow = 0;
    auto bufferJoined = executionEngine->getBuffer(joinSchema).getBuffer();
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(joinSchema, bufferJoined.getBufferSize());

    // Perform NLJ so that we can check for correctness later on
    std::vector<Runtime::MemoryLayouts::DynamicTupleBuffer> nljBuffers;
    for (auto outer = 0UL; outer < leftBuffer.getNumberOfTuples(); ++outer) {
        auto timeStampLeft = leftBuffer[outer][timeStampFieldName].read<uint64_t>();
        if (timeStampLeft > lastTupleTimeStampWindow) {
            lastTupleTimeStampWindow += windowSize;
            firstTupleTimeStampWindow += windowSize;

            nljBuffers.emplace_back(Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, bufferJoined));
            bufferJoined = executionEngine->getBuffer(joinSchema).getBuffer();
        }

        for (auto inner = 0UL; inner < rightBuffer.getNumberOfTuples(); ++inner) {
            auto timeStampRight = rightBuffer[inner][timeStampFieldName].read<uint64_t>();
            if (timeStampRight > lastTupleTimeStampWindow || timeStampRight < firstTupleTimeStampWindow) {
                continue;
            }

            auto leftKey = leftBuffer[outer][joinFieldNameLeft].read<uint64_t>();
            auto rightKey = rightBuffer[inner][joinFieldNameRight].read<uint64_t>();

            if (leftKey == rightKey) {
                auto dynamicBufJoined = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, bufferJoined);
                auto posNewTuple = dynamicBufJoined.getNumberOfTuples();
                dynamicBufJoined[posNewTuple][0].write<uint64_t>(leftKey);

                dynamicBufJoined[posNewTuple][1].write<uint64_t>(leftBuffer[outer][0].read<uint64_t>());
                dynamicBufJoined[posNewTuple][2].write<uint64_t>(leftBuffer[outer][1].read<uint64_t>());
                dynamicBufJoined[posNewTuple][3].write<uint64_t>(leftBuffer[outer][2].read<uint64_t>());

                dynamicBufJoined[posNewTuple][4].write<uint64_t>(rightBuffer[inner][0].read<uint64_t>());
                dynamicBufJoined[posNewTuple][5].write<uint64_t>(rightBuffer[inner][1].read<uint64_t>());
                dynamicBufJoined[posNewTuple][6].write<uint64_t>(rightBuffer[inner][2].read<uint64_t>());

                dynamicBufJoined.setNumberOfTuples(posNewTuple + 1);
                if (dynamicBufJoined.getNumberOfTuples() >= dynamicBufJoined.getCapacity()) {
                    nljBuffers.emplace_back(Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, bufferJoined));
                    bufferJoined = executionEngine->getBuffer(joinSchema).getBuffer();
                }
            }
        }
    }
}

std::vector<Runtime::TupleBuffer> mergeBuffersSameWindow(std::vector<Runtime::TupleBuffer>& buffers, SchemaPtr schema,
                                                         const std::string& timeStampFieldName,
                                                         BufferManagerPtr bufferManager,
                                                         uint64_t windowSize) {
    if (buffers.size() == 0) {
        return std::vector<Runtime::TupleBuffer>();
    }

    if (schema->getLayoutType() == Schema::COLUMNAR_LAYOUT) {
        NES_FATAL_ERROR("Column layout is not support for this function currently!");
    }

    NES_INFO("Merging buffers together!");

    std::vector<Runtime::TupleBuffer> retVector;

    auto curBuffer = bufferManager->getBufferBlocking();
    auto numberOfTuplesInBuffer = 0UL;
    auto lastTimeStamp = windowSize - 1;
    for (auto buf : buffers) {
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bufferManager->getBufferSize());
        auto dynamicTupleBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buf);

        for (auto curTuple = 0UL; curTuple < dynamicTupleBuffer.getNumberOfTuples(); ++curTuple) {
            if (dynamicTupleBuffer[curTuple][timeStampFieldName].read<uint64_t>() > lastTimeStamp ||
                numberOfTuplesInBuffer >= memoryLayout->getCapacity()) {

                if (dynamicTupleBuffer[curTuple][timeStampFieldName].read<uint64_t>() > lastTimeStamp) {
                    lastTimeStamp += windowSize;
                }


                curBuffer.setNumberOfTuples(numberOfTuplesInBuffer);
                //                    NES_DEBUG("Merged buffer to new buffer = " << Util::printTupleBufferAsCSV(curBuffer, schema));
                retVector.emplace_back(std::move(curBuffer));

                curBuffer = bufferManager->getBufferBlocking();
                numberOfTuplesInBuffer = 0;
            }

            // TODO rewrite this here so that we also support column layout
            // We just have to replace the mempcy with multiple writes and reads to dynamicTupleBuffer
            memcpy(curBuffer.getBuffer() + schema->getSchemaSizeInBytes() * numberOfTuplesInBuffer,
                   buf.getBuffer() + schema->getSchemaSizeInBytes() * curTuple, schema->getSchemaSizeInBytes());
            numberOfTuplesInBuffer += 1;
            curBuffer.setNumberOfTuples(numberOfTuplesInBuffer);

            //                NES_DEBUG("Merged buffer to new buffer = " << Util::printTupleBufferAsCSV(curBuffer, schema));
        }
    }

    if (numberOfTuplesInBuffer > 0) {
        curBuffer.setNumberOfTuples(numberOfTuplesInBuffer);
        //            NES_DEBUG("Merged buffer to new buffer = " << Util::printTupleBufferAsCSV(curBuffer, schema));
        retVector.emplace_back(std::move(curBuffer));
    }

    return retVector;
}


std::vector<Runtime::TupleBuffer> sortBuffersInTupleBuffer(std::vector<Runtime::TupleBuffer>& buffersToSort, SchemaPtr schema,
                                                           const std::string& sortFieldName, BufferManagerPtr bufferManager) {
    if (buffersToSort.size() == 0) {
        return std::vector<Runtime::TupleBuffer>();
    }
    if (schema->getLayoutType() == Schema::COLUMNAR_LAYOUT) {
        NES_FATAL_ERROR("Column layout is not support for this function currently!");
    }


    std::vector<Runtime::TupleBuffer> retVector;
    for (auto bufRead : buffersToSort) {
        std::vector<size_t> indexAlreadyInNewBuffer;
        auto memLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bufferManager->getBufferSize());
        auto dynamicTupleBuf = Runtime::MemoryLayouts::DynamicTupleBuffer(memLayout, bufRead);

        NES_DEBUG("Buffer before sorting is " << Util::printTupleBufferAsCSV(bufRead, schema));

        auto bufRet = bufferManager->getBufferBlocking();

        for (auto outer = 0UL; outer < bufRead.getNumberOfTuples(); ++outer) {
            auto smallestIndex = bufRead.getNumberOfTuples() + 1;
            for (auto inner = 0UL; inner < bufRead.getNumberOfTuples(); ++inner) {
                if (std::find(indexAlreadyInNewBuffer.begin(), indexAlreadyInNewBuffer.end(), inner)
                    != indexAlreadyInNewBuffer.end()) {
                    // If we have already moved this index into the
                    continue;
                }

                auto sortValueCur = dynamicTupleBuf[inner][sortFieldName].read<uint64_t>();
                auto sortValueOld = dynamicTupleBuf[smallestIndex][sortFieldName].read<uint64_t>();

                if (smallestIndex == bufRead.getNumberOfTuples() + 1) {
                    smallestIndex = inner;
                    continue;
                } else if (sortValueCur < sortValueOld) {
                    smallestIndex = inner;
                }
            }
            indexAlreadyInNewBuffer.emplace_back(smallestIndex);
            auto posRet = bufRet.getNumberOfTuples();
            memcpy(bufRet.getBuffer() + posRet * schema->getSchemaSizeInBytes(),
                   bufRead.getBuffer() + smallestIndex * schema->getSchemaSizeInBytes(),
                   schema->getSchemaSizeInBytes());
            bufRet.setNumberOfTuples(posRet + 1);
        }
        NES_DEBUG("Buffer after sorting is " << Util::printTupleBufferAsCSV(bufRet, schema));
        retVector.emplace_back(bufRet);
        bufRet = bufferManager->getBufferBlocking();
    }

    return retVector;
}

TEST_P(StreamJoinQueryExecutionTest, streamJoinSimple) {

- rewrite this to read data for both sources from a csv file and also read the expected buffers from another csv file.
- then run read buffers and compare them to the expected ones
- adapt LowerPhysicalToNautilusOperators to new Join
- check if new stream join is semantically similar to old one. if this is the case, then test both joins, the old and the new one. old with default_query_compiler and stream join with nautilus_query_compiler


    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = leftSchema->get(1)->getName();
    const auto joinFieldNameRight = rightSchema->get(1)->getName();

    ASSERT_EQ(leftSchema->get(2)->getName(), rightSchema->get(2)->getName());
    const auto timeStampField = leftSchema->get(2)->getName();
    const auto windowSize = 10UL;
    const auto numberOfTuplesToProduce = 10 * windowSize;

    auto testSourceDescriptorLeft = executionEngine->createDataSource(leftSchema);
    auto testSourceDescriptorRight = executionEngine->createDataSource(rightSchema);

    auto sourceNameLeft = testSourceDescriptorLeft->getLogicalSourceName();
    auto sourceNameRight = testSourceDescriptorRight->getLogicalSourceName();

    ASSERT_EQ(leftSchema->getLayoutType(), rightSchema->getLayoutType());
    const auto joinSchema = Schema::create(leftSchema->getLayoutType());
    joinSchema->addField(sourceNameLeft + sourceNameRight + "$key", leftSchema->get(joinFieldNameLeft)->getDataType());
    for (const auto& field : leftSchema->fields) {
        joinSchema->addField(field->getName(), field->getDataType());
    }
    for (const auto& field : rightSchema->fields) {
        joinSchema->addField(field->getName(), field->getDataType());
    }

    auto inputBufferLeft = executionEngine->getBuffer(leftSchema);
    auto inputBufferRight = executionEngine->getBuffer(rightSchema);

    auto nljBuffers = fillBuffersAndNLJ(inputBufferLeft, inputBufferRight, executionEngine,
                                        numberOfTuplesToProduce, windowSize, timeStampField, joinSchema,
                                        joinFieldNameLeft, joinFieldNameRight);

    auto testSink = executionEngine->createDateSink(joinSchema, nljBuffers.size());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptorLeft).joinWith(TestQuery::from(testSourceDescriptorRight))
                     .where(Attribute(joinFieldNameLeft))
                     .equalsTo(Attribute(joinFieldNameRight))
                     .window(TumblingWindow::of(EventTime(Attribute(timeStampField)), Milliseconds(windowSize)))
                     .sink(testSinkDescriptor);

    auto plan = executionEngine->submitQuery(query.getQueryPlan());

    auto sourceLeft = executionEngine->getDataSource(plan, 0);
    auto sourceRight = executionEngine->getDataSource(plan, 1);




    sourceLeft->emitBuffer(inputBufferLeft);
    sourceRight->emitBuffer(inputBufferRight);

    testSink->waitTillCompleted();
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), nljBuffers.size());

    auto mergedEmittedBuffers = mergeBuffersSameWindow(pipelineExecCtxSink.emittedBuffers,
                                                       joinSchema, timeStampField,
                                                       executionEngine->getBufferManager(), windowSize);
    auto sortedMergedEmittedBuffers = sortBuffersInTupleBuffer(mergedEmittedBuffers,
                                                               joinSchema, timeStampField, executionEngine->getBufferManager());
    auto sortNLJBuffers = sortBuffersInTupleBuffer(nljBuffers, joinSchema, timeStampField, executionEngine->getBufferManager());

    nljBuffers.clear();


    // TODO add here merge and sort testSink buffers
    for (auto i = 0; i < nljBuffers.size(); ++i) {
        auto nljBuffer = nljBuffers[i];
        auto testSinkBuffer = testSink->get(i);

        ASSERT_TRUE(memcmp(testSinkBuffer.getBuffer(), nljBuffer.getBuffer(), testSinkBuffer.getNumberOfTuples() * joinSchema->getSchemaSizeInBytes()) == 0);
    }


    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}


INSTANTIATE_TEST_CASE_P(testStreamJoinQueries,
                        StreamJoinQueryExecutionTest,
                        ::testing::Values(QueryCompilation::QueryCompilerOptions::QueryCompiler::DEFAULT_QUERY_COMPILER,
                                          QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER),
                        [](const testing::TestParamInfo<StreamJoinQueryExecutionTest::ParamType>& info) {
                            return magic_enum::enum_flags_name(info.param);
                        });


} // namespace NES::Runtime::Execution