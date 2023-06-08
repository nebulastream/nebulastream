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
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/JoinPhases/StreamHashJoinBuild.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/JoinPhases/StreamHashJoinSink.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/StreamHashJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/UtilityFunctions.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Util/Common.hpp>

namespace NES::Runtime::Execution {

class HashJoinOperatorTest : public Testing::NESBaseTest {
  public:
    std::shared_ptr<Runtime::BufferManager> bm;
    std::vector<TupleBuffer> emittedBuffers;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("HashJoinOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup HashJoinOperatorTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NESBaseTest::SetUp();
        NES_INFO("Setup HashJoinOperatorTest test case.");
        bm = std::make_shared<Runtime::BufferManager>();
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        NES_INFO("Tear down HashJoinOperatorTest test case.");
        NESBaseTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down HashJoinOperatorTest test class."); }
};

struct HashJoinBuildHelper {
    size_t pageSize;
    size_t numPartitions;
    size_t numberOfTuplesToProduce;
    size_t numberOfBuffersPerWorker;
    size_t noWorkerThreads;
    size_t totalNumSources;
    size_t joinSizeInByte;
    size_t windowSize;
    Operators::StreamHashJoinBuildPtr hashJoinBuild;
    std::string joinFieldName;
    BufferManagerPtr bufferManager;
    SchemaPtr schema;
    std::string timeStampField;
    HashJoinOperatorTest* hashJoinOperatorTest;
    bool isLeftSide;

    HashJoinBuildHelper(Operators::StreamHashJoinBuildPtr hashJoinBuild,
                        const std::string& joinFieldName,
                        BufferManagerPtr bufferManager,
                        SchemaPtr schema,
                        const std::string& timeStampField,
                        HashJoinOperatorTest* hashJoinOperatorTest,
                        bool isLeftSide)
        : pageSize(PAGE_SIZE), numPartitions(NUM_PARTITIONS), numberOfTuplesToProduce(100), numberOfBuffersPerWorker(128),
          noWorkerThreads(1), totalNumSources(2), joinSizeInByte(1 * 1024 * 1024), windowSize(1000), hashJoinBuild(hashJoinBuild),
          joinFieldName(joinFieldName), bufferManager(bufferManager), schema(schema), timeStampField(timeStampField),
          hashJoinOperatorTest(hashJoinOperatorTest), isLeftSide(isLeftSide) {}
};

bool hashJoinBuildAndCheck(HashJoinBuildHelper buildHelper) {
    auto workerContext =
        std::make_shared<WorkerContext>(/*workerId*/ 0, buildHelper.bufferManager, buildHelper.numberOfBuffersPerWorker);
    auto hashJoinOpHandler = Operators::StreamHashJoinOperatorHandler::create(buildHelper.schema,
                                                                              buildHelper.schema,
                                                                              buildHelper.joinFieldName,
                                                                              buildHelper.joinFieldName,
                                                                              std::vector<::OriginId>({1}),
                                                                              buildHelper.windowSize,
                                                                              buildHelper.joinSizeInByte,
                                                                              buildHelper.pageSize,
                                                                              NUM_PREALLOC_PAGES,
                                                                              buildHelper.numPartitions);

    auto hashJoinOperatorTest = buildHelper.hashJoinOperatorTest;
    auto pipelineContext = PipelineExecutionContext(
        -1,// mock pipeline id
        0, // mock query id
        nullptr,
        buildHelper.noWorkerThreads,
        [&hashJoinOperatorTest](TupleBuffer& buffer, Runtime::WorkerContextRef) {
            hashJoinOperatorTest->emittedBuffers.emplace_back(std::move(buffer));
        },
        [&hashJoinOperatorTest](TupleBuffer& buffer) {
            hashJoinOperatorTest->emittedBuffers.emplace_back(std::move(buffer));
        },
        {hashJoinOpHandler});

    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));

    buildHelper.hashJoinBuild->setup(executionContext);

    // Execute record and thus fill the hash table
    for (auto i = 0UL; i < buildHelper.numberOfTuplesToProduce + 1; ++i) {
        auto record = Nautilus::Record({{buildHelper.schema->get(0)->getName(), Value<UInt64>((uint64_t) i)},
                                        {buildHelper.schema->get(1)->getName(), Value<UInt64>((uint64_t) (i % 10) + 1)},
                                        {buildHelper.schema->get(2)->getName(), Value<UInt64>((uint64_t) i)}});
        buildHelper.hashJoinBuild->execute(executionContext, record);

        uint64_t joinKey = record.read(buildHelper.joinFieldName).as<UInt64>().getValue().getValue();
        uint64_t timeStamp = record.read(buildHelper.timeStampField).as<UInt64>().getValue().getValue();
        auto hash = ::NES::Util::murmurHash(joinKey);
        auto window = hashJoinOpHandler->getWindowByTimestamp(timeStamp);
        auto hashWindow = static_cast<StreamHashJoinWindow*>(window.value().get());

        auto hashTable = hashWindow->getLocalHashTable(workerContext->getId(), buildHelper.isLeftSide);

        auto bucket = hashTable->getBucketLinkedList(hashTable->getBucketPos(hash));

        bool correctlyInserted = false;
        for (auto&& page : bucket->getPages()) {
            for (auto k = 0UL; k < page->size(); ++k) {
                uint8_t* recordPtr = page->operator[](k);
                auto bucketBuffer = Util::getBufferFromPointer(recordPtr, buildHelper.schema, buildHelper.bufferManager);
                auto recordBuffer = Util::getBufferFromRecord(record, buildHelper.schema, buildHelper.bufferManager);

                if (memcmp(bucketBuffer.getBuffer(), recordBuffer.getBuffer(), buildHelper.schema->getSchemaSizeInBytes()) == 0) {
                    correctlyInserted = true;
                    break;
                }
            }
        }

        if (!correctlyInserted) {
            auto recordBuffer = Util::getBufferFromRecord(record, buildHelper.schema, buildHelper.bufferManager);
            NES_ERROR("Could not find record " << Util::printTupleBufferAsCSV(recordBuffer, buildHelper.schema) << " in bucket!");
            return false;
        }
    }

    return true;
}

struct HashJoinSinkHelper {
    size_t pageSize;
    size_t numPartitions;
    size_t numberOfTuplesToProduce;
    size_t numberOfBuffersPerWorker;
    size_t noWorkerThreads;
    size_t numSourcesLeft, numSourcesRight;
    size_t joinSizeInByte;
    size_t windowSize;
    std::string joinFieldNameLeft, joinFieldNameRight;
    BufferManagerPtr bufferManager;
    SchemaPtr leftSchema, rightSchema;
    std::string timeStampField;
    HashJoinOperatorTest* hashJoinOperatorTest;

    HashJoinSinkHelper(const std::string& joinFieldNameLeft,
                       const std::string& joinFieldNameRight,
                       BufferManagerPtr bufferManager,
                       SchemaPtr leftSchema,
                       SchemaPtr rightSchema,
                       const std::string& timeStampField,
                       HashJoinOperatorTest* hashJoinOperatorTest)
        : pageSize(PAGE_SIZE), numPartitions(NUM_PARTITIONS), numberOfTuplesToProduce(100), numberOfBuffersPerWorker(128),
          noWorkerThreads(1), numSourcesLeft(1), numSourcesRight(1), joinSizeInByte(1 * 1024 * 1024), windowSize(1000),
          joinFieldNameLeft(joinFieldNameLeft), joinFieldNameRight(joinFieldNameRight), bufferManager(bufferManager),
          leftSchema(leftSchema), rightSchema(rightSchema), timeStampField(timeStampField),
          hashJoinOperatorTest(hashJoinOperatorTest) {}
};

bool checkIfBufferFoundAndRemove(std::vector<Runtime::TupleBuffer>& emittedBuffers,
                                 Runtime::TupleBuffer expectedBuffer,
                                 SchemaPtr joinSchema,
                                 uint64_t& removedBuffer) {

    bool foundBuffer = false;
    NES_TRACE("NLJ buffer = " << Util::printTupleBufferAsCSV(expectedBuffer, joinSchema));

    for (auto tupleBufferIt = emittedBuffers.begin(); tupleBufferIt != emittedBuffers.end(); ++tupleBufferIt) {
        NES_TRACE("Comparing versus " << Util::printTupleBufferAsCSV(*tupleBufferIt, joinSchema));
        if (memcmp(tupleBufferIt->getBuffer(),
                   expectedBuffer.getBuffer(),
                   joinSchema->getSchemaSizeInBytes() * expectedBuffer.getNumberOfTuples())
            == 0) {
            NES_TRACE("Removing buffer #" << removedBuffer << " " << Util::printTupleBufferAsCSV(*tupleBufferIt, joinSchema)
                                          << " of size " << joinSchema->getSchemaSizeInBytes());
            emittedBuffers.erase(tupleBufferIt);
            foundBuffer = true;
            removedBuffer += 1;
            break;
        }
    }
    return foundBuffer;
}

bool hashJoinSinkAndCheck(HashJoinSinkHelper hashJoinSinkHelper) {

    if (!hashJoinSinkHelper.leftSchema->contains(hashJoinSinkHelper.joinFieldNameLeft)) {
        NES_ERROR("JoinFieldNameLeft " << hashJoinSinkHelper.joinFieldNameLeft << " is not in leftSchema!");
        return false;
    }
    if (!hashJoinSinkHelper.rightSchema->contains(hashJoinSinkHelper.joinFieldNameRight)) {
        NES_ERROR("JoinFieldNameLeft " << hashJoinSinkHelper.joinFieldNameRight << " is not in leftSchema!");
        return false;
    }

    auto workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0,
                                                         hashJoinSinkHelper.bufferManager,
                                                         hashJoinSinkHelper.numberOfBuffersPerWorker);
    auto hashJoinOpHandler = Operators::StreamHashJoinOperatorHandler::create(hashJoinSinkHelper.leftSchema,
                                                                              hashJoinSinkHelper.rightSchema,
                                                                              hashJoinSinkHelper.joinFieldNameLeft,
                                                                              hashJoinSinkHelper.joinFieldNameRight,
                                                                              std::vector<::OriginId>({1, 2}),
                                                                              hashJoinSinkHelper.windowSize,
                                                                              hashJoinSinkHelper.joinSizeInByte,
                                                                              hashJoinSinkHelper.pageSize,
                                                                              NUM_PREALLOC_PAGES,
                                                                              hashJoinSinkHelper.numPartitions);

    auto hashJoinOperatorTest = hashJoinSinkHelper.hashJoinOperatorTest;
    auto pipelineContext = PipelineExecutionContext(
        0,// mock pipeline id
        1,// mock query id
        hashJoinSinkHelper.bufferManager,
        hashJoinSinkHelper.noWorkerThreads,
        [&hashJoinOperatorTest](TupleBuffer& buffer, Runtime::WorkerContextRef) {
            hashJoinOperatorTest->emittedBuffers.emplace_back(std::move(buffer));
        },
        [&hashJoinOperatorTest](TupleBuffer& buffer) {
            hashJoinOperatorTest->emittedBuffers.emplace_back(std::move(buffer));
        },
        {hashJoinOpHandler});

    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));

    auto handlerIndex = 0UL;
    auto readTsField = std::make_shared<Expressions::ReadFieldExpression>(hashJoinSinkHelper.timeStampField);

    auto hashJoinBuildLeft = std::make_shared<Operators::StreamHashJoinBuild>(
        handlerIndex,
        /*isLeftSide*/ true,
        hashJoinSinkHelper.joinFieldNameLeft,
        hashJoinSinkHelper.timeStampField,
        hashJoinSinkHelper.leftSchema,
        std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsField));
    auto hashJoinBuildRight = std::make_shared<Operators::StreamHashJoinBuild>(
        handlerIndex,
        /*isLeftSide*/ false,
        hashJoinSinkHelper.joinFieldNameRight,
        hashJoinSinkHelper.timeStampField,
        hashJoinSinkHelper.rightSchema,
        std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsField));
    auto hashJoinSink = std::make_shared<Operators::StreamHashJoinSink>(handlerIndex);

    hashJoinBuildLeft->setup(executionContext);
    hashJoinBuildRight->setup(executionContext);

    std::vector<std::vector<Nautilus::Record>> leftRecords;
    std::vector<std::vector<Nautilus::Record>> rightRecords;

    uint64_t lastTupleTimeStampWindow = hashJoinSinkHelper.windowSize - 1;
    std::vector<Nautilus::Record> tmpRecordsLeft, tmpRecordsRight;

    //create buffers
    for (auto i = 0UL; i < hashJoinSinkHelper.numberOfTuplesToProduce + 1; ++i) {
        auto recordLeft =
            Nautilus::Record({{hashJoinSinkHelper.leftSchema->get(0)->getName(), Value<UInt64>((uint64_t) i)},
                              {hashJoinSinkHelper.leftSchema->get(1)->getName(), Value<UInt64>((uint64_t) (i % 10) + 10)},
                              {hashJoinSinkHelper.leftSchema->get(2)->getName(), Value<UInt64>((uint64_t) i)}});
        NES_DEBUG("Tuple id=" << i << " key=" << (i % 10) + 10 << " ts=" << i);
        auto recordRight =
            Nautilus::Record({{hashJoinSinkHelper.rightSchema->get(0)->getName(), Value<UInt64>((uint64_t) i + 1000)},
                              {hashJoinSinkHelper.rightSchema->get(1)->getName(), Value<UInt64>((uint64_t) (i % 10) + 10)},
                              {hashJoinSinkHelper.rightSchema->get(2)->getName(), Value<UInt64>((uint64_t) i)}});

        if (recordRight.read(hashJoinSinkHelper.timeStampField) > lastTupleTimeStampWindow) {
            NES_DEBUG("rects=" << recordRight.read(hashJoinSinkHelper.timeStampField) << " >= " << lastTupleTimeStampWindow)
            leftRecords.push_back(std::vector(tmpRecordsLeft.begin(), tmpRecordsLeft.end()));
            rightRecords.push_back(std::vector(tmpRecordsRight.begin(), tmpRecordsRight.end()));

            tmpRecordsLeft = std::vector<Nautilus::Record>();
            tmpRecordsRight = std::vector<Nautilus::Record>();

            lastTupleTimeStampWindow += hashJoinSinkHelper.windowSize;
        }

        tmpRecordsLeft.emplace_back(recordLeft);
        tmpRecordsRight.emplace_back(recordRight);
    }

    NES_DEBUG("filling left side");
    //push buffers to build left
    //for all record buffers
    for (auto i = 0UL; i < leftRecords.size(); i++) {
        auto tupleBuffer = hashJoinSinkHelper.bufferManager->getBufferBlocking();
        RecordBuffer recordBufferLeft = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
        uint64_t size = leftRecords[i].size();
        recordBufferLeft.setNumRecords(uint64_t(0));
        //for one record in the buffer
        for (auto u = 0UL; u < leftRecords[i].size(); u++) {
            hashJoinBuildLeft->execute(executionContext, leftRecords[i][u]);
            NES_DEBUG("Tuple insert id=" << i << " key=" << leftRecords[i][u].read("f2_left")
                                         << " ts=" << leftRecords[i][u].read("timestamp"));
        }
        executionContext.setWatermarkTs(leftRecords[i][size - 1].read("timestamp").as<UInt64>());
        executionContext.setCurrentTs(leftRecords[i][size - 1].read("timestamp").as<UInt64>());
        executionContext.setOrigin(uint64_t(1));
        executionContext.setSequenceNumber(uint64_t(i));
        NES_DEBUG("trigger left with ts=" << leftRecords[i][size - 1].read("timestamp"));

        hashJoinBuildLeft->close(executionContext, recordBufferLeft);
    }
    NES_DEBUG("filling right side");
    for (auto i = 0UL; i < rightRecords.size(); i++) {
        auto tupleBuffer = hashJoinSinkHelper.bufferManager->getBufferBlocking();
        RecordBuffer recordBufferRight = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
        uint64_t size = rightRecords[i].size();
        recordBufferRight.setNumRecords(uint64_t(0));
        //for one record in the buffer
        for (auto u = 0UL; u < rightRecords[i].size(); u++) {
            hashJoinBuildRight->execute(executionContext, rightRecords[i][u]);
        }
        executionContext.setWatermarkTs(rightRecords[i][size - 1].read("timestamp").as<UInt64>());
        executionContext.setCurrentTs(rightRecords[i][size - 1].read("timestamp").as<UInt64>());
        executionContext.setOrigin(uint64_t(2));
        executionContext.setSequenceNumber(uint64_t(i));
        NES_DEBUG("trigger right with ts=" << rightRecords[i][size - 1].read("timestamp"));
        hashJoinBuildRight->close(executionContext, recordBufferRight);
    }

    NES_DEBUG("trigger sink");
    auto numberOfEmittedBuffersBuild = hashJoinOperatorTest->emittedBuffers.size();
    for (auto cnt = 0UL; cnt < numberOfEmittedBuffersBuild; ++cnt) {
        auto tupleBuffer = hashJoinOperatorTest->emittedBuffers[cnt];
        RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
        hashJoinSink->open(executionContext, recordBuffer);
    }

    // Delete all buffers that have been emitted from the build phase
    hashJoinOperatorTest->emittedBuffers.erase(hashJoinOperatorTest->emittedBuffers.begin(),
                                               hashJoinOperatorTest->emittedBuffers.begin() + numberOfEmittedBuffersBuild);

    auto joinSchema = Util::createJoinSchema(hashJoinSinkHelper.leftSchema,
                                             hashJoinSinkHelper.rightSchema,
                                             hashJoinSinkHelper.joinFieldNameLeft);

    /* Checking if all windows have been deleted except for one.
     * We require always one window as we do not know here if we have to take care of more tuples*/
    if (hashJoinOpHandler->as<Operators::StreamJoinOperatorHandler>()->getNumberOfWindows() != 1) {
        NES_ERROR("Not exactly one active window!");
        return false;
    }

    uint64_t removedBuffer = 0UL;
    auto sizeJoinedTuple = joinSchema->getSchemaSizeInBytes();
    auto buffer = hashJoinSinkHelper.bufferManager->getBufferBlocking();
    auto tuplePerBuffer = hashJoinSinkHelper.bufferManager->getBufferSize() / sizeJoinedTuple;
    for (auto& buf : hashJoinOperatorTest->emittedBuffers) {
        NES_DEBUG("buf=" << Util::printTupleBufferAsCSV(buf, joinSchema));
    }

    auto mergedEmittedBuffers = Util::mergeBuffersSameWindow(hashJoinOperatorTest->emittedBuffers,
                                                             joinSchema,
                                                             hashJoinSinkHelper.timeStampField,
                                                             hashJoinSinkHelper.bufferManager,
                                                             hashJoinSinkHelper.windowSize);
    auto sortedEmittedBuffers = Util::sortBuffersInTupleBuffer(mergedEmittedBuffers,
                                                               joinSchema,
                                                               hashJoinSinkHelper.timeStampField,
                                                               hashJoinSinkHelper.bufferManager);
    hashJoinOperatorTest->emittedBuffers.clear();
    mergedEmittedBuffers.clear();

    // Window start and end is always an ui64
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto const timeStampFieldType = physicalDataTypeFactory.getPhysicalType(
        hashJoinSinkHelper.leftSchema->get(hashJoinSinkHelper.timeStampField)->getDataType());
    const auto sizeOfWindowStart = timeStampFieldType->size();
    const auto sizeOfWindowEnd = timeStampFieldType->size();

    //TODO: this function only checks on buffer basis and not if matches are across buffer
    //NOTE: In addition the last buffer contains the last window but this is not triggered as there is no watermark for it later
    for (auto curWindow = 0UL; curWindow < leftRecords.size() - 1; ++curWindow) {
        auto numberOfTuplesInBuffer = 0UL;
        for (auto& leftRecord : leftRecords[curWindow]) {
            for (auto& rightRecord : rightRecords[curWindow]) {
                if (leftRecord.read(hashJoinSinkHelper.joinFieldNameLeft)
                    == rightRecord.read(hashJoinSinkHelper.joinFieldNameRight)) {
                    // We expect to have at least one more buffer that was created by our join
                    if (sortedEmittedBuffers.size() == 0) {
                        NES_ERROR("Expected at least one buffer!");
                        //                        return false;
                    }

                    int8_t* bufferPtr = (int8_t*) buffer.getBuffer() + numberOfTuplesInBuffer * sizeJoinedTuple;
                    // Writing window start and end
                    uint64_t windowStart = curWindow * hashJoinSinkHelper.windowSize;
                    uint64_t windowEnd = ((curWindow + 1) * hashJoinSinkHelper.windowSize);

                    memcpy(bufferPtr, &windowStart, sizeOfWindowStart);
                    memcpy(bufferPtr + sizeOfWindowStart, &windowEnd, sizeOfWindowEnd);

                    // Writing the key
                    bufferPtr += sizeOfWindowStart + sizeOfWindowEnd;
                    auto keyRef = Nautilus::Value<Nautilus::MemRef>(bufferPtr);
                    keyRef.store(leftRecord.read(hashJoinSinkHelper.joinFieldNameLeft));
                    auto const fieldType = physicalDataTypeFactory.getPhysicalType(
                        hashJoinSinkHelper.leftSchema->get(hashJoinSinkHelper.joinFieldNameLeft)->getDataType());
                    bufferPtr += fieldType->size();

                    // Writing the left and right
                    Util::writeNautilusRecord(0,
                                              bufferPtr,
                                              leftRecord,
                                              hashJoinSinkHelper.leftSchema,
                                              hashJoinSinkHelper.bufferManager);
                    Util::writeNautilusRecord(0,
                                              bufferPtr + hashJoinSinkHelper.leftSchema->getSchemaSizeInBytes(),
                                              rightRecord,
                                              hashJoinSinkHelper.rightSchema,
                                              hashJoinSinkHelper.bufferManager);
                    numberOfTuplesInBuffer += 1;
                    buffer.setNumberOfTuples(numberOfTuplesInBuffer);

                    NES_DEBUG(" write windowStart=" << windowStart << " windowEnd=" << windowEnd
                                                    << " left=" << leftRecord.toString() << " right=" << rightRecord.toString());
                    if (numberOfTuplesInBuffer >= tuplePerBuffer) {
                        NES_DEBUG("wrote buffer=" << Util::printTupleBufferAsCSV(buffer, joinSchema));
                        std::vector<Runtime::TupleBuffer> bufVec({buffer});
                        auto sortedBuffer = Util::sortBuffersInTupleBuffer(bufVec,
                                                                           joinSchema,
                                                                           hashJoinSinkHelper.timeStampField,
                                                                           hashJoinSinkHelper.bufferManager);

                        bool foundBuffer =
                            checkIfBufferFoundAndRemove(sortedEmittedBuffers, sortedBuffer[0], joinSchema, removedBuffer);

                        if (!foundBuffer) {
                            NES_ERROR("Could not find buffer " << Util::printTupleBufferAsCSV(buffer, joinSchema)
                                                               << " in emittedBuffers!");
                            //                            return false;
                        } else {
                            NES_WARNING("Found buffer buffer " << Util::printTupleBufferAsCSV(buffer, joinSchema)
                                                               << " in emittedBuffers!");
                        }

                        numberOfTuplesInBuffer = 0;
                        buffer = hashJoinSinkHelper.bufferManager->getBufferBlocking();
                    }
                }//end of found a match
            }
        }//end of for loop over left

        if (numberOfTuplesInBuffer > 0) {
            std::vector<Runtime::TupleBuffer> bufVec({buffer});
            auto sortedBuffer = Util::sortBuffersInTupleBuffer(bufVec,
                                                               joinSchema,
                                                               hashJoinSinkHelper.timeStampField,
                                                               hashJoinSinkHelper.bufferManager);
            bool foundBuffer = checkIfBufferFoundAndRemove(sortedEmittedBuffers, sortedBuffer[0], joinSchema, removedBuffer);
            if (!foundBuffer) {
                NES_ERROR("Could not find buffer " << Util::printTupleBufferAsCSV(buffer, joinSchema) << " in emittedBuffers!");
                return false;
            }
        }
    }//end of for loop over windows

    // Make sure that after we have joined all records together no more buffer exist
    if (sortedEmittedBuffers.size() > 0) {
        NES_ERROR("Have not removed all buffers. So some tuples have not been joined together!");
        return false;
    }

    return true;
}

class TestRunner : public NES::Exceptions::ErrorListener {
  public:
    void onFatalError(int signalNumber, std::string callStack) override {
        std::ostringstream fatalErrorMessage;
        fatalErrorMessage << "onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] callstack "
                          << callStack;

        NES_FATAL_ERROR(fatalErrorMessage.str());
        std::cerr << fatalErrorMessage.str() << std::endl;
    }

    void onFatalException(std::shared_ptr<std::exception> exceptionPtr, std::string callStack) override {
        std::ostringstream fatalExceptionMessage;
        fatalExceptionMessage << "onFatalException: exception=[" << exceptionPtr->what() << "] callstack=\n" << callStack;

        NES_FATAL_ERROR(fatalExceptionMessage.str());
        std::cerr << fatalExceptionMessage.str() << std::endl;
    }
};

TEST_F(HashJoinOperatorTest, joinBuildTest) {
    // Activating and installing error listener
    auto runner = std::make_shared<TestRunner>();
    NES::Exceptions::installGlobalErrorListener(runner);

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "f2_left";
    const auto timeStampField = "timestamp";
    const auto isLeftSide = true;

    auto handlerIndex = 0;
    auto readTsField = std::make_shared<Expressions::ReadFieldExpression>(timeStampField);

    auto hashJoinBuild = std::make_shared<Operators::StreamHashJoinBuild>(
        handlerIndex,
        isLeftSide,
        joinFieldNameLeft,
        timeStampField,
        leftSchema,
        std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsField));

    HashJoinBuildHelper buildHelper(hashJoinBuild, joinFieldNameLeft, bm, leftSchema, timeStampField, this, isLeftSide);
    ASSERT_TRUE(hashJoinBuildAndCheck(buildHelper));
    // As we are only building here the left side, we do not emit any buffers
    ASSERT_EQ(emittedBuffers.size(), 0);
}

TEST_F(HashJoinOperatorTest, joinBuildTestRight) {
    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);

    const auto joinFieldNameRight = "f2_right";
    const auto timeStampField = "timestamp";
    const auto isLeftSide = false;

    auto readTsField = std::make_shared<Expressions::ReadFieldExpression>(timeStampField);
    auto handlerIndex = 0;
    auto hashJoinBuild = std::make_shared<Operators::StreamHashJoinBuild>(
        handlerIndex,
        isLeftSide,
        joinFieldNameRight,
        timeStampField,
        rightSchema,
        std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsField));
    HashJoinBuildHelper buildHelper(hashJoinBuild, joinFieldNameRight, bm, rightSchema, timeStampField, this, isLeftSide);

    ASSERT_TRUE(hashJoinBuildAndCheck(buildHelper));
    // As we are only building here the left side, we do not emit any buffers
    ASSERT_EQ(emittedBuffers.size(), 0);
}

TEST_F(HashJoinOperatorTest, joinBuildTestMultiplePagesPerBucket) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "f2_left";
    const auto timeStampField = "timestamp";
    const auto isLeftSide = true;

    auto handlerIndex = 0;
    auto readTsField = std::make_shared<Expressions::ReadFieldExpression>(timeStampField);

    auto hashJoinBuild = std::make_shared<Operators::StreamHashJoinBuild>(
        handlerIndex,
        isLeftSide,
        joinFieldNameLeft,
        timeStampField,
        leftSchema,
        std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsField));

    HashJoinBuildHelper buildHelper(hashJoinBuild, joinFieldNameLeft, bm, leftSchema, timeStampField, this, isLeftSide);
    buildHelper.pageSize = leftSchema->getSchemaSizeInBytes() * 2;
    buildHelper.numPartitions = 1;

    ASSERT_TRUE(hashJoinBuildAndCheck(buildHelper));
    // As we are only building here the left side, we do not emit any buffers
    ASSERT_EQ(emittedBuffers.size(), 0);
}

TEST_F(HashJoinOperatorTest, joinBuildTestMultipleWindows) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "f2_left";
    const auto timeStampField = "timestamp";
    const auto handlerIndex = 0;
    const auto isLeftSide = true;

    auto readTsField = std::make_shared<Expressions::ReadFieldExpression>(timeStampField);

    auto hashJoinBuild = std::make_shared<Operators::StreamHashJoinBuild>(
        handlerIndex,
        isLeftSide,
        joinFieldNameLeft,
        timeStampField,
        leftSchema,
        std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsField));

    HashJoinBuildHelper buildHelper(hashJoinBuild, joinFieldNameLeft, bm, leftSchema, timeStampField, this, isLeftSide);
    buildHelper.pageSize = leftSchema->getSchemaSizeInBytes() * 2, buildHelper.numPartitions = 1;
    buildHelper.windowSize = 5;

    ASSERT_TRUE(hashJoinBuildAndCheck(buildHelper));
    // As we are only building here the left side, we do not emit any buffers
    ASSERT_EQ(emittedBuffers.size(), 0);
}

TEST_F(HashJoinOperatorTest, joinSinkTest) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);

    ASSERT_EQ(leftSchema->getSchemaSizeInBytes(), rightSchema->getSchemaSizeInBytes());

    HashJoinSinkHelper hashJoinSinkHelper("f2_left", "f2_right", bm, leftSchema, rightSchema, "timestamp", this);
    hashJoinSinkHelper.pageSize = 2 * leftSchema->getSchemaSizeInBytes();
    hashJoinSinkHelper.numPartitions = 2;
    hashJoinSinkHelper.windowSize = 20;

    ASSERT_TRUE(hashJoinSinkAndCheck(hashJoinSinkHelper));
}

TEST_F(HashJoinOperatorTest, joinSinkTestMultipleBuckets) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);

    ASSERT_EQ(leftSchema->getSchemaSizeInBytes(), rightSchema->getSchemaSizeInBytes());

    HashJoinSinkHelper hashJoinSinkHelper("f2_left", "f2_right", bm, leftSchema, rightSchema, "timestamp", this);
    hashJoinSinkHelper.windowSize = 10;

    ASSERT_TRUE(hashJoinSinkAndCheck(hashJoinSinkHelper));
}

TEST_F(HashJoinOperatorTest, joinSinkTestMultipleWindows) {

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);

    ASSERT_EQ(leftSchema->getSchemaSizeInBytes(), rightSchema->getSchemaSizeInBytes());

    HashJoinSinkHelper hashJoinSinkHelper("f2_left", "f2_right", bm, leftSchema, rightSchema, "timestamp", this);
    hashJoinSinkHelper.numPartitions = 1;
    hashJoinSinkHelper.windowSize = 10;

    ASSERT_TRUE(hashJoinSinkAndCheck(hashJoinSinkHelper));
}

}// namespace NES::Runtime::Execution