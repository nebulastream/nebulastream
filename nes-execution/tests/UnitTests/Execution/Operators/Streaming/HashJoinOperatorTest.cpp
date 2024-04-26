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
#include <API/TimeUnit.hpp>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJProbe.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJSlice.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/Slicing/HJBuildSlicing.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/Slicing/HJOperatorHandlerSlicing.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <TestUtils/UtilityFunctions.hpp>
#include <Util/Common.hpp>
#include <Util/TestTupleBuffer.hpp>

namespace NES::Runtime::Execution {

class HashJoinOperatorTest : public Testing::BaseUnitTest {
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
        BaseUnitTest::SetUp();
        NES_INFO("Setup HashJoinOperatorTest test case.");
        bm = std::make_shared<Runtime::BufferManager>();
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        NES_INFO("Tear down HashJoinOperatorTest test case.");
        BaseUnitTest::TearDown();
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
    size_t preAllocPageCnt;
    size_t joinSizeInByte;
    size_t windowSize;
    Operators::HJBuildSlicingPtr hashJoinBuild;
    std::string joinFieldName;
    BufferManagerPtr bufferManager;
    SchemaPtr schema;
    std::string timeStampField;
    HashJoinOperatorTest* hashJoinOperatorTest;
    QueryCompilation::JoinBuildSideType joinBuildSide;

    HashJoinBuildHelper(Operators::HJBuildSlicingPtr hashJoinBuild,
                        const std::string& joinFieldName,
                        BufferManagerPtr bufferManager,
                        SchemaPtr schema,
                        const std::string& timeStampField,
                        HashJoinOperatorTest* hashJoinOperatorTest,
                        QueryCompilation::JoinBuildSideType joinBuildSide)
        : pageSize(131072), numPartitions(1), numberOfTuplesToProduce(100), numberOfBuffersPerWorker(128), noWorkerThreads(1),
          preAllocPageCnt(1), joinSizeInByte(1 * 1024 * 1024), windowSize(1000), hashJoinBuild(hashJoinBuild),
          joinFieldName(joinFieldName), bufferManager(bufferManager), schema(schema), timeStampField(timeStampField),
          hashJoinOperatorTest(hashJoinOperatorTest), joinBuildSide(joinBuildSide) {}
};

bool hashJoinBuildAndCheck(HashJoinBuildHelper buildHelper) {
    OriginId outputOriginId = OriginId(1);
    auto workerContext =
        std::make_shared<WorkerContext>(/*workerId*/ 0, buildHelper.bufferManager, buildHelper.numberOfBuffersPerWorker);
    auto hashJoinOpHandler = std::dynamic_pointer_cast<Operators::HJOperatorHandlerSlicing>(
        Operators::HJOperatorHandlerSlicing::create(std::vector({OriginId(1)}),
                                                    outputOriginId,
                                                    buildHelper.windowSize,
                                                    buildHelper.windowSize,
                                                    buildHelper.schema,
                                                    buildHelper.schema,
                                                    QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL,
                                                    buildHelper.joinSizeInByte,
                                                    buildHelper.preAllocPageCnt,
                                                    buildHelper.pageSize,
                                                    buildHelper.numPartitions));

    auto hashJoinOperatorTest = buildHelper.hashJoinOperatorTest;
    auto pipelineContext = PipelineExecutionContext(
        INVALID_PIPELINE_ID,// mock pipeline id
        INVALID_DECOMPOSED_QUERY_PLAN_ID, // mock query id
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

        if (i == 0) {
            auto tupleBuffer = Util::getBufferFromRecord(record, buildHelper.schema, buildHelper.bufferManager);
            RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
            buildHelper.hashJoinBuild->open(executionContext, recordBuffer);
        }

        buildHelper.hashJoinBuild->execute(executionContext, record);

        uint64_t joinKey = record.read(buildHelper.joinFieldName).as<UInt64>().getValue().getValue();
        uint64_t timeStamp = record.read(buildHelper.timeStampField).as<UInt64>().getValue().getValue();
        auto hash = ::NES::Util::murmurHash(joinKey);
        auto window = hashJoinOpHandler->getSliceByTimestampOrCreateIt(timeStamp);
        auto hashWindow = static_cast<HJSlice*>(window.get());

        auto hashTable = hashWindow->getHashTable(buildHelper.joinBuildSide, workerContext->getId());

        auto bucket = hashTable->getBucketLinkedList(hashTable->getBucketPos(hash));

        bool correctlyInserted = false;
        for (auto&& page : bucket->getPages()) {
            for (auto k = 0UL; k < page->size(); ++k) {
                uint8_t* recordPtr = page.get()->operator[](k);
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
            NES_ERROR("Could not find record {} in bucket!", Util::printTupleBufferAsCSV(recordBuffer, buildHelper.schema));
            return false;
        }
    }

    return true;
}

struct HashJoinProbeHelper {
    size_t pageSize;
    size_t numPartitions;
    size_t numberOfTuplesToProduce;
    size_t numberOfBuffersPerWorker;
    size_t noWorkerThreads;
    size_t joinSizeInByte;
    size_t preAllocPageCnt;
    uint64_t windowSize;
    std::string joinFieldNameLeft, joinFieldNameRight;
    BufferManagerPtr bufferManager;
    SchemaPtr leftSchema, rightSchema;
    std::string timeStampFieldLeft;
    std::string timeStampFieldRight;
    HashJoinOperatorTest* hashJoinOperatorTest;

    HashJoinProbeHelper(const std::string& joinFieldNameLeft,
                        const std::string& joinFieldNameRight,
                        BufferManagerPtr bufferManager,
                        SchemaPtr leftSchema,
                        SchemaPtr rightSchema,
                        const std::string& timeStampFieldLeft,
                        const std::string& timeStampFieldRight,
                        HashJoinOperatorTest* hashJoinOperatorTest)
        : pageSize(131072), numPartitions(1), numberOfTuplesToProduce(100), numberOfBuffersPerWorker(128), noWorkerThreads(1),
          joinSizeInByte(1 * 1024 * 1024), preAllocPageCnt(1), windowSize(1000), joinFieldNameLeft(joinFieldNameLeft),
          joinFieldNameRight(joinFieldNameRight), bufferManager(bufferManager), leftSchema(leftSchema), rightSchema(rightSchema),
          timeStampFieldLeft(timeStampFieldLeft), timeStampFieldRight(timeStampFieldRight),
          hashJoinOperatorTest(hashJoinOperatorTest) {}
};

uint64_t calculateExpNoTuplesInWindow(uint64_t totalTuples, uint64_t windowIdentifier, uint64_t windowSize) {
    std::vector<uint64_t> tmpVec;
    while (totalTuples > windowSize) {
        tmpVec.emplace_back(windowSize);
        totalTuples -= windowSize;
    }
    tmpVec.emplace_back(totalTuples);
    auto noWindow = (windowIdentifier - 1) / windowSize;
    return tmpVec[noWindow];
}

bool hashJoinProbeAndCheck(HashJoinProbeHelper hashJoinProbeHelper) {

    auto workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0,
                                                         hashJoinProbeHelper.bufferManager,
                                                         hashJoinProbeHelper.numberOfBuffersPerWorker);
    auto inputOriginIds = std::vector({OriginId(1), OriginId(2)});
    OriginId outputOriginId = OriginId(3);
    auto hashJoinOpHandler = Operators::HJOperatorHandlerSlicing::create(inputOriginIds,
                                                                         outputOriginId,
                                                                         hashJoinProbeHelper.windowSize,
                                                                         hashJoinProbeHelper.windowSize,
                                                                         hashJoinProbeHelper.leftSchema,
                                                                         hashJoinProbeHelper.rightSchema,
                                                                         QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL,
                                                                         hashJoinProbeHelper.joinSizeInByte,
                                                                         hashJoinProbeHelper.preAllocPageCnt,
                                                                         hashJoinProbeHelper.pageSize,
                                                                         hashJoinProbeHelper.numPartitions);

    auto hashJoinOperatorTest = hashJoinProbeHelper.hashJoinOperatorTest;
    auto pipelineContext = PipelineExecutionContext(
        INVALID_PIPELINE_ID,// mock pipeline id
        DecomposedQueryPlanId(1),// mock query id
        hashJoinProbeHelper.bufferManager,
        hashJoinProbeHelper.noWorkerThreads,
        [&hashJoinOperatorTest](TupleBuffer& buffer, Runtime::WorkerContextRef) {
            hashJoinOperatorTest->emittedBuffers.emplace_back(std::move(buffer));
        },
        [&hashJoinOperatorTest](TupleBuffer& buffer) {
            hashJoinOperatorTest->emittedBuffers.emplace_back(std::move(buffer));
        },
        {hashJoinOpHandler});

    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));

    auto handlerIndex = 0_u64;
    auto readTsFieldLeft = std::make_shared<Expressions::ReadFieldExpression>(hashJoinProbeHelper.timeStampFieldLeft);
    auto readTsFieldRight = std::make_shared<Expressions::ReadFieldExpression>(hashJoinProbeHelper.timeStampFieldRight);

    auto hashJoinBuildLeft = std::make_shared<Operators::HJBuildSlicing>(
        handlerIndex,
        hashJoinProbeHelper.leftSchema,
        hashJoinProbeHelper.joinFieldNameLeft,
        QueryCompilation::JoinBuildSideType::Left,
        hashJoinProbeHelper.leftSchema->getSchemaSizeInBytes(),
        std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldLeft, Windowing::TimeUnit::Milliseconds()),
        QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL,
        QueryCompilation::WindowingStrategy::SLICING);
    auto hashJoinBuildRight = std::make_shared<Operators::HJBuildSlicing>(
        handlerIndex,
        hashJoinProbeHelper.rightSchema,
        hashJoinProbeHelper.joinFieldNameRight,
        QueryCompilation::JoinBuildSideType::Right,
        hashJoinProbeHelper.rightSchema->getSchemaSizeInBytes(),
        std::make_unique<Runtime::Execution::Operators:: EventTimeFunction>(readTsFieldRight, Windowing::TimeUnit::Milliseconds()),
        QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL,
        QueryCompilation::WindowingStrategy::SLICING);

    Operators::JoinSchema joinSchema(hashJoinProbeHelper.leftSchema,
                                     hashJoinProbeHelper.rightSchema,
                                     Util::createJoinSchema(hashJoinProbeHelper.leftSchema,
                                                            hashJoinProbeHelper.rightSchema,
                                                            hashJoinProbeHelper.joinFieldNameLeft));
    Operators::WindowMetaData windowMetaData(joinSchema.joinSchema->get(0)->getName(),
                                             joinSchema.joinSchema->get(1)->getName(),
                                             joinSchema.joinSchema->get(2)->getName());

    auto hashJoinProbe = std::make_shared<Operators::HJProbe>(handlerIndex,
                                                              joinSchema,
                                                              hashJoinProbeHelper.joinFieldNameLeft,
                                                              hashJoinProbeHelper.joinFieldNameRight,
                                                              windowMetaData,
                                                              QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL,
                                                              QueryCompilation::WindowingStrategy::SLICING,
                                                              /*withDeletion*/ false);
    auto collector = std::make_shared<Operators::CollectOperator>();
    hashJoinProbe->setChild(collector);
    hashJoinBuildLeft->setup(executionContext);
    hashJoinBuildRight->setup(executionContext);

    std::vector<std::vector<Nautilus::Record>> leftRecords;
    std::vector<std::vector<Nautilus::Record>> rightRecords;

    uint64_t lastTupleTimeStampWindow = hashJoinProbeHelper.windowSize - 1;
    std::vector<Nautilus::Record> tmpRecordsLeft, tmpRecordsRight;

    //create buffers
    for (auto i = 0UL; i < hashJoinProbeHelper.numberOfTuplesToProduce + 1; ++i) {
        auto recordLeft =
            Nautilus::Record({{hashJoinProbeHelper.leftSchema->get(0)->getName(), Value<UInt64>((uint64_t) i)},
                              {hashJoinProbeHelper.leftSchema->get(1)->getName(), Value<UInt64>((uint64_t) (i % 10) + 10)},
                              {hashJoinProbeHelper.leftSchema->get(2)->getName(), Value<UInt64>((uint64_t) i)}});
        NES_DEBUG("Tuple left id={} key={} ts={}", i, (i % 10) + 10, i);
        auto recordRight =
            Nautilus::Record({{hashJoinProbeHelper.rightSchema->get(0)->getName(), Value<UInt64>((uint64_t) i + 1000)},
                              {hashJoinProbeHelper.rightSchema->get(1)->getName(), Value<UInt64>((uint64_t) (i % 10) + 10)},
                              {hashJoinProbeHelper.rightSchema->get(2)->getName(), Value<UInt64>((uint64_t) i)}});
        NES_DEBUG("Tuple right f1_left={} kef2_left(key)={} ts={}", i + 1000, (i % 10) + 10, i);

        if (recordRight.read(hashJoinProbeHelper.timeStampFieldRight) > lastTupleTimeStampWindow) {
            NES_DEBUG("rects={} >= {}",
                      recordRight.read(hashJoinProbeHelper.timeStampFieldRight)->toString(),
                      lastTupleTimeStampWindow);
            leftRecords.push_back(std::vector(tmpRecordsLeft.begin(), tmpRecordsLeft.end()));
            rightRecords.push_back(std::vector(tmpRecordsRight.begin(), tmpRecordsRight.end()));

            tmpRecordsLeft = std::vector<Nautilus::Record>();
            tmpRecordsRight = std::vector<Nautilus::Record>();

            lastTupleTimeStampWindow += hashJoinProbeHelper.windowSize;
        }

        tmpRecordsLeft.emplace_back(recordLeft);
        tmpRecordsRight.emplace_back(recordRight);
    }

    NES_DEBUG("filling left side with size = {}", leftRecords.size());
    //push buffers to build left
    //for all record buffers
    for (auto i = 0UL; i < leftRecords.size(); i++) {
        auto tupleBuffer = hashJoinProbeHelper.bufferManager->getBufferBlocking();
        RecordBuffer recordBufferLeft = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
        if (i == 0) {
            hashJoinBuildLeft->open(executionContext, recordBufferLeft);
        }
        uint64_t size = leftRecords[i].size();
        recordBufferLeft.setNumRecords(uint64_t(0));
        //for one record in the buffer
        for (auto& u : leftRecords[i]) {
            hashJoinBuildLeft->execute(executionContext, u);
            NES_DEBUG("Insert left tuple {}", u.toString());
        }
        executionContext.setWatermarkTs(leftRecords[i][size - 1].read(hashJoinProbeHelper.timeStampFieldLeft).as<UInt64>());
        executionContext.setCurrentTs(leftRecords[i][size - 1].read(hashJoinProbeHelper.timeStampFieldLeft).as<UInt64>());
        executionContext.setOrigin(inputOriginIds[0].getRawValue());
        executionContext.setSequenceNumber(uint64_t(i + 1));
        executionContext.setChunkNumber(uint64_t(1));
        executionContext.setLastChunk(true);
        NES_DEBUG("trigger left with ts={}", leftRecords[i][size - 1].read(hashJoinProbeHelper.timeStampFieldLeft)->toString());

        hashJoinBuildLeft->close(executionContext, recordBufferLeft);
    }

    NES_DEBUG("filling right side with size = {}", rightRecords.size());
    for (auto i = 0UL; i < rightRecords.size(); i++) {
        auto tupleBuffer = hashJoinProbeHelper.bufferManager->getBufferBlocking();
        RecordBuffer recordBufferRight = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
        if (i == 0) {
            hashJoinBuildRight->open(executionContext, recordBufferRight);
        }
        uint64_t size = rightRecords[i].size();
        recordBufferRight.setNumRecords(uint64_t(0));
        //for one record in the buffer
        for (auto u = 0UL; u < rightRecords[i].size(); u++) {
            hashJoinBuildRight->execute(executionContext, rightRecords[i][u]);
            NES_DEBUG("Insert right tuple {}", rightRecords[i][u].toString());
        }
        executionContext.setWatermarkTs(rightRecords[i][size - 1].read(hashJoinProbeHelper.timeStampFieldRight).as<UInt64>());
        executionContext.setCurrentTs(rightRecords[i][size - 1].read(hashJoinProbeHelper.timeStampFieldRight).as<UInt64>());
        executionContext.setOrigin(inputOriginIds[1].getRawValue());
        executionContext.setSequenceNumber(uint64_t(i + 1));
        executionContext.setChunkNumber(uint64_t(1));
        executionContext.setLastChunk(true);
        NES_DEBUG("trigger right with ts={}",
                  rightRecords[i][size - 1].read(hashJoinProbeHelper.timeStampFieldRight)->toString());
        hashJoinBuildRight->close(executionContext, recordBufferRight);
    }

    auto numberOfEmittedBuffersBuild = hashJoinOperatorTest->emittedBuffers.size();
    NES_DEBUG("trigger Probe for numberOfEmittedBuffersBuild = {}", numberOfEmittedBuffersBuild);
    for (auto cnt = 0UL; cnt < numberOfEmittedBuffersBuild; ++cnt) {
        auto tupleBuffer = hashJoinOperatorTest->emittedBuffers[cnt];
        RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
        hashJoinProbe->open(executionContext, recordBuffer);
    }

    // Delete all buffers that have been emitted from the build phase
    hashJoinOperatorTest->emittedBuffers.erase(hashJoinOperatorTest->emittedBuffers.begin(),
                                               hashJoinOperatorTest->emittedBuffers.begin() + numberOfEmittedBuffersBuild);

    /* Checking if all windows have been deleted except for one.
     * We require always one window as we do not know here if we have to take care of more tuples*/
    if (hashJoinOpHandler->as<Operators::StreamJoinOperatorHandler>()->getNumberOfSlices() != 1) {
        NES_ERROR("Not exactly one active window! {}",
                  hashJoinOpHandler->as<Operators::StreamJoinOperatorHandler>()->getNumberOfSlices());
        //TODO: this is tricky now we can either activate deletion but then the later code cannot check the window size or we test this here
        //        return false;
    }

    Value<UInt64> zeroValue((uint64_t) 0UL);
    auto maxWindowIdentifier = std::ceil((double) hashJoinProbeHelper.numberOfTuplesToProduce / hashJoinProbeHelper.windowSize)
        * hashJoinProbeHelper.windowSize;
    for (auto windowIdentifier = hashJoinProbeHelper.windowSize; windowIdentifier < maxWindowIdentifier;
         windowIdentifier += hashJoinProbeHelper.windowSize) {
        auto expectedNumberOfTuplesInWindowLeft = calculateExpNoTuplesInWindow(hashJoinProbeHelper.numberOfTuplesToProduce,
                                                                               windowIdentifier,
                                                                               hashJoinProbeHelper.windowSize);
        auto expectedNumberOfTuplesInWindowRight = calculateExpNoTuplesInWindow(hashJoinProbeHelper.numberOfTuplesToProduce,
                                                                                windowIdentifier,
                                                                                hashJoinProbeHelper.windowSize);

        auto existingNumberOfTuplesInWindowLeft =
            hashJoinOpHandler->getNumberOfTuplesInSlice(windowIdentifier, QueryCompilation::JoinBuildSideType::Left);

        auto existingNumberOfTuplesInWindowRight =
            hashJoinOpHandler->getNumberOfTuplesInSlice(windowIdentifier, QueryCompilation::JoinBuildSideType::Right);

        if (existingNumberOfTuplesInWindowLeft != expectedNumberOfTuplesInWindowLeft
            || existingNumberOfTuplesInWindowRight != expectedNumberOfTuplesInWindowRight) {
            NES_ERROR(
                "wrong number of inputs are created existingNumberOfTuplesInWindowLeft={} expectedNumberOfTuplesInWindowLeft={} "
                "existingNumberOfTuplesInWindowRight={} expectedNumberOfTuplesInWindowRight={} windowIdentifier={}",
                existingNumberOfTuplesInWindowLeft,
                expectedNumberOfTuplesInWindowLeft,
                existingNumberOfTuplesInWindowRight,
                expectedNumberOfTuplesInWindowRight,
                windowIdentifier);
            EXPECT_TRUE(false);
            EXIT_FAILURE;
        }

        for (auto& leftRecordOuter : leftRecords) {
            for (auto& leftRecordInner : leftRecordOuter) {
                for (auto& rightRecordOuter : rightRecords) {
                    for (auto& rightRecordInner : rightRecordOuter) {
                        auto timestampLeftVal = leftRecordInner.read(hashJoinProbeHelper.timeStampFieldLeft)
                                                    .getValue()
                                                    .staticCast<UInt64>()
                                                    .getValue();
                        auto timestampRightVal = rightRecordInner.read(hashJoinProbeHelper.timeStampFieldRight)
                                                     .getValue()
                                                     .staticCast<UInt64>()
                                                     .getValue();

                        auto windowStart = windowIdentifier - hashJoinProbeHelper.windowSize;
                        auto windowEnd = windowIdentifier;
                        auto leftKey = leftRecordInner.read(hashJoinProbeHelper.joinFieldNameLeft);
                        auto rightKey = rightRecordInner.read(hashJoinProbeHelper.joinFieldNameRight);

                        auto sizeOfWindowStart = sizeof(uint64_t);
                        auto sizeOfWindowEnd = sizeof(uint64_t);

                        DefaultPhysicalTypeFactory physicalDataTypeFactory;
                        auto joinKeySize =
                            physicalDataTypeFactory
                                .getPhysicalType(
                                    hashJoinProbeHelper.leftSchema->get(hashJoinProbeHelper.joinFieldNameLeft)->getDataType())
                                ->size();
                        auto leftTupleSize = hashJoinProbeHelper.leftSchema->getSchemaSizeInBytes();
                        auto rightTupleSize = hashJoinProbeHelper.rightSchema->getSchemaSizeInBytes();

                        if (windowStart <= timestampLeftVal && timestampLeftVal < windowEnd && windowStart <= timestampRightVal
                            && timestampRightVal < windowEnd && leftKey == rightKey) {
                            Record joinedRecord;
                            Nautilus::Value<Any> windowStartVal(windowStart);
                            Nautilus::Value<Any> windowEndVal(windowEnd);
                            joinedRecord.write(joinSchema.joinSchema->get(0)->getName(), windowStartVal);
                            joinedRecord.write(joinSchema.joinSchema->get(1)->getName(), windowEndVal);
                            joinedRecord.write(joinSchema.joinSchema->get(2)->getName(),
                                               leftRecordInner.read(hashJoinProbeHelper.joinFieldNameLeft));

                            // Writing the leftSchema fields
                            for (auto& field : hashJoinProbeHelper.leftSchema->fields) {
                                joinedRecord.write(field->getName(), leftRecordInner.read(field->getName()));
                            }

                            // Writing the rightSchema fields
                            for (auto& field : hashJoinProbeHelper.rightSchema->fields) {
                                joinedRecord.write(field->getName(), rightRecordInner.read(field->getName()));
                            }

                            // Check if this joinedRecord is in the emitted records
                            auto it = std::find(collector->records.begin(), collector->records.end(), joinedRecord);
                            if (it == collector->records.end()) {
                                NES_ERROR("Could not find joinedRecord {} in the emitted records!", joinedRecord.toString());
                                return false;
                            }

                            collector->records.erase(it);
                        }
                    }
                }
            }
        }
    }

    return true;
}

class TestRunner : public NES::Exceptions::ErrorListener {
  public:
    void onFatalError(int signalNumber, std::string callStack) override {
        std::ostringstream fatalErrorMessage;
        fatalErrorMessage << "onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] callstack "
                          << callStack;

        NES_FATAL_ERROR("{}", fatalErrorMessage.str());
        std::cerr << fatalErrorMessage.str() << std::endl;
    }

    void onFatalException(std::shared_ptr<std::exception> exceptionPtr, std::string callStack) override {
        std::ostringstream fatalExceptionMessage;
        fatalExceptionMessage << "onFatalException: exception=[" << exceptionPtr->what() << "] callstack=\n" << callStack;

        NES_FATAL_ERROR("{}", fatalExceptionMessage.str());
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
                                ->addField("left$timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "f2_left";
    const auto timeStampField = "left$timestamp";
    const auto isLeftSide = QueryCompilation::JoinBuildSideType::Left;

    auto handlerIndex = 0;
    auto readTsField = std::make_shared<Expressions::ReadFieldExpression>(timeStampField);

    auto hashJoinBuild = std::make_shared<Operators::HJBuildSlicing>(
        handlerIndex,
        leftSchema,
        joinFieldNameLeft,
        isLeftSide,
        leftSchema->getSchemaSizeInBytes(),
        std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsField, Windowing::TimeUnit::Milliseconds()),
        QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL,
        QueryCompilation::WindowingStrategy::SLICING);

    HashJoinBuildHelper buildHelper(hashJoinBuild, joinFieldNameLeft, bm, leftSchema, timeStampField, this, isLeftSide);
    ASSERT_TRUE(hashJoinBuildAndCheck(buildHelper));
    // As we are only building here the left side, we do not emit any buffers
    ASSERT_EQ(emittedBuffers.size(), 0);
}

TEST_F(HashJoinOperatorTest, joinBuildTestRight) {
    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)
                                 ->addField("left$timestamp", BasicType::UINT64);

    const auto joinFieldNameRight = "f2_right";
    const auto timeStampField = "left$timestamp";
    const auto isLeftSide = QueryCompilation::JoinBuildSideType::Right;

    auto readTsField = std::make_shared<Expressions::ReadFieldExpression>(timeStampField);
    auto handlerIndex = 0;
    auto hashJoinBuild = std::make_shared<Operators::HJBuildSlicing>(
        handlerIndex,
        rightSchema,
        joinFieldNameRight,
        isLeftSide,
        rightSchema->getSchemaSizeInBytes(),
        std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsField, Windowing::TimeUnit::Milliseconds()),
        QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL,
        QueryCompilation::WindowingStrategy::SLICING);
    HashJoinBuildHelper buildHelper(hashJoinBuild, joinFieldNameRight, bm, rightSchema, timeStampField, this, isLeftSide);

    ASSERT_TRUE(hashJoinBuildAndCheck(buildHelper));
    // As we are only building here the left side, we do not emit any buffers
    ASSERT_EQ(emittedBuffers.size(), 0);
}

TEST_F(HashJoinOperatorTest, joinBuildTestMultiplePagesPerBucket) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("left$timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "f2_left";
    const auto timeStampField = "left$timestamp";
    const auto isLeftSide = QueryCompilation::JoinBuildSideType::Left;

    auto handlerIndex = 0;
    auto readTsField = std::make_shared<Expressions::ReadFieldExpression>(timeStampField);

    auto hashJoinBuild = std::make_shared<Operators::HJBuildSlicing>(
        handlerIndex,
        leftSchema,
        joinFieldNameLeft,
        isLeftSide,
        leftSchema->getSchemaSizeInBytes(),
        std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsField, Windowing::TimeUnit::Milliseconds()),
        QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL,
        QueryCompilation::WindowingStrategy::SLICING);

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
                                ->addField("left$timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "f2_left";
    const auto timeStampField = "left$timestamp";
    const auto handlerIndex = 0;
    const auto isLeftSide = QueryCompilation::JoinBuildSideType::Left;

    auto readTsField = std::make_shared<Expressions::ReadFieldExpression>(timeStampField);

    auto hashJoinBuild = std::make_shared<Operators::HJBuildSlicing>(
        handlerIndex,
        leftSchema,
        joinFieldNameLeft,
        isLeftSide,
        leftSchema->getSchemaSizeInBytes(),
        std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsField, Windowing::TimeUnit::Milliseconds()),
        QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL,
        QueryCompilation::WindowingStrategy::SLICING);

    HashJoinBuildHelper buildHelper(hashJoinBuild, joinFieldNameLeft, bm, leftSchema, timeStampField, this, isLeftSide);
    buildHelper.pageSize = leftSchema->getSchemaSizeInBytes() * 2, buildHelper.numPartitions = 1;
    buildHelper.windowSize = 5;

    ASSERT_TRUE(hashJoinBuildAndCheck(buildHelper));
    // As we are only building here the left side, we do not emit any buffers
    ASSERT_EQ(emittedBuffers.size(), 0);
}

TEST_F(HashJoinOperatorTest, joinProbeTest) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("left$f1_left", BasicType::UINT64)
                                ->addField("left$f2_left", BasicType::UINT64)
                                ->addField("left$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("right$f1_right", BasicType::UINT64)
                                 ->addField("right$f2_right", BasicType::UINT64)
                                 ->addField("right$timestamp", BasicType::UINT64);

    ASSERT_EQ(leftSchema->getSchemaSizeInBytes(), rightSchema->getSchemaSizeInBytes());

    HashJoinProbeHelper
        hashJoinProbeHelper("left$f2_left", "right$f2_right", bm, leftSchema, rightSchema, "left$timestamp", "right$timestamp", this);
    hashJoinProbeHelper.pageSize = 2 * leftSchema->getSchemaSizeInBytes();
    hashJoinProbeHelper.numPartitions = 2;
    hashJoinProbeHelper.windowSize = 20;

    ASSERT_TRUE(hashJoinProbeAndCheck(hashJoinProbeHelper));
}

TEST_F(HashJoinOperatorTest, joinProbeTestMultipleBuckets) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("left$f1_left", BasicType::UINT64)
                                ->addField("left$f2_left", BasicType::UINT64)
                                ->addField("left$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("right$f1_right", BasicType::UINT64)
                                 ->addField("right$f2_right", BasicType::UINT64)
                                 ->addField("right$timestamp", BasicType::UINT64);

    ASSERT_EQ(leftSchema->getSchemaSizeInBytes(), rightSchema->getSchemaSizeInBytes());

    HashJoinProbeHelper hashJoinProbeHelper("left$f2_left",
                                            "right$f2_right",
                                            bm,
                                            leftSchema,
                                            rightSchema,
                                            "left$timestamp",
                                            "right$timestamp",
                                            this);
    hashJoinProbeHelper.windowSize = 10;

    ASSERT_TRUE(hashJoinProbeAndCheck(hashJoinProbeHelper));
}

TEST_F(HashJoinOperatorTest, joinProbeTestMultipleWindows) {

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("left$f1_left", BasicType::UINT64)
                                ->addField("left$f2_left", BasicType::UINT64)
                                ->addField("left$timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("right$f1_right", BasicType::UINT64)
                                 ->addField("right$f2_right", BasicType::UINT64)
                                 ->addField("right$timestamp", BasicType::UINT64);
    ASSERT_EQ(leftSchema->getSchemaSizeInBytes(), rightSchema->getSchemaSizeInBytes());

    HashJoinProbeHelper
        hashJoinProbeHelper("left$f2_left", "right$f2_right", bm, leftSchema, rightSchema, "left$timestamp", "right$timestamp", this);
    hashJoinProbeHelper.numPartitions = 1;
    hashJoinProbeHelper.windowSize = 10;

    ASSERT_TRUE(hashJoinProbeAndCheck(hashJoinProbeHelper));
}

}// namespace NES::Runtime::Execution