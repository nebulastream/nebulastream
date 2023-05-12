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

#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Experimental/Benchmarking/MicroBenchmarkSchemas.hpp>
#include <Experimental/Operators/SynopsesOperator.hpp>
#include <Experimental/Parsing/SynopsisAggregationConfig.hpp>
#include <Experimental/Synopses/Samples/RandomSampleWithoutReplacement.hpp>
#include <Experimental/Synopses/Samples/SRSWoROperatorHandler.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <random>

namespace NES::ASP {
    /**
     * @brief MockedPipelineExecutionContext for our executable pipeline
     */
    class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
      public:
        MockedPipelineExecutionContext(std::vector<Runtime::Execution::OperatorHandlerPtr> handlers = {})
            : PipelineExecutionContext(
                -1,// mock pipeline id
                1,         // mock query id
                nullptr,
                1, //noWorkerThreads
                {},
                {},
                handlers){};
    };


    class SimpleRandomSampleWithoutReplacementTest : public Testing::NESBaseTest,
                                                     public ::testing::WithParamInterface<Parsing::Aggregation_Type> {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("SimpleRandomSampleWithoutReplacementTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup SimpleRandomSampleWithoutReplacementTest test class.");
        }

        /* Will be called before a test is executed. */
        void SetUp() override {
            NESBaseTest::SetUp();
            NES_INFO("Setup SimpleRandomSampleWithoutReplacementTest test case.");
            bufferManager = std::make_shared<Runtime::BufferManager>();
            aggregationType = this->GetParam();
        }

        Runtime::BufferManagerPtr bufferManager;
        Parsing::Aggregation_Type aggregationType;
    };


    std::vector<Nautilus::Record> fillBuffer(Schema& schema, size_t numberOfTuplesToProduce) {
        std::vector<Nautilus::Record> allRecords;
        for (auto i = 0UL; i < numberOfTuplesToProduce; ++i) {
            auto record = Nautilus::Record({
                {schema.get(0)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) i)},
                {schema.get(1)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) (i % 100))},
                {schema.get(2)->getName(), Nautilus::Value<Nautilus::UInt64>((uint64_t) (i))}
            });

            allRecords.emplace_back(record);
        }
        return allRecords;
    }

    /**
     * @brief Tests SRSWoR with 100 tuples and the same sample size. Compares the approximate output with the exact query
     */
    TEST_P(SimpleRandomSampleWithoutReplacementTest, testWithSmallNumberOfTuples) {
        if (aggregationType == Parsing::Aggregation_Type::NONE) {
            // We do not check anything for the aggregation type none
            ASSERT_TRUE(true);
        }
        NES_INFO("Running test for " << magic_enum::enum_name(aggregationType));

        auto aggregationString = "value";
        auto approximateString = "aggregation";
        auto timestampFieldName = "ts";
        auto numberOfTuplesToProduce = 100;
        auto sampleSize = numberOfTuplesToProduce;
        auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                               ->addField("id", BasicType::UINT64)
                               ->addField(aggregationString, BasicType::INT64)
                               ->addField(timestampFieldName, BasicType::UINT64);
        auto outputSchema = Benchmarking::getOutputSchemaFromTypeAndInputSchema(aggregationType, *inputSchema, aggregationString);

        auto allRecords = fillBuffer(*inputSchema, numberOfTuplesToProduce);
        auto aggregationConfig = Parsing::SynopsisAggregationConfig::create(aggregationType, aggregationString, approximateString,
                                                                            timestampFieldName, inputSchema, outputSchema);
        auto sampleSynopsis = RandomSampleWithoutReplacement(aggregationConfig, sampleSize);

        auto exactAggValue = aggregationConfig.createAggregationValue();
        auto exactAggValueMemRef = Nautilus::MemRef((int8_t*)exactAggValue.get());
        auto exactAggFunction = aggregationConfig.createAggregationFunction();

        auto workerContext = std::make_shared<Runtime::WorkerContext>(0, bufferManager, 100);
        auto handlerIndex = 0;
        auto opHandler = std::make_shared<SRSWoROperatorHandler>();
        std::vector<Runtime::Execution::OperatorHandlerPtr> opHandlers = {opHandler};
        auto pipelineContext = std::make_shared<MockedPipelineExecutionContext>(opHandlers);
        Runtime::Execution::ExecutionContext executionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                                              Nautilus::Value<Nautilus::MemRef>((int8_t*) pipelineContext.get()));
        sampleSynopsis.setup(handlerIndex, executionContext);

        exactAggFunction->reset(exactAggValueMemRef);
        for (auto& record : allRecords) {
            sampleSynopsis.addToSynopsis(handlerIndex, executionContext, record);
            exactAggFunction->lift(exactAggValueMemRef, record);
        }

        auto approximateBuffers = sampleSynopsis.getApproximate(handlerIndex, executionContext, bufferManager);
        EXPECT_EQ(approximateBuffers.size(), 1);

        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(approximateBuffers[0],
                                                                                                  outputSchema);
        Nautilus::Record exactRecord;
        exactAggFunction->lower(exactAggValueMemRef, exactRecord);
        ASSERT_EQ(exactRecord.getAllFields().size(), 1);
        auto fieldIdentifier = exactRecord.getAllFields()[0];
        auto exactValue = exactRecord.read(fieldIdentifier);


        EXPECT_EQ(dynamicBuffer.getNumberOfTuples(), 1);
        if (outputSchema->get(approximateString)->getDataType()->isEquals(DataTypeFactory::createType(BasicType::INT64))) {
            EXPECT_EQ(dynamicBuffer[0][approximateString].read<int64_t>(), exactValue.getValue().staticCast<Nautilus::Int64>().getValue());
        } else if (outputSchema->get(approximateString)->getDataType()->isEquals(DataTypeFactory::createType(BasicType::FLOAT64))) {
            EXPECT_EQ(dynamicBuffer[0][approximateString].read<double>(), exactValue.getValue().staticCast<Nautilus::Double>().getValue());
        } else {
            NES_NOT_IMPLEMENTED();
        }
    }

    /**
     * @brief Returns the scaling factor, which is the numberOfTuples/sampleSize
     * @param type
     * @param sampleSize
     * @param numberOfTuples
     * @return ScalingFactor
     */
    double getScalingFactor(Parsing::Aggregation_Type& type, size_t sampleSize, size_t numberOfTuples) {
        if (type == Parsing::Aggregation_Type::SUM || type == Parsing::Aggregation_Type::COUNT) {
            return (double)numberOfTuples / sampleSize;
        }
        return 1;
    }

    /**
     * @brief Tests with a sample size that is smaller than the number of tuples
     */
    TEST_P(SimpleRandomSampleWithoutReplacementTest, testWithSmallerSampleSizeThan) {
        if (aggregationType == Parsing::Aggregation_Type::NONE) {
            // We do not check anything for the aggregation type none
            ASSERT_TRUE(true);
        }
        NES_INFO("Running test for " << magic_enum::enum_name(aggregationType));

        auto aggregationString = "value";
        auto approximateString = "aggregation";
        auto timestampFieldName = "ts";
        auto numberOfTuplesToProduce = 100;
        auto sampleSize = 10;
        auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                    ->addField("id", BasicType::UINT64)
                                    ->addField(aggregationString, BasicType::INT64)
                                    ->addField(timestampFieldName, BasicType::UINT64);
        auto outputSchema = Benchmarking::getOutputSchemaFromTypeAndInputSchema(aggregationType, *inputSchema, aggregationString);


        // Computing the position of the records
        std::mt19937 generator(GENERATOR_SEED_DEFAULT);
        std::uniform_int_distribution<uint64_t> uniformIntDistribution(0, numberOfTuplesToProduce - 1);
        std::vector<size_t> posOfRecordsForSamples;
        for (auto i = 0; i < sampleSize; ++i) {
            uint64_t pos = uniformIntDistribution(generator);
            while (std::find(posOfRecordsForSamples.begin(), posOfRecordsForSamples.end(), pos) != posOfRecordsForSamples.end()) {
                pos = uniformIntDistribution(generator);
            }

            posOfRecordsForSamples.emplace_back(pos);
        }


        auto allRecords = fillBuffer(*inputSchema, numberOfTuplesToProduce);
        auto synopsisConfig = ASP::Parsing::SynopsisConfiguration::create(ASP::Parsing::Synopsis_Type::SRSWoR, sampleSize);
        auto aggregationConfig = Parsing::SynopsisAggregationConfig::create(aggregationType, aggregationString, approximateString,
                                                                            timestampFieldName, inputSchema, outputSchema);
        auto synopsis = ASP::AbstractSynopsis::create(*synopsisConfig, aggregationConfig);


        auto exactAggValue = aggregationConfig.createAggregationValue();
        auto exactAggValueMemRef = Nautilus::MemRef((int8_t*)exactAggValue.get());
        auto exactAggFunction = aggregationConfig.createAggregationFunction();
        exactAggFunction->reset(exactAggValueMemRef);

        auto workerContext = std::make_shared<Runtime::WorkerContext>(0, bufferManager, 100);
        auto handlerIndex = 0;
        auto opHandler = std::make_shared<SRSWoROperatorHandler>();
        std::vector<Runtime::Execution::OperatorHandlerPtr> opHandlers = {opHandler};
        auto pipelineContext = std::make_shared<MockedPipelineExecutionContext>(opHandlers);
        Runtime::Execution::ExecutionContext executionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                                              Nautilus::Value<Nautilus::MemRef>((int8_t*) pipelineContext.get()));
        auto synopsesOperator = std::make_shared<Runtime::Execution::Operators::SynopsesOperator>(handlerIndex, synopsis);

        synopsesOperator->setup(executionContext);
        for (auto& record : allRecords) {
            synopsesOperator->execute(executionContext, record);
        }
        auto approximateBuffers = synopsis->getApproximate(handlerIndex, executionContext, bufferManager);
        EXPECT_EQ(approximateBuffers.size(), 1);

        for (auto& pos : posOfRecordsForSamples) {
            exactAggFunction->lift(exactAggValueMemRef, allRecords[pos]);
        }

        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(approximateBuffers[0],
                                                                                                  outputSchema);
        Nautilus::Record exactRecord;
        exactAggFunction->lower(exactAggValueMemRef, exactRecord);
        ASSERT_EQ(exactRecord.getAllFields().size(), 1);
        auto fieldIdentifier = exactRecord.getAllFields()[0];
        auto exactValue = exactRecord.read(fieldIdentifier);

        auto scalingFactor = getScalingFactor(aggregationType, sampleSize, numberOfTuplesToProduce);
        EXPECT_EQ(dynamicBuffer.getNumberOfTuples(), 1);
        if (outputSchema->get(approximateString)->getDataType()->isEquals(DataTypeFactory::createType(BasicType::INT64))) {
            EXPECT_EQ(dynamicBuffer[0][approximateString].read<int64_t>(),
                exactValue.getValue().staticCast<Nautilus::Int64>().getValue() * scalingFactor);
        } else if (outputSchema->get(approximateString)->getDataType()->isEquals(DataTypeFactory::createType(BasicType::FLOAT64))) {
            EXPECT_EQ(dynamicBuffer[0][approximateString].read<double>(),
                exactValue.getValue().staticCast<Nautilus::Double>().getValue() * scalingFactor);
        } else {
            NES_NOT_IMPLEMENTED();
        }
    }

    INSTANTIATE_TEST_SUITE_P(SimpleTest,
                            SimpleRandomSampleWithoutReplacementTest,
                            ::testing::Values( Parsing::Aggregation_Type::MIN, Parsing::Aggregation_Type::MAX,
                                               Parsing::Aggregation_Type::SUM, Parsing::Aggregation_Type::AVERAGE,
                                               Parsing::Aggregation_Type::COUNT),
                            [](const testing::TestParamInfo<SimpleRandomSampleWithoutReplacementTest::ParamType>& info) {
                                return std::string(magic_enum::enum_name(info.param));
                            });
} // namespace NES::ASP
