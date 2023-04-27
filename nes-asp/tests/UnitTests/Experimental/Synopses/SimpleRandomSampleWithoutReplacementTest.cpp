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
#include <Experimental/Parsing/SynopsisAggregationConfig.hpp>
#include <Experimental/Synopses/Samples/SimpleRandomSampleWithoutReplacement.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <random>

namespace NES::ASP {
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
            NES_INFO("Setup SynopsisPipelineTest test case.");
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
        auto sampleSynopsis = SimpleRandomSampleWithoutReplacement(aggregationConfig, sampleSize);
        sampleSynopsis.setBufferManager(bufferManager);

        auto exactAggValue = aggregationConfig.createAggregationValue();
        auto exactAggValueMemRef = Nautilus::MemRef((int8_t*)exactAggValue.get());
        auto exactAggFunction = aggregationConfig.createAggregationFunction();
        exactAggFunction->reset(exactAggValueMemRef);
        for (auto& record : allRecords) {
            sampleSynopsis.addToSynopsis(record);
            auto tmpValue = record.read(aggregationString);
            exactAggFunction->lift(exactAggValueMemRef, tmpValue);
        }

        auto approximateBuffers = sampleSynopsis.getApproximate(bufferManager);
        EXPECT_EQ(approximateBuffers.size(), 1);

        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(approximateBuffers[0],
                                                                                                  outputSchema);
        auto exactValue = exactAggFunction->lower(exactAggValueMemRef);
        EXPECT_EQ(dynamicBuffer.getNumberOfTuples(), 1);
        if (outputSchema->get(approximateString)->getDataType()->isEquals(DataTypeFactory::createType(BasicType::INT64))) {
            EXPECT_EQ(dynamicBuffer[0][approximateString].read<int64_t>(), exactValue.getValue().staticCast<Nautilus::Int64>().getValue());
        } else if (outputSchema->get(approximateString)->getDataType()->isEquals(DataTypeFactory::createType(BasicType::FLOAT64))) {
            EXPECT_EQ(dynamicBuffer[0][approximateString].read<double>(), exactValue.getValue().staticCast<Nautilus::Double>().getValue());
        } else {
            NES_NOT_IMPLEMENTED();
        }
    }

    double getScalingFactor(Parsing::Aggregation_Type& type, size_t sampleSize, size_t numberOfTuples) {
        if (type == Parsing::Aggregation_Type::SUM || type == Parsing::Aggregation_Type::COUNT) {
            return (double)numberOfTuples / sampleSize;
        }
        return 1;
    }

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
        auto aggregationConfig = Parsing::SynopsisAggregationConfig::create(aggregationType, aggregationString, approximateString,
                                                                            timestampFieldName, inputSchema, outputSchema);
        auto sampleSynopsis = SimpleRandomSampleWithoutReplacement(aggregationConfig, sampleSize);
        sampleSynopsis.setBufferManager(bufferManager);

        auto exactAggValue = aggregationConfig.createAggregationValue();
        auto exactAggValueMemRef = Nautilus::MemRef((int8_t*)exactAggValue.get());
        auto exactAggFunction = aggregationConfig.createAggregationFunction();
        exactAggFunction->reset(exactAggValueMemRef);
        for (auto& record : allRecords) {
            sampleSynopsis.addToSynopsis(record);
        }

        for (auto& pos : posOfRecordsForSamples) {
            auto tmpValue = allRecords[pos].read(aggregationString);
            exactAggFunction->lift(exactAggValueMemRef, tmpValue);
        }

        auto approximateBuffers = sampleSynopsis.getApproximate(bufferManager);
        EXPECT_EQ(approximateBuffers.size(), 1);

        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(approximateBuffers[0],
                                                                                                  outputSchema);
        auto exactValue = exactAggFunction->lower(exactAggValueMemRef);
        auto scalingFactor = getScalingFactor(aggregationType, sampleSize, numberOfTuplesToProduce);
        EXPECT_EQ(dynamicBuffer.getNumberOfTuples(), 1);
        if (outputSchema->get(approximateString)->getDataType()->isEquals(DataTypeFactory::createType(BasicType::INT64))) {
            EXPECT_EQ(dynamicBuffer[0][approximateString].read<int64_t>() * 1.0,
                exactValue.getValue().staticCast<Nautilus::Int64>().getValue() * scalingFactor);
        } else if (outputSchema->get(approximateString)->getDataType()->isEquals(DataTypeFactory::createType(BasicType::FLOAT64))) {
            EXPECT_EQ(dynamicBuffer[0][approximateString].read<double>() * 1.0,
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