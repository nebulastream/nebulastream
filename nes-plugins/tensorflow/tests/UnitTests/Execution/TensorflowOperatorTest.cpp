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

#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Tensorflow/TensorflowInferenceOperator.hpp>
#include <Execution/Operators/Tensorflow/TensorflowInferenceOperatorHandler.hpp>
#include <Execution/Operators/ThresholdWindow/NonKeyedThresholdWindow/NonKeyedThresholdWindowOperatorHandler.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Operators {

struct Output {
    float iris0;
    float iris1;
    float iris2;
};

class TensorflowOperatorTest : public Testing::BaseUnitTest {
  public:
    std::vector<Expressions::ExpressionPtr> aggFieldAccessExpressionsVector;
    std::vector<Nautilus::Record::RecordFieldIdentifier> resultFieldVector;
    std::vector<Aggregation::AggregationFunctionPtr> aggVector;
    std::vector<std::unique_ptr<Aggregation::AggregationValue>> aggValues;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TensorflowOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TensorflowOperatorTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down TensorflowOperatorTest test class."); }
};

/**
 * @brief Tests the infer model operator for bool input.
 */
TEST_F(TensorflowOperatorTest, testInferModelForBoolInput) {
    std::string f1 = "f1";
    std::string f2 = "f2";
    std::string f3 = "f3";
    std::string f4 = "f4";

    auto iris0 = "iris0";
    auto iris1 = "iris1";
    auto iris2 = "iris2";

    std::vector<std::string> inputFields;
    inputFields.emplace_back(f1);
    inputFields.emplace_back(f2);
    inputFields.emplace_back(f3);
    inputFields.emplace_back(f4);

    std::vector<std::string> outputFields;
    outputFields.emplace_back(iris0);
    outputFields.emplace_back(iris1);
    outputFields.emplace_back(iris2);

    auto inferModelOperator = std::make_shared<TensorflowInferenceOperator>(0, inputFields, outputFields);

    auto collector = std::make_shared<CollectOperator>();
    inferModelOperator->setChild(collector);

    auto handler =
        std::make_shared<TensorflowInferenceOperatorHandler>(std::filesystem::path(TEST_DATA_DIRECTORY) / "iris_95acc.tflite");
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    inferModelOperator->setup(ctx);

    auto firstRecord = Record({{"f1", Value<>(false)}, {"f2", Value<>(true)}, {"f3", Value<>(false)}, {"f4", Value<>(true)}});
    auto secondRecord = Record({{"f1", Value<>(false)}, {"f2", Value<>(true)}, {"f3", Value<>(false)}, {"f4", Value<>(true)}});
    inferModelOperator->execute(ctx, firstRecord);
    inferModelOperator->execute(ctx, secondRecord);

    //Assertion
    std::vector<Output> expectedOutput{{0.43428239, 0.31287873, 0.25283894}, {0.43428239, 0.31287873, 0.25283894}};
    auto delta = 0.0000001;
    EXPECT_EQ(collector->records.size(), 2);
    EXPECT_EQ(collector->records[0].read(f1), firstRecord.read(f1));
    EXPECT_EQ(collector->records[0].read(f2), firstRecord.read(f2));
    EXPECT_EQ(collector->records[0].read(f3), firstRecord.read(f3));
    EXPECT_EQ(collector->records[0].read(f4), firstRecord.read(f4));
    EXPECT_NEAR(collector->records[0].read(iris0).as<Float>()->getValue(), expectedOutput[0].iris0, delta);
    EXPECT_NEAR(collector->records[0].read(iris1).as<Float>()->getValue(), expectedOutput[0].iris1, delta);
    EXPECT_NEAR(collector->records[0].read(iris2).as<Float>()->getValue(), expectedOutput[0].iris2, delta);

    EXPECT_EQ(collector->records[1].read(f1), secondRecord.read(f1));
    EXPECT_EQ(collector->records[1].read(f2), secondRecord.read(f2));
    EXPECT_EQ(collector->records[1].read(f3), secondRecord.read(f3));
    EXPECT_EQ(collector->records[1].read(f4), secondRecord.read(f4));
    EXPECT_NEAR(collector->records[1].read(iris0).as<Float>()->getValue(), expectedOutput[1].iris0, delta);
    EXPECT_NEAR(collector->records[1].read(iris1).as<Float>()->getValue(), expectedOutput[1].iris1, delta);
    EXPECT_NEAR(collector->records[1].read(iris2).as<Float>()->getValue(), expectedOutput[1].iris2, delta);

    inferModelOperator->terminate(ctx);
}

/**
 * @brief Tests the infer model operator for float input.
 */
TEST_F(TensorflowOperatorTest, testInferModelForFloatInput) {
    std::string f1 = "f1";
    std::string f2 = "f2";
    std::string f3 = "f3";
    std::string f4 = "f4";

    auto iris0 = "iris0";
    auto iris1 = "iris1";
    auto iris2 = "iris2";

    std::vector<std::string> inputFields;
    inputFields.emplace_back(f1);
    inputFields.emplace_back(f2);
    inputFields.emplace_back(f3);
    inputFields.emplace_back(f4);

    std::vector<std::string> outputFields;
    outputFields.emplace_back(iris0);
    outputFields.emplace_back(iris1);
    outputFields.emplace_back(iris2);

    auto inferModelOperator = std::make_shared<TensorflowInferenceOperator>(0, inputFields, outputFields);

    auto collector = std::make_shared<CollectOperator>();
    inferModelOperator->setChild(collector);

    auto handler =
        std::make_shared<TensorflowInferenceOperatorHandler>(std::filesystem::path(TEST_DATA_DIRECTORY) / "iris_95acc.tflite");
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    inferModelOperator->setup(ctx);

    auto firstRecord = Record({{"f1", Value<Float>((float_t) 5.1)},
                               {"f2", Value<Float>((float_t) 3.5)},
                               {"f3", Value<Float>((float_t) 1.4)},
                               {"f4", Value<Float>((float_t) 0.2)}});
    auto secondRecord = Record({{"f1", Value<Float>((float_t) 4.9)},
                                {"f2", Value<Float>((float_t) 3.0)},
                                {"f3", Value<Float>((float_t) 1.4)},
                                {"f4", Value<Float>((float_t) 0.2)}});
    auto thirdRecord = Record({{"f1", Value<Float>((float_t) 4.7)},
                               {"f2", Value<Float>((float_t) 3.2)},
                               {"f3", Value<Float>((float_t) 1.3)},
                               {"f4", Value<Float>((float_t) 0.2)}});
    auto fourthRecord = Record({{"f1", Value<Float>((float_t) 4.6)},
                                {"f2", Value<Float>((float_t) 3.1)},
                                {"f3", Value<Float>((float_t) 1.5)},
                                {"f4", Value<Float>((float_t) 0.2)}});
    inferModelOperator->execute(ctx, firstRecord);
    inferModelOperator->execute(ctx, secondRecord);
    inferModelOperator->execute(ctx, thirdRecord);
    inferModelOperator->execute(ctx, fourthRecord);

    //Assertion
    std::vector<Output> expectedOutput{{0.86352879, 0.12861125, 0.0078599472},
                                       {0.82480621, 0.16215269, 0.0130410725},
                                       {0.84584343, 0.14335836, 0.010798273},
                                       {0.81788188, 0.16869366, 0.013424426}};
    auto delta = 0.0000001;
    EXPECT_EQ(collector->records.size(), 4);
    EXPECT_EQ(collector->records[0].read(f1), firstRecord.read(f1));
    EXPECT_EQ(collector->records[0].read(f2), firstRecord.read(f2));
    EXPECT_EQ(collector->records[0].read(f3), firstRecord.read(f3));
    EXPECT_EQ(collector->records[0].read(f4), firstRecord.read(f4));
    EXPECT_NEAR(collector->records[0].read(iris0).as<Float>()->getValue(), expectedOutput[0].iris0, delta);
    EXPECT_NEAR(collector->records[0].read(iris1).as<Float>()->getValue(), expectedOutput[0].iris1, delta);
    EXPECT_NEAR(collector->records[0].read(iris2).as<Float>()->getValue(), expectedOutput[0].iris2, delta);

    EXPECT_EQ(collector->records[1].read(f1), secondRecord.read(f1));
    EXPECT_EQ(collector->records[1].read(f2), secondRecord.read(f2));
    EXPECT_EQ(collector->records[1].read(f3), secondRecord.read(f3));
    EXPECT_EQ(collector->records[1].read(f4), secondRecord.read(f4));
    EXPECT_NEAR(collector->records[1].read(iris0).as<Float>()->getValue(), expectedOutput[1].iris0, delta);
    EXPECT_NEAR(collector->records[1].read(iris1).as<Float>()->getValue(), expectedOutput[1].iris1, delta);
    EXPECT_NEAR(collector->records[1].read(iris2).as<Float>()->getValue(), expectedOutput[1].iris2, delta);

    EXPECT_EQ(collector->records[2].read(f1), thirdRecord.read(f1));
    EXPECT_EQ(collector->records[2].read(f2), thirdRecord.read(f2));
    EXPECT_EQ(collector->records[2].read(f3), thirdRecord.read(f3));
    EXPECT_EQ(collector->records[2].read(f4), thirdRecord.read(f4));
    EXPECT_NEAR(collector->records[2].read(iris0).as<Float>()->getValue(), expectedOutput[2].iris0, delta);
    EXPECT_NEAR(collector->records[2].read(iris1).as<Float>()->getValue(), expectedOutput[2].iris1, delta);
    EXPECT_NEAR(collector->records[2].read(iris2).as<Float>()->getValue(), expectedOutput[2].iris2, delta);

    EXPECT_EQ(collector->records[3].read(f1), fourthRecord.read(f1));
    EXPECT_EQ(collector->records[3].read(f2), fourthRecord.read(f2));
    EXPECT_EQ(collector->records[3].read(f3), fourthRecord.read(f3));
    EXPECT_EQ(collector->records[3].read(f4), fourthRecord.read(f4));
    EXPECT_NEAR(collector->records[3].read(iris0).as<Float>()->getValue(), expectedOutput[3].iris0, delta);
    EXPECT_NEAR(collector->records[3].read(iris1).as<Float>()->getValue(), expectedOutput[3].iris1, delta);
    EXPECT_NEAR(collector->records[3].read(iris2).as<Float>()->getValue(), expectedOutput[3].iris2, delta);

    inferModelOperator->terminate(ctx);
}

/**
 * @brief Tests the infer model operator for int input.
 */
TEST_F(TensorflowOperatorTest, testInferModelForIntInput) {
    std::string f1 = "f1";
    std::string f2 = "f2";
    std::string f3 = "f3";
    std::string f4 = "f4";

    auto iris0 = "iris0";
    auto iris1 = "iris1";
    auto iris2 = "iris2";

    std::vector<std::string> inputFields;
    inputFields.emplace_back(f1);
    inputFields.emplace_back(f2);
    inputFields.emplace_back(f3);
    inputFields.emplace_back(f4);

    std::vector<std::string> outputFields;
    outputFields.emplace_back(iris0);
    outputFields.emplace_back(iris1);
    outputFields.emplace_back(iris2);

    auto inferModelOperator = std::make_shared<TensorflowInferenceOperator>(0, inputFields, outputFields);

    auto collector = std::make_shared<CollectOperator>();
    inferModelOperator->setChild(collector);

    auto handler =
        std::make_shared<TensorflowInferenceOperatorHandler>(std::filesystem::path(TEST_DATA_DIRECTORY) / "iris_95acc.tflite");
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    inferModelOperator->setup(ctx);

    auto firstRecord = Record({{"f1", Value<>(5)}, {"f2", Value<>(3)}, {"f3", Value<>(1)}, {"f4", Value<>(0)}});
    auto secondRecord = Record({{"f1", Value<>(4)}, {"f2", Value<>(3)}, {"f3", Value<>(1)}, {"f4", Value<>(0)}});
    inferModelOperator->execute(ctx, firstRecord);
    inferModelOperator->execute(ctx, secondRecord);

    //Assertion
    std::vector<Output> expectedOutput{{0.43428239, 0.31287873, 0.25283894}, {0.43428239, 0.31287873, 0.25283894}};
    auto delta = 0.0000001;
    EXPECT_EQ(collector->records.size(), 2);
    EXPECT_EQ(collector->records[0].numberOfFields(), 7);
    EXPECT_EQ(collector->records[0].read(f1), firstRecord.read(f1));
    EXPECT_EQ(collector->records[0].read(f2), firstRecord.read(f2));
    EXPECT_EQ(collector->records[0].read(f3), firstRecord.read(f3));
    EXPECT_EQ(collector->records[0].read(f4), firstRecord.read(f4));
    EXPECT_NEAR(collector->records[0].read(iris0).as<Float>()->getValue(), expectedOutput[0].iris0, delta);
    EXPECT_NEAR(collector->records[0].read(iris1).as<Float>()->getValue(), expectedOutput[0].iris1, delta);
    EXPECT_NEAR(collector->records[0].read(iris2).as<Float>()->getValue(), expectedOutput[0].iris2, delta);

    EXPECT_EQ(collector->records[1].read(f1), secondRecord.read(f1));
    EXPECT_EQ(collector->records[1].read(f2), secondRecord.read(f2));
    EXPECT_EQ(collector->records[1].read(f3), secondRecord.read(f3));
    EXPECT_EQ(collector->records[1].read(f4), secondRecord.read(f4));
    EXPECT_NEAR(collector->records[1].read(iris0).as<Float>()->getValue(), expectedOutput[1].iris0, delta);
    EXPECT_NEAR(collector->records[1].read(iris1).as<Float>()->getValue(), expectedOutput[1].iris1, delta);
    EXPECT_NEAR(collector->records[1].read(iris2).as<Float>()->getValue(), expectedOutput[1].iris2, delta);

    inferModelOperator->terminate(ctx);
}

}// namespace NES::Runtime::Execution::Operators
