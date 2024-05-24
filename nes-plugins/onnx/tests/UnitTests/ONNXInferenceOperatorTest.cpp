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
#include <Execution/Operators/ONNX/ONNXInferenceOperator.hpp>
#include <Execution/Operators/ONNX/ONNXInferenceOperatorHandler.hpp>
#include <Execution/Operators/ThresholdWindow/NonKeyedThresholdWindow/NonKeyedThresholdWindowOperatorHandler.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <cpp-base64/base64.h>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

namespace NES::Runtime::Execution::Operators {

struct Output {
    float iris0;
    float iris1;
    float iris2;
};

class ONNXInferenceOperatorTest : public Testing::BaseUnitTest {
  public:
    std::vector<Expressions::ExpressionPtr> aggFieldAccessExpressionsVector;
    std::vector<Nautilus::Record::RecordFieldIdentifier> resultFieldVector;
    std::vector<Aggregation::AggregationFunctionPtr> aggVector;
    std::vector<std::unique_ptr<Aggregation::AggregationValue>> aggValues;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ONNXInferenceOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ONNXInferenceOperatorTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ONNXInferenceOperatorTest test class."); }
};

static std::string_view from_text_value_type(const NES::Nautilus::Value<Text>& value) {
    auto textValue = reinterpret_cast<TextValue*>(value.getValue().getReference().value->getValue().value);
    return {textValue->str(), textValue->length()};
}

static std::vector<float> decode_floats_from_base64(const std::string_view& base64_encoded) {

    std::vector<uint8_t> decoded;
    auto bytes = base64_decode(base64_encoded);
    std::copy(bytes.begin(), bytes.end(), std::back_inserter(decoded));

    std::vector<float> result;
    for (size_t i = 0; i < (decoded.size() / sizeof(float)); ++i) {
        result.push_back(*reinterpret_cast<float*>(decoded.data() + (i * sizeof(float))));
    }
    return result;
}

std::string read_file(const std::string& file_name) {
    std::ifstream t(std::string(TEST_DATA_DIRECTORY) + file_name);
    NES_ASSERT(t.is_open(), "Could not open file: " + std::string(TEST_DATA_DIRECTORY) + file_name);
    std::stringstream buffer;
    buffer << t.rdbuf();

    return buffer.str();
}

TEST_F(ONNXInferenceOperatorTest, testInferModelForONNXRuntimeYOLOV8) {
    std::string data = "data";
    auto data_out = "data";

    std::vector<std::string> inputFields;
    inputFields.emplace_back(data);

    std::vector<std::string> outputFields;
    outputFields.emplace_back(data_out);

    auto inferModelOperator = std::make_shared<ONNXInferenceOperator>(0, inputFields, outputFields);

    auto collector = std::make_shared<CollectOperator>();
    inferModelOperator->setChild(collector);

    auto handler = std::make_shared<ONNXInferenceOperatorHandler>(std::string(TEST_DATA_DIRECTORY) + "yolov8n-cls.onnx");

    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto buffer_manager = std::make_shared<BufferManager>();
    // needed for Text Allocation
    auto worker_context = std::make_shared<WorkerContext>(0, buffer_manager, 10);

    auto ctx = ExecutionContext(Value<MemRef>((int8_t*) worker_context.get()), Value<MemRef>((int8_t*) &pipelineContext));

    inferModelOperator->setup(ctx);

    auto firstRecord = Record({{"data", Value<Text>(read_file("base64_encoded_cropped_cat.txt").c_str())}});

    //evaluated using python onnxruntime
    auto expected_outputs = decode_floats_from_base64(read_file("base64_encoded_cropped_cat_output.txt"));

    inferModelOperator->execute(ctx, firstRecord);

    auto delta = 0.000001;

    EXPECT_EQ(collector->records.size(), 1);

    auto outputs = decode_floats_from_base64(from_text_value_type(collector->records[0].read(data_out).as<Text>()));
    EXPECT_EQ(collector->records[0].read(data), firstRecord.read(data));
    EXPECT_EQ(outputs.size(), expected_outputs.size());

    for (size_t i = 0; i < outputs.size(); i++) {
        EXPECT_NEAR(outputs[i], expected_outputs[i], delta);
    }

    collector->records.clear();

    inferModelOperator->terminate(ctx);
}

TEST_F(ONNXInferenceOperatorTest, testInferModelForONNXRuntimeBase64Encoding) {
    std::string data = "data";

    auto data_out = "data";

    std::vector<std::string> inputFields;
    inputFields.emplace_back(data);

    std::vector<std::string> outputFields;
    outputFields.emplace_back(data_out);

    auto inferModelOperator = std::make_shared<ONNXInferenceOperator>(0, inputFields, outputFields);

    auto collector = std::make_shared<CollectOperator>();
    inferModelOperator->setChild(collector);

    auto handler = std::make_shared<ONNXInferenceOperatorHandler>(std::string(TEST_DATA_DIRECTORY) + "iris_95acc.onnx");
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto buffer_manager = std::make_shared<BufferManager>();
    // needed for Text Allocation
    auto worker_context = std::make_shared<WorkerContext>(0, buffer_manager, 10);

    auto ctx = ExecutionContext(Value<MemRef>((int8_t*) worker_context.get()), Value<MemRef>((int8_t*) &pipelineContext));

    inferModelOperator->setup(ctx);

    auto firstRecord = Record({{"data", Value<Text>("MzOjQAAAYEAzM7M/zcxMPg==")}});
    auto secondRecord = Record({{"data", Value<Text>("zcycQAAAQEAzM7M/zcxMPg==")}});
    auto thirdRecord = Record({{"data", Value<Text>("ZmaWQM3MTEBmZqY/zcxMPg==")}});
    auto fourthRecord = Record({{"data", Value<Text>("MzOTQGZmRkAAAMA/zcxMPg==")}});

    inferModelOperator->execute(ctx, firstRecord);
    inferModelOperator->execute(ctx, secondRecord);
    inferModelOperator->execute(ctx, thirdRecord);
    inferModelOperator->execute(ctx, fourthRecord);

    //Assertion, values from running the onnx model in python
    std::vector<Output> expectedOutput{{0.8635288, 0.12861131, 0.00785995},
                                       {0.8248063, 0.16215266, 0.01304107},
                                       {0.8458433, 0.14335841, 0.01079828},
                                       {0.8178819, 0.16869366, 0.01342443}};
    auto delta = 0.0000001;

    EXPECT_EQ(collector->records.size(), 4);

    auto outputs = decode_floats_from_base64(from_text_value_type(collector->records[0].read(data_out).as<Text>()));
    EXPECT_EQ(collector->records[0].read(data), firstRecord.read(data));
    EXPECT_NEAR(outputs[0], expectedOutput[0].iris0, delta);
    EXPECT_NEAR(outputs[1], expectedOutput[0].iris1, delta);
    EXPECT_NEAR(outputs[2], expectedOutput[0].iris2, delta);

    outputs = decode_floats_from_base64(from_text_value_type(collector->records[1].read(data_out).as<Text>()));
    EXPECT_EQ(collector->records[1].read(data), secondRecord.read(data));
    EXPECT_NEAR(outputs[0], expectedOutput[1].iris0, delta);
    EXPECT_NEAR(outputs[1], expectedOutput[1].iris1, delta);
    EXPECT_NEAR(outputs[2], expectedOutput[1].iris2, delta);

    outputs = decode_floats_from_base64(from_text_value_type(collector->records[2].read(data_out).as<Text>()));
    EXPECT_EQ(collector->records[2].read(data), thirdRecord.read(data));
    EXPECT_NEAR(outputs[0], expectedOutput[2].iris0, delta);
    EXPECT_NEAR(outputs[1], expectedOutput[2].iris1, delta);
    EXPECT_NEAR(outputs[2], expectedOutput[2].iris2, delta);

    outputs = decode_floats_from_base64(from_text_value_type(collector->records[3].read(data_out).as<Text>()));
    EXPECT_EQ(collector->records[3].read(data), fourthRecord.read(data));
    EXPECT_NEAR(outputs[0], expectedOutput[3].iris0, delta);
    EXPECT_NEAR(outputs[1], expectedOutput[3].iris1, delta);
    EXPECT_NEAR(outputs[2], expectedOutput[3].iris2, delta);

    collector->records.clear();

    inferModelOperator->terminate(ctx);
}

TEST_F(ONNXInferenceOperatorTest, testInferModelForONNXRuntime) {
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

    auto inferModelOperator = std::make_shared<ONNXInferenceOperator>(0, inputFields, outputFields);

    auto collector = std::make_shared<CollectOperator>();
    inferModelOperator->setChild(collector);

    auto handler = std::make_shared<ONNXInferenceOperatorHandler>(std::string(TEST_DATA_DIRECTORY) + "iris_95acc.onnx");
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));

    inferModelOperator->setup(ctx);

    auto firstRecord = Record({{"f1", Value<Float>((Float) 5.1)},
                               {"f2", Value<Float>((Float) 3.5)},
                               {"f3", Value<Float>((Float) 1.4)},
                               {"f4", Value<Float>((Float) 0.2)}});
    auto secondRecord = Record({{"f1", Value<Float>((Float) 4.9)},
                                {"f2", Value<Float>((Float) 3.0)},
                                {"f3", Value<Float>((Float) 1.4)},
                                {"f4", Value<Float>((Float) 0.2)}});
    auto thirdRecord = Record({{"f1", Value<Float>((Float) 4.7)},
                               {"f2", Value<Float>((Float) 3.2)},
                               {"f3", Value<Float>((Float) 1.3)},
                               {"f4", Value<Float>((Float) 0.2)}});
    auto fourthRecord = Record({{"f1", Value<Float>((Float) 4.6)},
                                {"f2", Value<Float>((Float) 3.1)},
                                {"f3", Value<Float>((Float) 1.5)},
                                {"f4", Value<Float>((Float) 0.2)}});
    inferModelOperator->execute(ctx, firstRecord);
    inferModelOperator->execute(ctx, secondRecord);
    inferModelOperator->execute(ctx, thirdRecord);
    inferModelOperator->execute(ctx, fourthRecord);

    //Assertion, values from running the onnx model in python
    std::vector<Output> expectedOutput{{0.8635288, 0.12861131, 0.00785995},
                                       {0.8248063, 0.16215266, 0.01304107},
                                       {0.8458433, 0.14335841, 0.01079828},
                                       {0.8178819, 0.16869366, 0.01342443}};
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

}// namespace NES::Runtime::Execution::Operators
