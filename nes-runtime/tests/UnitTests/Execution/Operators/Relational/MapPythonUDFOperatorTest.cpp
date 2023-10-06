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

#ifdef NAUTILUS_PYTHON_UDF_ENABLED
#include <API/Schema.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/PythonUDF/MapPythonUDF.hpp>
#include <Execution/Operators/Relational/PythonUDF/PythonUDFOperatorHandler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Operators {
class MapPythonUdfOperatorTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MapPythonUdfOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup MapPythonUdfOperatorTest test class." << std::endl;
    }
};

std::string method = "map";
SchemaPtr inputSchema, outputSchema;
std::string function, functionName;
std::map<std::string, std::string> modulesToImport;

/**
* @brief Test simple UDF with integer objects as input and output
* The UDF increments incoming tuples by 10.
*/
TEST_F(MapPythonUdfOperatorTest, IntegerUDFTest) {
    inputSchema = Schema::create()->addField("id", BasicType::INT32);
    outputSchema = Schema::create()->addField("id", BasicType::INT32);
    function = "def integer_test(x):\n\ty = x + 10\n\treturn y\n";
    functionName = "integer_test";

    int32_t initialValue = 42;
    auto handler = std::make_shared<PythonUDFOperatorHandler>(function, functionName, modulesToImport, inputSchema, outputSchema);
    auto map = MapPythonUDF(0, inputSchema, outputSchema);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<Int32>(initialValue)}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("id"), initialValue + 10);
}

/**
* @brief Test simple UDF with long objects as input and output
* The UDF increments incoming tuples by 10.
*/
TEST_F(MapPythonUdfOperatorTest, LongUDFTest) {
    inputSchema = Schema::create()->addField("id", BasicType::INT64);
    outputSchema = Schema::create()->addField("id", BasicType::INT64);
    function = "def long_test(x):\n\ty = x + 10\n\treturn y\n";
    functionName = "long_test";

    int64_t initialValue = 42;
    auto handler = std::make_shared<PythonUDFOperatorHandler>(function, functionName, modulesToImport, inputSchema, outputSchema);
    auto map = MapPythonUDF(0, inputSchema, outputSchema);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<Int64>(initialValue)}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("id"), initialValue + 10);
}

/**
* @brief Test simple UDF with double objects as input and output
* The UDF increments incoming tuples by 10.
*/
TEST_F(MapPythonUdfOperatorTest, DoubleUDFTest) {
    inputSchema = Schema::create()->addField("id", BasicType::FLOAT64);
    outputSchema = Schema::create()->addField("id", BasicType::FLOAT64);
    function = "def double_test(x):\n\ty = x + 10.0\n\treturn y\n";
    functionName = "double_test";

    double initialValue = 42.0;
    auto handler = std::make_shared<PythonUDFOperatorHandler>(function, functionName, modulesToImport, inputSchema, outputSchema);
    auto map = MapPythonUDF(0, inputSchema, outputSchema);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<Double>(initialValue)}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("id"), initialValue + 10.0);
}

/**
* @brief Test simple UDF with float objects as input and output
* The UDF increments incoming tuples by 10.
*/
TEST_F(MapPythonUdfOperatorTest, FloatUDFTest) {
    inputSchema = Schema::create()->addField("id", BasicType::FLOAT32);
    outputSchema = Schema::create()->addField("id", BasicType::FLOAT32);
    function = "def float_test(x):\n\ty = x + 10.0\n\treturn y\n";
    functionName = "float_test";

    float initialValue = 42.0;
    auto handler = std::make_shared<PythonUDFOperatorHandler>(function, functionName, modulesToImport, inputSchema, outputSchema);
    auto map = MapPythonUDF(0, inputSchema, outputSchema);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<Float>(initialValue)}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("id"), initialValue + 10.0);
}

/**
* @brief Test simple UDF with boolean objects as input and output
* The UDF sets incoming tuples to false.
*/
TEST_F(MapPythonUdfOperatorTest, BooleanUDFTest) {
    inputSchema = Schema::create()->addField("id", BasicType::BOOLEAN);
    outputSchema = Schema::create()->addField("id", BasicType::BOOLEAN);
    function = "def boolean_test(x):\n\tx = False\n\treturn x\n";
    functionName = "boolean_test";

    auto initialValue = true;
    auto handler = std::make_shared<PythonUDFOperatorHandler>(function, functionName, modulesToImport, inputSchema, outputSchema);
    auto map = MapPythonUDF(0, inputSchema, outputSchema);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<Boolean>(initialValue)}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("id"), false);
}

/**
* @brief Test simple UDF with importing a module and using it
* The UDF increments incoming tuples by 10.
*/
TEST_F(MapPythonUdfOperatorTest, MathImportUDFTest) {
    inputSchema = Schema::create()->addField("x", BasicType::INT32)->addField("y", BasicType::INT32);
    outputSchema = Schema::create()->addField("z", BasicType::FLOAT32);
    function = "def math_pow_test(x, y):\n\tz = math.pow(x, y)\n\treturn z\n";
    functionName = "math_pow_test";
    modulesToImport["math"] = "";

    int32_t x = 3;
    int32_t y = 2;
    auto handler = std::make_shared<PythonUDFOperatorHandler>(function, functionName, modulesToImport, inputSchema, outputSchema);
    auto map = MapPythonUDF(0, inputSchema, outputSchema);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"x", Value<Int32>(x)},
                          {"y", Value<Int32>(y)}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("z"), 9.0);
}

/**
* @brief Test simple UDF with importing a module with an alias and using it
* The UDF increments incoming tuples by 10.
*/
TEST_F(MapPythonUdfOperatorTest, MathImportWithAliasUDFTest) {
    inputSchema = Schema::create()->addField("x", BasicType::INT32)->addField("y", BasicType::INT32);
    outputSchema = Schema::create()->addField("z", BasicType::FLOAT32);
    function = "def math_pow_test(x, y):\n\tz = m.pow(x, y)\n\treturn z\n";
    functionName = "math_pow_test";
    modulesToImport["math"] = "m";

    int32_t x = 3;
    int32_t y = 2;
    auto handler = std::make_shared<PythonUDFOperatorHandler>(function, functionName, modulesToImport, inputSchema, outputSchema);
    auto map = MapPythonUDF(0, inputSchema, outputSchema);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"x", Value<Int32>(x)},
                          {"y", Value<Int32>(y)}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("z"), 9.0);
}

/**
* @brief Test simple UDF with importing a module with an alias and using it
* The UDF uses the power function of numpy and computes a numpy array
*/
TEST_F(MapPythonUdfOperatorTest, NumpyImportWithAliasUDFTest) {
    inputSchema = Schema::create()
                      ->addField("x", BasicType::INT32)
                      ->addField("y", BasicType::INT32)
                      ->addField("z", BasicType::INT32);
    outputSchema = Schema::create()
                       ->addField("x", BasicType::INT32)
                       ->addField("y", BasicType::INT32);
    function = "def np_pow_test(x, y, z):"
               "\n\ttest_array = np.array([x, y])"
               "\n\tresult = np.power(test_array, z)"
               "\n\treturn result[0].item(), result[1].item()\n";
    functionName = "np_pow_test";
    modulesToImport["numpy"] = "np";

    int32_t x = 3;
    int32_t y = 4;
    int32_t z = 2;
    auto handler = std::make_shared<PythonUDFOperatorHandler>(function, functionName, modulesToImport, inputSchema, outputSchema);
    auto map = MapPythonUDF(0, inputSchema, outputSchema);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"x", Value<Int32>(x)},
                          {"y", Value<Int32>(y)},
                          {"z", Value<Int32>(z)}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("x"), 9);
    ASSERT_EQ(record.read("y"), 16);
}
  
* @brief Test simple UDF with string objects as input and output
* The UDF sets incoming tuples to false.
*/
TEST_F(MapPythonUdfOperatorTest, StringUDFTest) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<Runtime::WorkerContext>(-1, bm, 1024);
    inputSchema = Schema::create()->addField("id",  BasicType::TEXT);
    outputSchema = Schema::create()->addField("id",  BasicType::TEXT);
    function = "def string_test(x):\n\tx = \"new_value\"\n\treturn x\n";
    functionName = "string_test";

    auto initialValue = "old_value";
    auto handler = std::make_shared<PythonUDFOperatorHandler>(function, functionName, inputSchema, outputSchema);

    auto map = MapPythonUDF(0, inputSchema, outputSchema);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>((int8_t*) &wc), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<Text>(initialValue)}});
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(nullptr));
    map.setup(ctx);
    map.open(ctx, recordBuffer);
    map.execute(ctx, record);
    ASSERT_EQ(record.read("id"), Value<Text>("new_value"));
}

/**
* @brief Test simple python UDF
* The UDF sets the bool to false, numerics +10 and appends to strings the postfix 'appended'.
*/
TEST_F(MapPythonUdfOperatorTest, ComplexMapFunction) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<Runtime::WorkerContext>(-1, bm, 1024);
    inputSchema = Schema::create()
                      ->addField("byteVariable", BasicType::INT8)
                      ->addField("shortVariable", BasicType::INT16)
                      ->addField("intVariable", BasicType::INT32)
                      ->addField("longVariable", BasicType::INT64)
                      ->addField("floatVariable", BasicType::FLOAT32)
                      ->addField("doubleVariable", BasicType::FLOAT64)
                      ->addField("booleanVariable", BasicType::BOOLEAN)
                      ->addField("stringVariable",  BasicType::TEXT);
    outputSchema = Schema::create()
                       ->addField("byteVariable", BasicType::INT8)
                       ->addField("shortVariable", BasicType::INT16)
                       ->addField("intVariable", BasicType::INT32)
                       ->addField("longVariable", BasicType::INT64)
                       ->addField("floatVariable", BasicType::FLOAT32)
                       ->addField("doubleVariable", BasicType::FLOAT64)
                       ->addField("booleanVariable", BasicType::BOOLEAN)
                       ->addField("stringVariable",  BasicType::TEXT);
    function = "def complex_test(byte_var, short_var, int_var, long_var, float_var, double_var, boolean_var, string_var):"
               "\n\tbyte_var = byte_var + 10"
               "\n\tshort_var = short_var + 10"
               "\n\tint_var = int_var + 10"
               "\n\tlong_var = long_var + 10"
               "\n\tfloat_var = float_var + 10.0"
               "\n\tdouble_var = double_var + 10.0"
               "\n\tboolean_var = False"
               "\n\tstring_var += \"appended\""
               "\n\treturn byte_var, short_var, int_var, long_var, float_var, double_var, False, string_var\n";
    functionName = "complex_test";

    int8_t initialByte = 11;
    int16_t initialShort = 12;
    int32_t initialInt = 13;
    int64_t initialLong = 14;
    float initialFloat = 15.0;
    double initialDouble = 16.0;
    bool initialBool = true;
    auto initialString = "x";
    auto handler = std::make_shared<PythonUDFOperatorHandler>(function, functionName, modulesToImport, inputSchema, outputSchema);
    auto map = MapPythonUDF(0, inputSchema, outputSchema);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>((int8_t*) &wc), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"byteVariable", Value<Int8>(initialByte)},
                          {"shortVariable", Value<Int16>(initialShort)},
                          {"intVariable", Value<Int32>(initialInt)},
                          {"longVariable", Value<Int64>(initialLong)},
                          {"floatVariable", Value<Float>(initialFloat)},
                          {"doubleVariable", Value<Double>(initialDouble)},
                          {"booleanVariable", Value<Boolean>(initialBool)},
                          {"stringVariable", Value<Text>("x")}});
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(nullptr));
    map.setup(ctx);
    map.open(ctx, recordBuffer);
    map.execute(ctx, record);
    EXPECT_EQ(record.read("byteVariable"), initialByte + 10);
    EXPECT_EQ(record.read("shortVariable"), initialShort + 10);
    EXPECT_EQ(record.read("intVariable"), initialInt + 10);
    EXPECT_EQ(record.read("longVariable"), initialLong + 10);
    EXPECT_EQ(record.read("floatVariable"), initialFloat + 10.0);
    EXPECT_EQ(record.read("doubleVariable"), initialDouble + 10.0);
    EXPECT_EQ(record.read("booleanVariable"), false);
    EXPECT_EQ(record.read("stringVariable"), Value<Text>("xappended"));
}

}// namespace NES::Runtime::Execution::Operators
#endif//NAUTILUS_PYTHON_UDF_ENABLED