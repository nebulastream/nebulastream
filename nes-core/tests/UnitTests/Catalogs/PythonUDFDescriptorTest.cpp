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
#include <gtest/gtest.h>

using namespace std::string_literals;

#include <Operators/LogicalOperators/UDFs/PythonUDFDescriptor.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Operators/Exceptions/UDFException.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PythonUDFDescriptorBuilder.hpp>

namespace NES::Catalogs::UDF {

class PythonUDFDescriptorTest : public Testing::BaseUnitTest {
  protected:
    static void SetUpTestCase() { NES::Logger::setupLogging("UdfTest.log", NES::LogLevel::LOG_DEBUG); }
};

// The following tests check that constructing a Python UDF descriptor with invalid data is not possible.
// In order to eliminate the need to change every construction of a PythonUDFDescriptor instance when adding fields to the
// PythonUDFDescriptor class, the construction is done using a builder pattern.
// By default, the PythonUDFDescriptorBuilder contains valid data for all fields required for the Python UDF descriptor,
// e.g., class name, method name, serialized instance, bytecode list, and others.
const std::string functionName = "udf_function";
const std::string functionString = "def udf_function(x):\n\ty = x + 10\n\treturn y\n";
const SchemaPtr inputSchema = std::make_shared<Schema>()->addField("inputAttribute", DataTypeFactory::createUInt64());
const SchemaPtr outputSchema = std::make_shared<Schema>()->addField("outputAttribute", DataTypeFactory::createUInt64());

TEST_F(PythonUDFDescriptorTest, NoExceptionIsThrownForValidData) {
    EXPECT_NO_THROW(PythonUDFDescriptorBuilder::createDefaultPythonUDFDescriptor());
}

TEST_F(PythonUDFDescriptorTest, TheFunctionNameMustNotBeEmtpy) {
    EXPECT_THROW(PythonUDFDescriptorBuilder{}
                     .setFunctionName("")
                     .setFunctionString(functionString)
                     .setInputSchema(inputSchema)
                     .setOutputSchema(outputSchema)
                     .build(),
                 UDFException);
}

TEST_F(PythonUDFDescriptorTest, TheOutputSchemaMustNotBeEmpty) {
    EXPECT_THROW(PythonUDFDescriptorBuilder{}
                     .setFunctionName(functionName)
                     .setFunctionString(functionString)
                     .setInputSchema(inputSchema)
                     .setOutputSchema(std::make_shared<Schema>())// empty list
                     .build(),
                 UDFException);
}

TEST_F(PythonUDFDescriptorTest, TheFunctionStringMustNotBeEmpty) {
    EXPECT_THROW(PythonUDFDescriptorBuilder{}
                     .setFunctionName(functionName)
                     .setFunctionString("")
                     .setInputSchema(inputSchema)
                     .setOutputSchema(outputSchema)
                     .build(),
                 UDFException);
}

// The following tests verify the equality logic of the Python UDF descriptor.

TEST_F(PythonUDFDescriptorTest, Equality) {
    // when
    auto descriptor1 = PythonUDFDescriptorBuilder::createDefaultPythonUDFDescriptor();
    auto descriptor2 = PythonUDFDescriptorBuilder::createDefaultPythonUDFDescriptor();
    // then
    ASSERT_TRUE(*descriptor1 == *descriptor2);// Compare pointed-to variables, not the pointers.
}

TEST_F(PythonUDFDescriptorTest, InEquality) {
    // when
    auto descriptor = PythonUDFDescriptorBuilder::createDefaultPythonUDFDescriptor();
    // Check a different function name
    auto differentFunctionName = "different_function_name";
    auto descriptorWithDifferentFunctionName = PythonUDFDescriptorBuilder{}
                                                   .setFunctionName(differentFunctionName)
                                                   .setFunctionString(functionString)
                                                   .setInputSchema(inputSchema)
                                                   .setOutputSchema(outputSchema)
                                                   .build();
    EXPECT_FALSE(*descriptor == *descriptorWithDifferentFunctionName);

    // different function string
    auto differentFunctionString = "print(\"Hello World\")";
    auto descriptorWithDifferentFunctionString = PythonUDFDescriptorBuilder{}
                                                     .setFunctionName(functionName)
                                                     .setFunctionString(differentFunctionString)
                                                     .setInputSchema(inputSchema)
                                                     .setOutputSchema(outputSchema)
                                                     .build();
    EXPECT_FALSE(*descriptor == *descriptorWithDifferentFunctionString);
}

}// namespace NES::Catalogs::UDF
