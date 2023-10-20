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

#include <Operators/LogicalOperators/UDFs/JavaUDFDescriptor.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Exceptions/UDFException.hpp>
#include <Util/JavaUDFDescriptorBuilder.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Catalogs::UDF {

class JavaUDFDescriptorTest : public Testing::BaseUnitTest {
  protected:
    static void SetUpTestCase() { NES::Logger::setupLogging("UdfTest.log", NES::LogLevel::LOG_DEBUG); }
};

// The following tests check that constructing a Java UDF descriptor with invalid data is not possible.
// In order to eliminate the need to change every construction of a JavaUDFDescriptor instance when adding fields to the
// JavaUDFDescriptor class, the construction is done using a builder pattern.
// By default, the JavaUDFDescriptorBuilder contains valid data for all fields required for the Java UDF descriptor,
// e.g., class name, method name, serialized instance, bytecode list, and others.

TEST_F(JavaUDFDescriptorTest, NoExceptionIsThrownForValidData) {
    EXPECT_NO_THROW(JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor());
}

TEST_F(JavaUDFDescriptorTest, TheFullyQualifiedNameMustNotBeEmpty) {
    EXPECT_THROW(JavaUDFDescriptorBuilder{}
                     .setClassName("")// empty name
                     .build(),
                 UDFException);
}

TEST_F(JavaUDFDescriptorTest, TheMethodNameMustNotBeEmtpy) {
    EXPECT_THROW(JavaUDFDescriptorBuilder{}
                     .setMethodName("")// empty name
                     .build(),
                 UDFException);
}

TEST_F(JavaUDFDescriptorTest, TheListOfByteCodeDefinitionsMustNotBeEmpty) {
    EXPECT_THROW(JavaUDFDescriptorBuilder{}
                     .setByteCodeList(jni::JavaUDFByteCodeList{})// empty list
                     .build(),
                 UDFException);
}

TEST_F(JavaUDFDescriptorTest, TheListOfByteCodeDefinitionsMustContainTheFullyQualifiedNameOfTheUdfClass) {
    // when
    auto unknownClassName = "some_other_package.some_other_class_name"s;
    auto byteCodeList = jni::JavaUDFByteCodeList{{"some_package.unknown_class_name"s, jni::JavaByteCode{1}}};
    // then
    EXPECT_THROW(JavaUDFDescriptorBuilder{}.setClassName(unknownClassName).setByteCodeList(byteCodeList).build(), UDFException);
}

TEST_F(JavaUDFDescriptorTest, TheListOfByteCodeDefinitionsMustNotContainEmptyByteCode) {
    // when
    auto className = "some_package.my_udf"s;
    auto byteCodeListWithEmptyByteCode = jni::JavaUDFByteCodeList{{className, jni::JavaByteCode{}}};// empty byte array
    // then
    EXPECT_THROW(JavaUDFDescriptorBuilder{}.setClassName(className).setByteCodeList(byteCodeListWithEmptyByteCode).build(),
                 UDFException);
}

TEST_F(JavaUDFDescriptorTest, TheOutputSchemaMustNotBeEmpty) {
    EXPECT_THROW(JavaUDFDescriptorBuilder{}
                     .setOutputSchema(std::make_shared<Schema>())// empty list
                     .build(),
                 UDFException);
}

TEST_F(JavaUDFDescriptorTest, TheInputClassNameMustNotBeEmpty) {
    EXPECT_THROW(JavaUDFDescriptorBuilder{}
                     .setInputClassName(""s)// empty string
                     .build(),
                 UDFException);
}

TEST_F(JavaUDFDescriptorTest, TheOutputClassNameMustNotBeEmpty) {
    EXPECT_THROW(JavaUDFDescriptorBuilder{}
                     .setOutputClassName(""s)// empty string
                     .build(),
                 UDFException);
}

// The following tests verify the equality logic of the Java UDF descriptor.

TEST_F(JavaUDFDescriptorTest, Equality) {
    // when
    auto descriptor1 = JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor();
    auto descriptor2 = JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor();
    // then
    ASSERT_TRUE(*descriptor1 == *descriptor2);// Compare pointed-to variables, not the pointers.
}

TEST_F(JavaUDFDescriptorTest, InEquality) {
    // when
    auto descriptor = JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor();
    // Check a different class name. In this case, the byte code list must contain the byte code for the different class name.
    auto differentClassName = "different_class_name"s;
    auto descriptorWithDifferentClassName =
        JavaUDFDescriptorBuilder{}.setClassName(differentClassName).setByteCodeList({{differentClassName, {1}}}).build();
    EXPECT_FALSE(*descriptor == *descriptorWithDifferentClassName);
    // Check a different method name.
    auto descriptorWithDifferentMethodName = JavaUDFDescriptorBuilder{}.setMethodName("different_method_name").build();
    EXPECT_FALSE(*descriptor == *descriptorWithDifferentMethodName);
    // Check a different serialized instance (internal state of the UDF).
    auto descriptorWithDifferentSerializedInstance = JavaUDFDescriptorBuilder{}.setInstance({2}).build();
    EXPECT_FALSE(*descriptor == *descriptorWithDifferentSerializedInstance);
    // Check a different byte code definition of the UDF class.
    auto descriptorWithDifferentByteCode =
        JavaUDFDescriptorBuilder{}.setByteCodeList({{descriptor->getClassName(), {2}}}).build();
    EXPECT_FALSE(*descriptor == *descriptorWithDifferentByteCode);
    // Check a different byte code list, i.e., additional dependencies.
    auto descriptorWithDifferentByteCodeList =
        JavaUDFDescriptorBuilder{}.setByteCodeList({{descriptor->getClassName(), {1}}, {differentClassName, {1}}}).build();
    EXPECT_FALSE(*descriptor == *descriptorWithDifferentByteCodeList);
    // Check a different input type.
    auto descriptorWithDifferentInputType = JavaUDFDescriptorBuilder{}.setInputClassName("different_input_type").build();
    // Check a different return type.
    auto descriptorWithDifferentOutputType = JavaUDFDescriptorBuilder{}.setOutputClassName("different_output_type").build();
    EXPECT_FALSE(*descriptor == *descriptorWithDifferentOutputType);
    // The output schema is ignored because it can be derived from the method signature.
    // We trust the Java client to do this correctly; it would cause errors otherwise during query execution.
}

}// namespace NES::Catalogs::UDF
