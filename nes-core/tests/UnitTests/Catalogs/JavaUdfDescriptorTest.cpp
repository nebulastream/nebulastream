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

#include <NesBaseTest.hpp>
#include <gtest/gtest.h>

using namespace std::string_literals;

#include <Catalogs/UDF/JavaUdfDescriptor.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Exceptions/UdfException.hpp>
#include <Util/JavaUdfDescriptorBuilder.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Catalogs::UDF {

class JavaUdfDescriptorTest : public Testing::NESBaseTest {
  protected:
    static void SetUpTestCase() { NES::Logger::setupLogging("UdfTest.log", NES::LogLevel::LOG_DEBUG); }
};

// The following tests check that constructing a Java UDF descriptor with invalid data is not possible.
// In order to eliminate the need to change every construction of a JavaUdfDescriptor instance when adding fields to the
// JavaUdfDescriptor class, the construction is done using a builder pattern.
// By default, the JavaUdfDescriptorBuilder contains valid data for all fields required for the Java UDF descriptor,
// e.g., class name, method name, serialized instance, bytecode list, and others.

TEST_F(JavaUdfDescriptorTest, NoExceptionIsThrownForValidData) {
    EXPECT_NO_THROW(JavaUdfDescriptorBuilder::createDefaultJavaUdfDescriptor());
}

TEST_F(JavaUdfDescriptorTest, TheFullyQualifiedNameMustNotBeEmpty) {
    EXPECT_THROW(JavaUdfDescriptorBuilder{}
                     .setClassName("")// empty name
                     .build(),
                 UdfException);
}

TEST_F(JavaUdfDescriptorTest, TheMethodNameMustNotBeEmtpy) {
    EXPECT_THROW(JavaUdfDescriptorBuilder{}
                     .setMethodName("")// empty name
                     .build(),
                 UdfException);
}

TEST_F(JavaUdfDescriptorTest, TheInstanceMustNotBeEmpty) {
    EXPECT_THROW(JavaUdfDescriptorBuilder{}
                     .setInstance(JavaSerializedInstance{})// empty byte array
                     .build(),
                 UdfException);
}

TEST_F(JavaUdfDescriptorTest, TheListOfByteCodeDefinitionsMustNotBeEmpty) {
    EXPECT_THROW(JavaUdfDescriptorBuilder{}
                     .setByteCodeList(JavaUdfByteCodeList{})// empty list
                     .build(),
                 UdfException);
}

TEST_F(JavaUdfDescriptorTest, TheListOfByteCodeDefinitionsMustContainTheFullyQualifiedNameOfTheUdfClass) {
    // when
    auto unknownClassName = "some_other_package.some_other_class_name"s;
    auto byteCodeList = JavaUdfByteCodeList{{"some_package.unknown_class_name"s, JavaByteCode{1}}};
    // then
    EXPECT_THROW(JavaUdfDescriptorBuilder{}.setClassName(unknownClassName).setByteCodeList(byteCodeList).build(), UdfException);
}

TEST_F(JavaUdfDescriptorTest, TheListOfByteCodeDefinitionsMustNotContainEmptyByteCode) {
    // when
    auto className = "some_package.my_udf"s;
    auto byteCodeListWithEmptyByteCode = JavaUdfByteCodeList{{className, JavaByteCode{}}};// empty byte array
    // then
    EXPECT_THROW(JavaUdfDescriptorBuilder{}.setClassName(className).setByteCodeList(byteCodeListWithEmptyByteCode).build(),
                 UdfException);
}

TEST_F(JavaUdfDescriptorTest, TheOutputSchemaMustNotBeEmpty) {
    EXPECT_THROW(JavaUdfDescriptorBuilder{}
                     .setOutputSchema(std::make_shared<Schema>())// empty list
                     .build(),
                 UdfException);
}

TEST_F(JavaUdfDescriptorTest, TheInputClassNameMustNotBeEmpty) {
    EXPECT_THROW(JavaUdfDescriptorBuilder{}
                     .setInputClassName(""s)// empty string
                     .build(),
                 UdfException);
}

TEST_F(JavaUdfDescriptorTest, TheOutputClassNameMustNotBeEmpty) {
    EXPECT_THROW(JavaUdfDescriptorBuilder{}
                     .setOutputClassName(""s)// empty string
                     .build(),
                 UdfException);
}

// The following tests verify the equality logic of the Java UDF descriptor.

TEST_F(JavaUdfDescriptorTest, Equality) {
    // when
    auto descriptor1 = JavaUdfDescriptorBuilder::createDefaultJavaUdfDescriptor();
    auto descriptor2 = JavaUdfDescriptorBuilder::createDefaultJavaUdfDescriptor();
    // then
    ASSERT_TRUE(*descriptor1 == *descriptor2);// Compare pointed-to variables, not the pointers.
}

TEST_F(JavaUdfDescriptorTest, InEquality) {
    // when
    auto descriptor = JavaUdfDescriptorBuilder::createDefaultJavaUdfDescriptor();
    // Check a different class name. In this case, the byte code list must contain the byte code for the different class name.
    auto differentClassName = "different_class_name"s;
    auto descriptorWithDifferentClassName =
        JavaUdfDescriptorBuilder{}.setClassName(differentClassName).setByteCodeList({{differentClassName, {1}}}).build();
    EXPECT_FALSE(*descriptor == *descriptorWithDifferentClassName);
    // Check a different method name.
    auto descriptorWithDifferentMethodName = JavaUdfDescriptorBuilder{}.setMethodName("different_method_name").build();
    EXPECT_FALSE(*descriptor == *descriptorWithDifferentMethodName);
    // Check a different serialized instance (internal state of the UDF).
    auto descriptorWithDifferentSerializedInstance = JavaUdfDescriptorBuilder{}.setInstance({2}).build();
    EXPECT_FALSE(*descriptor == *descriptorWithDifferentSerializedInstance);
    // Check a different byte code definition of the UDF class.
    auto descriptorWithDifferentByteCode =
        JavaUdfDescriptorBuilder{}.setByteCodeList({{descriptor->getClassName(), {2}}}).build();
    EXPECT_FALSE(*descriptor == *descriptorWithDifferentByteCode);
    // Check a different byte code list, i.e., additional dependencies.
    auto descriptorWithDifferentByteCodeList =
        JavaUdfDescriptorBuilder{}.setByteCodeList({{descriptor->getClassName(), {1}}, {differentClassName, {1}}}).build();
    EXPECT_FALSE(*descriptor == *descriptorWithDifferentByteCodeList);
    // Check a different input type.
    auto descriptorWithDifferentInputType = JavaUdfDescriptorBuilder{}.setInputClassName("different_input_type").build();
    // Check a different return type.
    auto descriptorWithDifferentOutputType = JavaUdfDescriptorBuilder{}.setOutputClassName("different_output_type").build();
    EXPECT_FALSE(*descriptor == *descriptorWithDifferentOutputType);
    // The output schema is ignored because it can be derived from the method signature.
    // We trust the Java client to do this correctly; it would cause errors otherwise during query execution.
}

}// namespace NES::Catalogs::UDF