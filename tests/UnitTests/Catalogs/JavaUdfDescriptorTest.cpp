/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <gtest/gtest.h>

using namespace std::string_literals;

#include <Catalogs/JavaUdfDescriptor.hpp>
#include <Exceptions/UdfException.hpp>
#include <Util/Logger.hpp>

namespace NES {

class JavaUdfDescriptorTest : public testing::Test {
  protected:
    static void SetUpTestCase() {
        NES::setupLogging("UdfTest.log", NES::LOG_DEBUG);
    }

    const std::string className{"some_package.class_name"};
    const std::string methodName{ "udf_method" };
    const JavaSerializedInstance serializedInstance{1}; // byte-array containing 1 byte
    const JavaUdfByteCodeList byteCodeList{ {"some_package.class_name"s, JavaByteCode{1} } };
};

TEST_F(JavaUdfDescriptorTest, TheFullyQualifiedNameMustNotBeEmpty) {
    EXPECT_THROW(JavaUdfDescriptor(""s, methodName, serializedInstance, byteCodeList), UdfException);
}

TEST_F(JavaUdfDescriptorTest, TheMethodNameMustNotBeEmtpy) {
    EXPECT_THROW(JavaUdfDescriptor(className, ""s, serializedInstance, byteCodeList), UdfException);
}

TEST_F(JavaUdfDescriptorTest, TheInstanceMustNotBeEmpty) {
    // when
    auto emptyInstance = JavaSerializedInstance {}; // empty byte array
    // then
    EXPECT_THROW(JavaUdfDescriptor(className, methodName, emptyInstance, byteCodeList), UdfException);
}

TEST_F(JavaUdfDescriptorTest, TheListOfByteCodeDefinitionsMustNotBeEmpty) {
    // when
    auto emptyByteCodeList = JavaUdfByteCodeList {}; // empty list
    // then
    EXPECT_THROW(JavaUdfDescriptor(className, methodName, serializedInstance, emptyByteCodeList), UdfException);
}

TEST_F(JavaUdfDescriptorTest, TheListOfByteCodeDefinitionsMustContainTheFullyQualifiedNameOfTheUdfClass) {
    // when
    auto unknownClassName = "some_other_package.some_other_class_name"s;
    auto byteCodeList = JavaUdfByteCodeList { {"some_package.unknown_class_name"s, JavaByteCode{1} } };
    // then
    EXPECT_THROW(JavaUdfDescriptor(unknownClassName, methodName, serializedInstance, byteCodeList), UdfException);
}

TEST_F(JavaUdfDescriptorTest, TheListOfByteCodeDefinitionsMustNotContainEmptyByteCode) {
    // when
    auto byteCodeListWithEmptyByteCode = JavaUdfByteCodeList { {className, JavaByteCode {} } }; // empty byte array
    // then
    EXPECT_THROW(JavaUdfDescriptor(className, methodName, serializedInstance, byteCodeListWithEmptyByteCode), UdfException);
}

} // namespace NES