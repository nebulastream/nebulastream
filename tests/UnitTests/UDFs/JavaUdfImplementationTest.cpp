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

#include "UDFs/JavaUdfImplementation.hpp"
#include "UDFs/UdfException.hpp"
#include "Util/Logger.hpp"

using namespace NES;

class JavaUdfImplementationTest : public testing::Test {
  protected:
    static void SetUpTestCase() {
        NES::setupLogging("UdfTest.log", NES::LOG_DEBUG);
    }

    const std::string fqName{"some_package.fq_name"};
    const std::string methodName{ "udf_method" };
    const JavaSerializedInstance serializedInstance{1}; // byte-array containing 1 byte
    const JavaUdfByteCodeList byteCodeList{ {"some_package.fq_name"s, JavaByteCode{1} } };
};

TEST_F(JavaUdfImplementationTest, TheFullyQualifiedNameMustNotBeEmpty) {
    EXPECT_THROW(JavaUdfImplementation(""s, methodName, serializedInstance, byteCodeList), UdfException);
}

TEST_F(JavaUdfImplementationTest, TheMethodNameMustNotBeEmtpy) {
    EXPECT_THROW(JavaUdfImplementation(fqName, ""s, serializedInstance, byteCodeList), UdfException);
}

TEST_F(JavaUdfImplementationTest, TheInstanceMustNotBeEmpty) {
    // when
    auto emptyInstance = JavaSerializedInstance {}; // empty byte array
    // then
    EXPECT_THROW(JavaUdfImplementation(fqName, methodName, emptyInstance, byteCodeList), UdfException);
}

TEST_F(JavaUdfImplementationTest, TheListOfByteCodeDefinitionsMustNotBeEmpty) {
    // when
    auto emptyByteCodeList = JavaUdfByteCodeList {}; // empty list
    // then
    EXPECT_THROW(JavaUdfImplementation(fqName, methodName, serializedInstance, emptyByteCodeList), UdfException);
}

TEST_F(JavaUdfImplementationTest, TheListOfByteCodeDefinitionsMustContainTheFullyQualifiedNameOfTheUdfClass) {
    // when
    auto unknownFqName = "some_other_package.some_other_fq_name"s;
    auto byteCodeList = JavaUdfByteCodeList { {"some_package.unknownFqName"s, JavaByteCode{1} } };
    // then
    EXPECT_THROW(JavaUdfImplementation(unknownFqName, methodName, serializedInstance, byteCodeList), UdfException);
}

TEST_F(JavaUdfImplementationTest, TheListOfByteCodeDefinitionsMustNotContainEmptyByteCode) {
    // when
    auto byteCodeListWithEmptyByteCode = JavaUdfByteCodeList { {fqName, JavaByteCode {} } }; // empty byte array
    // then
    EXPECT_THROW(JavaUdfImplementation(fqName, methodName, serializedInstance, byteCodeListWithEmptyByteCode), UdfException);
}