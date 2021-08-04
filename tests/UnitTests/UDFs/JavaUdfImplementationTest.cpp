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

    const std::string fq_name_ {"some_package.fq_name"};
    const std::string method_name_ { "udf_method" };
    const JavaSerializedInstance serialized_instance_ {1}; // byte-array containing 1 byte
    const JavaUdfByteCodeList byte_code_list_ { {"some_package.fq_name"s, JavaByteCode{1} } };
};

TEST_F(JavaUdfImplementationTest, TheFullyQualifiedNameMustNotBeEmpty) {
    EXPECT_THROW(JavaUdfImplementation(""s, method_name_, serialized_instance_, byte_code_list_), UdfException);
}

TEST_F(JavaUdfImplementationTest, TheMethodNameMustNotBeEmtpy) {
    EXPECT_THROW(JavaUdfImplementation(fq_name_, ""s, serialized_instance_, byte_code_list_), UdfException);
}

TEST_F(JavaUdfImplementationTest, TheInstanceMustNotBeEmpty) {
    // when
    auto empty_instance = JavaSerializedInstance {}; // empty byte array
    // then
    EXPECT_THROW(JavaUdfImplementation(fq_name_, method_name_, empty_instance, byte_code_list_), UdfException);
}

TEST_F(JavaUdfImplementationTest, TheListOfByteCodeDefinitionsMustNotBeEmpty) {
    // when
    auto empty_byte_code_list = JavaUdfByteCodeList {}; // empty list
    // then
    EXPECT_THROW(JavaUdfImplementation(fq_name_, method_name_, serialized_instance_, empty_byte_code_list), UdfException);
}

TEST_F(JavaUdfImplementationTest, TheListOfByteCodeDefinitionsMustContainTheFullyQualifiedNameOfTheUdfClass) {
    // when
    auto fq_name = "some_other_package.some_other_fq_name"s;
    auto byte_code_list = JavaUdfByteCodeList { {"some_package.fq_name"s, JavaByteCode{1} } };
    // then
    EXPECT_THROW(JavaUdfImplementation(fq_name, method_name_, serialized_instance_, byte_code_list), UdfException);
}

TEST_F(JavaUdfImplementationTest, TheListOfByteCodeDefinitionsMustNotContainEmptyByteCode) {
    // when
    auto byte_code_list_with_empty_byte_code = JavaUdfByteCodeList { {fq_name_, JavaByteCode {} } }; // empty byte array
    // then
    EXPECT_THROW(JavaUdfImplementation(fq_name_, method_name_, serialized_instance_, byte_code_list_with_empty_byte_code), UdfException);
}