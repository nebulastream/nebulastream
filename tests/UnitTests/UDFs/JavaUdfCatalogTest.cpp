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

#include "UDFs/JavaUdfCatalog.hpp"
#include "UDFs/UdfException.hpp"
#include "Util/Logger.hpp"

using namespace NES;

class JavaUdfCatalogTest : public testing::Test {
  protected:
    static void SetUpTestCase() {
        NES::setupLogging("UdfTest.log", NES::LOG_DEBUG);
    }

    static std::shared_ptr<JavaUdfImplementation> createImplementation() {
        auto fq_name = "some_package.my_udf"s;
        auto method_name = "udf_method"s;
        auto instance = JavaSerializedInstance { 1 }; // byte-array containing 1 byte
        auto byte_code_list = JavaUdfByteCodeList { {"some_package.my_udf"s, JavaByteCode{1} } };
        return std::make_shared<JavaUdfImplementation>(fq_name, method_name, instance, byte_code_list);
    }

  protected:
    JavaUdfCatalog udf_catalog_{};
};

// RegisterJavaUdf

TEST_F(JavaUdfCatalogTest, RetrieveRegisteredUdfImplementation)
{
    // given
    auto udf_name = "my_udf"s;
    auto udf_implementation = createImplementation();
    // when
    udf_catalog_.registerJavaUdf(udf_name, udf_implementation);
    // then
    ASSERT_EQ(udf_implementation, udf_catalog_.getUdfImplementation(udf_name));
}

TEST_F(JavaUdfCatalogTest, RegisteredImplementationMustNotBeNull) {
    EXPECT_THROW(udf_catalog_.registerJavaUdf("my_udf", nullptr), UdfException);
}

TEST_F(JavaUdfCatalogTest, CannotRegisterUdfUnderExistingName) {
    // given
    auto udf_name = "my_udf"s;
    auto udf_implementation_1 = createImplementation();
    udf_catalog_.registerJavaUdf(udf_name, udf_implementation_1);
    // then
    auto udf_implementation_2 = createImplementation();
    EXPECT_THROW(udf_catalog_.registerJavaUdf(udf_name, udf_implementation_2), UdfException);
}

// GetUdfImplementation

TEST_F(JavaUdfCatalogTest, ReturnNullptrIfUdfIsNotKnown) {
    ASSERT_EQ(udf_catalog_.getUdfImplementation("unknown_udf"), nullptr);
}

// RemoveUdf

TEST_F(JavaUdfCatalogTest, CannotRemoveUnknownUdf) {
    ASSERT_EQ(udf_catalog_.removeUdf("unknown_udf"), false);
}

TEST_F(JavaUdfCatalogTest, SignalRemovalOfUdf) {
    // given
    auto udf_name = "my_udf"s;
    auto udf_implementation = createImplementation();
    udf_catalog_.registerJavaUdf(udf_name, udf_implementation);
    // then
    ASSERT_EQ(udf_catalog_.removeUdf(udf_name), true);
}

TEST_F(JavaUdfCatalogTest, AfterRemovalTheUdfDoesNotExist) {
    // given
    auto udf_name = "my_udf"s;
    auto udf_implementation = createImplementation();
    udf_catalog_.registerJavaUdf(udf_name, udf_implementation);
    udf_catalog_.removeUdf(udf_name);
    // then
    ASSERT_EQ(udf_catalog_.getUdfImplementation(udf_name), nullptr);
}

TEST_F(JavaUdfCatalogTest, AfterRemovalUdfWithSameNameCanBeAddedAgain) {
    // given
    auto udf_name = "my_udf"s;
    auto udf_implementation_1 = createImplementation();
    udf_catalog_.registerJavaUdf(udf_name, udf_implementation_1);
    udf_catalog_.removeUdf(udf_name);
    // then
    auto udf_implementation_2 = createImplementation();
    EXPECT_NO_THROW(udf_catalog_.registerJavaUdf(udf_name, udf_implementation_2));
}

// RemoveUdf

TEST_F(JavaUdfCatalogTest, ReturnListOfKnownUds) {
    // given
    auto udf_name = "my_udf"s;
    auto udf_implementation_1 = createImplementation();
    udf_catalog_.registerJavaUdf(udf_name, udf_implementation_1);
    // then
    auto udfs = udf_catalog_.listUdfs();
    // In gmock, we could use ASSERT_EQ(udfs, ContainerEq({ udf_name }));
    ASSERT_EQ(udfs.size(), 1U);
    ASSERT_EQ(udfs.front(), udf_name);
}