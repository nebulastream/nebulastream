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

#include <UDFs/JavaUdfCatalog.hpp>
#include <UDFs/UdfException.hpp>
#include <Util/Logger.hpp>

using namespace NES;

class JavaUdfCatalogTest : public testing::Test {
  protected:
    static void SetUpTestCase() {
        NES::setupLogging("UdfTest.log", NES::LOG_DEBUG);
    }

    static std::shared_ptr<JavaUdfDescriptor> createDescriptor() {
        auto fqName = "some_package.my_udf"s;
        auto methodName = "udf_method"s;
        auto instance = JavaSerializedInstance { 1 }; // byte-array containing 1 byte
        auto byteCodeList = JavaUdfByteCodeList { {"some_package.my_udf"s, JavaByteCode{1} } };
        return std::make_shared<JavaUdfDescriptor>(fqName, methodName, instance, byteCodeList);
    }

  protected:
    JavaUdfCatalog udfCatalog{};
};

// RegisterJavaUdf

TEST_F(JavaUdfCatalogTest, RetrieveRegisteredUdfDescriptor)
{
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor = createDescriptor();
    // when
    udfCatalog.registerJavaUdf(udfName, udfDescriptor);
    // then
    ASSERT_EQ(udfDescriptor, udfCatalog.getUdfDescriptor(udfName));
}

TEST_F(JavaUdfCatalogTest, RegisteredDescriptorMustNotBeNull) {
    EXPECT_THROW(udfCatalog.registerJavaUdf("my_udf", nullptr), UdfException);
}

TEST_F(JavaUdfCatalogTest, CannotRegisterUdfUnderExistingName) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor1 = createDescriptor();
    udfCatalog.registerJavaUdf(udfName, udfDescriptor1);
    // then
    auto udfDescriptor2 = createDescriptor();
    EXPECT_THROW(udfCatalog.registerJavaUdf(udfName, udfDescriptor2), UdfException);
}

// GetUdfDescriptor

TEST_F(JavaUdfCatalogTest, ReturnNullptrIfUdfIsNotKnown) {
    ASSERT_EQ(udfCatalog.getUdfDescriptor("unknown_udf"), nullptr);
}

// RemoveUdf

TEST_F(JavaUdfCatalogTest, CannotRemoveUnknownUdf) {
    ASSERT_EQ(udfCatalog.removeUdf("unknown_udf"), false);
}

TEST_F(JavaUdfCatalogTest, SignalRemovalOfUdf) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor = createDescriptor();
    udfCatalog.registerJavaUdf(udfName, udfDescriptor);
    // then
    ASSERT_EQ(udfCatalog.removeUdf(udfName), true);
}

TEST_F(JavaUdfCatalogTest, AfterRemovalTheUdfDoesNotExist) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor = createDescriptor();
    udfCatalog.registerJavaUdf(udfName, udfDescriptor);
    udfCatalog.removeUdf(udfName);
    // then
    ASSERT_EQ(udfCatalog.getUdfDescriptor(udfName), nullptr);
}

TEST_F(JavaUdfCatalogTest, AfterRemovalUdfWithSameNameCanBeAddedAgain) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor1 = createDescriptor();
    udfCatalog.registerJavaUdf(udfName, udfDescriptor1);
    udfCatalog.removeUdf(udfName);
    // then
    auto udfDescriptor2 = createDescriptor();
    EXPECT_NO_THROW(udfCatalog.registerJavaUdf(udfName, udfDescriptor2));
}

// RemoveUdf

TEST_F(JavaUdfCatalogTest, ReturnListOfKnownUds) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor1 = createDescriptor();
    udfCatalog.registerJavaUdf(udfName, udfDescriptor1);
    // then
    auto udfs = udfCatalog.listUdfs();
    // In gmock, we could use ASSERT_EQ(udfs, ContainerEq({ udfName }));
    ASSERT_EQ(udfs.size(), 1U);
    ASSERT_EQ(udfs.front(), udfName);
}