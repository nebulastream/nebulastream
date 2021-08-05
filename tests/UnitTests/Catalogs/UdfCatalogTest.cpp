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

#include <Catalogs/UdfCatalog.hpp>
#include <Exceptions/UdfException.hpp>
#include <Util/Logger.hpp>

namespace NES {

class UdfCatalogTest : public testing::Test {
  protected:
    static void SetUpTestCase() {
        NES::setupLogging("UdfTest.log", NES::LOG_DEBUG);
    }

    static std::shared_ptr<JavaUdfDescriptor> createDescriptor() {
        auto className = "some_package.my_udf"s;
        auto methodName = "udf_method"s;
        auto instance = JavaSerializedInstance { 1 }; // byte-array containing 1 byte
        auto byteCodeList = JavaUdfByteCodeList { {"some_package.my_udf"s, JavaByteCode{1} } };
        return std::make_shared<JavaUdfDescriptor>(className, methodName, instance, byteCodeList);
    }

  protected:
    UdfCatalog udfCatalog{};
};

// RegisterJavaUdf

TEST_F(UdfCatalogTest, RetrieveRegisteredUdfDescriptor)
{
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor = createDescriptor();
    // when
    udfCatalog.registerJavaUdf(udfName, udfDescriptor);
    // then
    ASSERT_EQ(udfDescriptor, udfCatalog.getUdfDescriptor(udfName));
}

TEST_F(UdfCatalogTest, RegisteredDescriptorMustNotBeNull) {
    EXPECT_THROW(udfCatalog.registerJavaUdf("my_udf", nullptr), UdfException);
}

TEST_F(UdfCatalogTest, CannotRegisterUdfUnderExistingName) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor1 = createDescriptor();
    udfCatalog.registerJavaUdf(udfName, udfDescriptor1);
    // then
    auto udfDescriptor2 = createDescriptor();
    EXPECT_THROW(udfCatalog.registerJavaUdf(udfName, udfDescriptor2), UdfException);
}

// GetUdfDescriptor

TEST_F(UdfCatalogTest, ReturnNullptrIfUdfIsNotKnown) {
    ASSERT_EQ(udfCatalog.getUdfDescriptor("unknown_udf"), nullptr);
}

// RemoveUdf

TEST_F(UdfCatalogTest, CannotRemoveUnknownUdf) {
    ASSERT_EQ(udfCatalog.removeUdf("unknown_udf"), false);
}

TEST_F(UdfCatalogTest, SignalRemovalOfUdf) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor = createDescriptor();
    udfCatalog.registerJavaUdf(udfName, udfDescriptor);
    // then
    ASSERT_EQ(udfCatalog.removeUdf(udfName), true);
}

TEST_F(UdfCatalogTest, AfterRemovalTheUdfDoesNotExist) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor = createDescriptor();
    udfCatalog.registerJavaUdf(udfName, udfDescriptor);
    udfCatalog.removeUdf(udfName);
    // then
    ASSERT_EQ(udfCatalog.getUdfDescriptor(udfName), nullptr);
}

TEST_F(UdfCatalogTest, AfterRemovalUdfWithSameNameCanBeAddedAgain) {
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

TEST_F(UdfCatalogTest, ReturnListOfKnownUds) {
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

} // namespace NES