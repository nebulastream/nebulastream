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

#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Exceptions/UdfException.hpp>
#include <Util/JavaUdfDescriptorBuilder.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Catalogs::UDF {

class UdfCatalogTest : public Testing::NESBaseTest {
  protected:
    static void SetUpTestCase() { NES::Logger::setupLogging("UdfTest.log", NES::LogLevel::LOG_DEBUG); }

    static PythonUdfDescriptorPtr createPythonDescriptor() {
        auto methodName = "python_udf_method"s;
        int numberOfArgs = 2;
        DataTypePtr returnType = DataTypeFactory::createInt32();
        return PythonUdfDescriptor::create(methodName, numberOfArgs, returnType);
    }

  protected:
    UdfCatalog udfCatalog{};
};

// Test behavior of RegisterJavaUdf

TEST_F(UdfCatalogTest, RetrieveRegisteredJavaUdfDescriptor) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor = JavaUdfDescriptorBuilder::createDefaultJavaUdfDescriptor();
    // when
    udfCatalog.registerUdf(udfName, udfDescriptor);
    // then
    ASSERT_EQ(udfDescriptor, UdfDescriptor::as<JavaUdfDescriptor>(udfCatalog.getUdfDescriptor(udfName)));
}

/**
 * @brief test behaviour of registerPythonUdf
 */
TEST_F(UdfCatalogTest, RetrieveRegisteredPythonUdfDescriptor) {
    auto udfName = "py_udf"s;
    auto udfDescriptor = createPythonDescriptor();
    udfCatalog.registerUdf(udfName, udfDescriptor);
    ASSERT_EQ(udfDescriptor, udfCatalog.getUdfDescriptor(udfName));
}

TEST_F(UdfCatalogTest, RegisteredDescriptorMustNotBeNull) {
    EXPECT_THROW(udfCatalog.registerUdf("my_udf", nullptr), UdfException);
}

TEST_F(UdfCatalogTest, CannotRegisterUdfUnderExistingName) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor1 = JavaUdfDescriptorBuilder::createDefaultJavaUdfDescriptor();
    udfCatalog.registerUdf(udfName, udfDescriptor1);
    // then
    auto udfDescriptor2 = JavaUdfDescriptorBuilder::createDefaultJavaUdfDescriptor();
    EXPECT_THROW(udfCatalog.registerUdf(udfName, udfDescriptor2), UdfException);
}

// Test behavior of GetUdfDescriptor

TEST_F(UdfCatalogTest, ReturnNullptrIfUdfIsNotKnown) { ASSERT_EQ(udfCatalog.getUdfDescriptor("unknown_udf"), nullptr); }

// Test behavior of RemoveUdf

TEST_F(UdfCatalogTest, CannotRemoveUnknownUdf) { ASSERT_EQ(udfCatalog.removeUdf("unknown_udf"), false); }

TEST_F(UdfCatalogTest, SignalRemovalOfUdf) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor = JavaUdfDescriptorBuilder::createDefaultJavaUdfDescriptor();
    udfCatalog.registerUdf(udfName, udfDescriptor);
    // then
    ASSERT_EQ(udfCatalog.removeUdf(udfName), true);
}

TEST_F(UdfCatalogTest, AfterRemovalTheUdfDoesNotExist) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor = JavaUdfDescriptorBuilder::createDefaultJavaUdfDescriptor();
    udfCatalog.registerUdf(udfName, udfDescriptor);
    udfCatalog.removeUdf(udfName);
    // then
    ASSERT_EQ(udfCatalog.getUdfDescriptor(udfName), nullptr);
}

TEST_F(UdfCatalogTest, AfterRemovalUdfWithSameNameCanBeAddedAgain) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor1 = JavaUdfDescriptorBuilder::createDefaultJavaUdfDescriptor();
    udfCatalog.registerUdf(udfName, udfDescriptor1);
    udfCatalog.removeUdf(udfName);
    // then
    auto udfDescriptor2 = JavaUdfDescriptorBuilder::createDefaultJavaUdfDescriptor();
    EXPECT_NO_THROW(udfCatalog.registerUdf(udfName, udfDescriptor2));
}

// Test behavior of RemoveUdf

TEST_F(UdfCatalogTest, ReturnListOfKnownUds) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor1 = JavaUdfDescriptorBuilder::createDefaultJavaUdfDescriptor();
    udfCatalog.registerUdf(udfName, udfDescriptor1);
    // then
    auto udfs = udfCatalog.listUdfs();
    // In gmock, we could use ASSERT_EQ(udfs, ContainerEq({ udfName }));
    ASSERT_EQ(udfs.size(), 1U);
    ASSERT_EQ(udfs.front(), udfName);
}

}// namespace NES::Catalogs::UDF