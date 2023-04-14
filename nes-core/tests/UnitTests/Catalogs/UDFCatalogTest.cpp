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

#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Exceptions/UDFException.hpp>
#include <Util/JavaUDFDescriptorBuilder.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Catalogs::UDF {

class UDFCatalogTest : public Testing::NESBaseTest {
  protected:
    static void SetUpTestCase() { NES::Logger::setupLogging("UdfTest.log", NES::LogLevel::LOG_DEBUG); }

    static PythonUdfDescriptorPtr createPythonDescriptor() {
        auto methodName = "python_udf_method"s;
        int numberOfArgs = 2;
        DataTypePtr returnType = DataTypeFactory::createInt32();
        return PythonUDFDescriptor::create(methodName, numberOfArgs, returnType);
    }

  protected:
    UDFCatalog udfCatalog{};
};

// Test behavior of RegisterJavaUdf

TEST_F(UDFCatalogTest, RetrieveRegisteredJavaUdfDescriptor) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor = JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor();
    // when
    udfCatalog.registerUDF(udfName, udfDescriptor);
    // then
    ASSERT_EQ(udfDescriptor, UDFDescriptor::as<JavaUDFDescriptor>(udfCatalog.getUDFDescriptor(udfName)));
}

/**
 * @brief test behaviour of registerPythonUdf
 */
TEST_F(UDFCatalogTest, RetrieveRegisteredPythonUdfDescriptor) {
    auto udfName = "py_udf"s;
    auto udfDescriptor = createPythonDescriptor();
    udfCatalog.registerUDF(udfName, udfDescriptor);
    ASSERT_EQ(udfDescriptor, udfCatalog.getUDFDescriptor(udfName));
}

TEST_F(UDFCatalogTest, RegisteredDescriptorMustNotBeNull) {
    EXPECT_THROW(udfCatalog.registerUDF("my_udf", nullptr), UDFException);
}

TEST_F(UDFCatalogTest, CannotRegisterUdfUnderExistingName) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor1 = JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor();
    udfCatalog.registerUDF(udfName, udfDescriptor1);
    // then
    auto udfDescriptor2 = JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor();
    EXPECT_THROW(udfCatalog.registerUDF(udfName, udfDescriptor2), UDFException);
}

// Test behavior of GetUdfDescriptor

TEST_F(UDFCatalogTest, ReturnNullptrIfUdfIsNotKnown) { ASSERT_EQ(udfCatalog.getUDFDescriptor("unknown_udf"), nullptr); }

// Test behavior of RemoveUdf

TEST_F(UDFCatalogTest, CannotRemoveUnknownUdf) { ASSERT_EQ(udfCatalog.removeUDF("unknown_udf"), false); }

TEST_F(UDFCatalogTest, SignalRemovalOfUdf) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor = JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor();
    udfCatalog.registerUDF(udfName, udfDescriptor);
    // then
    ASSERT_EQ(udfCatalog.removeUDF(udfName), true);
}

TEST_F(UDFCatalogTest, AfterRemovalTheUdfDoesNotExist) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor = JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor();
    udfCatalog.registerUDF(udfName, udfDescriptor);
    udfCatalog.removeUDF(udfName);
    // then
    ASSERT_EQ(udfCatalog.getUDFDescriptor(udfName), nullptr);
}

TEST_F(UDFCatalogTest, AfterRemovalUdfWithSameNameCanBeAddedAgain) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor1 = JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor();
    udfCatalog.registerUDF(udfName, udfDescriptor1);
    udfCatalog.removeUDF(udfName);
    // then
    auto udfDescriptor2 = JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor();
    EXPECT_NO_THROW(udfCatalog.registerUDF(udfName, udfDescriptor2));
}

// Test behavior of RemoveUdf

TEST_F(UDFCatalogTest, ReturnListOfKnownUds) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor1 = JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor();
    udfCatalog.registerUDF(udfName, udfDescriptor1);
    // then
    auto udfs = udfCatalog.listUDFs();
    // In gmock, we could use ASSERT_EQ(udfs, ContainerEq({ udfName }));
    ASSERT_EQ(udfs.size(), 1U);
    ASSERT_EQ(udfs.front(), udfName);
}

}// namespace NES::Catalogs::UDF
