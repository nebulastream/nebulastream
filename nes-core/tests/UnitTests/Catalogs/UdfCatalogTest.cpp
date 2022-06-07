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
#include <Common/DataTypes/DataTypeFactory.hpp>

using namespace std::string_literals;

#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Exceptions/UdfException.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Catalogs {

class UdfCatalogTest : public Testing::NESBaseTest {
  protected:
    static void SetUpTestCase() { NES::Logger::setupLogging("UdfTest.log", NES::LogLevel::LOG_DEBUG); }

    static JavaUdfDescriptorPtr createJavaDescriptor() {
        auto className = "some_package.my_udf"s;
        auto methodName = "udf_method"s;
        auto instance = JavaSerializedInstance{1};// byte-array containing 1 byte
        auto byteCodeList = JavaUdfByteCodeList{{"some_package.my_udf"s, JavaByteCode{1}}};
        return JavaUdfDescriptor::create(className, methodName, instance, byteCodeList);
    }

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
    auto udfDescriptor = createJavaDescriptor();
    // when
    udfCatalog.registerJavaUdf(udfName, udfDescriptor);
    // then
    ASSERT_EQ(udfDescriptor, udfCatalog.getJavaUdfDescriptor(udfName));
}

/**
 * @brief test behaviour of registerPythonUdf
 */
TEST_F(UdfCatalogTest, RetrieveRegisteredPythonUdfDescriptor) {
    auto udfName = "py_udf"s;
    auto udfDescriptor = createPythonDescriptor();
    udfCatalog.registerPythonUdf(udfName, udfDescriptor);
    ASSERT_EQ(udfDescriptor, udfCatalog.getPythonUdfDescriptor(udfName));
}

TEST_F(UdfCatalogTest, RetrieveCorrectUdfDescriptors) {
    auto pyUdfName = "py_udf"s;
    auto javaUdfName = "java_udf"s;
    auto pyUdfDescriptor = createPythonDescriptor();
    auto javaUdfDescriptor = createJavaDescriptor();
    udfCatalog.registerPythonUdf(pyUdfName, pyUdfDescriptor);
    udfCatalog.registerJavaUdf(javaUdfName, javaUdfDescriptor);

    auto wrongUdfDescriptor = udfCatalog.getPythonUdfDescriptor(javaUdfName);
    ASSERT_EQ(wrongUdfDescriptor, nullptr);
}

TEST_F(UdfCatalogTest, RegisteredDescriptorMustNotBeNull) {
    EXPECT_THROW(udfCatalog.registerJavaUdf("my_udf", nullptr), UdfException);
}

TEST_F(UdfCatalogTest, CannotRegisterUdfUnderExistingName) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor1 = createJavaDescriptor();
    udfCatalog.registerJavaUdf(udfName, udfDescriptor1);
    // then
    auto udfDescriptor2 = createJavaDescriptor();
    EXPECT_THROW(udfCatalog.registerJavaUdf(udfName, udfDescriptor2), UdfException);
}

// Test behavior of GetUdfDescriptor

TEST_F(UdfCatalogTest, ReturnNullptrIfUdfIsNotKnown) { ASSERT_EQ(udfCatalog.getJavaUdfDescriptor("unknown_udf"), nullptr); }

// Test behavior of RemoveUdf

TEST_F(UdfCatalogTest, CannotRemoveUnknownUdf) { ASSERT_EQ(udfCatalog.removeUdf("unknown_udf"), false); }

TEST_F(UdfCatalogTest, SignalRemovalOfUdf) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor = createJavaDescriptor();
    udfCatalog.registerJavaUdf(udfName, udfDescriptor);
    // then
    ASSERT_EQ(udfCatalog.removeUdf(udfName), true);
}

TEST_F(UdfCatalogTest, AfterRemovalTheUdfDoesNotExist) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor = createJavaDescriptor();
    udfCatalog.registerJavaUdf(udfName, udfDescriptor);
    udfCatalog.removeUdf(udfName);
    // then
    ASSERT_EQ(udfCatalog.getJavaUdfDescriptor(udfName), nullptr);
}

TEST_F(UdfCatalogTest, AfterRemovalUdfWithSameNameCanBeAddedAgain) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor1 = createJavaDescriptor();
    udfCatalog.registerJavaUdf(udfName, udfDescriptor1);
    udfCatalog.removeUdf(udfName);
    // then
    auto udfDescriptor2 = createJavaDescriptor();
    EXPECT_NO_THROW(udfCatalog.registerJavaUdf(udfName, udfDescriptor2));
}

// Test behavior of RemoveUdf

TEST_F(UdfCatalogTest, ReturnListOfKnownUds) {
    // given
    auto udfName = "my_udf"s;
    auto udfDescriptor1 = createJavaDescriptor();
    udfCatalog.registerJavaUdf(udfName, udfDescriptor1);
    // then
    auto udfs = udfCatalog.listUdfs();
    // In gmock, we could use ASSERT_EQ(udfs, ContainerEq({ udfName }));
    ASSERT_EQ(udfs.size(), 1U);
    ASSERT_EQ(udfs.front(), udfName);
}

}// namespace NES::Catalogs