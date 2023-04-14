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

#include <Catalogs/UDF/PythonUDFDescriptor.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Exceptions/UDFException.hpp>
#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>

using namespace std::string_literals;

namespace NES::Catalogs::UDF {

class PythonUDFDescriptorTest : public Testing::NESBaseTest {
  protected:
    static void SetUpTestCase() { NES::Logger::setupLogging("UdfTest.log", NES::LogLevel::LOG_DEBUG); }

    const std::string methodName{"py_method"};
    int numberOfArgs = 2;
    DataTypePtr undefinedType = DataTypeFactory::createUndefined();
    DataTypePtr returnType = DataTypeFactory::createInt32();
};

TEST_F(PythonUDFDescriptorTest, methodNameMustNotBeEmpty) {
    EXPECT_THROW(PythonUDFDescriptor(""s, numberOfArgs, returnType), UDFException);
}

TEST_F(PythonUDFDescriptorTest, numberOfArgsMustBePositive) {
    EXPECT_THROW(PythonUDFDescriptor(methodName, -1, returnType), UDFException);
}

TEST_F(PythonUDFDescriptorTest, returnTypeMustBeDefined) {
    EXPECT_THROW(PythonUDFDescriptor(methodName, numberOfArgs, undefinedType), UDFException);
}

}// namespace NES::Catalogs::UDF