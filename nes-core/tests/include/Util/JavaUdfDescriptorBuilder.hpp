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

#ifndef NES_JAVAUDFDESCRIPTORBUILDER_H
#define NES_JAVAUDFDESCRIPTORBUILDER_H

#include <string>

using namespace std::string_literals;

#include <Catalogs/UDF/JavaUdfDescriptor.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES::Catalogs::UDF {

/**
 * Utility class to create default and non-default Java UDF descriptors for testing.
 *
 * The class JavaUdfDescriptor performs a number of checks in its constructor.
 * Creating the required inputs everytime a test needs a Java UDF descriptor leads to code repetition.
 */
class JavaUdfDescriptorBuilder {
  public:
    /**
     * Create a default Java UDF descriptor that can be used in tests.
     * @return A Java UDF descriptor instance.
     */
    static JavaUdfDescriptorPtr createDefaultJavaUdfDescriptor() {
        auto className = "some_package.my_udf"s;
        auto methodName = "udf_method"s;
        auto instance = JavaSerializedInstance{1};// byte-array containing 1 byte
        auto byteCodeList = JavaUdfByteCodeList{{"some_package.my_udf"s, JavaByteCode{1}}};
        auto outputSchema = std::make_shared<Schema>()->addField("attribute", DataTypeFactory::createUInt64());
        return JavaUdfDescriptor::create(className, methodName, instance, byteCodeList, outputSchema);
    }
};

}// namespace NES::Catalogs::UDF

#endif//NES_JAVAUDFDESCRIPTORBUILDER_H
