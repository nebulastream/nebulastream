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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Operators/LogicalOperators/UDFs/JavaUDFDescriptor.hpp>
#include <string>
using namespace std::string_literals;

namespace NES::Catalogs::UDF {

/**
 * Utility class to create default and non-default Java UDF descriptors for testing.
 *
 * The class JavaUDFDescriptor performs a number of checks in its constructor.
 * Creating the required inputs everytime a test needs a Java UDF descriptor leads to code repetition.
 */
class JavaUDFDescriptorBuilder {
  public:
    /**
     * Create a new builder for a JavaUDFDescriptor with valid default values for the fields required by the JavaUDFDescriptor.
     */
    JavaUDFDescriptorBuilder() = default;

    /**
     * @return A Java UDF descriptor with the fields either set to default values or with explicitly specified values in setters.
     */
    JavaUDFDescriptorPtr build() const;

    /**
     * Load the bytecode for each class specified in the byte code list.
     * @return The JavaUDFDescriptorBuilder.
     */
    JavaUDFDescriptorBuilder& loadByteCodeFrom(std::string_view classFilePath);

    /**
     * Set the class name of the Java UDF descriptor.
     * @param newClassName The class name of the Java UDF descriptor.
     * @return The JavaUDFDescriptorBuilder instance.
     */
    JavaUDFDescriptorBuilder& setClassName(std::string_view newClassName);

    /**
     * Set the method name of the Java UDF descriptor.
     * @param newMethodName The method name of the Java UDF descriptor.
     * @return The JavaUDFDescriptorBuilder instance.
     */
    JavaUDFDescriptorBuilder& setMethodName(std::string_view newMethodName);

    /**
     * Set the serialized Java instance of the Java UDF descriptor.
     * @param newInstance The serialized Java instance of the Java UDF descriptor.
     * @return The JavaUDFDescriptorBuilder instance.
     */
    JavaUDFDescriptorBuilder& setInstance(const jni::JavaSerializedInstance& newInstance);

    /**
     * Set the bytecode list of the Java UDF descriptor.
     * @param newByteCodeList The bytecode list of the Java UDF descriptor.
     * @return The JavaUDFDescriptorBuilder instance.
     */
    JavaUDFDescriptorBuilder& setByteCodeList(const jni::JavaUDFByteCodeList& newByteCodeList);

    /**
     * Set the input schema of the Java UDF descriptor.
     * @param newInputSchema The input schema of the Java UDF descriptor.
     * @return The JavaUDFDescriptorBuilder instance.
     */
    JavaUDFDescriptorBuilder& setInputSchema(const SchemaPtr& newInputSchema) {
        this->inputSchema = newInputSchema;
        return *this;
    }

    /**
     * Set the output schema of the Java UDF descriptor.
     * @param newOutputSchema The output schema of the Java UDF descriptor.
     * @return The JavaUDFDescriptorBuilder instance.
     */
    JavaUDFDescriptorBuilder& setOutputSchema(const SchemaPtr& newOutputSchema);

    /**
     * Set the class name of the input type of the UDF method.
     * @param newInputClassName The class name of the input type of the UDF method.
     * @return The JavaUDFDescriptorBuilder instance.
     */
    JavaUDFDescriptorBuilder& setInputClassName(std::string_view newInputClassName);

    /**
     * Set the class name of the return type of the UDF method.
     * @param newOutputClassName The class name of the return type of the UDF method.
     * @return The JavaUDFDescriptorBuilder instance.
     */
    JavaUDFDescriptorBuilder& setOutputClassName(std::string_view newOutputClassName);

    /**
     * Create a default Java UDF descriptor that can be used in tests.
     * @return A Java UDF descriptor instance.
     */
    static JavaUDFDescriptorPtr createDefaultJavaUDFDescriptor();

  private:
    std::string className = "some_package.my_udf";
    std::string methodName = "udf_method";
    jni::JavaSerializedInstance instance = jni::JavaSerializedInstance{1};// byte-array containing 1 byte
    jni::JavaUDFByteCodeList byteCodeList = jni::JavaUDFByteCodeList{{"some_package.my_udf"s, jni::JavaByteCode{1}}};
    SchemaPtr inputSchema = std::make_shared<Schema>()->addField("inputAttribute", DataTypeFactory::createUInt64());
    SchemaPtr outputSchema = std::make_shared<Schema>()->addField("outputAttribute", DataTypeFactory::createUInt64());
    std::string inputClassName = "some_package.my_input_type";
    std::string outputClassName = "some_package.my_output_type";
};

}// namespace NES::Catalogs::UDF

#endif//NES_JAVAUDFDESCRIPTORBUILDER_H
