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
     * Create a new builder for a JavaUdfDescriptor with valid default values for the fields required by the JavaUdfDescriptor.
     */
    JavaUdfDescriptorBuilder() = default;

    /**
     * @return A Java UDF descriptor with the fields either set to default values or with explicitly specified values in setters.
     */
    JavaUdfDescriptorPtr build() {
        return JavaUdfDescriptor::create(className,
                                         methodName,
                                         instance,
                                         byteCodeList,
                                         outputSchema,
                                         inputClassName,
                                         outputClassName);
    }

    /**
     * Set the class name of the Java UDF descriptor.
     * @param newClassName The class name of the Java UDF descriptor.
     * @return The JavaUdfDescriptorBuilder instance.
     */
    JavaUdfDescriptorBuilder& setClassName(const std::string& newClassName) {
        this->className = newClassName;
        return *this;
    }

    /**
     * Set the method name of the Java UDF descriptor.
     * @param newMethodName The method name of the Java UDF descriptor.
     * @return The JavaUdfDescriptorBuilder instance.
     */
    JavaUdfDescriptorBuilder& setMethodName(const std::string& newMethodName) {
        this->methodName = newMethodName;
        return *this;
    }

    /**
     * Set the serialized Java instance of the Java UDF descriptor.
     * @param newInstance The serialized Java instance of the Java UDF descriptor.
     * @return The JavaUdfDescriptorBuilder instance.
     */
    JavaUdfDescriptorBuilder& setInstance(const JavaSerializedInstance& newInstance) {
        this->instance = newInstance;
        return *this;
    }

    /**
     * Set the bytecode list of the Java UDF descriptor.
     * @param newByteCodeList The bytecode list of the Java UDF descriptor.
     * @return The JavaUdfDescriptorBuilder instance.
     */
    JavaUdfDescriptorBuilder& setByteCodeList(const JavaUdfByteCodeList& newByteCodeList) {
        this->byteCodeList = newByteCodeList;
        return *this;
    }

    /**
     * Set the output schema of the Java UDF descriptor.
     * @param newOutputSchema The output schema of the Java UDF descriptor.
     * @return The JavaUdfDescriptorBuilder instance.
     */
    JavaUdfDescriptorBuilder& setOutputSchema(const SchemaPtr& newOutputSchema) {
        this->outputSchema = newOutputSchema;
        return *this;
    }

    /**
     * Set the class name of the input type of the UDF method.
     * @param newInputClassName The class name of the input type of the UDF method.
     * @return The JavaUdfDescriptorBuilder instance.
     */
    JavaUdfDescriptorBuilder& setInputClassName(const std::string newInputClassName) {
        this->inputClassName = newInputClassName;
        return *this;
    }

    /**
     * Set the class name of the return type of the UDF method.
     * @param newOutputClassName The class name of the return type of the UDF method.
     * @return The JavaUdfDescriptorBuilder instance.
     */
    JavaUdfDescriptorBuilder& setOutputClassName(const std::string newOutputClassName) {
        this->outputClassName = newOutputClassName;
        return *this;
    }

    /**
     * Create a default Java UDF descriptor that can be used in tests.
     * @return A Java UDF descriptor instance.
     */
    static JavaUdfDescriptorPtr createDefaultJavaUdfDescriptor() { return JavaUdfDescriptorBuilder().build(); }

  private:
    std::string className = "some_package.my_udf";
    std::string methodName = "udf_method";
    JavaSerializedInstance instance = JavaSerializedInstance{1};// byte-array containing 1 byte
    JavaUdfByteCodeList byteCodeList = JavaUdfByteCodeList{{"some_package.my_udf"s, JavaByteCode{1}}};
    SchemaPtr outputSchema = std::make_shared<Schema>()->addField("attribute", DataTypeFactory::createUInt64());
    std::string inputClassName = "some_package.my_input_type";
    std::string outputClassName = "some_package.my_output_type";
};

}// namespace NES::Catalogs::UDF

#endif//NES_JAVAUDFDESCRIPTORBUILDER_H
