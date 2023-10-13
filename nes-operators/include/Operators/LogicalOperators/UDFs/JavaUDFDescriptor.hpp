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

#ifndef NES_CORE_INCLUDE_CATALOGS_UDF_JAVAUDFDESCRIPTOR_HPP_
#define NES_CORE_INCLUDE_CATALOGS_UDF_JAVAUDFDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/UDFs/UDFDescriptor.hpp>
#include <memory>
#include <string>
#include <vector>

namespace NES::jni {
// Utility types
using JavaSerializedInstance = std::vector<char>;
using JavaByteCode = std::vector<char>;
// We use a vector for JavaUDFByteCodeList because we need to insert the classes into the JVM in a well-defined order
// that is provided by the Java client.
using JavaClassDefinition = std::pair<std::string, JavaByteCode>;
using JavaUDFByteCodeList = std::vector<JavaClassDefinition>;
}// namespace NES::jni

namespace NES::Catalogs::UDF {

class JavaUDFDescriptor;
using JavaUDFDescriptorPtr = std::shared_ptr<JavaUDFDescriptor>;

/**
 * @brief Container for all the data required to execute a Java UDF inside an embedded JVM.
 */
class JavaUDFDescriptor : public UDFDescriptor {
  public:
    /**
     * @brief Construct a container of the data required to execute a Java UDF inside an embedded JVM.
     * @param className The fully-qualified class name of the UDF implementing the UDF.
     * @param methodName The method name of the UDF function.
     * @param serializedInstance A serialized instance of the UDF class which stores captured free variables.
     * @param byteCodeList A list of fully-qualified class names and their bytecode required to execute the UDF.
     * @param inputSchema The schema of the input stream on which the UDF is applied. The input schema must
     *                    correspond to the structure of the POJO which is passed to the UDF. Can be empty or nullptr.
     * @param outputSchema The schema after the application of the UDF to an input stream. The output schema must
     *                     correspond to the structure of the POJO returned by the UDF.
     * @param inputClassName The fully-qualified class name of the input type of the UDF method.
     * @param outputClassName The fully-qualified class name of the return type of the UDF method.
     * @throws UdfException If className is empty or methodName is empty.
     * @throws UdfException If byteCodeList does not contain an entry for getClassName.
     * @throws UdfException If serializedInstance or any of the bytecode entries in byteCodeList are a 0-size byte array.
     * @throws UdfException If outputSchema is empty.
     */
    JavaUDFDescriptor(const std::string& className,
                      const std::string& methodName,
                      const jni::JavaSerializedInstance& serializedInstance,
                      const jni::JavaUDFByteCodeList& byteCodeList,
                      const SchemaPtr inputSchema,
                      const SchemaPtr outputSchema,
                      const std::string& inputClassName,
                      const std::string& outputClassName);

    /**
     * @brief Factory method to create a JavaUdfDescriptorPtr instance.
     * @param className The fully-qualified class name of the UDF implementing the UDF.
     * @param methodName The method name of the UDF function.
     * @param serializedInstance A serialized instance of the UDF class which stores captured free variables.
     * @param byteCodeList A list of fully-qualified class names and their bytecode required to execute the UDF.
     * @param inputSchema The schema of the input stream on which the UDF is applied. The input schema must
     *                    correspond to the structure of the POJO which is passed to the UDF. Can be empty or nullptr.
     * @param outputSchema The schema after the application of the UDF to an input stream. The output schema must
     *                     correspond to the structure of the POJO returned by the UDF.
     * @param inputClassName The fully-qualified class name of the input type of the UDF method.
     * @param outputClassName The fully-qualified class name of the return type of the UDF method.
     * @throws UdfException If className is empty or methodName is empty.
     * @throws UdfException If byteCodeList does not contain an entry for getClassName.
     * @throws UdfException If serializedInstance or any of the bytecode entries in byteCodeList are a 0-size byte array.
     * @throws UdfException If outputSchema is empty.
     * @return A std::shared_ptr pointing to the newly constructed Java UDF descriptor.
     */
    static JavaUDFDescriptorPtr create(const std::string& className,
                                       const std::string& methodName,
                                       const jni::JavaSerializedInstance& serializedInstance,
                                       const jni::JavaUDFByteCodeList& byteCodeList,
                                       const SchemaPtr inputSchema,
                                       const SchemaPtr outputSchema,
                                       const std::string& inputClassName,
                                       const std::string& outputClassName) {
        return std::make_shared<JavaUDFDescriptor>(className,
                                                   methodName,
                                                   serializedInstance,
                                                   byteCodeList,
                                                   inputSchema,
                                                   outputSchema,
                                                   inputClassName,
                                                   outputClassName);
    }

    /**
     * @brief Return the fully-qualified class name of the class implementing the UDF.
     * @return Fully-qualified class name of the class implementing the UDF.
     */
    const std::string& getClassName() const { return className; }

    /**
     * @brief Return the serialized instance of the UDF class which stores captured free variables.
     * @return Serialized instance of the UDF class.
     */
    const jni::JavaSerializedInstance& getSerializedInstance() const { return serializedInstance; }

    /**
     * @brief Return the list of classes and their bytecode required to execute the UDF.
     * @return A map containing class names as keys and their bytecode as values.
     */
    const jni::JavaUDFByteCodeList& getByteCodeList() const { return byteCodeList; }

    /**
     * @brief Return the fully-qualified class name of the input type of the UDF method.
     * @return Fully-qualified class name of the input type of the UDF method.
     */
    const std::string& getInputClassName() const { return inputClassName; }

    /**
     * @brief Return the fully-qualified class name of the return type of the UDF method.
     * @return Fully-qualified class name of the return type of the UDF method.
     */
    const std::string& getOutputClassName() const { return outputClassName; }

    /**
     * @brief Generates the infer string signature required for the logical operator
     * @return the infer string signature stream
     */
    std::stringstream generateInferStringSignature() override;

    /**
     * Compare to Java UDF descriptors.
     *
     * @param other The other JavaUDFDescriptor in the comparison.
     * @return True, if both JavaUdfDescriptors are the same, i.e., same UDF class and method name,
     * same serialized instance (state), and same byte code list; False, otherwise.
     */
    bool operator==(const JavaUDFDescriptor& other) const;

  private:
    const std::string className;
    const jni::JavaSerializedInstance serializedInstance;
    const jni::JavaUDFByteCodeList byteCodeList;
    const std::string inputClassName;
    const std::string outputClassName;
};

}// namespace NES::Catalogs::UDF
#endif// NES_CORE_INCLUDE_CATALOGS_UDF_JAVAUDFDESCRIPTOR_HPP_
