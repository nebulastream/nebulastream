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

#ifndef NES_INCLUDE_CATALOGS_UDF_JAVAUDFDESCRIPTOR_HPP_
#define NES_INCLUDE_CATALOGS_UDF_JAVAUDFDESCRIPTOR_HPP_

#include <Catalogs/UDF/UdfDescriptor.hpp>
#include <API/Schema.hpp>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace NES::Catalogs::UDF {

// Utility types
using JavaSerializedInstance = std::vector<char>;
using JavaByteCode = std::vector<char>;
using JavaUdfByteCodeList = std::unordered_map<std::string, JavaByteCode>;

class JavaUdfDescriptor;
using JavaUdfDescriptorPtr = std::shared_ptr<JavaUdfDescriptor>;

/**
 * @brief Container for all the data required to execute a Java UDF inside an embedded JVM.
 */
class JavaUdfDescriptor : public UdfDescriptor {
  public:
    /**
     * @brief Construct a container of the data required to execute a Java UDF inside an embedded JVM.
     * @param className The fully-qualified class name of the UDF implementing the UDF.
     * @param methodName The method name of the UDF function.
     * @param serializedInstance A serialized instance of the UDF class which stores captured free variables.
     * @param byteCodeList A list of fully-qualified class names and their bytecode required to execute the UDF.
     * @param outputSchema The schema after the application of the UDF to an input stream. The output schema must
     *                     correspond to the structure of the POJO returned by the UDF.
     * @throws UdfException If className is empty or methodName is empty.
     * @throws UdfException If byteCodeList does not contain an entry for getClassName.
     * @throws UdfException If serializedInstance or any of the bytecode entries in byteCodeList are a 0-size byte array.
     * @throws UdfException If outputSchema is empty.
     */
    JavaUdfDescriptor(const std::string& className,
                      const std::string& methodName,
                      const JavaSerializedInstance& serializedInstance,
                      const JavaUdfByteCodeList& byteCodeList,
                      const SchemaPtr outputSchema);

    /**
     * @brief Factory method to create a JavaUdfDescriptorPtr instance.
     * @param className The fully-qualified class name of the UDF implementing the UDF.
     * @param methodName The method name of the UDF function.
     * @param serializedInstance A serialized instance of the UDF class which stores captured free variables.
     * @param byteCodeList A list of fully-qualified class names and their bytecode required to execute the UDF.
     * @param outputSchema The schema after the application of the UDF to an input stream. The output schema must
     *                     correspond to the structure of the POJO returned by the UDF.
     * @throws UdfException If className is empty or methodName is empty.
     * @throws UdfException If byteCodeList does not contain an entry for getClassName.
     * @throws UdfException If serializedInstance or any of the bytecode entries in byteCodeList are a 0-size byte array.
     * @throws UdfException If outputSchema is empty.
     * @return A std::shared_ptr pointing to the newly constructed Java UDF descriptor.
     */
    static JavaUdfDescriptorPtr create(const std::string& className,
                                       const std::string& methodName,
                                       const JavaSerializedInstance& serializedInstance,
                                       const JavaUdfByteCodeList& byteCodeList,
                                       const SchemaPtr outputSchema) {
        return std::make_shared<JavaUdfDescriptor>(className, methodName, serializedInstance, byteCodeList, outputSchema);
    }

    /**
     * @brief Return the fully-qualified class name of the class implementing the UDF.
     * @return Fully-qualified class name of the class implementing the UDF.
     */
    [[nodiscard]] const std::string& getClassName() const { return className; }

    /**
     * @brief Return the serialized instance of the UDF class which stores captured free variables.
     * @return Serialized instance of the UDF class.
     */
    [[nodiscard]] const JavaSerializedInstance& getSerializedInstance() const { return serializedInstance; }

    /**
     * @brief Return the list of classes and their bytecode required to execute the UDF.
     * @return A map containing class names as keys and their bytecode as values.
     */
    [[nodiscard]] const JavaUdfByteCodeList& getByteCodeList() const { return byteCodeList; }

    // TODO
    [[nodiscard]] const SchemaPtr& getOutputSchema() const { return outputSchema; }

  private:
    const std::string className;
    const JavaSerializedInstance serializedInstance;
    const JavaUdfByteCodeList byteCodeList;
    const SchemaPtr outputSchema;
};

}// namespace NES::Catalogs::UDF
#endif// NES_INCLUDE_CATALOGS_UDF_JAVAUDFDESCRIPTOR_HPP_
