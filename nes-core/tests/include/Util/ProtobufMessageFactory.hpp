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

#ifndef NES_TESTS_UTIL_PROTOBUF_MESSAGE_FACTORY_HPP_
#define NES_TESTS_UTIL_PROTOBUF_MESSAGE_FACTORY_HPP_

#include <API/AttributeField.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <UdfCatalogService.pb.h>
#include <Util/JavaUDFDescriptorBuilder.hpp>
#include <Util/PythonUDFDescriptorBuilder.hpp>

namespace NES {

/**
 * @brief Factory for Protobuf messages used in test code.
 */
class ProtobufMessageFactory {
  public:
    /**
     * @brief Construct a RegisterUDFRequest protobuf message.
     * @see UDFCatalog::registerJavaUdf
     */
    static RegisterUDFRequest createRegisterJavaUdfRequest(const std::string& udfName,
                                                               const std::string& udfClassName,
                                                               const std::string& methodName,
                                                               const jni::JavaSerializedInstance& serializedInstance,
                                                               const jni::JavaUDFByteCodeList& byteCodeList,
                                                               const SchemaPtr& outputSchema,
                                                               const std::string& inputClassName,
                                                               const std::string& outputClassName) {
        auto request = RegisterUDFRequest{};
        // Set udfName
        request.set_udfname(udfName);
        auto* udfDescriptor = request.mutable_udfdescriptor();

        // Set udfClassName, methodName, and serializedInstance
        auto javaUDFDescriptor = JavaUdfDescriptorMessage{};
        javaUDFDescriptor.set_udf_class_name(udfClassName);
        javaUDFDescriptor.set_udf_method_name(methodName);
        javaUDFDescriptor.set_serialized_instance(serializedInstance.data(), serializedInstance.size());
        // Set byteCodeList
        for (const auto& [className, byteCode] : byteCodeList) {
            auto* javaClass = javaUDFDescriptor.add_classes();
            javaClass->set_class_name(className);
            javaClass->set_byte_code(std::string{byteCode.begin(), byteCode.end()});
        }
        // Set outputSchema
        SchemaSerializationUtil::serializeSchema(outputSchema, javaUDFDescriptor.mutable_outputschema());
        // Set input and output types
        javaUDFDescriptor.set_input_class_name(inputClassName);
        javaUDFDescriptor.set_output_class_name(outputClassName);

        udfDescriptor->mutable_descriptormessage()->PackFrom(javaUDFDescriptor);
        return request;
    }

  public:
    /**
     * @brief Construct a RegisterJavaUdfRequest protobuf message.
     * @see UDFCatalog::registerJavaUdf
     */
    static RegisterUDFRequest createRegisterPythonUdfRequest(const std::string& udfName,
                                                             const std::string& functionName,
                                                             const std::string& functionString,
                                                             const SchemaPtr& outputSchema) {
        auto request = RegisterUDFRequest{};
        // Set udfName
        request.set_udfname(udfName);
        auto* udfDescriptor = request.mutable_udfdescriptor();

        // Set udfClassName, methodName, and serializedInstance
        auto pythonDescriptor = PythonUDFDescriptorMessage{};
        pythonDescriptor.set_udf_method_name(functionName);
        pythonDescriptor.set_function_string(functionString);
        // Set outputSchema
        SchemaSerializationUtil::serializeSchema(outputSchema, pythonDescriptor.mutable_outputschema());

        udfDescriptor->mutable_descriptormessage()->PackFrom(pythonDescriptor);
        return request;
    }

    static RegisterUDFRequest createDefaultRegisterJavaUdfRequest() {
        auto javaUdfDescriptor = Catalogs::UDF::JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor();
        return createRegisterJavaUdfRequest("my_udf",
                                            javaUdfDescriptor->getClassName(),
                                            javaUdfDescriptor->getMethodName(),
                                            javaUdfDescriptor->getSerializedInstance(),
                                            javaUdfDescriptor->getByteCodeList(),
                                            javaUdfDescriptor->getOutputSchema(),
                                            javaUdfDescriptor->getInputClassName(),
                                            javaUdfDescriptor->getOutputClassName());
    }

    static RegisterUDFRequest createDefaultRegisterPythonUdfRequest() {
        auto pythonDescriptor = Catalogs::UDF::PythonUDFDescriptorBuilder::createDefaultPythonUDFDescriptor();
        return createRegisterPythonUdfRequest("my_udf",
                                              pythonDescriptor->getMethodName(),
                                              pythonDescriptor->getFunctionString(),
                                              pythonDescriptor->getOutputSchema());
    }
};

}// namespace NES
#endif// NES_TESTS_UTIL_PROTOBUF_MESSAGE_FACTORY_HPP_
