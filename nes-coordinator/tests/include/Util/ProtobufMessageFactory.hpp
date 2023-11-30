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

#ifndef NES_COORDINATOR_TESTS_INCLUDE_UTIL_PROTOBUFMESSAGEFACTORY_HPP_
#define NES_COORDINATOR_TESTS_INCLUDE_UTIL_PROTOBUFMESSAGEFACTORY_HPP_

#include <API/AttributeField.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Operators/Serialization/SchemaSerializationUtil.hpp>
#include <UdfCatalogService.pb.h>
#include <Util/JavaUDFDescriptorBuilder.hpp>

namespace NES {

/**
 * @brief Factory for Protobuf messages used in test code.
 */
class ProtobufMessageFactory {
  public:
    /**
     * @brief Construct a RegisterJavaUdfRequest protobuf message.
     * @see UDFCatalog::registerJavaUdf
     */
    static RegisterJavaUdfRequest createRegisterJavaUdfRequest(const std::string& udfName,
                                                               const std::string& udfClassName,
                                                               const std::string& methodName,
                                                               const jni::JavaSerializedInstance& serializedInstance,
                                                               const jni::JavaUDFByteCodeList& byteCodeList,
                                                               const SchemaPtr& outputSchema,
                                                               const std::string& inputClassName,
                                                               const std::string& outputClassName) {
        auto request = RegisterJavaUdfRequest{};
        // Set udfName
        request.set_udf_name(udfName);
        auto* udfDescriptor = request.mutable_java_udf_descriptor();
        // Set udfClassName, methodName, and serializedInstance
        udfDescriptor->set_udf_class_name(udfClassName);
        udfDescriptor->set_udf_method_name(methodName);
        udfDescriptor->set_serialized_instance(serializedInstance.data(), serializedInstance.size());
        // Set byteCodeList
        for (const auto& [className, byteCode] : byteCodeList) {
            auto* javaClass = udfDescriptor->add_classes();
            javaClass->set_class_name(className);
            javaClass->set_byte_code(std::string{byteCode.begin(), byteCode.end()});
        }
        // Set outputSchema
        SchemaSerializationUtil::serializeSchema(outputSchema, udfDescriptor->mutable_outputschema());
        // Set input and output types
        udfDescriptor->set_input_class_name(inputClassName);
        udfDescriptor->set_output_class_name(outputClassName);
        return request;
    }

    static RegisterJavaUdfRequest createDefaultRegisterJavaUdfRequest() {
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
};

}// namespace NES
#endif// NES_COORDINATOR_TESTS_INCLUDE_UTIL_PROTOBUFMESSAGEFACTORY_HPP_
