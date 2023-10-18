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

#include <Operators/Serialization/SchemaSerializationUtil.hpp>
#include <Operators/Serialization/UDFSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

void UDFSerializationUtil::serializeJavaUDFDescriptor(const Catalogs::UDF::UDFDescriptorPtr& udfDescriptor,
                                                      JavaUdfDescriptorMessage& JavaUdfDescriptorMessage) {
    auto javaUDFDescriptor = udfDescriptor->as<Catalogs::UDF::JavaUDFDescriptor>(udfDescriptor);
    // Serialize UDF class name and method name.
    JavaUdfDescriptorMessage.set_udf_class_name(javaUDFDescriptor->getClassName());
    JavaUdfDescriptorMessage.set_udf_method_name(javaUDFDescriptor->getMethodName());
    // Serialize UDF instance.
    JavaUdfDescriptorMessage.set_serialized_instance(javaUDFDescriptor->getSerializedInstance().data(),
                                                     javaUDFDescriptor->getSerializedInstance().size());
    // Serialize bytecode of dependent classes.
    for (const auto& [className, byteCode] : javaUDFDescriptor->getByteCodeList()) {
        auto* javaClass = JavaUdfDescriptorMessage.add_classes();
        javaClass->set_class_name(className);
        javaClass->set_byte_code(byteCode.data(), byteCode.size());
    }
    // Serialize the input and output schema.
    SchemaSerializationUtil::serializeSchema(javaUDFDescriptor->getInputSchema(), JavaUdfDescriptorMessage.mutable_inputschema());
    SchemaSerializationUtil::serializeSchema(javaUDFDescriptor->getOutputSchema(),
                                             JavaUdfDescriptorMessage.mutable_outputschema());
    // Serialize the input and output class names.
    JavaUdfDescriptorMessage.set_input_class_name(javaUDFDescriptor->getInputClassName());
    JavaUdfDescriptorMessage.set_output_class_name(javaUDFDescriptor->getOutputClassName());
}

Catalogs::UDF::JavaUDFDescriptorPtr
UDFSerializationUtil::deserializeJavaUDFDescriptor(const JavaUdfDescriptorMessage& JavaUdfDescriptorMessage) {
    // C++ represents the bytes type of serialized_instance and byte_code as std::strings
    // which have to be converted to typed byte arrays.
    auto serializedInstance = jni::JavaSerializedInstance{JavaUdfDescriptorMessage.serialized_instance().begin(),
                                                          JavaUdfDescriptorMessage.serialized_instance().end()};
    auto javaUdfByteCodeList = jni::JavaUDFByteCodeList{};
    javaUdfByteCodeList.reserve(JavaUdfDescriptorMessage.classes().size());
    for (const auto& classDefinition : JavaUdfDescriptorMessage.classes()) {
        NES_DEBUG("Deserialized Java UDF class: {}", classDefinition.class_name());
        javaUdfByteCodeList.emplace_back(
            classDefinition.class_name(),
            jni::JavaByteCode{classDefinition.byte_code().begin(), classDefinition.byte_code().end()});
    }
    // Deserialize the input and output schema.
    auto inputSchema = SchemaSerializationUtil::deserializeSchema(JavaUdfDescriptorMessage.inputschema());
    auto outputSchema = SchemaSerializationUtil::deserializeSchema(JavaUdfDescriptorMessage.outputschema());
    // Create Java UDF descriptor.
    return Catalogs::UDF::JavaUDFDescriptor::create(JavaUdfDescriptorMessage.udf_class_name(),
                                                    JavaUdfDescriptorMessage.udf_method_name(),
                                                    serializedInstance,
                                                    javaUdfByteCodeList,
                                                    inputSchema,
                                                    outputSchema,
                                                    JavaUdfDescriptorMessage.input_class_name(),
                                                    JavaUdfDescriptorMessage.output_class_name());
}

}// namespace NES
