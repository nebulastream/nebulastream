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

#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <GRPC/Serialization/UdfSerializationUtil.hpp>

namespace NES {

void UdfSerializationUtil::serializeJavaUdfDescriptor(const Catalogs::UDF::JavaUdfDescriptor& javaUdfDescriptor,
                                                      JavaUdfDescriptorMessage& javaUdfDescriptorMessage) {
    // Serialize UDF class name and method name.
    javaUdfDescriptorMessage.set_udf_class_name(javaUdfDescriptor.getClassName());
    javaUdfDescriptorMessage.set_udf_method_name(javaUdfDescriptor.getMethodName());
    // Serialize UDF instance.
    javaUdfDescriptorMessage.set_serialized_instance(javaUdfDescriptor.getSerializedInstance().data(),
                                                     javaUdfDescriptor.getSerializedInstance().size());
    // Serialize bytecode of dependent classes.
    for (const auto& [className, byteCode] : javaUdfDescriptor.getByteCodeList()) {
        auto* javaClass = javaUdfDescriptorMessage.add_classes();
        javaClass->set_class_name(className);
        javaClass->set_byte_code(byteCode.data(), byteCode.size());
    }
    // Serialize schema.
    SchemaSerializationUtil::serializeSchema(javaUdfDescriptor.getOutputSchema(),
                                             javaUdfDescriptorMessage.mutable_outputschema());
    // Serialize the input and output class names.
    javaUdfDescriptorMessage.set_input_class_name(javaUdfDescriptor.getInputClassName());
    javaUdfDescriptorMessage.set_output_class_name(javaUdfDescriptor.getOutputClassName());
}

Catalogs::UDF::JavaUdfDescriptorPtr
UdfSerializationUtil::deserializeJavaUdfDescriptor(const JavaUdfDescriptorMessage& javaUdfDescriptorMessage) {
    // C++ represents the bytes type of serialized_instance and byte_code as std::strings
    // which have to be converted to typed byte arrays.
    auto serializedInstance = Catalogs::UDF::JavaSerializedInstance{javaUdfDescriptorMessage.serialized_instance().begin(),
                                                                    javaUdfDescriptorMessage.serialized_instance().end()};
    auto javaUdfByteCodeList = Catalogs::UDF::JavaUdfByteCodeList{};
    javaUdfByteCodeList.reserve(javaUdfDescriptorMessage.classes().size());
    for (const auto& classDefinition : javaUdfDescriptorMessage.classes()) {
        javaUdfByteCodeList.insert(
            {classDefinition.class_name(),
             Catalogs::UDF::JavaByteCode{classDefinition.byte_code().begin(), classDefinition.byte_code().end()}});
    }
    // Deserialize the output schema.
    auto outputSchema = SchemaSerializationUtil::deserializeSchema(javaUdfDescriptorMessage.outputschema());
    // Create Java UDF descriptor.
    return Catalogs::UDF::JavaUdfDescriptor::create(javaUdfDescriptorMessage.udf_class_name(),
                                                    javaUdfDescriptorMessage.udf_method_name(),
                                                    serializedInstance,
                                                    javaUdfByteCodeList,
                                                    outputSchema,
                                                    javaUdfDescriptorMessage.input_class_name(),
                                                    javaUdfDescriptorMessage.output_class_name());
}

}// namespace NES