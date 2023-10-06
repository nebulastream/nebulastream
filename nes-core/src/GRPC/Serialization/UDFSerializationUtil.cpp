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
#include <GRPC/Serialization/UDFSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

void UDFSerializationUtil::serializeUDFDescriptor(const Catalogs::UDF::UDFDescriptorPtr& udfDescriptor,
                                                      UDFDescriptorMessage& udfDescriptorMessage) {
    if (udfDescriptor->instanceOf<Catalogs::UDF::JavaUDFDescriptor>()) {
        auto javaUDFDescriptor = Catalogs::UDF::UDFDescriptor::as<Catalogs::UDF::JavaUDFDescriptor>(udfDescriptor);
        JavaUdfDescriptorMessage javaUDFDescriptorMessage;
        // Serialize UDF class name and method name.
        javaUDFDescriptorMessage.set_udf_class_name(javaUDFDescriptor->getClassName());
        javaUDFDescriptorMessage.set_udf_method_name(javaUDFDescriptor->getMethodName());
        // Serialize UDF instance.
        javaUDFDescriptorMessage.set_serialized_instance(javaUDFDescriptor->getSerializedInstance().data(),
                                                         javaUDFDescriptor->getSerializedInstance().size());
        // Serialize bytecode of dependent classes.
        for (const auto& [className, byteCode] : javaUDFDescriptor->getByteCodeList()) {
            auto* javaClass = javaUDFDescriptorMessage.add_classes();
            javaClass->set_class_name(className);
            javaClass->set_byte_code(byteCode.data(), byteCode.size());
        }
        // Serialize the input and output schema.
        SchemaSerializationUtil::serializeSchema(javaUDFDescriptor->getInputSchema(), javaUDFDescriptorMessage.mutable_inputschema());
        SchemaSerializationUtil::serializeSchema(javaUDFDescriptor->getOutputSchema(),
                                                 javaUDFDescriptorMessage.mutable_outputschema());
        // Serialize the input and output class names.
        javaUDFDescriptorMessage.set_input_class_name(javaUDFDescriptor->getInputClassName());
        javaUDFDescriptorMessage.set_output_class_name(javaUDFDescriptor->getOutputClassName());
        udfDescriptorMessage.mutable_descriptormessage()->PackFrom(javaUDFDescriptorMessage);
    } else if (udfDescriptor->instanceOf<Catalogs::UDF::PythonUDFDescriptor>()) {
        auto pythonUDFDescriptor = Catalogs::UDF::UDFDescriptor::as<Catalogs::UDF::PythonUDFDescriptor>(udfDescriptor);
        PythonUDFDescriptorMessage pythonUDFDescriptorMessage;
        // Serialize UDF name and the string of the python function
        pythonUDFDescriptorMessage.set_udf_method_name(pythonUDFDescriptor->getMethodName());
        pythonUDFDescriptorMessage.set_function_string(pythonUDFDescriptor->getFunctionString());
        auto modulesToImport = pythonUDFDescriptorMessage.mutable_modules_to_import();
        modulesToImport->insert(pythonUDFDescriptor->getModulesToImport().begin(), pythonUDFDescriptor->getModulesToImport().end());
        // Serialize the input and output schema.
        SchemaSerializationUtil::serializeSchema(pythonUDFDescriptor->getInputSchema(), pythonUDFDescriptorMessage.mutable_inputschema());
        SchemaSerializationUtil::serializeSchema(pythonUDFDescriptor->getOutputSchema(), pythonUDFDescriptorMessage.mutable_outputschema());
        udfDescriptorMessage.mutable_descriptormessage()->PackFrom(pythonUDFDescriptorMessage);
    }
}

Catalogs::UDF::UDFDescriptorPtr
UDFSerializationUtil::deserializeUDFDescriptor(const UDFDescriptorMessage& udfDescriptorMessage) {
    JavaUdfDescriptorMessage javaUdfDescriptorMessage;
    PythonUDFDescriptorMessage pythonUdfDescriptorMessage;
    if (udfDescriptorMessage.descriptormessage().UnpackTo(&javaUdfDescriptorMessage)) {
        // successfully unpacked to JavaUdfDescriptorMessage
        // C++ represents the bytes type of serialized_instance and byte_code as std::strings
        // which have to be converted to typed byte arrays.
        auto serializedInstance = jni::JavaSerializedInstance{javaUdfDescriptorMessage.serialized_instance().begin(),
                                                              javaUdfDescriptorMessage.serialized_instance().end()};
        auto javaUdfByteCodeList = jni::JavaUDFByteCodeList{};
        javaUdfByteCodeList.reserve(javaUdfDescriptorMessage.classes().size());
        for (const auto& classDefinition : javaUdfDescriptorMessage.classes()) {
            NES_DEBUG("Deserialized Java UDF class: {}", classDefinition.class_name());
            javaUdfByteCodeList.emplace_back(
                classDefinition.class_name(),
                jni::JavaByteCode{classDefinition.byte_code().begin(), classDefinition.byte_code().end()});
        }
        // Deserialize the input and output schema.
        auto inputSchema = SchemaSerializationUtil::deserializeSchema(javaUdfDescriptorMessage.inputschema());
        auto outputSchema = SchemaSerializationUtil::deserializeSchema(javaUdfDescriptorMessage.outputschema());
        // Create Java UDF descriptor.
        return Catalogs::UDF::JavaUDFDescriptor::create(javaUdfDescriptorMessage.udf_class_name(),
                                                        javaUdfDescriptorMessage.udf_method_name(),
                                                        serializedInstance,
                                                        javaUdfByteCodeList,
                                                        inputSchema,
                                                        outputSchema,
                                                        javaUdfDescriptorMessage.input_class_name(),
                                                        javaUdfDescriptorMessage.output_class_name());

    } else if (udfDescriptorMessage.descriptormessage().UnpackTo(&pythonUdfDescriptorMessage)) {
        // Deserialize the input and output schema.
        auto inputSchema = SchemaSerializationUtil::deserializeSchema(pythonUdfDescriptorMessage.inputschema());
        auto outputSchema = SchemaSerializationUtil::deserializeSchema(pythonUdfDescriptorMessage.outputschema());
        const std::map<std::string, std::string> modulesToImport(pythonUdfDescriptorMessage.modules_to_import().begin(),
                                                           pythonUdfDescriptorMessage.modules_to_import().end());
        // Create Python UDF Descriptor
        return Catalogs::UDF::PythonUDFDescriptor::create(pythonUdfDescriptorMessage.udf_method_name(),
                                                          pythonUdfDescriptorMessage.function_string(),
                                                          modulesToImport,
                                                          inputSchema,
                                                          outputSchema);
    } else {
        NES_THROW_RUNTIME_ERROR("Could not parse the UDFDescriptorMessage. NES does not support this type of UDF Descriptor.");
    }
}

}// namespace NES
