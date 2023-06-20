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

#include <Catalogs/UDF/JavaUDFDescriptor.hpp>
#include <Exceptions/UDFException.hpp>

namespace NES::Catalogs::UDF {

JavaUDFDescriptor::JavaUDFDescriptor(const std::string& className,
                                     const std::string& methodName,
                                     const JavaSerializedInstance& serializedInstance,
                                     const JavaUDFByteCodeList& byteCodeList,
                                     const SchemaPtr inputSchema,
                                     const SchemaPtr outputSchema,
                                     const std::string& inputClassName,
                                     const std::string& outputClassName)
    : UDFDescriptor(methodName), className(className), serializedInstance(serializedInstance), byteCodeList(byteCodeList),
      inputSchema(inputSchema), outputSchema(outputSchema), inputClassName(inputClassName), outputClassName(outputClassName) {
    if (className.empty()) {
        throw UDFException("The class name of a Java UDF must not be empty");
    }
    if (methodName.empty()) {
        throw UDFException("The method name of a Java UDF must not be empty");
    }
    if (inputClassName.empty()) {
        throw UDFException("The class name of the UDF method input type must not be empty.");
    }
    if (outputClassName.empty()) {
        throw UDFException("The class name of the UDF method return type must not be empty.");
    }
    // This check is implied by the check that the class name of the UDF is contained.
    // We keep it here for clarity of the error message.
    if (byteCodeList.empty()) {
        throw UDFException("The bytecode list of classes implementing the UDF must not be empty");
    }
    auto classByteCode = std::find_if(byteCodeList.cbegin(), byteCodeList.cend(), [&](const JavaClassDefinition& c) {
        return c.first == className;
    });
    if (classByteCode == byteCodeList.end()) {
        throw UDFException("The bytecode list of classes implementing the UDF must contain the fully-qualified name of the UDF");
        // We could also check whether the input and output types are contained in the bytecode list.
        // But then we would have to distinguish between custom types (i.e., newly defined POJOs) and existing Java types.
        // This does not seem to be worth the effort here, because if a custom type is missing, the JVM will through an exception
        // when deserializing the UDF instance.
    }
    for (const auto& [_, value] : byteCodeList) {
        if (value.empty()) {
            throw UDFException("The bytecode of a class must not not be empty");
        }
    }
    if (outputSchema->empty()) {
        throw UDFException("The output schema of a Java UDF must not be empty");
    }
    // We allow the input schema to be empty for now so that we don't have to serialize it in the Java client. A possible
    // improvement would be to serialize it in the Java client and compare it to the output schema of the parent operator.
}

bool JavaUDFDescriptor::operator==(const JavaUDFDescriptor& other) const {
    return className == other.className && getMethodName() == other.getMethodName()
        && inputSchema->equals(other.inputSchema, true) && outputSchema->equals(other.outputSchema, true)
        && serializedInstance == other.serializedInstance && byteCodeList == other.byteCodeList
        && inputClassName == other.inputClassName && outputClassName == other.outputClassName;
}

void JavaUDFDescriptor::setInputSchema(const SchemaPtr& inputSchema) { JavaUDFDescriptor::inputSchema = inputSchema; }

}// namespace NES::Catalogs::UDF