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

#include <Catalogs/UDF/JavaUdfDescriptor.hpp>
#include <Exceptions/UdfException.hpp>

namespace NES::Catalogs::UDF {

JavaUdfDescriptor::JavaUdfDescriptor(const std::string& className,
                                     const std::string& methodName,
                                     const JavaSerializedInstance& serializedInstance,
                                     const JavaUdfByteCodeList& byteCodeList,
                                     const SchemaPtr outputSchema,
                                     const std::string& inputClassName,
                                     const std::string& outputClassName)
    : UdfDescriptor(methodName), className(className), serializedInstance(serializedInstance), byteCodeList(byteCodeList),
      outputSchema(outputSchema), inputClassName(inputClassName), outputClassName(outputClassName) {
    if (className.empty()) {
        throw UdfException("The class name of a Java UDF must not be empty");
    }
    if (methodName.empty()) {
        throw UdfException("The method name of a Java UDF must not be empty");
    }
    if (serializedInstance.empty()) {
        throw UdfException("The serialized instance of a Java UDF must not be empty");
    }
    if (inputClassName.empty()) {
        throw UdfException("The class name of the UDF method input type must not be empty.");
    }
    if (outputClassName.empty()) {
        throw UdfException("The class name of the UDF method return type must not be empty.");
    }
    // This check is implied by the check that the class name of the UDF is contained.
    // We keep it here for clarity of the error message.
    if (byteCodeList.empty()) {
        throw UdfException("The bytecode list of classes implementing the UDF must not be empty");
    }
    if (byteCodeList.find(className) == byteCodeList.end()) {
        throw UdfException("The bytecode list of classes implementing the UDF must contain the fully-qualified name of the UDF");
        // We could also check whether the input and output types are contained in the bytecode list.
        // But then we would have to distinguish between custom types (i.e., newly defined POJOs) and existing Java types.
        // This does not seem to be worth the effort here, because if a custom type is missing, the JVM will through an exception
        // when deserializing the UDF instance.
    }
    for (const auto& [_, value] : byteCodeList) {
        if (value.empty()) {
            throw UdfException("The bytecode of a class must not not be empty");
        }
    }
    if (outputSchema->empty()) {
        throw UdfException("The output schema of a Java UDF must not be empty");
    }
}

bool JavaUdfDescriptor::operator==(const JavaUdfDescriptor& other) const {
    return className == other.className && getMethodName() == other.getMethodName()
        && serializedInstance == other.serializedInstance && byteCodeList == other.byteCodeList
        && inputClassName == other.inputClassName && outputClassName == other.outputClassName;
}

}// namespace NES::Catalogs::UDF