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

#include <Operators/Exceptions/UDFException.hpp>
#include <Operators/LogicalOperators/UDFs/JavaUDFDescriptor.hpp>
#include <algorithm>
#include <numeric>
#include <sstream>

namespace NES::Catalogs::UDF {

JavaUDFDescriptor::JavaUDFDescriptor(const std::string& className,
                                     const std::string& methodName,
                                     const jni::JavaSerializedInstance& serializedInstance,
                                     const jni::JavaUDFByteCodeList& byteCodeList,
                                     const SchemaPtr inputSchema,
                                     const SchemaPtr outputSchema,
                                     const std::string& inputClassName,
                                     const std::string& outputClassName)
    : UDFDescriptor(methodName, inputSchema, outputSchema), className(className), serializedInstance(serializedInstance),
      byteCodeList(byteCodeList), inputClassName(inputClassName), outputClassName(outputClassName) {

    if (className.empty()) {
        throw UDFException("The class name of a Java UDF must not be empty");
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
    if (!getClassByteCode(getClassName()).has_value()) {
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
}

 const std::optional<jni::JavaByteCode> JavaUDFDescriptor::getClassByteCode(const std::string& className) const {
    const auto classByteCode = std::find_if(byteCodeList.cbegin(), byteCodeList.cend(), [&](const jni::JavaClassDefinition& c) {
        return c.first == className;
    });
    return classByteCode == byteCodeList.end() ? std::nullopt : std::optional(classByteCode->second);
}

std::stringstream JavaUDFDescriptor::generateInferStringSignature() {
    // Infer signature for this operator based on the UDF metadata (class name and UDF method), the serialized instance,
    // and the byte code list. We can ignore the schema information because it is determined by the UDF method signature.
    auto signatureStream = std::stringstream{};
    auto elementHash = std::hash<jni::JavaSerializedInstance::value_type>{};

    // Hash the contents of a byte array (i.e., the serialized instance and the byte code of a class)
    // based on the hashes of the individual elements.
    auto charArrayHashHelper = [&elementHash](std::size_t h, char v) {
        return h = h * 31 + elementHash(v);
    };

    // Compute hashed value of the UDF instance.
    auto& instance = getSerializedInstance();
    auto instanceHash = std::accumulate(instance.begin(), instance.end(), instance.size(), charArrayHashHelper);

    // Compute hashed value of the UDF byte code list.
    auto stringHash = std::hash<std::string>{};
    auto& byteCodeList = getByteCodeList();
    auto byteCodeListHash =
        std::accumulate(byteCodeList.begin(),
                        byteCodeList.end(),
                        byteCodeList.size(),
                        [&stringHash, &charArrayHashHelper](std::size_t h, jni::JavaUDFByteCodeList::value_type v) {
                            /* It is not possible to hash unordered_map directly in C++, this will be
                                     * investigated in issue #3584
                                     */

                            auto& className = v.first;
                            h = h * 31 + stringHash(className);
                            auto& byteCode = v.second;
                            h = h * 31 + std::accumulate(byteCode.begin(), byteCode.end(), byteCode.size(), charArrayHashHelper);
                            return h;
                        });

    signatureStream << "JAVA_UDF(" << className << "." << getMethodName() << ", instance=" << instanceHash
                    << ", byteCode=" << byteCodeListHash << ")";

    return signatureStream;
}

bool JavaUDFDescriptor::operator==(const JavaUDFDescriptor& other) const {
    return className == other.className && getMethodName() == other.getMethodName()
        && getInputSchema()->equals(other.getInputSchema(), true) && getOutputSchema()->equals(other.getOutputSchema(), true)
        && serializedInstance == other.serializedInstance && byteCodeList == other.byteCodeList
        && inputClassName == other.inputClassName && outputClassName == other.outputClassName;
}
}// namespace NES::Catalogs::UDF