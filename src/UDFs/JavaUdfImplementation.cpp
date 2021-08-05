/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <UDFs/JavaUdfImplementation.hpp>
#include <UDFs/UdfException.hpp>

using namespace NES;

JavaUdfImplementation::JavaUdfImplementation(const std::string& fqName,
                                             const std::string& methodName,
                                             const JavaSerializedInstance& serializedInstance,
                                             const JavaUdfByteCodeList& byteCodeList)
    : fqName(fqName), methodName(methodName), serializedInstance(serializedInstance), byteCodeList(byteCodeList)
{
    if (fqName.empty()) {
        throw UdfException("The fully-qualified name of a Java UDF must not be empty");
    }
    if (methodName.empty()) {
        throw UdfException("The method name of a Java UDF must not be empty");
    }
    if (serializedInstance.empty()) {
        throw UdfException("The serialized instance of a Java UDF must not be empty");
    }
    // This check is implied by the check that the FQ name of the UDF is contained.
    // We keep it here for clarity of the error message.
    if (byteCodeList.empty()) {
        throw UdfException("The bytecode list of classes implementing the UDF must not be empty");
    }
    if (byteCodeList.find(fqName) == byteCodeList.end()) {
        throw UdfException("The bytecode list of classes implementing the UDF must contain the fully-qualified name of the UDF");
    }
    for (const auto& [_, value] : byteCodeList) {
        if (value.empty()) {
            throw UdfException("The bytecode of a class must not not be empty");
        }
    }
}
