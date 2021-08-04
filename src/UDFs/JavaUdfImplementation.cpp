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

JavaUdfImplementation::JavaUdfImplementation(const std::string& fq_name,
                                             const std::string& method_name,
                                             const JavaSerializedInstance& serialized_instance,
                                             const JavaUdfByteCodeList& byte_code_list)
    : fq_name_(fq_name),
      method_name_(method_name),
      serialized_instance_(serialized_instance),
      byte_code_list_(byte_code_list)
{
    if (fq_name.empty()) {
        throw UdfException("The fully-qualified name of a Java UDF must not be empty");
    }
    if (method_name.empty()) {
        throw UdfException("The method name of a Java UDF must not be empty");
    }
    if (serialized_instance.empty()) {
        throw UdfException("The serialized instance of a Java UDF must not be empty");
    }
    // This check is implied by the check that the FQ name of the UDF is contained.
    // We keep it here for clarity of the error message.
    if (byte_code_list.empty()) {
        throw UdfException("The bytecode list of classes implementing the UDF must not be empty");
    }
    if (byte_code_list.find(fq_name) == byte_code_list.end()) {
        throw UdfException("The bytecode list of classes implementing the UDF must contain the fully-qualified name of the UDF");
    }
    for (const auto& [_, value] : byte_code_list) {
        if (value.empty()) {
            throw UdfException("The bytecode of a class must not not be empty");
        }
    }
}
