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

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

namespace NES {

// Utility types
using JavaSerializedInstance = std::vector<char>;
using JavaByteCode = std::vector<char>;
using JavaUdfByteCodeList = std::unordered_map<std::string, JavaByteCode>;

class JavaUdfImplementation {
  public:
    // TODO Signature could be improved with move semantics?
    JavaUdfImplementation(const std::string& fq_name,
                          const std::string& method_name,
                          const JavaSerializedInstance& serialized_instance,
                          const JavaUdfByteCodeList& byte_code_list);

    const std::string& fqName() const { return fq_name_; }
    const std::string& methodName() const { return method_name_; }
    const JavaSerializedInstance& serializedInstance() const { return serialized_instance_; }
    // When the UDF is loaded into a JVM, the entire list of bytecode is simply injected into a JVM.
    // We never need to look up the bytecode of an individual class.
    // So we don't need to return the underlying storage of the class bytecode and an iterator is enough.
    JavaUdfByteCodeList::const_iterator begin() const { return byte_code_list_.begin(); }
    JavaUdfByteCodeList::const_iterator end() const { return byte_code_list_.end(); }

  private:
    const std::string fq_name_;
    const std::string method_name_;
    const JavaSerializedInstance serialized_instance_;
    const JavaUdfByteCodeList byte_code_list_;
};

// This must be a shared pointer.
// The UDF implementation is owned by the UDF catalog, but it is passed to a query operator during query rewrite.
// If the UDF is removed from the catalog while a query that uses the UDF is still active in the system,
// the pointer to the UDF must stay valid.
using JavaUdfImplementationPtr = std::shared_ptr<JavaUdfImplementation>;

}