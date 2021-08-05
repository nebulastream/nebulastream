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

namespace NES::Catalogs {

// Utility types
using JavaSerializedInstance = std::vector<char>;
using JavaByteCode = std::vector<char>;
using JavaUdfByteCodeList = std::unordered_map<std::string, JavaByteCode>;

/**
 * @brief Container for all the data required to execute a Java UDF inside an embedded JVM.
 */
class JavaUdfDescriptor {
  public:
    /**
     * @brief Construct a container of the data required to execute a Java UDF inside an embedded JVM.
     * @param className The fully-qualified class name of the UDF implementing the UDF.
     * @param methodName The method name of the UDF function.
     * @param serializedInstance A serialized instance of the UDF class which stores captured free variables.
     * @param byteCodeList A list of fully-qualified class names and their bytecode required to execute the UDF.
     * @throws UdfException If className is empty or methodName is empty.
     * @throws UdfException If byteCodeList does not contain an entry for getClassName.
     * @throws UdfException If serializedInstance or any of the bytecode entries in byteCodeList are a 0-size byte array.
     */
    // TODO #2079 Signature could be improved with move semantics?
    JavaUdfDescriptor(const std::string& className,
                      const std::string& methodName,
                      const JavaSerializedInstance& serializedInstance,
                      const JavaUdfByteCodeList& byteCodeList);

    /**
     * @brief Return the fully-qualified class name of the class implementing the UDF.
     * @return Fully-qualified class name of the class implementing the UDF.
     */
    const std::string& getClassName() const { return className; }

    /**
     * @brief Return the name of the UDF method.
     * @return The name of the UDF method.
     */
    const std::string& getMethodName() const { return methodName; }

    /**
     * @brief Return the serialized instance of the UDF class which stores captured free variables.
     * @return Serialized instance of the UDF class.
     */
    const JavaSerializedInstance& getSerializedInstance() const { return serializedInstance; }

    /**
     * @brief Return the start of an iterator over the list of classes and bytecode required to execute the UDF.
     * @return Const-iterator to byte_code_list.
     */
    // When the UDF is loaded into a JVM, the entire list of bytecode is simply injected into a JVM.
    // We never need to look up the bytecode of an individual class.
    // So we don't need to return the underlying storage of the class bytecode and an iterator is enough.
    JavaUdfByteCodeList::const_iterator begin() const { return byteCodeList.begin(); }

    /**
     * @brief Return the end of an iterator over the list of classes and bytecode required to execute the UDF.
     * @return Const-iterator to byte_code_list.
     */
    JavaUdfByteCodeList::const_iterator end() const { return byteCodeList.end(); }

  private:
    const std::string className;
    const std::string methodName;
    const JavaSerializedInstance serializedInstance;
    const JavaUdfByteCodeList byteCodeList;
};

// TODO #2079 Change to unique_ptr?
// This is a shared_ptr for now.
// The UDF implementation is owned by the UDF catalog, but it is passed to a query operator during query rewrite.
// If the UDF is removed from the catalog while a query that uses the UDF is still active in the system,
// the pointer to the UDF must stay valid.
// It would be better to have the UDF catalog consume a unique_ptr to signal that it owns the implementation object
// and then return shared_ptrs when the implementation is retrieved.
using JavaUdfDescriptorPtr = std::shared_ptr<JavaUdfDescriptor>;

} // namespace NES::Catalogs