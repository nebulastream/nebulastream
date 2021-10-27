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

#ifndef NES_INCLUDE_QUERY_COMPILER_GENERATABLE_TYPES_GENERATABLE_TYPES_FACTORY_HPP_
#define NES_INCLUDE_QUERY_COMPILER_GENERATABLE_TYPES_GENERATABLE_TYPES_FACTORY_HPP_
#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>
#include <memory>
namespace NES {
namespace QueryCompilation {

/**
 * @brief The compiler type factory creates generatable data types, which are required during query compilation.
 */
class GeneratableTypesFactory {
  public:
    /**
     * @brief Create a annonymus data type, which is used to represent types of the Runtime system that are not covered by the nes type system.
     * @param type
     * @return GeneratableDataTypePtr
     */
    static GeneratableDataTypePtr createAnonymusDataType(const std::string& type);

    /**
    * @brief Create a generatable data type, which corresponds to a particular nes data type.
    * @param type nes data type
    * @return GeneratableDataTypePtr
    */
    GeneratableDataTypePtr createDataType(const DataTypePtr& type);

    /**
    * @brief Create a user defined type, which corresponds to a struct declaration. This is used to represent the input and output of a pipeline.
    * @param structDeclaration the struct declaration.
    * @return GeneratableDataTypePtr
    */
    static GeneratableDataTypePtr createUserDefinedType(const StructDeclaration& structDeclaration);

    /**
     * @brief Create a reference from a GeneratableDataType
     * @param type GeneratableDataTypePtr
     * @return GeneratableDataTypePtr
     */
    static GeneratableDataTypePtr createReference(const GeneratableDataTypePtr& type);

    /**
    * @brief Create a pointer from a GeneratableDataType
    * @param type GeneratableDataTypePtr
    * @return GeneratableDataTypePtr
    */
    static GeneratableDataTypePtr createPointer(const GeneratableDataTypePtr& type);

    /**
    * @brief Create a value type from a GeneratableDataType
    * @param type GeneratableDataTypePtr
    * @return GeneratableDataTypePtr
    */
    static GeneratableValueTypePtr createValueType(const ValueTypePtr& valueType);
};
}// namespace QueryCompilation
}// namespace NES

#endif  // NES_INCLUDE_QUERY_COMPILER_GENERATABLE_TYPES_GENERATABLE_TYPES_FACTORY_HPP_
