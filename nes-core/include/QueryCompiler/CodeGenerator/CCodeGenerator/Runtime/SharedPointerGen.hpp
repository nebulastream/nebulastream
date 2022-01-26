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
#ifndef NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_RUNTIME_SHAREDPOINTERGEN_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_RUNTIME_SHAREDPOINTERGEN_HPP_
#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>
namespace NES {
namespace QueryCompilation {
/**
 * @brief Utility to generate Runtime calls for shared pointers.
 */
class SharedPointerGen {
  public:
    /**
     * @brief Create a shared pointer type
     * @param type GeneratableDataTypePtr
     * @return GeneratableDataTypePtr
     */
    static GeneratableDataTypePtr createSharedPtrType(const GeneratableDataTypePtr& type);

    /**
     * @brief Creates function call to make shared.
     * @param type GeneratableDataTypePtr
     * @return StatementPtr
     */
    static StatementPtr makeShared(const GeneratableDataTypePtr& type);
};
}// namespace QueryCompilation
}// namespace NES

#endif  // NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_RUNTIME_SHAREDPOINTERGEN_HPP_
