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
#ifndef NES_INCLUDE_COMPILER_CPPCOMPILER_SHAREDLIBRARY_HPP_
#define NES_INCLUDE_COMPILER_CPPCOMPILER_SHAREDLIBRARY_HPP_
#include <Compiler/DynamicObject.hpp>
#include <memory>
namespace NES::Compiler {

class SharedLibrary;
using SharedLibraryPtr = std::shared_ptr<SharedLibrary>;

/**
 * @brief Represents a @DynamicObject, which relies on a shared library.
 */
class SharedLibrary : public DynamicObject {
  public:
    /**
     * @brief Creates a new @SharedLibrary Object
     * @param shareLib
     */
    explicit SharedLibrary(void* shareLib);
    /**
     * @brief Loads a shared library from a specific path.
     * @param filePath
     * @return SharedLibraryPtr
     */
    static SharedLibraryPtr load(const std::string& filePath);

    /**
     * @brief Destructor for the shared library.
     * Automatically unloads the shared library from memory.
     */
    ~SharedLibrary() override;

  protected:
    /**
     * @brief Returns a untyped function pointer to a specific symbol.
     * @param member on the dynamic object, currently provided as a MangledName.
     * @return function ptr
     */
    [[nodiscard]] void* getInvocableFunctionPtr(const std::string& member) override;

  private:
    void* shareLib;
};

}// namespace NES::Compiler

#endif//NES_INCLUDE_COMPILER_CPPCOMPILER_SHAREDLIBRARY_HPP_
