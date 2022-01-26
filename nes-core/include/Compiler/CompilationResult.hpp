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
#ifndef NES_INCLUDE_COMPILER_COMPILATIONRESULT_HPP_
#define NES_INCLUDE_COMPILER_COMPILATIONRESULT_HPP_

#include <Compiler/CompilerForwardDeclarations.hpp>
#include <Util/Timer.hpp>

namespace NES::Compiler {

/**
 * @brief Result for a specific @CompilationRequest.
 * Contains a reference to the @DynmaicObject created by the compiler.
 */
class CompilationResult {
  public:
    /**
     * @brief Constructor for a Compilation result.
     * @param dynamicObject The dynamic object created by the @LanguageCompiler
     * @param timer timer object to measure the time it takes to handle the @CompilationRequest
     */
    CompilationResult(std::shared_ptr<DynamicObject> dynamicObject, Timer<>&& timer);

    /**
     * @brief Returns the dynamic object created by the Compiler
     * @return std::shared_ptr<DynamicObject>
     */
    [[nodiscard]] std::shared_ptr<DynamicObject> getDynamicObject() const;

    /**
     * @brief Returns the compilation time
     * @return compilation time
     */
    [[nodiscard]] uint64_t getCompilationTime() const;

  private:
    const std::shared_ptr<DynamicObject> dynamicObject;
    Timer<std::chrono::nanoseconds, std::milli, double, std::chrono::high_resolution_clock> timer;
};

}// namespace NES::Compiler

#endif  // NES_INCLUDE_COMPILER_COMPILATIONRESULT_HPP_
