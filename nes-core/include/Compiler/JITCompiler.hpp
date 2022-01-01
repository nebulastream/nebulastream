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
#ifndef NES_INCLUDE_COMPILER_JIT_COMPILER_HPP_
#define NES_INCLUDE_COMPILER_JIT_COMPILER_HPP_
#include <Compiler/CompilerForwardDeclarations.hpp>
#include <future>
#include <map>
#include <vector>

namespace NES::Compiler {

/**
 * @brief The JIT compiler handles compilation requests and dispatches them to the right language compiler implementation.
 */
class JITCompiler {
  public:
    /**
     * @brief Constructor to create a new jit compiler with a fixed set of language compilers.
     * @param languageCompilers set of language compilers.
     */
    JITCompiler(std::map<const std::string, std::shared_ptr<const LanguageCompiler>> languageCompilers);
    /**
     * @brief Processes a compilation request and dispatches it to the correct compiler implementation.
     * @param request Compilation request
     * @return Future of the CompilationResult
     */
    [[nodiscard]] std::future<CompilationResult> compile(std::shared_ptr<const CompilationRequest> request) const;

  private:
    /**
     * @brief Processes a compilation request and dispatches it to the correct compiler implementation.
     * @param request Compilation request
     * @return Future of the CompilationResult
     */
    [[nodiscard]] CompilationResult handleRequest(std::shared_ptr<const CompilationRequest> request) const;
    const std::map<const std::string, std::shared_ptr<const LanguageCompiler>> languageCompilers;
};

}// namespace NES::Compiler

#endif// NES_INCLUDE_COMPILER_JIT_COMPILER_HPP_
