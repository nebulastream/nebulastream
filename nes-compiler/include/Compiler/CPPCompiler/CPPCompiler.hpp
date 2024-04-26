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
#ifndef NES_COMPILER_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILER_HPP_
#define NES_COMPILER_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILER_HPP_
#include <Compiler/CompilerForwardDeclarations.hpp>
#include <Compiler/LanguageCompiler.hpp>
#include <Compiler/Util/ExecutablePath.hpp>
#include <filesystem>
#include <vector>

namespace NES::Compiler {
class CPPCompilerFlags;
class File;
class ClangFormat;

/**
 * @brief A @LanguageCompiler for C++.
 * Relies on clang++ for compilation.
 */
class CPPCompiler : public LanguageCompiler {
  public:
    CPPCompiler();
    ~CPPCompiler() noexcept;

    /**
     * @brief Creates a new instance of the cpp compiler.
     * @return std::shared_ptr<LanguageCompiler>
     */
    static std::shared_ptr<LanguageCompiler> create();
    /**
     * @brief Handles a compilation request. Implementations have to be thread safe.
     * @param request CompilationRequest
     * @return CompilationResult
     */
    [[nodiscard]] CompilationResult compile(std::shared_ptr<const CompilationRequest> request) const override;
    /**
    * @brief Returns the language for, which this compiler can handle compilation requests
    * @return language
    */
    [[nodiscard]] Language getLanguage() const override;

  private:
    std::unique_ptr<ClangFormat> format;
    ExecutablePath::RuntimePathConfig runtimePathConfig;
};

}// namespace NES::Compiler

#endif // NES_COMPILER_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILER_HPP_
