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
#ifndef NES_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILER_HPP_
#define NES_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILER_HPP_
#include <Compiler/CompilerForwardDeclarations.hpp>
#include <Compiler/LanguageCompiler.hpp>
#include <filesystem>

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
    [[nodiscard]] std::string getLanguage() const override;

    ~CPPCompiler() override = default;

  private:
    /**
     * @brief Compiles a source code file to a shared lib.
     * @param flags compilation flags
     * @param sourceFile the source code file
     * @param libraryFileName the target lib name
     */
    void compileSharedLib(CPPCompilerFlags flags, std::shared_ptr<File> sourceFile, std::string libraryFileName) const;
    std::unique_ptr<ClangFormat> format;

#ifdef __APPLE__
    std::filesystem::path libNesPath;
#endif
};

}// namespace NES::Compiler

#endif  // NES_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILER_HPP_
