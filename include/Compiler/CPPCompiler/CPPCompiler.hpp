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
namespace NES::Compiler {
class CPPCompilerFlags;
class File;
class ClangFormat;
class CPPCompiler : public LanguageCompiler {
  public:

    static std::shared_ptr<LanguageCompiler> create();
    CPPCompiler();
    [[nodiscard]]  CompilationResult compile(std::unique_ptr<const CompilationRequest> request) const override;
    [[nodiscard]]  Language getLanguage() const override;
    ~CPPCompiler() override = default;

  private:
    void compileSharedLib(CPPCompilerFlags flags, std::shared_ptr<File> sourceFile, std::string libraryFileName) const;
    std::unique_ptr<ClangFormat> format;
};

}// namespace NES::Compiler

#endif//NES_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILER_HPP_
