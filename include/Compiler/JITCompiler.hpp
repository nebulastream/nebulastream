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
#ifndef NES_INCLUDE_COMPILER_JITCOMPILER_HPP_
#define NES_INCLUDE_COMPILER_JITCOMPILER_HPP_
#include <Compiler/Language.hpp>
#include <Compiler/CompilerForwardDeclarations.hpp>
#include <map>
#include <future>
#include <vector>

namespace NES::Compiler {

class JITCompiler {
  public:
    JITCompiler(std::map<const Language, std::shared_ptr<const LanguageCompiler>> languageCompilers);
    [[nodiscard]] std::future<const CompilationResult> compile(std::unique_ptr<const CompilationRequest> request) const;
  private:
    const std::map<const Language, std::shared_ptr<const LanguageCompiler>> languageCompilers;
};

}// namespace NES::Compiler

#endif//NES_INCLUDE_COMPILER_JITCOMPILER_HPP_
