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
#ifndef NES_INCLUDE_COMPILER_COMPILATIONREQUEST_HPP_
#define NES_INCLUDE_COMPILER_COMPILATIONREQUEST_HPP_

#include <Compiler/CompilerForwardDeclarations.hpp>

namespace NES::Compiler {
class CompilationRequest {
  public:
    CompilationRequest(std::unique_ptr<SourceCode> sourceCode,
                       std::string name,
                       bool profileCompilation,
                       bool profileExecution,
                       bool optimizeCompilation,
                       bool debug);
    std::unique_ptr<CompilationRequest> create(std::unique_ptr<SourceCode> sourceCode,
                                               std::string name,
                                               bool profileCompilation,
                                               bool profileExecution,
                                               bool optimizeCompilation,
                                               bool debug);

    [[nodiscard]] const std::shared_ptr<SourceCode> getSourceCode() const;
    [[nodiscard]] bool enableDebugging() const;
    [[nodiscard]] bool enableOptimizations() const;
    [[nodiscard]] bool enableCompilationProfiling() const;
    [[nodiscard]] bool enableExecutionProfiling() const;
    [[nodiscard]] std::string getName() const;

  private:
    const std::shared_ptr<SourceCode> sourceCode;
    const std::string name;
    const bool profileCompilation;
    const bool profileExecution;
    const bool optimizeCompilation;
    const bool debug;
};

}// namespace NES::Compiler

#endif//NES_INCLUDE_COMPILER_COMPILATIONREQUEST_HPP_
