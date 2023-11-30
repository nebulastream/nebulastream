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
#ifndef NES_COMPILER_INCLUDE_COMPILER_COMPILATIONREQUEST_HPP_
#define NES_COMPILER_INCLUDE_COMPILER_COMPILATIONREQUEST_HPP_

#include <Compiler/CompilerForwardDeclarations.hpp>

#include <memory>
#include <vector>

namespace NES::Compiler {

/**
 * @brief Represents a request to compile a specific source code artifact.
 * A compilation request consists of a unique identifier, a source code artifact and a set of compilation options.
 */
class CompilationRequest {
  public:
    /**
     * @brief Creates a new @CompilationRequest.
     * @param sourceCode The source code artifact that should be compiled
     * @param name the identifier of this request
     * @param profileCompilation enables profiling for the compilation
     * @param profileExecution enables profiling for the execution
     * @param optimizeCompilation enables optimizations
     * @param debug enables debug options
     * @param externalApis a vector of external APIs to add to the compilation process (empty by default)
     * @return std::unique_ptr<CompilationRequest>
     */
    CompilationRequest(std::unique_ptr<SourceCode> sourceCode,
                       std::string name,
                       bool profileCompilation,
                       bool profileExecution,
                       bool optimizeCompilation,
                       bool debug,
                       std::vector<std::shared_ptr<ExternalAPI>> externalApis = {});
    bool operator==(const CompilationRequest& rhs) const;
    bool operator!=(const CompilationRequest& rhs) const;

    /**
     * @brief Creates a new @CompilationRequest.
     * @param sourceCode The source code artifact that should be compiled
     * @param name the identifier of this request
     * @param profileCompilation enables profiling for the compilation
     * @param profileExecution enables profiling for the execution
     * @param optimizeCompilation enables optimizations
     * @param debug enables debug options
     * @param externalApis a vector of external APIs to add to the compilation process (empty by default)
     * @return std::unique_ptr<CompilationRequest>
     */
    static std::shared_ptr<CompilationRequest> create(std::unique_ptr<SourceCode> sourceCode,
                                                      std::string name,
                                                      bool profileCompilation,
                                                      bool profileExecution,
                                                      bool optimizeCompilation,
                                                      bool debug,
                                                      std::vector<std::shared_ptr<ExternalAPI>> externalApis = {});

    /**
     * @brief Returns the source code artifact
     * @return @SourceCode
     */
    [[nodiscard]] const std::shared_ptr<SourceCode> getSourceCode() const;

    /**
     * @brief Indicates if debugging should be enabled.
     * @return debug flag
     */
    [[nodiscard]] bool enableDebugging() const;

    /**
     * @brief Indicates if optimizations should be enabled.
     * @return optimization flag
     */
    [[nodiscard]] bool enableOptimizations() const;

    /**
     * @brief Indicates if the compilation process should be profiled.
     * @return profile compilation flag
     */
    [[nodiscard]] bool enableCompilationProfiling() const;

    /**
     * @brief Indicates if the execution should be profiled.
     * @return profile compilation flag
     */
    [[nodiscard]] bool enableExecutionProfiling() const;

    /**
     * @brief Returns the identifier
     * @return name
     */
    [[nodiscard]] std::string getName() const;

    /**
     * @brief Returns the external APIs
     * @return std::vector<std::shared_ptr<ExternalAPI>>
     */
    [[nodiscard]] std::vector<std::shared_ptr<ExternalAPI>> getExternalAPIs() const;

  private:
    const std::shared_ptr<SourceCode> sourceCode;
    const std::string name;
    const bool profileCompilation;
    const bool profileExecution;
    const bool optimizeCompilation;
    const bool debug;
    const std::vector<std::shared_ptr<ExternalAPI>> externalApis;
};

}// namespace NES::Compiler

#endif  // NES_COMPILER_INCLUDE_COMPILER_COMPILATIONREQUEST_HPP_
