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
#ifndef NES_COMPILER_INCLUDE_COMPILER_JITCOMPILERBUILDER_HPP_
#define NES_COMPILER_INCLUDE_COMPILER_JITCOMPILERBUILDER_HPP_
#include <Compiler/CompilerForwardDeclarations.hpp>
#include <map>
#include <memory>
namespace NES::Compiler {

/**
 * @brief Builder for a new JIT compiler.
 */
class JITCompilerBuilder {
  public:
    /**
     * @brief Constructor for a new JIT compiler object.
     */
    JITCompilerBuilder() = default;
    /**
     * @brief Registers a new language compiler to this JIT compiler.
     * @param languageCompiler
     * @return JITCompilerBuilder
     */
    JITCompilerBuilder& registerLanguageCompiler(const std::shared_ptr<const LanguageCompiler> languageCompiler);

    /**
     * @brief Enables or disables the compilation cache
     * @param useCompilationCache
     * @return JITCompilerBuilder
     */
    JITCompilerBuilder& setUseCompilationCache(bool useCompilationCache);
    /**
     * @brief Creates a instance of the JITCompiler containing all language compilers.
     * @return JITCompiler
     */
    std::shared_ptr<JITCompiler> build();

  private:
    std::map<const Language, std::shared_ptr<const LanguageCompiler>> languageCompilers;
    bool useCompilationCache = false;
};

}// namespace NES::Compiler

#endif // NES_COMPILER_INCLUDE_COMPILER_JITCOMPILERBUILDER_HPP_
