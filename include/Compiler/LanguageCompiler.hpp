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
#ifndef NES_INCLUDE_COMPILER_LANGUAGECOMPILER_HPP_
#define NES_INCLUDE_COMPILER_LANGUAGECOMPILER_HPP_
namespace NES::Compiler {

/**
 * @brief Abstract class for a language specific compiler.
 * A language specific compiler, receives compilation requests and returns compilation results.
 * All methods are expected to be thread safe.
 */
class LanguageCompiler {
  public:
    /**
     * @brief Handles a compilation request. Implementations have to be thread safe.
     * @param request CompilationRequest
     * @return CompilationResult
     */
    [[nodiscard]] virtual CompilationResult compile(std::shared_ptr<const CompilationRequest> request) const = 0;

    /**
     * @brief Returns the language for, which this compiler can handle compilation requests
     * @return language
     */
    [[nodiscard]] virtual std::string getLanguage() const = 0;

    /**
     * @brief Destructor for the language compiler.
     */
    virtual ~LanguageCompiler() = default;
};

}// namespace NES::Compiler

#endif//NES_INCLUDE_COMPILER_LANGUAGECOMPILER_HPP_
