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

#ifndef NES_COMPILER_INCLUDE_COMPILER_COMPILATIONCACHE_HPP_
#define NES_COMPILER_INCLUDE_COMPILER_COMPILATIONCACHE_HPP_
#include <Compiler/CompilationResult.hpp>
#include <Compiler/CompilerForwardDeclarations.hpp>
#include <Compiler/SourceCode.hpp>
#include <mutex>
#include <unordered_map>

namespace NES::Compiler {

/**
 * This class is used to cache the already generated binaries to not compile the the query over and over again
 */
class CompilationCache {
  public:
    /**
     * @brief check if the binary for a query already exists
     * @param code
     * @return bool if for this code the binary already exists
     */
    bool contains(const SourceCode& code);

    /**
     * @brief inserts a compilation result for a new source code
     * @param sourceCode reference to the source code
     * @param compilationResult reference to the compilation result
     */
    void insert(const SourceCode& code, CompilationResult& compilationResult);

    /**
     * @brief method to retrieve the compilation result for a given source code
     * @param code
     * @return compilation result
     */
    CompilationResult get(const SourceCode& code);

  private:
    std::unordered_map<const SourceCode, CompilationResult> compilationReuseMap;
    std::recursive_mutex mutex;
};

}// namespace NES::Compiler

#endif // NES_COMPILER_INCLUDE_COMPILER_COMPILATIONCACHE_HPP_
