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

#ifndef NES_COMPILATIONCACHE_HPP
#define NES_COMPILATIONCACHE_HPP
#include <Compiler/CompilerForwardDeclarations.hpp>
#include <map>
#include <string>
#include <mutex>

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
    bool exists(std::shared_ptr<SourceCode> code);

    /**
     * @brief inserts a compilation result for a new source code
     * @param souceCode
     * @param compilationResult
     */
    void insert(std::pair<std::shared_ptr<SourceCode>, CompilationResult> newEntry);

    /**
     * @brief method to retrieve the compilation result for a given source code
     * @param code
     * @return compilation result
     */
    CompilationResult get(std::shared_ptr<SourceCode> code);

  private:
    std::map<std::shared_ptr<SourceCode>, CompilationResult> compilationReuseMap;
    std::mutex mutex;
};

}// namespace NES::Compiler

#endif//NES_COMPILATIONCACHE_HPP
