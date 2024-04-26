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

#ifndef NES_COMPILER_INCLUDE_COMPILER_EXTERNALAPI_HPP_
#define NES_COMPILER_INCLUDE_COMPILER_EXTERNALAPI_HPP_

#include <Compiler/CompilerFlags.hpp>

namespace NES::Compiler {

/**
 * @brief A class which inherits from `ExternalAPI` can be passed to a `CompilationRequest` to augment the
 * call to the compiler command with flags that are necessary for the correct compiling and linking
 * of a third-party library.
 *
 * An example for an external API would be CUDA, as we need to give the clang compiler CUDA-specific flags
 * that are not part of the default compiler call mechanism.
 */
class ExternalAPI {
  public:
    /**
     * @brief Get the compiler flags specific to the external API.
     * @return CompilerFlags
     */
    virtual const CompilerFlags getCompilerFlags() const = 0;

    virtual ~ExternalAPI() = default;
};

}// namespace NES::Compiler

#endif // NES_COMPILER_INCLUDE_COMPILER_EXTERNALAPI_HPP_
