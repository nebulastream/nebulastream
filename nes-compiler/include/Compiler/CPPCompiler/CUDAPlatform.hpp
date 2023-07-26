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
#ifndef NES_COMPILER_INCLUDE_COMPILER_CPPCOMPILER_CUDAPLATFORM_HPP_
#define NES_COMPILER_INCLUDE_COMPILER_CPPCOMPILER_CUDAPLATFORM_HPP_

#include <Compiler/ExternalAPI.hpp>

namespace NES::Compiler {

/**
 * @brief This class specifies the compiler flags for the CUDA platform.
 * Note that the compiler flags are only viable for clang.
 */
class CUDAPlatform : public ExternalAPI {
  public:
    explicit CUDAPlatform(const std::string& cudaSdkPath);

    const CompilerFlags getCompilerFlags() const override;

  private:
    std::string cudaSdkPath;
};

}// namespace NES::Compiler

#endif // NES_COMPILER_INCLUDE_COMPILER_CPPCOMPILER_CUDAPLATFORM_HPP_
