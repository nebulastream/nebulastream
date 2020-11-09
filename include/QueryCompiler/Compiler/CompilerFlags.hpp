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

#ifndef NES_INCLUDE_QUERYCOMPILER_COMPILER_COMPILERFLAGS_HPP_
#define NES_INCLUDE_QUERYCOMPILER_COMPILER_COMPILERFLAGS_HPP_

#include <memory>
#include <string>
#include <vector>

namespace NES {

class CompilerFlags;
typedef std::shared_ptr<CompilerFlags> CompilerFlagsPtr;

class CompilerFlags {
  public:
    // sets the cpp language version for the code
    inline static const std::string CXX_VERSION = "-std=c++17";
    // disables trigraphs
    inline static const std::string NO_TRIGRAPHS = "-fno-trigraphs";
    //Position Independent Code means that the generated machine code is not dependent on being located at a specific address in order to work.
    inline static const std::string FPIC = "-fpic";
    // Turn warnings into errors.
    inline static const std::string WERROR = "-Werror";
    // warning: equality comparison with extraneous parentheses
    inline static const std::string WPARENTHESES_EQUALITY = "-Wparentheses-equality";

    inline static const std::string ALL_OPTIMIZATIONS = "-O3";
    inline static const std::string DEBUGGING = "-g";

    // Vector extensions
    inline static const std::string SSE_4_1 = "-msse4.1";
    inline static const std::string SSE_4_2 = "-msse4.2";
    inline static const std::string AVX = "-mavx";
    inline static const std::string AVX2 = "-mavx2";

    static CompilerFlagsPtr create();
    static CompilerFlagsPtr createDefaultCompilerFlags();
    static CompilerFlagsPtr createOptimizingCompilerFlags();
    static CompilerFlagsPtr createDebuggingCompilerFlags();

    std::vector<std::string> getFlags();
    void addFlag(std::string flag);

  private:
    CompilerFlags();
    std::vector<std::string> compilerFlags;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_COMPILER_COMPILERFLAGS_HPP_
