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
#ifndef NES_INCLUDE_COMPILER_CPP_COMPILER_CPP_COMPILER_FLAGS_HPP_
#define NES_INCLUDE_COMPILER_CPP_COMPILER_CPP_COMPILER_FLAGS_HPP_
#include <string>
#include <vector>
namespace NES::Compiler {

/**
 * @brief Represents compilation flags available to the C++ compiler.
 */
class CPPCompilerFlags {
  public:
    // sets the cpp language version for the code
    inline static const std::string CXX_VERSION = "-std=c++20";
    // disables trigraphs
    inline static const std::string NO_TRIGRAPHS = "-fno-trigraphs";
    //Position Independent Code means that the generated machine code is not dependent on being located at a specific address in order to work.
    inline static const std::string FPIC = "-fpic";
    // Turn warnings into errors.
    inline static const std::string WERROR = "-Werror";
    // warning: equality comparison with extraneous parentheses
    inline static const std::string WPARENTHESES_EQUALITY = "-Wparentheses-equality";

    //    inline static const std::string LOGGING_FATAL_FLAG = "-DNES_LOGGING_FATAL_ERROR_LEVEL=1";
    //    inline static const std::string LOGGING_FATAL_FLAG = "-DNES_LOGGING_TRACE_LEVEL=1";

    inline static const std::string ALL_OPTIMIZATIONS = "-O3";
    inline static const std::string TUNE = "-mtune=native";
    inline static const std::string ARCH = "-march=native";
    // ARM suggests to use cpu type instead of tune/arch
    inline static const std::string CPU = "-mcpu=native";
    // LLVM defines specific to this cpu type, for now a14 == m1
    // https://github.com/llvm/llvm-project/blob/llvmorg-13.0.0/llvm/include/llvm/Support/AArch64TargetParser.def
    // See issue: https://github.com/nebulastream/nebulastream/issues/2248
    inline static const std::string M1_CPU = "-mcpu=apple-a14";
    inline static const std::string DEBUGGING = "-g";
    // Enables tracing for compilation time with chrome::tracing
    inline static const std::string TRACE_COMPILATION_TIME = "-ftime-trace";

    // Vector extensions
    inline static const std::string SSE_4_1 = "-msse4.1";
    inline static const std::string SSE_4_2 = "-msse4.2";
    inline static const std::string AVX = "-mavx";
    inline static const std::string AVX2 = "-mavx2";

    static CPPCompilerFlags create();
    static CPPCompilerFlags createDefaultCompilerFlags();
    void enableOptimizationFlags();
    void enableDebugFlags();

    [[nodiscard]] std::vector<std::string> getFlags() const;
    void addFlag(const std::string& flag);

  private:
    CPPCompilerFlags() = default;
    std::vector<std::string> compilerFlags;
};

}// namespace NES::Compiler

#endif// NES_INCLUDE_COMPILER_CPP_COMPILER_CPP_COMPILER_FLAGS_HPP_
