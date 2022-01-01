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
#include <Compiler/CPPCompiler/CPPCompilerFlags.hpp>

namespace NES::Compiler {

CPPCompilerFlags CPPCompilerFlags::create() { return CPPCompilerFlags(); }

CPPCompilerFlags CPPCompilerFlags::createDefaultCompilerFlags() {
    auto flags = create();
    flags.addFlag(CXX_VERSION);
    flags.addFlag(NO_TRIGRAPHS);
    flags.addFlag(FPIC);
    flags.addFlag(WPARENTHESES_EQUALITY);
#ifdef __APPLE__
    flags.addFlag(std::string("-isysroot ") + std::string(NES_OSX_SYSROOT));
#endif
    return flags;
}

void CPPCompilerFlags::enableDebugFlags() { addFlag(DEBUGGING); }

void CPPCompilerFlags::enableOptimizationFlags() {
    addFlag(ALL_OPTIMIZATIONS);
#if !defined(__aarch64__)
    // use -mcpu=native instead of TUNE/ARCH for arm64, below
    // -march=native is supported on Intel Macs clang but not on M1 Macs clang
    addFlag(TUNE);
    addFlag(ARCH);
#endif
#if defined(__aarch64__) && defined(__APPLE__)
    // M1 specific string
    addFlag(M1_CPU);
#endif
#if defined(__aarch64__) && !defined(__APPLE__)
    // generic arm64 cpu
    addFlag(CPU);
#endif
}

std::vector<std::string> CPPCompilerFlags::getFlags() const { return compilerFlags; }

void CPPCompilerFlags::addFlag(const std::string& flag) { compilerFlags.emplace_back(flag); }

}// namespace NES::Compiler