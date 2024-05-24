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
#include <Compiler/CPPCompiler/CPPCompilerFlags.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Compiler {

void CPPCompilerFlags::addDefaultCompilerFlags() {
    addFlag(CXX_VERSION);
    addFlag(NO_TRIGRAPHS);
    addFlag(FPIC);
    addFlag(WPARENTHESES_EQUALITY);
#if defined(__SSE4_2__)
    addFlag(SSE_4_2);
#endif
#ifdef __APPLE__
    addFlag(std::string("-isysroot ") + std::string(NES_OSX_SYSROOT));
    addFlag(std::string("-DTARGET_OS_IPHONE=0"));
    addFlag(std::string("-DTARGET_OS_SIMULATOR=0"));
#endif
}

void CPPCompilerFlags::addSharedLibraryFlag() {
    NES_DEBUG("Compile as shared library.");
    addFlag(SHARED);
}

void CPPCompilerFlags::enableDebugFlags() {
    NES_DEBUG("Compile with debugging.");
    addFlag(GENERATE_DEBUG_SYMBOLS);
}

void CPPCompilerFlags::enableOptimizationFlags() {
    NES_DEBUG("Compile with optimizations.");
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

void CPPCompilerFlags::enableProfilingFlags() {
    NES_DEBUG("Compilation Time tracing is activated open: chrome://tracing/");
    addFlag(CPPCompilerFlags::TRACE_COMPILATION_TIME);
}

}// namespace NES::Compiler
