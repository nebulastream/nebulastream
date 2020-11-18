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

#include <QueryCompiler/Compiler/CompilerFlags.hpp>

namespace NES {

CompilerFlags::CompilerFlags() {}

CompilerFlagsPtr CompilerFlags::create() { return std::make_shared<CompilerFlags>(CompilerFlags()); }

CompilerFlagsPtr CompilerFlags::createDefaultCompilerFlags() {
    auto flags = create();
    flags->addFlag(CompilerFlags::CXX_VERSION);
    flags->addFlag(CompilerFlags::NO_TRIGRAPHS);
    flags->addFlag(CompilerFlags::FPIC);
    flags->addFlag(CompilerFlags::WERROR);
    flags->addFlag(CompilerFlags::WPARENTHESES_EQUALITY);
    return flags;
}

CompilerFlagsPtr CompilerFlags::createOptimizingCompilerFlags() {
    auto flags = createDefaultCompilerFlags();
    flags->addFlag(CompilerFlags::ALL_OPTIMIZATIONS);
#ifdef SSE41_FOUND
    flags->addFlag(CompilerFlags::SSE_4_1);
#endif
#ifdef SSE42_FOUND
    flags->addFlag(CompilerFlags::SSE_4_2);
#endif
#ifdef AVX_FOUND
    flags->addFlag(CompilerFlags::AVX);
#endif
#ifdef AVX2_FOUND
    flags->addFlag(CompilerFlags::AVX2);
#endif
    return flags;
}

void CompilerFlags::addFlag(std::string flag) { compilerFlags.push_back(flag); }

std::vector<std::string> CompilerFlags::getFlags() { return compilerFlags; }

CompilerFlagsPtr CompilerFlags::createDebuggingCompilerFlags() {
    auto flags = createDefaultCompilerFlags();
    flags->addFlag(CompilerFlags::DEBUGGING);
    return flags;
}

}// namespace NES