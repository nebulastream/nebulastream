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
    return flags;
}

void CPPCompilerFlags::enableDebugFlags() { addFlag(DEBUGGING); }

void CPPCompilerFlags::enableOptimizationFlags() {
    addFlag(ALL_OPTIMIZATIONS);
    addFlag(TUNE);
    addFlag(ARCH);
}

std::vector<std::string> CPPCompilerFlags::getFlags() const { return compilerFlags; }

void CPPCompilerFlags::addFlag(const std::string& flag) { compilerFlags.emplace_back(flag); }

}// namespace NES::Compiler