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