

#include <QueryCompiler/Compiler/CompilerFlags.hpp>

namespace NES {

CompilerFlags::CompilerFlags() {}

CompilerFlagsPtr CompilerFlags::create() {
    return std::make_shared<CompilerFlags>(CompilerFlags());
}

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

void CompilerFlags::addFlag(std::string flag) {
    compilerFlags.push_back(flag);
}

std::vector<std::string> CompilerFlags::getFlags() {
    return compilerFlags;
}

CompilerFlagsPtr CompilerFlags::createDebuggingCompilerFlags() {
    auto flags = createDefaultCompilerFlags();
    flags->addFlag(CompilerFlags::DEBUGGING);
    return flags;
}

}// namespace NES