#ifndef NES_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILER_HPP_
#define NES_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILER_HPP_

namespace NES::Compiler {

class CPPCompiler: {
    const std::unique_ptr<CompilationResult> compile(const std::unique_ptr<CompilationRequest> request) const;
};

}// namespace NES::Compiler

#endif//NES_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILER_HPP_
