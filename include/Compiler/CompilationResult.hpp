#ifndef NES_INCLUDE_COMPILER_COMPILATIONRESULT_HPP_
#define NES_INCLUDE_COMPILER_COMPILATIONRESULT_HPP_

namespace NES::Compiler {

class CompilationResult {
  private:
    std::shared_ptr<CompiledCode> compiledCode;

};

}// namespace NES::Compiler

#endif//NES_INCLUDE_COMPILER_COMPILATIONRESULT_HPP_
