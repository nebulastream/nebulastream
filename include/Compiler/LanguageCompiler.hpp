#ifndef NES_INCLUDE_COMPILER_LANGUAGECOMPILER_HPP_
#define NES_INCLUDE_COMPILER_LANGUAGECOMPILER_HPP_
#include <Compiler/Language.hpp>
namespace NES::Compiler {

class LanguageCompiler {
  public:
    const CompilationResult compile(const std::unique_ptr<CompilationRequest> request) const;
    const Language getLanguage() const;
};

}// namespace NES::Compiler

#endif//NES_INCLUDE_COMPILER_LANGUAGECOMPILER_HPP_
