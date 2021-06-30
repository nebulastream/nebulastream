#ifndef NES_INCLUDE_COMPILER_LANGUAGECOMPILER_HPP_
#define NES_INCLUDE_COMPILER_LANGUAGECOMPILER_HPP_
#include <Compiler/Language.hpp>
namespace NES::Compiler {

class LanguageCompiler {
  public:
    [[nodiscard]] virtual std::unique_ptr<const CompilationResult>
    compile(std::unique_ptr<const CompilationRequest> request) const = 0;
    [[nodiscard]] virtual Language getLanguage() const = 0;
    virtual ~LanguageCompiler() = default;
};

}// namespace NES::Compiler

#endif//NES_INCLUDE_COMPILER_LANGUAGECOMPILER_HPP_
