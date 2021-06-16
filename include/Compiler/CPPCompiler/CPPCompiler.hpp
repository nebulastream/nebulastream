#ifndef NES_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILER_HPP_
#define NES_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILER_HPP_
#include <Compiler/CompilerForwardDeclarations.hpp>
#include <Compiler/LanguageCompiler.hpp>
namespace NES::Compiler {
class CPPCompilerFlags;
class CPPCompiler : public LanguageCompiler {
  public:
    static std::shared_ptr<LanguageCompiler> create();
    [[nodiscard]]  std::unique_ptr<const CompilationResult> compile(std::unique_ptr<const CompilationRequest> request) const override;
    [[nodiscard]]  Language getLanguage() const override;
    ~CPPCompiler() override;
};

}// namespace NES::Compiler

#endif//NES_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILER_HPP_
