#ifndef NES_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILER_HPP_
#define NES_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILER_HPP_
#include <Compiler/CompilerForwardDeclarations.hpp>
#include <Compiler/LanguageCompiler.hpp>
namespace NES::Compiler {
class CPPCompilerFlags;
class File;
class ClangFormat;
class CPPCompiler : public LanguageCompiler {
  public:

    static std::shared_ptr<LanguageCompiler> create();
    CPPCompiler();
    [[nodiscard]]  std::unique_ptr<const CompilationResult> compile(std::unique_ptr<const CompilationRequest> request) const override;
    [[nodiscard]]  Language getLanguage() const override;
    ~CPPCompiler() override = default;

  private:
    void compileSharedLib(CPPCompilerFlags flags, std::shared_ptr<File> sourceFile, std::string libraryFileName);
    std::unique_ptr<ClangFormat> format;
};

}// namespace NES::Compiler

#endif//NES_INCLUDE_COMPILER_CPPCOMPILER_CPPCOMPILER_HPP_
