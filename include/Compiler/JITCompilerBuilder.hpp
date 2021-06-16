#ifndef NES_INCLUDE_COMPILER_JITCOMPILERBUILDER_HPP_
#define NES_INCLUDE_COMPILER_JITCOMPILERBUILDER_HPP_
#include <memory>
#include <map>
#include <Compiler/Language.hpp>
#include <Compiler/CompilerForwardDeclarations.hpp>
namespace NES::Compiler {

class JITCompilerBuilder {
  public:
    JITCompilerBuilder();
    void registerLanguageCompiler(const std::shared_ptr<const LanguageCompiler>& languageCompiler);
    std::shared_ptr<JITCompiler> build();
  private:
    std::map<const Language, std::shared_ptr<const LanguageCompiler>> languageCompilers;
};

}// namespace NES::Compiler

#endif//NES_INCLUDE_COMPILER_JITCOMPILERBUILDER_HPP_
