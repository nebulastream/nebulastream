#ifndef NES_INCLUDE_COMPILER_SOURCECODE_HPP_
#define NES_INCLUDE_COMPILER_SOURCECODE_HPP_
#include <Compiler/Language.hpp>
namespace NES::Compiler {

class SourceCode {
  public:
    SourceCode(Language language, const std::string& code);
    const Language getLanguage() const;
    const std::string& getCode() const;

  private:
    const std::string code;
    const Language language;
};

}// namespace NES::Compiler

#endif//NES_INCLUDE_COMPILER_SOURCECODE_HPP_
