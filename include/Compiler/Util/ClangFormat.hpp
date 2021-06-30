#ifndef NES_INCLUDE_COMPILER_UTIL_FORMATER_HPP_
#define NES_INCLUDE_COMPILER_UTIL_FORMATER_HPP_
#include <Compiler/File.hpp>
#include <Compiler/Language.hpp>
namespace NES ::Compiler {

class ClangFormat {
  public:
    explicit ClangFormat(Language language);
    void formatFile(std::shared_ptr<File> file);

  private:
    Language language;
};

}// namespace NES::Compiler

#endif//NES_INCLUDE_COMPILER_UTIL_FORMATER_HPP_
