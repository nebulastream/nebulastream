#include <Compiler/SourceCode.hpp>

namespace NES::Compiler{

SourceCode::SourceCode(Language language, const std::string& code):  code(code), language(language) {}

Language SourceCode::getLanguage() const {
    return language;
}

const std::string & SourceCode::getCode() const {
    return code;
}



}