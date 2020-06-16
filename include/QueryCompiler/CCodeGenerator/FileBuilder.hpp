
#pragma once

#include <memory>
#include <string>
#include <sstream>
#include <QueryCompiler/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>

namespace NES {

class CodeFile {
  public:
    std::string code;
};

class FileBuilder {
  private:
    std::stringstream declations;

  public:
    static FileBuilder create(const std::string& file_name);
    FileBuilder& addDeclaration(const Declaration&);
    CodeFile build();
};

}// namespace NES
