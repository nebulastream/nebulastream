
#pragma once

#include <memory>
#include <string>

#include <QueryCompiler/CCodeGenerator/Declaration.hpp>
#include <QueryCompiler/CCodeGenerator/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>

#include <API/Types/DataTypes.hpp>

namespace NES {

class CodeFile {
  public:
    std::string code;
};

class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

PipelineStagePtr compile(const CodeFile& file);

class FileBuilder {
  private:
    std::stringstream declations;

  public:
    static FileBuilder create(const std::string& file_name);
    FileBuilder& addDeclaration(const Declaration&);
    CodeFile build();
};

} // namespace NES
