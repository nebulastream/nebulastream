
#pragma once

#include <memory>
#include <string>

#include <QueryCompiler/C_CodeGen/Declaration.hpp>
#include <QueryCompiler/C_CodeGen/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>

#include <Util/ErrorHandling.hpp>
#include <API/Types/DataTypes.hpp>

namespace iotdb {

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

} // namespace iotdb
