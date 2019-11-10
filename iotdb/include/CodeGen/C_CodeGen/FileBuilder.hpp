
#pragma once

#include <memory>
#include <string>

#include <CodeGen/C_CodeGen/Declaration.hpp>
#include <CodeGen/C_CodeGen/Statement.hpp>
#include <CodeGen/CodeExpression.hpp>

#include <Util/ErrorHandling.hpp>
#include "../DataTypes.hpp"

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
