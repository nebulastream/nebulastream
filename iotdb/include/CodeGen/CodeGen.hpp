#ifndef CODEGEN_H
#define CODEGEN_H

#include <Core/DataTypes.hpp>
#include "Operators/Operator.hpp"
#include <memory>

namespace iotdb {

class AttributeReference;
typedef std::shared_ptr<AttributeReference> AttributeReferencePtr;

class CodeGenerator;
typedef std::shared_ptr<CodeGenerator> CodeGeneratorPtr;

class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

//class Operator;
typedef std::unique_ptr<Operator> OperatorPtr;

class CompilerArgs;
class CodeGenArgs;

class CodeGenerator {
public:
  virtual bool addOperator(OperatorPtr, const CodeGenArgs &) = 0;
  virtual PipelineStagePtr compile(const CompilerArgs &) = 0;
  virtual ~CodeGenerator();
};

/** \brief factory method for creating a code generator */
CodeGeneratorPtr createCodeGenerator();
}
#endif
