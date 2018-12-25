
#include <core/DataTypes.hpp>
#include <memory>

namespace iotdb {

class AttributeReference;
typedef std::shared_ptr<AttributeReference> AttributeReferencePtr;

class CodeGenerator;
typedef std::shared_ptr<CodeGenerator> CodeGeneratorPtr;

class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

class Operator;
typedef std::unique_ptr<Operator> OperatorPtr;

class CompilerArgs;
class CodeGenArgs;

class CodeGenerator {
  virtual bool addOperator(OperatorPtr, const CodeGenArgs &) = 0;
  virtual PipelineStagePtr compile(const CompilerArgs &) = 0;
  virtual ~CodeGenerator();
};

/** \brief factory method for creating a code generator */
CodeGeneratorPtr createCodeGenerator();
}
