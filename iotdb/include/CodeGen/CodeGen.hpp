#ifndef CODEGEN_H
#define CODEGEN_H

#include "Operators/Operator.hpp"
#include <Core/DataTypes.hpp>
#include <memory>

namespace iotdb {

class AttributeReference;
typedef std::shared_ptr<AttributeReference> AttributeReferencePtr;

class CodeGenerator;
typedef std::shared_ptr<CodeGenerator> CodeGeneratorPtr;

class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

// class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

class CompilerArgs;
class CodeGenArgs;

class CompilerArgs {
};

class CodeGenArgs {
};

class CodeGenerator {
  public:
    CodeGenerator(const CodeGenArgs& args);
    // virtual bool addOperator(OperatorPtr) = 0;
    virtual bool generateCode(const DataSourcePtr& source, const PipelineContextPtr& context, std::ostream& out) = 0;
    virtual bool generateCode(const PredicatePtr& pred, const PipelineContextPtr& context, std::ostream& out) = 0;
    virtual bool generateCode(const DataSinkPtr& sink, const PipelineContextPtr& context, std::ostream& out) = 0;
    virtual PipelineStagePtr compile(const CompilerArgs&) = 0;
    virtual ~CodeGenerator();

    const Schema& getInputSchema() const { return input_schema_; };
    const Schema& getResultSchema() const { return result_schema_; };

  protected:
    CodeGenArgs args_;
    Schema input_schema_;
    Schema result_schema_;
};

/** \brief factory method for creating a code generator */
CodeGeneratorPtr createCodeGenerator();
/** \brief factory method for creating a pipeline context */
const PipelineContextPtr createPipelineContext();

} // namespace iotdb
#endif
