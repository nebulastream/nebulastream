#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEOPERATOR_HPP_

#include <Nodes/Operators/OperatorNode.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableOperatorForwardRef.hpp>
namespace NES {

class CodeGenerator;
typedef std::shared_ptr<CodeGenerator> CodeGeneratorPtr;

class PipelineContext;
typedef std::shared_ptr<PipelineContext> PipelineContextPtr;

class GeneratableOperator;
typedef std::shared_ptr<GeneratableOperator> GeneratableOperatorPtr;

class GeneratableOperator {

  public:
    virtual void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) = 0;

    virtual void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) = 0;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEOPERATOR_HPP_
