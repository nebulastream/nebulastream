#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEOPERATOR_HPP_

#include <Operators/OperatorNode.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableOperatorForwardRef.hpp>
namespace NES {

class CodeGenerator;
typedef std::shared_ptr<CodeGenerator> CodeGeneratorPtr;

class PipelineContext;
typedef std::shared_ptr<PipelineContext> PipelineContextPtr;

class GeneratableOperator;
typedef std::shared_ptr<GeneratableOperator> GeneratableOperatorPtr;

/**
 * @brief Base class for all generatable operators. It defines the general produce and consume methods as defined by Neumann.
 */
class GeneratableOperator {

  public:
    /**
     * @brief Produce function, which calls the child produce function and brakes pipelines if necessary.
     * @param codegen a pointer to the code generator.
     * @param context a pointer to the current pipeline context.
     */
    virtual void produce(CodeGeneratorPtr codegen, PipelineContextPtr context) = 0;

    /**
    * @brief Consume function, which generates code for the processing and calls the parent consume function.
    * @param codegen a pointer to the code generator.
    * @param context a pointer to the current pipeline context.
    */
    virtual void consume(CodeGeneratorPtr codegen, PipelineContextPtr context) = 0;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEOPERATOR_HPP_
