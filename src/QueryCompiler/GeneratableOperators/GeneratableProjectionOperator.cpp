

#include <QueryCompiler/GeneratableOperators/GeneratableProjectionOperator.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
namespace NES{

GeneratableProjectionOperator::GeneratableProjectionOperator(std::vector<ExpressionNodePtr> expressions, OperatorId id) :
                                                                                                                       ProjectionLogicalOperatorNode(expressions, id){
}

GeneratableProjectionOperatorPtr GeneratableProjectionOperator::create(ProjectionLogicalOperatorNodePtr projectLogicalOperator, OperatorId id) {
    return std::make_shared<GeneratableProjectionOperator>(GeneratableProjectionOperator(projectLogicalOperator->getExpressions(), id));
}

void GeneratableProjectionOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, context);
}

void GeneratableProjectionOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForProjection(getExpressions(), context);
    getParents()[0]->as<GeneratableOperator>()->consume(codegen, context);
}



const std::string GeneratableProjectionOperator::toString() const {
    std::stringstream ss;
    ss << "GENERATABLE_PROJECTION(" << outputSchema->toString() << ")";
    return ss.str();
}


}