
#include <Phases/TranslateToLegacyPlanPhase.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableFilterOperator.hpp>

namespace NES {

GeneratableFilterOperatorPtr GeneratableFilterOperator::create(FilterLogicalOperatorNodePtr filterLogicalOperator) {
    return std::make_shared<GeneratableFilterOperator>(GeneratableFilterOperator(filterLogicalOperator->getPredicate()));
}

void GeneratableFilterOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, context, out);
}

void GeneratableFilterOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {
    // todo remove if code gen can handle expressions
    auto predicate = TranslateToLegacyPlanPhase::create()->transformExpression(getPredicate());
    codegen->generateCodeForFilter(std::dynamic_pointer_cast<Predicate>(predicate), context);
    getParents()[0]->as<GeneratableOperator>()->consume(codegen, context, out);
}
GeneratableFilterOperator::GeneratableFilterOperator(const ExpressionNodePtr filterExpression) : FilterLogicalOperatorNode(filterExpression) {
}

}