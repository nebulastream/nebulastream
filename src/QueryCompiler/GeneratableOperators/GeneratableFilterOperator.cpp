
#include <Phases/TranslateToLegacyPlanPhase.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableFilterOperator.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

GeneratableFilterOperatorPtr GeneratableFilterOperator::create(FilterLogicalOperatorNodePtr filterLogicalOperator, OperatorId id) {
    auto generatableOperator = std::make_shared<GeneratableFilterOperator>(GeneratableFilterOperator(filterLogicalOperator->getPredicate(), id));
    generatableOperator->setId(UtilityFunctions::getNextOperatorId());
    return generatableOperator;
}

GeneratableFilterOperator::GeneratableFilterOperator(const ExpressionNodePtr filterExpression, OperatorId id)
    : FilterLogicalOperatorNode(filterExpression, id) {}

void GeneratableFilterOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, context);
}

void GeneratableFilterOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    // todo remove if code gen can handle expressions
    auto predicate = TranslateToLegacyPlanPhase::create()->transformExpression(getPredicate());
    codegen->generateCodeForFilter(std::dynamic_pointer_cast<Predicate>(predicate), context);
    getParents()[0]->as<GeneratableOperator>()->consume(codegen, context);
}

const std::string GeneratableFilterOperator::toString() const {
    std::stringstream ss;
    ss << "GENERATABLE_FILTER(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES