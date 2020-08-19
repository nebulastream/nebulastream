

#include <Phases/TranslateToLegacyPlanPhase.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableMapOperator.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>

namespace NES{

void GeneratableMapOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, context, out);
}
void GeneratableMapOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {
    auto field = getMapExpression()->getField();
    auto assignment = getMapExpression()->getAssignment();
    // todo remove if code gen can handle expressions
    auto mapExpression = TranslateToLegacyPlanPhase::create()->transformExpression(assignment);
    codegen->generateCodeForMap(AttributeField::create(field->getFieldName(), field->getStamp()), mapExpression, context);
    getParents()[0]->as<GeneratableOperator>()->consume(codegen, context, out);
}
GeneratableMapOperatorPtr GeneratableMapOperator::create(MapLogicalOperatorNodePtr mapLogicalOperatorNode) {
    return std::make_shared<GeneratableMapOperator>(GeneratableMapOperator(mapLogicalOperatorNode->getMapExpression()));
}

GeneratableMapOperator::GeneratableMapOperator(FieldAssignmentExpressionNodePtr mapExpression): MapLogicalOperatorNode(mapExpression) {
}
}