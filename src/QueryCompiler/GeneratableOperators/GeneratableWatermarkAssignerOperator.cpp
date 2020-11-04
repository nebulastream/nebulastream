#include <QueryCompiler/GeneratableOperators/GeneratableWatermarkAssignerOperator.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

GeneratableWatermarkAssignerOperatorPtr GeneratableWatermarkAssignerOperator::create(WatermarkAssignerLogicalOperatorNodePtr watermarkAssignerLogicalOperatorNode, OperatorId id) {
    auto generatableOperator = std::make_shared<GeneratableWatermarkAssignerOperator>(GeneratableWatermarkAssignerOperator(watermarkAssignerLogicalOperatorNode->getWatermarkStrategy(), id));
    return generatableOperator;
}

GeneratableWatermarkAssignerOperator::GeneratableWatermarkAssignerOperator(const Windowing::WatermarkStrategyPtr watermarkStrategy, OperatorId id)
    : WatermarkAssignerLogicalOperatorNode(watermarkStrategy, id){

}

void GeneratableWatermarkAssignerOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, context);
}
void GeneratableWatermarkAssignerOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {

    codegen->generateCodeForWatermarkAssigner(getWatermarkStrategy(), context);
    getParents()[0]->as<GeneratableOperator>()->consume(codegen, context);
}
const std::string GeneratableWatermarkAssignerOperator::toString() const {
    std::stringstream ss;
    ss << "GENERATABLE_WATERMARKASSIGNER(" << outputSchema->toString() << ")";
    return ss.str();}
}