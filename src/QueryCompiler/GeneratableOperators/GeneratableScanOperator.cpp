
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableScanOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

void GeneratableScanOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    this->consume(codegen, context);
    if (!getChildren().empty()) {
        getChildren()[0]->as<GeneratableOperator>()->produce(codegen, context);
    }
}

void GeneratableScanOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForScan(schema, context);
    getParents()[0]->as<GeneratableOperator>()->consume(codegen, context);
}

OperatorNodePtr GeneratableScanOperator::copy() {
    return GeneratableScanOperator::create(schema);
}

GeneratableScanOperatorPtr GeneratableScanOperator::create(SchemaPtr schema) {
    auto generatableOperator = std::make_shared<GeneratableScanOperator>(GeneratableScanOperator(schema));
    generatableOperator->setId(UtilityFunctions::getNextOperatorId());
    return generatableOperator;
}

GeneratableScanOperator::GeneratableScanOperator(SchemaPtr schema) : schema(schema) {}

const std::string GeneratableScanOperator::toString() const {
    std::stringstream ss;
    ss << "GENERATABLE_SCAN(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES