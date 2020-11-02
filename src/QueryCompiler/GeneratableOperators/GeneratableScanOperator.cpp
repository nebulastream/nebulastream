#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableScanOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>

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
    return GeneratableScanOperator::create(schema, id);
}

GeneratableScanOperatorPtr GeneratableScanOperator::create(SchemaPtr schema, OperatorId id) {
    return std::make_shared<GeneratableScanOperator>(GeneratableScanOperator(schema, id));
}

GeneratableScanOperator::GeneratableScanOperator(SchemaPtr schema, OperatorId id) : schema(schema), OperatorNode(id) {}

const std::string GeneratableScanOperator::toString() const {
    std::stringstream ss;
    ss << "GENERATABLE_SCAN(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES