
#include <QueryCompiler/GeneratableOperators/GeneratableScanOperator.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/PipelineContext.hpp>

namespace NES{

void GeneratableScanOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    this->consume(codegen, context);
    if(!getChildren().empty()) {
        getChildren()[0]->as<GeneratableOperator>()->produce(codegen, context);
    }
}

void GeneratableScanOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForScan(schema, context);
    getParents()[0]->as<GeneratableOperator>()->consume(codegen, context);
}
const std::string GeneratableScanOperator::toString() const {
    return "Scan()";
}
OperatorNodePtr GeneratableScanOperator::copy() {
    return shared_from_this()->as<OperatorNode>();
}
GeneratableScanOperatorPtr GeneratableScanOperator::create(SchemaPtr schema) {
    return std::make_shared<GeneratableScanOperator>(GeneratableScanOperator(schema));
}

GeneratableScanOperator::GeneratableScanOperator(SchemaPtr schema): schema(schema) {}

}