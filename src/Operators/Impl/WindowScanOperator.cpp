#include <iostream>

#include <Operators/Impl/WindowScanOperator.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/PipelineContext.hpp>

namespace NES {

WindowScanOperator::WindowScanOperator(const SchemaPtr schemaPtr) : Operator(), schemaPtr(schemaPtr) {}

WindowScanOperator::WindowScanOperator(const WindowScanOperator& other) : schemaPtr(other.schemaPtr) {}
WindowScanOperator::WindowScanOperator(){};

WindowScanOperator& WindowScanOperator::operator=(const WindowScanOperator& other) {
    if (this != &other) {
        schemaPtr = other.schemaPtr;
    }
    return *this;
}

void WindowScanOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {
    this->consume(codegen, context, out);
    auto newPipelineContext = createPipelineContext();
    this->getChildren()[0]->produce(codegen, newPipelineContext, out);
    context->addNextPipeline(newPipelineContext);
}

void WindowScanOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {
    codegen->generateCode(schemaPtr, context, out);
    getParent()->consume(codegen, context, out);
}

const OperatorPtr WindowScanOperator::copy() const { return std::make_shared<WindowScanOperator>(*this); }

const std::string WindowScanOperator::toString() const {
    std::stringstream ss;
    ss << "WindowScan()";
    return ss.str();
}

OperatorType WindowScanOperator::getOperatorType() const { return SOURCE_OP; }

WindowScanOperator::~WindowScanOperator() {}

const OperatorPtr createWindowScanOperator(const SchemaPtr schema) { return std::make_shared<WindowScanOperator>(schema); }

}// namespace NES
BOOST_CLASS_EXPORT(NES::WindowScanOperator);
