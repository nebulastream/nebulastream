#include <Operators/Impl/MergeOperator.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <iostream>
#include <sstream>
#include <vector>

namespace NES {

MergeOperator::MergeOperator(SchemaPtr schemaPtr) : Operator(), schemaPtr(schemaPtr) {}
MergeOperator::MergeOperator(const MergeOperator& other) : schemaPtr(other.schemaPtr) {}
MergeOperator& MergeOperator::operator=(const MergeOperator& other) {
    if (this != &other) {
        schemaPtr = other.schemaPtr;
    }
    return *this;
}

void MergeOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {
    auto newPipelineContext1 = createPipelineContext();
    getChildren()[0]->produce(codegen, newPipelineContext1, out);
    auto newPipelineContext2 = createPipelineContext();
    getChildren()[1]->produce(codegen, newPipelineContext2, out);

    context->addNextPipeline(newPipelineContext1);
    context->addNextPipeline(newPipelineContext2);
}

void MergeOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {

    codegen->generateCodeForEmit(schemaPtr, context, out);
}

const OperatorPtr MergeOperator::copy() const { return std::make_shared<MergeOperator>(*this); }

const std::string MergeOperator::toString() const {
    std::stringstream ss;
    ss << "MERGE("
       << ")";
    return ss.str();
}

OperatorType MergeOperator::getOperatorType() const { return SOURCE_OP; }//TODO: seems every leaf node of a pipeline should be a source.

const OperatorPtr createMergeOperator(SchemaPtr schemaPtr) { return std::make_shared<MergeOperator>(schemaPtr); }

}// namespace NES
