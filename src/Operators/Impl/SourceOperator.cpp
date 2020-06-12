#include <iostream>

#include <Operators/Impl/SourceOperator.hpp>
#include <QueryCompiler/CodeGenerator.hpp>

namespace NES {

SourceOperator::SourceOperator(const DataSourcePtr source) : Operator(), source(source) {}

SourceOperator::SourceOperator(const SourceOperator& other) : source(other.source) {}

SourceOperator& SourceOperator::operator=(const SourceOperator& other) {
    if (this != &other) {
        source = other.source;
    }
    return *this;
}

void SourceOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {
    this->consume(codegen, context, out);
}

void SourceOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {
    codegen->generateCodeForScan(source->getSchema(), context);
    getParent()->consume(codegen, context, out);
}

const OperatorPtr SourceOperator::copy() const {
    const OperatorPtr copy = std::make_shared<SourceOperator>(*this);
    copy->setOperatorId(this->getOperatorId());
    return copy;
}

const std::string SourceOperator::toString() const {
    std::stringstream ss;
    ss << "SOURCE(" << NES::toString(source) << ")";
    return ss.str();
}

OperatorType SourceOperator::getOperatorType() const { return SOURCE_OP; }

NES::DataSourcePtr SourceOperator::getDataSourcePtr() {
    return source;
}

SourceOperator::~SourceOperator() {}

SourceOperator::SourceOperator() {
}

const OperatorPtr createSourceOperator(const DataSourcePtr source) { return std::make_shared<SourceOperator>(source); }

}// namespace NES
