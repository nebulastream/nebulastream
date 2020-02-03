

#include <iostream>
#include <sstream>
#include <vector>

#include <QueryCompiler/CodeGenerator.hpp>
#include <Operators/Impl/SourceOperator.hpp>

namespace NES {

SourceOperator::SourceOperator(const DataSourcePtr source) : Operator(), source_(source) {}

SourceOperator::SourceOperator(const SourceOperator& other) : source_(other.source_) {}

SourceOperator& SourceOperator::operator=(const SourceOperator& other)
{
    if (this != &other) {
        source_ = other.source_;
    }
    return *this;
}

void SourceOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out)
{
    this->consume(codegen, context, out);
}

void SourceOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out)
{
    codegen->generateCode(source_->getSchema(), context, out);
    getParent()->consume(codegen,context,out);
}

const OperatorPtr SourceOperator::copy() const {
    const OperatorPtr copy = std::make_shared<SourceOperator>(*this);
    copy->setOperatorId(this->getOperatorId());
    return copy;
}

const std::string SourceOperator::toString() const
{
    std::stringstream ss;
    ss << "SOURCE(" << NES::toString(source_) << ")";
    return ss.str();
}

OperatorType SourceOperator::getOperatorType() const { return SOURCE_OP; }

NES::DataSourcePtr SourceOperator::getDataSourcePtr() {
    return source_;
}

SourceOperator::~SourceOperator() {}

SourceOperator::SourceOperator() {
}

const OperatorPtr createSourceOperator(const DataSourcePtr source) { return std::make_shared<SourceOperator>(source); }

} // namespace NES
BOOST_CLASS_EXPORT(NES::SourceOperator);
